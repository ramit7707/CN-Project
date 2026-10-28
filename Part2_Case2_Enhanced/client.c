#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

#include "protocol.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>

typedef struct {
    uint32_t worker_id;
    char server_ip[64];
    int server_port;
    uint64_t session_id;
    char target_dir[1024];
    volatile int *all_done_flag;
    pthread_mutex_t *stats_mutex;
    uint64_t *total_bytes_received;
} WorkerContext;

static void *client_worker_thread(void *arg) {
    WorkerContext *wctx = (WorkerContext *)arg;
    uint32_t wid = wctx->worker_id;

    while (!(*wctx->all_done_flag)) {
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
            usleep(50000);
            continue;
        }

        struct sockaddr_in serv_addr;
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(wctx->server_port);

        if (inet_pton(AF_INET, wctx->server_ip, &serv_addr.sin_addr) <= 0) {
            close(sockfd);
            break;
        }

        if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            close(sockfd);
            if (*wctx->all_done_flag) break;
            usleep(100000);
            continue;
        }

        /* Send MSG_WORKER_JOIN */
        struct MsgWorkerJoinPayload join_p;
        memset(&join_p, 0, sizeof(join_p));
        join_p.session_id = hton64(wctx->session_id);
        join_p.worker_id = htonl(wid);

        struct MsgHeader join_hdr;
        memset(&join_hdr, 0, sizeof(join_hdr));
        join_hdr.magic = PROTOCOL_MAGIC;
        join_hdr.msg_type = MSG_WORKER_JOIN;
        join_hdr.session_id = wctx->session_id;
        join_hdr.payload_len = sizeof(join_p);

        if (send_msg(sockfd, &join_hdr, &join_p) != 0) {
            close(sockfd);
            usleep(50000);
            continue;
        }

        /* Worker loop */
        int worker_finished = 0;
        while (!worker_finished && !(*wctx->all_done_flag)) {
            struct MsgHeader msg_hdr;
            void *msg_payload = NULL;
            int ret = recv_msg(sockfd, &msg_hdr, &msg_payload);
            if (ret <= 0) {
                /* Connection severed by server fault injection */
                printf("[WORKER %u] Connection lost! Reconnecting to resume work...\n", wid);
                break;
            }

            if (msg_hdr.msg_type == MSG_WORKER_CLOSE) {
                /* Server signals all chunks completed */
                if (msg_payload) free(msg_payload);
                worker_finished = 1;
                *wctx->all_done_flag = 1;
                break;
            }

            if (msg_hdr.msg_type != MSG_DATA) {
                if (msg_payload) free(msg_payload);
                break;
            }

            struct MsgDataPayloadHdr *d_hdr = (struct MsgDataPayloadHdr *)msg_payload;
            char fname[MAX_FILENAME_LEN];
            strncpy(fname, d_hdr->filename, sizeof(fname) - 1);
            fname[sizeof(fname) - 1] = '\0';
            uint64_t offset = ntoh64(d_hdr->offset);
            uint32_t chunk_size = ntohl(d_hdr->chunk_size);
            uint32_t chunk_id = ntohl(d_hdr->chunk_id);
            char *data = (char *)msg_payload + sizeof(struct MsgDataPayloadHdr);

            char fpath[1024];
            snprintf(fpath, sizeof(fpath), "%s/%s", wctx->target_dir, fname);

            int out_fd = open(fpath, O_WRONLY);
            if (out_fd >= 0) {
                /* Safe, concurrent thread-safe write to exact offset */
                pwrite(out_fd, data, chunk_size, (off_t)offset);
                close(out_fd);
            }

            pthread_mutex_lock(wctx->stats_mutex);
            *wctx->total_bytes_received += chunk_size;
            pthread_mutex_unlock(wctx->stats_mutex);

            free(msg_payload);

            /* Send MSG_ACK */
            struct MsgAckPayload ack_p;
            memset(&ack_p, 0, sizeof(ack_p));
            strncpy(ack_p.filename, fname, sizeof(ack_p.filename) - 1);
            ack_p.acked_offset = hton64(offset + chunk_size);
            ack_p.chunk_size = htonl(chunk_size);
            ack_p.chunk_id = htonl(chunk_id);
            ack_p.status = htonl(0);

            struct MsgHeader ack_hdr;
            memset(&ack_hdr, 0, sizeof(ack_hdr));
            ack_hdr.magic = PROTOCOL_MAGIC;
            ack_hdr.msg_type = MSG_ACK;
            ack_hdr.session_id = wctx->session_id;
            ack_hdr.transaction_id = chunk_id;
            ack_hdr.payload_len = sizeof(ack_p);

            if (send_msg(sockfd, &ack_hdr, &ack_p) != 0) {
                break;
            }
        }

        close(sockfd);
        if (worker_finished || *wctx->all_done_flag) break;
        usleep(50000);
    }

    return NULL;
}

int main(int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <server_ip> <server_port> <topic> <output_dir> [num_connections]\n", argv[0]);
        return 1;
    }

    const char *server_ip = argv[1];
    int server_port = atoi(argv[2]);
    const char *topic = argv[3];
    const char *output_dir = argv[4];

    int num_connections = DEFAULT_PARALLEL_CONNECTIONS;
    if (argc >= 6) {
        num_connections = atoi(argv[5]);
        if (num_connections <= 0) num_connections = DEFAULT_PARALLEL_CONNECTIONS;
        if (num_connections > 32) num_connections = 32;
    }

    if (server_port <= 0 || server_port > 65535) {
        fprintf(stderr, "Invalid server port: %s\n", argv[2]);
        return 1;
    }

    if (!is_valid_name(topic)) {
        fprintf(stderr, "Invalid topic name: '%s'\n", topic);
        return 1;
    }

    char target_dir[1024];
    snprintf(target_dir, sizeof(target_dir), "%s/%s", output_dir, topic);
    if (mkdir_p(target_dir) != 0) {
        fprintf(stderr, "Error creating output directory '%s': %s\n", target_dir, strerror(errno));
        return 1;
    }

    printf("====================================================\n");
    printf(" Part 2 Case 2 ENHANCED Client (Parallel TCP Streams)\n");
    printf(" Server: %s:%d | Topic: '%s'\n", server_ip, server_port, topic);
    printf(" Parallel Connections: %d | Target: %s\n", num_connections, target_dir);
    printf("====================================================\n");

    /* Step 1: Open primary control channel */
    int primary_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (primary_fd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(server_port);

    if (inet_pton(AF_INET, server_ip, &serv_addr.sin_addr) <= 0) {
        fprintf(stderr, "Invalid server IP: %s\n", server_ip);
        close(primary_fd);
        return 1;
    }

    if (connect(primary_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("connect");
        close(primary_fd);
        return 1;
    }

    double t_start = get_time_sec();

    /* Send MSG_GET */
    struct MsgGetPayload get_payload;
    memset(&get_payload, 0, sizeof(get_payload));
    strncpy(get_payload.topic, topic, sizeof(get_payload.topic) - 1);
    get_payload.session_id = 0;

    struct MsgHeader get_hdr;
    memset(&get_hdr, 0, sizeof(get_hdr));
    get_hdr.magic = PROTOCOL_MAGIC;
    get_hdr.msg_type = MSG_GET;
    get_hdr.payload_len = sizeof(get_payload);

    if (send_msg(primary_fd, &get_hdr, &get_payload) != 0) {
        fprintf(stderr, "Failed to send GET on primary channel.\n");
        close(primary_fd);
        return 1;
    }

    /* Recv MSG_OK */
    struct MsgHeader resp_hdr;
    void *resp_payload = NULL;
    int ret = recv_msg(primary_fd, &resp_hdr, &resp_payload);
    if (ret <= 0 || resp_hdr.msg_type != MSG_OK) {
        fprintf(stderr, "Primary channel: Expected MSG_OK, failed.\n");
        if (resp_payload) free(resp_payload);
        close(primary_fd);
        return 1;
    }

    struct MsgOkHeader *ok_hdr = (struct MsgOkHeader *)resp_payload;
    uint32_t num_files = ntohl(ok_hdr->num_files);
    uint64_t session_id = ntoh64(ok_hdr->session_id);

    printf("[CLIENT PRIMARY] Session 0x%016llx initialized. Preparing %u files...\n",
           (unsigned long long)session_id, num_files);

    /* Pre-create and allocate files for parallel pwrite */
    struct FileInfoEntry *entries = (struct FileInfoEntry *)((char *)resp_payload + sizeof(struct MsgOkHeader));
    uint64_t total_topic_bytes = 0;

    for (uint32_t i = 0; i < num_files; i++) {
        char fpath[1024];
        snprintf(fpath, sizeof(fpath), "%s/%s", target_dir, entries[i].filename);
        uint64_t fsize = ntoh64(entries[i].file_size);
        total_topic_bytes += fsize;

        int fd = open(fpath, O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0) {
            ftruncate(fd, (off_t)fsize);
            close(fd);
        }
    }
    free(resp_payload);

    /* Send MSG_START on primary channel */
    struct MsgStartPayload start_p;
    memset(&start_p, 0, sizeof(start_p));
    strncpy(start_p.topic, topic, sizeof(start_p.topic) - 1);
    start_p.session_id = hton64(session_id);
    start_p.num_workers = htonl((uint32_t)num_connections);

    struct MsgHeader start_hdr;
    memset(&start_hdr, 0, sizeof(start_hdr));
    start_hdr.magic = PROTOCOL_MAGIC;
    start_hdr.msg_type = MSG_START;
    start_hdr.session_id = session_id;
    start_hdr.payload_len = sizeof(start_p);

    send_msg(primary_fd, &start_hdr, &start_p);

    /* Spawn parallel worker threads */
    volatile int all_done = 0;
    pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;
    uint64_t total_transferred = 0;

    pthread_t workers[32];
    WorkerContext wctxs[32];

    printf("[CLIENT PRIMARY] Launching %d parallel TCP worker streams...\n", num_connections);

    for (int i = 0; i < num_connections; i++) {
        wctxs[i].worker_id = (uint32_t)(i + 1);
        strncpy(wctxs[i].server_ip, server_ip, sizeof(wctxs[i].server_ip) - 1);
        wctxs[i].server_port = server_port;
        wctxs[i].session_id = session_id;
        strncpy(wctxs[i].target_dir, target_dir, sizeof(wctxs[i].target_dir) - 1);
        wctxs[i].all_done_flag = &all_done;
        wctxs[i].stats_mutex = &stats_lock;
        wctxs[i].total_bytes_received = &total_transferred;

        pthread_create(&workers[i], NULL, client_worker_thread, &wctxs[i]);
    }

    /* Wait for all workers */
    for (int i = 0; i < num_connections; i++) {
        pthread_join(workers[i], NULL);
    }

    /* Send MSG_CLOSE on primary channel */
    struct MsgClosePayload close_p;
    memset(&close_p, 0, sizeof(close_p));
    strncpy(close_p.topic, topic, sizeof(close_p.topic) - 1);
    close_p.session_id = hton64(session_id);
    close_p.status_code = htonl(0);

    struct MsgHeader close_hdr;
    memset(&close_hdr, 0, sizeof(close_hdr));
    close_hdr.magic = PROTOCOL_MAGIC;
    close_hdr.msg_type = MSG_CLOSE;
    close_hdr.session_id = session_id;
    close_hdr.payload_len = sizeof(close_p);

    send_msg(primary_fd, &close_hdr, &close_p);

    /* Await MSG_CLOSE from server */
    struct MsgHeader final_hdr;
    void *final_payload = NULL;
    recv_msg(primary_fd, &final_hdr, &final_payload);
    if (final_payload) free(final_payload);

    close(primary_fd);

    double t_end = get_time_sec();
    double elapsed = t_end - t_start;
    double mbps = (elapsed > 0) ? ((double)total_topic_bytes / (1024.0 * 1024.0) / elapsed) : 0.0;

    printf("\n====================================================\n");
    printf(" Part 2 Case 2 ENHANCED Transfer Complete!\n");
    printf(" Session ID:                 0x%016llx\n", (unsigned long long)session_id);
    printf(" Parallel Connections:       %d\n", num_connections);
    printf(" Total Topic Bytes:          %llu\n", (unsigned long long)total_topic_bytes);
    printf(" Total Bytes Transferred:    %llu\n", (unsigned long long)total_transferred);
    printf(" Total Elapsed Time:         %.4f seconds\n", elapsed);
    printf(" Aggregate Throughput:       %.2f MB/s\n", mbps);
    printf("====================================================\n");

    return 0;
}
