#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

#include "protocol.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <dirent.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>

#define MAX_FILES_PER_TOPIC 8192

typedef struct {
    int client_fd;
    struct sockaddr_in client_addr;
    char topics_root[512];
    double failure_prob;
} ClientContext;

static void send_error(int sockfd, uint32_t err_code, const char *msg) {
    struct MsgHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic = PROTOCOL_MAGIC;
    hdr.msg_type = MSG_ERR;
    hdr.payload_len = sizeof(struct MsgErrPayload);

    struct MsgErrPayload payload;
    memset(&payload, 0, sizeof(payload));
    payload.error_code = htonl(err_code);
    strncpy(payload.error_msg, msg, sizeof(payload.error_msg) - 1);

    send_msg(sockfd, &hdr, &payload);
}

/* Serve client with probabilistic fault injection and NO session state */
static void handle_client_session(ClientContext *ctx) {
    int fd = ctx->client_fd;
    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &ctx->client_addr.sin_addr, client_ip, sizeof(client_ip));
    int client_port = ntohs(ctx->client_addr.sin_port);

    printf("[SERVER] Connection accepted from %s:%d\n", client_ip, client_port);

    struct MsgHeader req_hdr;
    void *req_payload = NULL;

    /* Step 1: Expect MSG_GET */
    int ret = recv_msg(fd, &req_hdr, &req_payload);
    if (ret <= 0) {
        printf("[SERVER] Client %s:%d disconnected before sending GET.\n", client_ip, client_port);
        close(fd);
        return;
    }

    if (req_hdr.msg_type != MSG_GET || req_hdr.payload_len < sizeof(struct MsgGetPayload)) {
        send_error(fd, ERR_INVALID_REQUEST, "Expected MSG_GET");
        if (req_payload) free(req_payload);
        close(fd);
        return;
    }

    struct MsgGetPayload *get_req = (struct MsgGetPayload *)req_payload;
    char topic[MAX_TOPIC_NAME];
    strncpy(topic, get_req->topic, sizeof(topic) - 1);
    topic[sizeof(topic) - 1] = '\0';
    free(req_payload);
    req_payload = NULL;

    printf("[SERVER] Client %s:%d requested topic: '%s'\n", client_ip, client_port, topic);

    if (!is_valid_name(topic)) {
        send_error(fd, ERR_PATH_TRAVERSAL, "Invalid topic name format or path traversal attempt");
        close(fd);
        return;
    }

    char topic_dir[1024];
    snprintf(topic_dir, sizeof(topic_dir), "%s/%s", ctx->topics_root, topic);

    struct stat st;
    if (stat(topic_dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
        send_error(fd, ERR_TOPIC_NOT_FOUND, "Requested topic directory not found on server");
        close(fd);
        return;
    }

    DIR *d = opendir(topic_dir);
    if (!d) {
        send_error(fd, ERR_FILE_IO, "Server could not open topic directory");
        close(fd);
        return;
    }

    struct FileInfoEntry *files = malloc(MAX_FILES_PER_TOPIC * sizeof(struct FileInfoEntry));
    if (!files) {
        closedir(d);
        send_error(fd, ERR_INTERNAL, "Memory allocation error");
        close(fd);
        return;
    }
    uint32_t file_count = 0;
    struct dirent *entry;

    while ((entry = readdir(d)) != NULL && file_count < MAX_FILES_PER_TOPIC) {
        if (entry->d_name[0] == '.') continue;
        if (!is_valid_name(entry->d_name)) continue;

        char fpath[1024];
        snprintf(fpath, sizeof(fpath), "%s/%s", topic_dir, entry->d_name);
        struct stat fst;
        if (stat(fpath, &fst) == 0 && S_ISREG(fst.st_mode)) {
            strncpy(files[file_count].filename, entry->d_name, MAX_FILENAME_LEN - 1);
            files[file_count].filename[MAX_FILENAME_LEN - 1] = '\0';
            files[file_count].file_size = (uint64_t)fst.st_size;
            file_count++;
        }
    }
    closedir(d);

    if (file_count == 0) {
        free(files);
        send_error(fd, ERR_EMPTY_TOPIC, "Topic directory has no regular files to distribute");
        close(fd);
        return;
    }

    /* Step 2: Send MSG_OK */
    size_t ok_payload_len = sizeof(struct MsgOkHeader) + file_count * sizeof(struct FileInfoEntry);
    char *ok_buf = malloc(ok_payload_len);
    if (!ok_buf) {
        send_error(fd, ERR_INTERNAL, "Memory allocation error");
        close(fd);
        return;
    }

    struct MsgOkHeader *ok_hdr = (struct MsgOkHeader *)ok_buf;
    ok_hdr->num_files = htonl(file_count);
    strncpy(ok_hdr->topic, topic, sizeof(ok_hdr->topic) - 1);

    struct FileInfoEntry *entries = (struct FileInfoEntry *)(ok_buf + sizeof(struct MsgOkHeader));
    for (uint32_t i = 0; i < file_count; i++) {
        strncpy(entries[i].filename, files[i].filename, sizeof(entries[i].filename) - 1);
        entries[i].file_size = hton64(files[i].file_size);
    }

    struct MsgHeader resp_hdr;
    memset(&resp_hdr, 0, sizeof(resp_hdr));
    resp_hdr.magic = PROTOCOL_MAGIC;
    resp_hdr.msg_type = MSG_OK;
    resp_hdr.payload_len = (uint32_t)ok_payload_len;

    if (send_msg(fd, &resp_hdr, ok_buf) != 0) {
        free(ok_buf);
        close(fd);
        return;
    }
    free(ok_buf);

    /* Step 3: Expect MSG_START */
    ret = recv_msg(fd, &req_hdr, &req_payload);
    if (ret <= 0) {
        close(fd);
        return;
    }

    if (req_hdr.msg_type != MSG_START || req_hdr.payload_len < sizeof(struct MsgStartPayload)) {
        send_error(fd, ERR_INVALID_START, "Expected MSG_START");
        if (req_payload) free(req_payload);
        close(fd);
        return;
    }

    struct MsgStartPayload *start_p = (struct MsgStartPayload *)req_payload;
    if (strncmp(start_p->topic, topic, sizeof(topic)) != 0) {
        send_error(fd, ERR_INVALID_START, "Topic mismatch in MSG_START");
        free(req_payload);
        close(fd);
        return;
    }
    free(req_payload);
    req_payload = NULL;

    printf("[SERVER] Transfer started for client %s:%d (Topic '%s')...\n", client_ip, client_port, topic);

    double t_start = get_time_sec();
    uint64_t total_bytes_sent = 0;

    /* Per-thread random state */
    unsigned int rand_seed = (unsigned int)(time(NULL) ^ (uintptr_t)pthread_self());

    /* Step 4: Transfer files with probabilistic fault injection */
    for (uint32_t i = 0; i < file_count; i++) {
        char fpath[1024];
        snprintf(fpath, sizeof(fpath), "%s/%s", topic_dir, files[i].filename);

        FILE *fp = fopen(fpath, "rb");
        if (!fp) {
            fprintf(stderr, "[SERVER] Warning: Could not open %s\n", fpath);
            continue;
        }

        struct MsgFileHdrPayload file_hdr_p;
        memset(&file_hdr_p, 0, sizeof(file_hdr_p));
        strncpy(file_hdr_p.filename, files[i].filename, sizeof(file_hdr_p.filename) - 1);
        file_hdr_p.file_size = hton64(files[i].file_size);
        file_hdr_p.start_offset = 0;
        file_hdr_p.mode = htonl(0644);

        struct MsgHeader file_hdr;
        memset(&file_hdr, 0, sizeof(file_hdr));
        file_hdr.magic = PROTOCOL_MAGIC;
        file_hdr.msg_type = MSG_FILE_HDR;
        file_hdr.payload_len = sizeof(file_hdr_p);

        if (send_msg(fd, &file_hdr, &file_hdr_p) != 0) {
            fclose(fp);
            close(fd);
            return;
        }

        uint64_t offset = 0;
        char chunk_buffer[CHUNK_SIZE];

        while (offset < files[i].file_size) {
            size_t to_read = CHUNK_SIZE;
            if (offset + to_read > files[i].file_size) {
                to_read = (size_t)(files[i].file_size - offset);
            }
            size_t bytes_read = fread(chunk_buffer, 1, to_read, fp);
            if (bytes_read == 0) break;

            size_t data_payload_len = sizeof(struct MsgDataPayloadHdr) + bytes_read;
            char *data_msg_buf = malloc(data_payload_len);
            if (!data_msg_buf) {
                fclose(fp);
                close(fd);
                return;
            }

            struct MsgDataPayloadHdr *d_hdr = (struct MsgDataPayloadHdr *)data_msg_buf;
            strncpy(d_hdr->filename, files[i].filename, sizeof(d_hdr->filename) - 1);
            d_hdr->offset = hton64(offset);
            d_hdr->chunk_size = htonl((uint32_t)bytes_read);
            memcpy(data_msg_buf + sizeof(struct MsgDataPayloadHdr), chunk_buffer, bytes_read);

            struct MsgHeader data_hdr;
            memset(&data_hdr, 0, sizeof(data_hdr));
            data_hdr.magic = PROTOCOL_MAGIC;
            data_hdr.msg_type = MSG_DATA;
            data_hdr.payload_len = (uint32_t)data_payload_len;

            if (send_msg(fd, &data_hdr, data_msg_buf) != 0) {
                free(data_msg_buf);
                fclose(fp);
                close(fd);
                return;
            }
            free(data_msg_buf);

            /* Await ACK */
            struct MsgHeader ack_hdr;
            void *ack_payload = NULL;
            ret = recv_msg(fd, &ack_hdr, &ack_payload);
            if (ret <= 0) {
                fclose(fp);
                close(fd);
                return;
            }
            if (ack_payload) free(ack_payload);

            offset += bytes_read;
            total_bytes_sent += bytes_read;

            /* FAULT INJECTION CHECK:
             * Decision made after successfully transmitting data chunk, before sending next
             */
            if (ctx->failure_prob > 0.0) {
                double roll = (double)rand_r(&rand_seed) / (double)RAND_MAX;
                if (roll < ctx->failure_prob) {
                    printf("[FAULT INJECTION] Terminating client connection %s:%d! (prob=%.4f, roll=%.4f)\n",
                           client_ip, client_port, ctx->failure_prob, roll);
                    fclose(fp);
                    close(fd);
                    return; /* Connection dropped abruptly. No session state retained! */
                }
            }
        }

        fclose(fp);
    }

    /* Step 5: Send MSG_CLOSE */
    struct MsgClosePayload close_p;
    memset(&close_p, 0, sizeof(close_p));
    strncpy(close_p.topic, topic, sizeof(close_p.topic) - 1);
    close_p.status_code = htonl(0);
    close_p.total_bytes_sent = hton64(total_bytes_sent);

    struct MsgHeader close_hdr;
    memset(&close_hdr, 0, sizeof(close_hdr));
    close_hdr.magic = PROTOCOL_MAGIC;
    close_hdr.msg_type = MSG_CLOSE;
    close_hdr.payload_len = sizeof(close_p);

    send_msg(fd, &close_hdr, &close_p);

    double t_end = get_time_sec();
    double elapsed = t_end - t_start;

    printf("[SERVER] Topic '%s' successfully delivered to %s:%d.\n", topic, client_ip, client_port);
    print_transfer_stats("SERVER", total_bytes_sent, elapsed);

    free(files);
    close(fd);
}

static void *worker_thread_func(void *arg) {
    ClientContext *ctx = (ClientContext *)arg;
    handle_client_session(ctx);
    free(ctx);
    return NULL;
}

int main(int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <port> <topics_root_dir> <failure_probability>\n", argv[0]);
        return 1;
    }

    int port = atoi(argv[1]);
    if (port <= 0 || port > 65535) {
        fprintf(stderr, "Invalid port: %s\n", argv[1]);
        return 1;
    }

    const char *topics_root = argv[2];
    struct stat st;
    if (stat(topics_root, &st) != 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Error: Topics root '%s' does not exist or is not a directory.\n", topics_root);
        return 1;
    }

    double failure_prob = atof(argv[3]);
    if (failure_prob < 0.0 || failure_prob > 1.0) {
        fprintf(stderr, "Error: Failure probability must be between 0.0 and 1.0 (got %s)\n", argv[3]);
        return 1;
    }

    printf("====================================================\n");
    printf(" Part 2 Case 1 Server (No Session Management)\n");
    printf(" Port: %d | Root: %s | Failure Prob: %.4f\n", port, topics_root, failure_prob);
    printf(" Mode: Multi-threaded (1 thread per client)\n");
    printf("====================================================\n");

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);

    if (bind(listen_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("bind");
        close(listen_fd);
        return 1;
    }

    if (listen(listen_fd, 64) < 0) {
        perror("listen");
        close(listen_fd);
        return 1;
    }

    printf("[SERVER] Listening on port %d...\n", port);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &addr_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            break;
        }

        ClientContext *ctx = malloc(sizeof(ClientContext));
        if (!ctx) {
            close(client_fd);
            continue;
        }
        ctx->client_fd = client_fd;
        ctx->client_addr = client_addr;
        strncpy(ctx->topics_root, topics_root, sizeof(ctx->topics_root) - 1);
        ctx->topics_root[sizeof(ctx->topics_root) - 1] = '\0';
        ctx->failure_prob = failure_prob;

        pthread_t tid;
        if (pthread_create(&tid, NULL, worker_thread_func, ctx) != 0) {
            perror("pthread_create");
            handle_client_session(ctx);
            free(ctx);
        } else {
            pthread_detach(tid);
        }
    }

    close(listen_fd);
    return 0;
}
