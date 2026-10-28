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

/* Server can either handle clients sequentially or concurrently. */
typedef enum {
    MODE_MULTI_THREADED = 0,
    MODE_SINGLE_THREADED = 1
} ServerMode;

/*
 * Stores the connection and shared server information needed to serve
 * one client. In multi-threaded mode, each worker receives its own context.
 */
typedef struct {
    int client_fd;
    struct sockaddr_in client_addr;
    char topics_root[512];
} ClientContext;

/*
 * Sends a standard MSG_ERR response to the client whenever a request is
 * invalid or the server cannot continue the transfer.
 */
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

/*
 * Handles the complete transfer session for one client:
 *
 * MSG_GET -> MSG_OK -> MSG_START
 * -> MSG_FILE_HDR + MSG_DATA/MSG_ACK for every file
 * -> MSG_CLOSE
 */
static void handle_client_session(ClientContext *ctx) {
    int fd = ctx->client_fd;

    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &ctx->client_addr.sin_addr,
              client_ip, sizeof(client_ip));

    int client_port = ntohs(ctx->client_addr.sin_port);

    printf("[SERVER] Connection from %s:%d accepted.\n",
           client_ip, client_port);

    struct MsgHeader req_hdr;
    void *req_payload = NULL;

    /*
     * Phase 1: Receive the topic requested by the client.
     */
    int ret = recv_msg(fd, &req_hdr, &req_payload);

    if (ret <= 0) {
        printf("[SERVER] Client %s:%d disconnected before sending GET.\n",
               client_ip, client_port);
        close(fd);
        return;
    }

    if (req_hdr.msg_type != MSG_GET ||
        req_hdr.payload_len < sizeof(struct MsgGetPayload)) {
        fprintf(stderr,
                "[SERVER] Invalid request from %s:%d: expected MSG_GET (type=%d)\n",
                client_ip, client_port, req_hdr.msg_type);

        send_error(fd, ERR_INVALID_REQUEST, "Expected MSG_GET");
        free(req_payload);
        close(fd);
        return;
    }

    struct MsgGetPayload *get_req =
        (struct MsgGetPayload *)req_payload;

    char topic[MAX_TOPIC_NAME];

    strncpy(topic, get_req->topic, sizeof(topic) - 1);
    topic[sizeof(topic) - 1] = '\0';

    free(req_payload);
    req_payload = NULL;

    printf("[SERVER] Client %s:%d requested topic: '%s'\n",
           client_ip, client_port, topic);

    /*
     * Validate the requested topic before using it in a file path.
     */
    if (!is_valid_name(topic)) {
        fprintf(stderr,
                "[SERVER] Path traversal or invalid topic name rejected: '%s'\n",
                topic);

        send_error(fd, ERR_PATH_TRAVERSAL,
                   "Invalid topic name format or path traversal attempt");

        close(fd);
        return;
    }

    /*
     * Locate the directory that represents the requested topic.
     */
    char topic_dir[1024];
    snprintf(topic_dir, sizeof(topic_dir), "%s/%s",
             ctx->topics_root, topic);

    struct stat st;

    if (stat(topic_dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr, "[SERVER] Topic directory not found: '%s'\n",
                topic_dir);

        send_error(fd, ERR_TOPIC_NOT_FOUND,
                   "Requested topic directory not found on server");

        close(fd);
        return;
    }

    /*
     * Scan the topic directory and collect metadata for its regular files.
     */
    DIR *d = opendir(topic_dir);

    if (!d) {
        fprintf(stderr, "[SERVER] Failed to open topic directory: %s\n",
                strerror(errno));

        send_error(fd, ERR_FILE_IO,
                   "Server could not open topic directory");

        close(fd);
        return;
    }

    struct FileInfoEntry *files =
        malloc(MAX_FILES_PER_TOPIC * sizeof(struct FileInfoEntry));

    if (!files) {
        closedir(d);
        send_error(fd, ERR_INTERNAL, "Memory allocation error");
        close(fd);
        return;
    }

    uint32_t file_count = 0;
    struct dirent *entry;

    while ((entry = readdir(d)) != NULL &&
           file_count < MAX_FILES_PER_TOPIC) {

        /* Ignore hidden entries and unsafe names. */
        if (entry->d_name[0] == '.') {
            continue;
        }

        if (!is_valid_name(entry->d_name)) {
            continue;
        }

        char fpath[1024];
        snprintf(fpath, sizeof(fpath), "%s/%s",
                 topic_dir, entry->d_name);

        struct stat fst;

        /* Only normal files are part of a topic transfer. */
        if (stat(fpath, &fst) == 0 && S_ISREG(fst.st_mode)) {
            strncpy(files[file_count].filename, entry->d_name,
                    MAX_FILENAME_LEN - 1);

            files[file_count].filename[MAX_FILENAME_LEN - 1] = '\0';

            files[file_count].file_size = (uint64_t)fst.st_size;

            file_count++;
        }
    }

    closedir(d);

    if (file_count == 0) {
        free(files);

        fprintf(stderr, "[SERVER] Topic '%s' has no regular files.\n",
                topic);

        send_error(fd, ERR_EMPTY_TOPIC,
                   "Topic directory has no regular files to distribute");

        close(fd);
        return;
    }

    /*
     * Phase 2: Confirm the topic and send the client the number and details
     * of available files.
     */
    printf("[SERVER] Found %u files in topic '%s'. Sending MSG_OK...\n",
           file_count, topic);

    size_t ok_payload_len =
        sizeof(struct MsgOkHeader) +
        file_count * sizeof(struct FileInfoEntry);

    char *ok_buf = malloc(ok_payload_len);

    if (!ok_buf) {
        send_error(fd, ERR_INTERNAL, "Memory allocation error");
        close(fd);
        return;
    }

    struct MsgOkHeader *ok_hdr =
        (struct MsgOkHeader *)ok_buf;

    ok_hdr->num_files = htonl(file_count);

    strncpy(ok_hdr->topic, topic, sizeof(ok_hdr->topic) - 1);

    struct FileInfoEntry *entries =
        (struct FileInfoEntry *)(ok_buf + sizeof(struct MsgOkHeader));

    for (uint32_t i = 0; i < file_count; i++) {
        strncpy(entries[i].filename, files[i].filename,
                sizeof(entries[i].filename) - 1);

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

    /*
     * Phase 3: Wait until the client confirms it is ready to receive data.
     */
    ret = recv_msg(fd, &req_hdr, &req_payload);

    if (ret <= 0) {
        close(fd);
        return;
    }

    if (req_hdr.msg_type != MSG_START ||
        req_hdr.payload_len < sizeof(struct MsgStartPayload)) {
        send_error(fd, ERR_INVALID_START, "Expected MSG_START");

        if (req_payload) {
            free(req_payload);
        }

        close(fd);
        return;
    }

    struct MsgStartPayload *start_p =
        (struct MsgStartPayload *)req_payload;

    /* Ensure the client starts transfer for the same topic it requested. */
    if (strncmp(start_p->topic, topic, sizeof(topic)) != 0) {
        send_error(fd, ERR_INVALID_START,
                   "Topic mismatch in MSG_START");

        free(req_payload);
        close(fd);
        return;
    }

    free(req_payload);
    req_payload = NULL;

    printf("[SERVER] Received MSG_START for topic '%s'. "
           "Beginning file transmission...\n", topic);

    double t_start = get_time_sec();
    uint64_t total_bytes_sent = 0;

    /*
     * Phase 4: Send every file in the selected topic directory.
     */
    for (uint32_t i = 0; i < file_count; i++) {
        char fpath[1024];

        snprintf(fpath, sizeof(fpath), "%s/%s",
                 topic_dir, files[i].filename);

        FILE *fp = fopen(fpath, "rb");

        if (!fp) {
            fprintf(stderr, "[SERVER] Warning: Could not open file %s: %s\n",
                    fpath, strerror(errno));
            continue;
        }

        /*
         * Announce the next file before sending its contents.
         */
        struct MsgFileHdrPayload file_hdr_p;
        memset(&file_hdr_p, 0, sizeof(file_hdr_p));

        strncpy(file_hdr_p.filename, files[i].filename,
                sizeof(file_hdr_p.filename) - 1);

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

        /*
         * Read the file in fixed-size chunks. Each chunk is sent as MSG_DATA,
         * then the server waits for MSG_ACK before continuing.
         */
        uint64_t offset = 0;
        char chunk_buffer[CHUNK_SIZE];

        while (offset < files[i].file_size) {
            size_t to_read = CHUNK_SIZE;

            /* The final chunk may be smaller than CHUNK_SIZE. */
            if (offset + to_read > files[i].file_size) {
                to_read = (size_t)(files[i].file_size - offset);
            }

            size_t bytes_read = fread(chunk_buffer, 1, to_read, fp);

            if (bytes_read == 0) {
                break;
            }
            /*
             * Build a data payload containing chunk metadata followed by
             * the actual file bytes.
             */
            size_t data_payload_len =
                sizeof(struct MsgDataPayloadHdr) + bytes_read;

            char *data_msg_buf = malloc(data_payload_len);

            if (!data_msg_buf) {
                fclose(fp);
                close(fd);
                return;
            }

            struct MsgDataPayloadHdr *d_hdr =
                (struct MsgDataPayloadHdr *)data_msg_buf;

            strncpy(d_hdr->filename, files[i].filename,
                    sizeof(d_hdr->filename) - 1);

            d_hdr->offset = hton64(offset);
            d_hdr->chunk_size = htonl((uint32_t)bytes_read);

            memcpy(data_msg_buf + sizeof(struct MsgDataPayloadHdr),
                   chunk_buffer, bytes_read);

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

            /*
             * Wait for client confirmation that this chunk was saved.
             */
            struct MsgHeader ack_hdr;
            void *ack_payload = NULL;

            ret = recv_msg(fd, &ack_hdr, &ack_payload);

            if (ret <= 0) {
                fclose(fp);
                close(fd);
                return;
            }

            if (ack_hdr.msg_type != MSG_ACK) {
                fprintf(stderr,
                        "[SERVER] Warning: expected ACK, got type %u\n",
                        ack_hdr.msg_type);
            }

            if (ack_payload) {
                free(ack_payload);
            }

            offset += bytes_read;
            total_bytes_sent += bytes_read;
        }

        fclose(fp);
    }

    /*
     * Phase 5: Inform the client that every file has been sent.
     */
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

    printf("[SERVER] Finished topic '%s' for client %s:%d.\n",
           topic, client_ip, client_port);

    print_transfer_stats("SERVER", total_bytes_sent, elapsed);

    free(files);
    close(fd);
}

/*
 * Entry point for a worker thread in multi-threaded mode.
 * One worker serves one connected client.
 */
static void *worker_thread_func(void *arg) {
    ClientContext *ctx = (ClientContext *)arg;

    handle_client_session(ctx);

    free(ctx);
    return NULL;
}

int main(int argc, char *argv[]) {
    /*
     * Ignore SIGPIPE so one disconnected client does not terminate
     * the complete server process.
     */
    signal(SIGPIPE, SIG_IGN);

    /*
     * Read server port, root dataset directory, and optional handling mode.
     */
    if (argc < 3) {
        fprintf(stderr,
                "Usage: %s <port> <topics_root_dir> [single|multi]\n",
                argv[0]);
        return 1;
    }

    int port = atoi(argv[1]);

    if (port <= 0 || port > 65535) {
        fprintf(stderr, "Invalid port number: %s\n", argv[1]);
        return 1;
    }

    const char *topics_root = argv[2];

    struct stat st;

    if (stat(topics_root, &st) != 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr,
                "Error: Topics root directory '%s' does not exist or is not a directory.\n",
                topics_root);
        return 1;
    }

    /* Multi-threaded mode is used unless "single" is explicitly selected. */
    ServerMode mode = MODE_MULTI_THREADED;

    if (argc >= 4 && strcmp(argv[3], "single") == 0) {
        mode = MODE_SINGLE_THREADED;
    }

    printf("====================================================\n");
    printf(" Part 1 File Distribution Server\n");
    printf(" Port: %d | Root: %s | Mode: %s\n",
           port, topics_root,
           (mode == MODE_SINGLE_THREADED)
               ? "SINGLE-THREADED"
               : "MULTI-THREADED (1 thread/client)");
    printf("====================================================\n");

    /*
     * Create, configure, bind, and listen on the TCP server socket.
     */
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (listen_fd < 0) {
        perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR,
               &opt, sizeof(opt));

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);

    if (bind(listen_fd, (struct sockaddr *)&serv_addr,
             sizeof(serv_addr)) < 0) {
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

    /*
     * Accept client connections forever. In single mode, serve each client
     * before accepting the next. In multi mode, create one detached thread
     * for every connected client.
     */
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);

        int client_fd = accept(listen_fd,
                               (struct sockaddr *)&client_addr,
                               &addr_len);

        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }

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

        strncpy(ctx->topics_root, topics_root,
                sizeof(ctx->topics_root) - 1);

        ctx->topics_root[sizeof(ctx->topics_root) - 1] = '\0';

        if (mode == MODE_SINGLE_THREADED) {
            /* Serve one client completely before accepting another client. */
            handle_client_session(ctx);
            free(ctx);
        } else {
            /* Serve each client independently in a detached worker thread. */
            pthread_t tid;

            if (pthread_create(&tid, NULL,
                               worker_thread_func, ctx) != 0) {
                perror("pthread_create");

                handle_client_session(ctx);
                free(ctx);
            } else {
                pthread_detach(tid);
            }
        }
    }

    close(listen_fd);
    return 0;
}