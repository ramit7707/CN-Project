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
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>

#define MAX_FILES_PER_TOPIC 8192
#define MAX_SESSIONS        32
#define SESSION_TIMEOUT_SEC 300

typedef struct {
    uint64_t session_id;
    char topic[MAX_TOPIC_NAME];
    uint32_t total_files;
    struct FileInfoEntry files[MAX_FILES_PER_TOPIC];
    uint32_t current_file_idx;
    uint64_t current_file_offset;
    time_t last_update;
    int is_active;
    int is_completed;
} SessionState;

static SessionState g_sessions[MAX_SESSIONS];
static pthread_mutex_t g_session_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    int client_fd;
    struct sockaddr_in client_addr;
    char topics_root[512];
    double failure_prob;
} ClientContext;

static uint64_t generate_unique_session_id(void) {
    static uint64_t counter = 1000;
    uint64_t r = ((uint64_t)rand() << 32) | (uint64_t)rand();
    return (r ^ ((uint64_t)time(NULL) << 16) ^ (++counter)) | 1ULL;
}

/* Background reaper thread to clean up timed-out / completed sessions */
static void *session_reaper_func(void *arg) {
    (void)arg;
    while (1) {
        sleep(10);
        time_t now = time(NULL);
        pthread_mutex_lock(&g_session_lock);
        for (int i = 0; i < MAX_SESSIONS; i++) {
            if (g_sessions[i].is_active) {
                if (g_sessions[i].is_completed || (now - g_sessions[i].last_update > SESSION_TIMEOUT_SEC)) {
                    printf("[SESSION CLEANUP] Expiring session 0x%016llx (Topic: %s, completed=%d)\n",
                           (unsigned long long)g_sessions[i].session_id,
                           g_sessions[i].topic, g_sessions[i].is_completed);
                    memset(&g_sessions[i], 0, sizeof(SessionState));
                }
            }
        }
        pthread_mutex_unlock(&g_session_lock);
    }
    return NULL;
}

static void send_error(int sockfd, uint32_t err_code, const char *msg, uint64_t session_id) {
    struct MsgHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic = PROTOCOL_MAGIC;
    hdr.msg_type = MSG_ERR;
    hdr.session_id = session_id;
    hdr.payload_len = sizeof(struct MsgErrPayload);

    struct MsgErrPayload payload;
    memset(&payload, 0, sizeof(payload));
    payload.error_code = htonl(err_code);
    strncpy(payload.error_msg, msg, sizeof(payload.error_msg) - 1);

    send_msg(sockfd, &hdr, &payload);
}

/* Multi-threaded client handler with session checkpointing and resume */
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
        send_error(fd, ERR_INVALID_REQUEST, "Expected MSG_GET", 0);
        if (req_payload) free(req_payload);
        close(fd);
        return;
    }

    struct MsgGetPayload *get_req = (struct MsgGetPayload *)req_payload;
    char topic[MAX_TOPIC_NAME];
    strncpy(topic, get_req->topic, sizeof(topic) - 1);
    topic[sizeof(topic) - 1] = '\0';
    uint64_t req_session_id = ntoh64(get_req->session_id);
    if (req_session_id == 0 && req_hdr.session_id != 0) {
        req_session_id = req_hdr.session_id;
    }
    free(req_payload);
    req_payload = NULL;

    if (!is_valid_name(topic)) {
        send_error(fd, ERR_PATH_TRAVERSAL, "Invalid topic name format or path traversal attempt", 0);
        close(fd);
        return;
    }

    char topic_dir[1024];
    snprintf(topic_dir, sizeof(topic_dir), "%s/%s", ctx->topics_root, topic);

    struct stat st;
    if (stat(topic_dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
        send_error(fd, ERR_TOPIC_NOT_FOUND, "Requested topic directory not found on server", 0);
        close(fd);
        return;
    }

    SessionState *session = NULL;
    int session_slot = -1;

    pthread_mutex_lock(&g_session_lock);

    if (req_session_id != 0) {
        /* Lookup existing session */
        for (int i = 0; i < MAX_SESSIONS; i++) {
            if (g_sessions[i].is_active && g_sessions[i].session_id == req_session_id) {
                if (strcmp(g_sessions[i].topic, topic) == 0 && !g_sessions[i].is_completed) {
                    session = &g_sessions[i];
                    session_slot = i;
                    session->last_update = time(NULL);
                    break;
                }
            }
        }
        if (!session) {
            pthread_mutex_unlock(&g_session_lock);
            printf("[SERVER] Session 0x%016llx expired or not found. Prompting client restart.\n",
                   (unsigned long long)req_session_id);
            send_error(fd, ERR_SESSION_NOT_FOUND, "Session expired or invalid. Please start new session.", req_session_id);
            close(fd);
            return;
        }
        printf("[SERVER] Resuming Session 0x%016llx for '%s' at file index %u (offset %llu)\n",
               (unsigned long long)session->session_id, topic,
               session->current_file_idx, (unsigned long long)session->current_file_offset);
    } else {
        /* Create new session */
        for (int i = 0; i < MAX_SESSIONS; i++) {
            if (!g_sessions[i].is_active) {
                session_slot = i;
                break;
            }
        }
        if (session_slot < 0) {
            pthread_mutex_unlock(&g_session_lock);
            send_error(fd, ERR_INTERNAL, "Server session table full", 0);
            close(fd);
            return;
        }

        session = &g_sessions[session_slot];
        memset(session, 0, sizeof(SessionState));
        session->session_id = generate_unique_session_id();
        strncpy(session->topic, topic, sizeof(session->topic) - 1);
        session->is_active = 1;
        session->last_update = time(NULL);

        /* Enumerate files */
        DIR *d = opendir(topic_dir);
        if (!d) {
            memset(session, 0, sizeof(SessionState));
            pthread_mutex_unlock(&g_session_lock);
            send_error(fd, ERR_FILE_IO, "Server could not open topic directory", 0);
            close(fd);
            return;
        }

        struct dirent *entry;
        while ((entry = readdir(d)) != NULL && session->total_files < MAX_FILES_PER_TOPIC) {
            if (entry->d_name[0] == '.') continue;
            if (!is_valid_name(entry->d_name)) continue;

            char fpath[1024];
            snprintf(fpath, sizeof(fpath), "%s/%s", topic_dir, entry->d_name);
            struct stat fst;
            if (stat(fpath, &fst) == 0 && S_ISREG(fst.st_mode)) {
                strncpy(session->files[session->total_files].filename, entry->d_name, MAX_FILENAME_LEN - 1);
                session->files[session->total_files].file_size = (uint64_t)fst.st_size;
                session->files[session->total_files].start_offset = 0;
                session->total_files++;
            }
        }
        closedir(d);

        if (session->total_files == 0) {
            memset(session, 0, sizeof(SessionState));
            pthread_mutex_unlock(&g_session_lock);
            send_error(fd, ERR_EMPTY_TOPIC, "Topic directory has no regular files", 0);
            close(fd);
            return;
        }

        printf("[SERVER] Created new Session 0x%016llx for '%s' (%u files)\n",
               (unsigned long long)session->session_id, topic, session->total_files);
    }

    uint64_t current_session_id = session->session_id;
    uint32_t total_files = session->total_files;
    uint32_t cur_file_idx = session->current_file_idx;
    uint64_t cur_file_offset = session->current_file_offset;

    pthread_mutex_unlock(&g_session_lock);

    /* Step 2: Send MSG_OK with session ID and file list */
    size_t ok_payload_len = sizeof(struct MsgOkHeader) + total_files * sizeof(struct FileInfoEntry);
    char *ok_buf = malloc(ok_payload_len);
    if (!ok_buf) {
        send_error(fd, ERR_INTERNAL, "Memory allocation error", current_session_id);
        close(fd);
        return;
    }

    struct MsgOkHeader *ok_hdr = (struct MsgOkHeader *)ok_buf;
    ok_hdr->num_files = htonl(total_files);
    ok_hdr->session_id = hton64(current_session_id);
    strncpy(ok_hdr->topic, topic, sizeof(ok_hdr->topic) - 1);

    struct FileInfoEntry *entries = (struct FileInfoEntry *)(ok_buf + sizeof(struct MsgOkHeader));
    pthread_mutex_lock(&g_session_lock);
    for (uint32_t i = 0; i < total_files; i++) {
        strncpy(entries[i].filename, session->files[i].filename, sizeof(entries[i].filename) - 1);
        entries[i].file_size = hton64(session->files[i].file_size);
        if (i < cur_file_idx) {
            entries[i].start_offset = hton64(session->files[i].file_size); /* already complete */
        } else if (i == cur_file_idx) {
            entries[i].start_offset = hton64(cur_file_offset);
        } else {
            entries[i].start_offset = 0;
        }
    }
    pthread_mutex_unlock(&g_session_lock);

    struct MsgHeader resp_hdr;
    memset(&resp_hdr, 0, sizeof(resp_hdr));
    resp_hdr.magic = PROTOCOL_MAGIC;
    resp_hdr.msg_type = MSG_OK;
    resp_hdr.session_id = current_session_id;
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
        send_error(fd, ERR_INVALID_START, "Expected MSG_START", current_session_id);
        if (req_payload) free(req_payload);
        close(fd);
        return;
    }

    struct MsgStartPayload *start_p = (struct MsgStartPayload *)req_payload;
    if (strncmp(start_p->topic, topic, sizeof(topic)) != 0) {
        send_error(fd, ERR_INVALID_START, "Topic mismatch in MSG_START", current_session_id);
        free(req_payload);
        close(fd);
        return;
    }
    free(req_payload);
    req_payload = NULL;

    printf("[SERVER] Client %s:%d confirmed START for session 0x%016llx\n",
           client_ip, client_port, (unsigned long long)current_session_id);

    double t_start = get_time_sec();
    uint64_t total_bytes_sent_conn = 0;
    unsigned int rand_seed = (unsigned int)(time(NULL) ^ (uintptr_t)pthread_self());

    /* Step 4: Transfer remaining files/offsets */
    for (uint32_t i = cur_file_idx; i < total_files; i++) {
        char fpath[1024];
        snprintf(fpath, sizeof(fpath), "%s/%s", topic_dir, session->files[i].filename);

        FILE *fp = fopen(fpath, "rb");
        if (!fp) {
            fprintf(stderr, "[SERVER] Warning: cannot open %s\n", fpath);
            continue;
        }

        uint64_t start_off = (i == cur_file_idx) ? cur_file_offset : 0;
        if (start_off >= session->files[i].file_size && session->files[i].file_size > 0) {
            fclose(fp);
            continue; /* File already completely transferred in previous session */
        }
        if (start_off > 0) {
            fseek(fp, (long)start_off, SEEK_SET);
            printf("[SERVER] Resuming file '%s' from byte offset %llu/%llu\n",
                   session->files[i].filename, (unsigned long long)start_off,
                   (unsigned long long)session->files[i].file_size);
        }

        struct MsgFileHdrPayload file_hdr_p;
        memset(&file_hdr_p, 0, sizeof(file_hdr_p));
        strncpy(file_hdr_p.filename, session->files[i].filename, sizeof(file_hdr_p.filename) - 1);
        file_hdr_p.file_size = hton64(session->files[i].file_size);
        file_hdr_p.start_offset = hton64(start_off);
        file_hdr_p.mode = htonl(0644);

        struct MsgHeader file_hdr;
        memset(&file_hdr, 0, sizeof(file_hdr));
        file_hdr.magic = PROTOCOL_MAGIC;
        file_hdr.msg_type = MSG_FILE_HDR;
        file_hdr.session_id = current_session_id;
        file_hdr.payload_len = sizeof(file_hdr_p);

        if (send_msg(fd, &file_hdr, &file_hdr_p) != 0) {
            fclose(fp);
            close(fd);
            return;
        }

        uint64_t offset = start_off;
        char chunk_buffer[CHUNK_SIZE];

        while (offset < session->files[i].file_size) {
            size_t to_read = CHUNK_SIZE;
            if (offset + to_read > session->files[i].file_size) {
                to_read = (size_t)(session->files[i].file_size - offset);
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
            strncpy(d_hdr->filename, session->files[i].filename, sizeof(d_hdr->filename) - 1);
            d_hdr->offset = hton64(offset);
            d_hdr->chunk_size = htonl((uint32_t)bytes_read);
            memcpy(data_msg_buf + sizeof(struct MsgDataPayloadHdr), chunk_buffer, bytes_read);

            struct MsgHeader data_hdr;
            memset(&data_hdr, 0, sizeof(data_hdr));
            data_hdr.magic = PROTOCOL_MAGIC;
            data_hdr.msg_type = MSG_DATA;
            data_hdr.session_id = current_session_id;
            data_hdr.payload_len = (uint32_t)data_payload_len;

            if (send_msg(fd, &data_hdr, data_msg_buf) != 0) {
                free(data_msg_buf);
                fclose(fp);
                close(fd);
                return;
            }
            free(data_msg_buf);

            /* Await ACK from client to confirm checkpoint update */
            struct MsgHeader ack_hdr;
            void *ack_payload = NULL;
            ret = recv_msg(fd, &ack_hdr, &ack_payload);
            if (ret <= 0) {
                fclose(fp);
                close(fd);
                return;
            }

            if (ack_hdr.msg_type == MSG_ACK && ack_hdr.payload_len >= sizeof(struct MsgAckPayload)) {
                struct MsgAckPayload *ack_p = (struct MsgAckPayload *)ack_payload;
                uint64_t acked_offset = ntoh64(ack_p->acked_offset);

                /* Update persistent session checkpoint */
                pthread_mutex_lock(&g_session_lock);
                if (acked_offset >= session->files[i].file_size) {
                    session->current_file_idx = i + 1;
                    session->current_file_offset = 0;
                } else {
                    session->current_file_idx = i;
                    session->current_file_offset = acked_offset;
                }
                session->last_update = time(NULL);
                pthread_mutex_unlock(&g_session_lock);
            }
            if (ack_payload) free(ack_payload);

            offset += bytes_read;
            total_bytes_sent_conn += bytes_read;

            /* FAULT INJECTION CHECK:
             * Terminate connection after chunk transmission
             */
            if (ctx->failure_prob > 0.0) {
                double roll = (double)rand_r(&rand_seed) / (double)RAND_MAX;
                if (roll < ctx->failure_prob) {
                    printf("[FAULT INJECTION] Severing connection for session 0x%016llx at file '%s' offset %llu (prob=%.4f, roll=%.4f)\n",
                           (unsigned long long)current_session_id, session->files[i].filename,
                           (unsigned long long)offset, ctx->failure_prob, roll);
                    fclose(fp);
                    close(fd);
                    return; /* Progress is saved in session state! */
                }
            }
        }

        fclose(fp);

        /* Advance to next file */
        pthread_mutex_lock(&g_session_lock);
        session->current_file_idx = i + 1;
        session->current_file_offset = 0;
        session->last_update = time(NULL);
        pthread_mutex_unlock(&g_session_lock);
    }

    /* All files completed */
    pthread_mutex_lock(&g_session_lock);
    session->is_completed = 1;
    pthread_mutex_unlock(&g_session_lock);

    /* Step 5: Send MSG_CLOSE */
    struct MsgClosePayload close_p;
    memset(&close_p, 0, sizeof(close_p));
    strncpy(close_p.topic, topic, sizeof(close_p.topic) - 1);
    close_p.session_id = hton64(current_session_id);
    close_p.status_code = htonl(0);
    close_p.total_bytes_sent = hton64(total_bytes_sent_conn);

    struct MsgHeader close_hdr;
    memset(&close_hdr, 0, sizeof(close_hdr));
    close_hdr.magic = PROTOCOL_MAGIC;
    close_hdr.msg_type = MSG_CLOSE;
    close_hdr.session_id = current_session_id;
    close_hdr.payload_len = sizeof(close_p);

    send_msg(fd, &close_hdr, &close_p);

    double t_end = get_time_sec();
    double elapsed = t_end - t_start;

    printf("[SERVER] Session 0x%016llx completed topic '%s'.\n",
           (unsigned long long)current_session_id, topic);
    print_transfer_stats("SERVER", total_bytes_sent_conn, elapsed);

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
    printf(" Part 2 Case 2 Server (With Session Management)\n");
    printf(" Port: %d | Root: %s | Failure Prob: %.4f\n", port, topics_root, failure_prob);
    printf(" Mode: Multi-threaded (1 thread/client + session checkpointing)\n");
    printf("====================================================\n");

    /* Spawn background session cleanup thread */
    pthread_t reaper_tid;
    pthread_create(&reaper_tid, NULL, session_reaper_func, NULL);
    pthread_detach(reaper_tid);

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
