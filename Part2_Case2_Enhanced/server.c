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
#define MAX_CHUNKS          32768
#define MAX_SESSIONS        16
#define SESSION_TIMEOUT_SEC 300

typedef enum {
    CHUNK_PENDING = 0,
    CHUNK_IN_PROGRESS = 1,
    CHUNK_COMPLETED = 2
} ChunkStatus;

typedef struct {
    uint32_t chunk_id;
    char filename[MAX_FILENAME_LEN];
    uint64_t file_offset;
    uint32_t chunk_size;
    ChunkStatus status;
    time_t assigned_time;
} ServerChunkTask;

typedef struct {
    uint64_t session_id;
    char topic[MAX_TOPIC_NAME];
    uint32_t total_files;
    struct FileInfoEntry files[MAX_FILES_PER_TOPIC];
    uint32_t total_chunks;
    uint32_t completed_chunks;
    ServerChunkTask chunks[MAX_CHUNKS];
    time_t last_update;
    int is_active;
    int is_completed;
    pthread_mutex_t mutex;
} EnhancedSession;

static EnhancedSession g_sessions[MAX_SESSIONS];
static pthread_mutex_t g_global_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    int client_fd;
    struct sockaddr_in client_addr;
    char topics_root[512];
    double failure_prob;
} ConnContext;

static uint64_t generate_unique_session_id(void) {
    static uint64_t counter = 5000;
    uint64_t r = ((uint64_t)rand() << 32) | (uint64_t)rand();
    return (r ^ ((uint64_t)time(NULL) << 16) ^ (++counter)) | 1ULL;
}

static void *reaper_thread_func(void *arg) {
    (void)arg;
    while (1) {
        sleep(10);
        time_t now = time(NULL);
        pthread_mutex_lock(&g_global_lock);
        for (int i = 0; i < MAX_SESSIONS; i++) {
            if (g_sessions[i].is_active) {
                if (g_sessions[i].is_completed || (now - g_sessions[i].last_update > SESSION_TIMEOUT_SEC)) {
                    printf("[ENHANCED CLEANUP] Expiring session 0x%016llx (Topic: %s)\n",
                           (unsigned long long)g_sessions[i].session_id, g_sessions[i].topic);
                    pthread_mutex_destroy(&g_sessions[i].mutex);
                    memset(&g_sessions[i], 0, sizeof(EnhancedSession));
                }
            }
        }
        pthread_mutex_unlock(&g_global_lock);
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

/* Serve a parallel worker channel */
static void handle_worker_channel(ConnContext *ctx, uint64_t session_id, uint32_t worker_id) {
    int fd = ctx->client_fd;
    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &ctx->client_addr.sin_addr, client_ip, sizeof(client_ip));

    EnhancedSession *sess = NULL;
    pthread_mutex_lock(&g_global_lock);
    for (int i = 0; i < MAX_SESSIONS; i++) {
        if (g_sessions[i].is_active && g_sessions[i].session_id == session_id) {
            sess = &g_sessions[i];
            break;
        }
    }
    pthread_mutex_unlock(&g_global_lock);

    if (!sess) {
        send_error(fd, ERR_SESSION_NOT_FOUND, "Session not found for worker", session_id);
        close(fd);
        return;
    }

    printf("[SERVER] Parallel Worker #%u joined session 0x%016llx from %s\n",
           worker_id, (unsigned long long)session_id, client_ip);

    unsigned int rand_seed = (unsigned int)(time(NULL) ^ (uintptr_t)pthread_self() ^ worker_id);
    char topic_dir[1024];
    snprintf(topic_dir, sizeof(topic_dir), "%s/%s", ctx->topics_root, sess->topic);

    while (1) {
        /* Request next chunk under session mutex */
        pthread_mutex_lock(&sess->mutex);

        if (sess->completed_chunks >= sess->total_chunks) {
            pthread_mutex_unlock(&sess->mutex);
            /* All chunks completed */
            struct MsgHeader close_hdr;
            memset(&close_hdr, 0, sizeof(close_hdr));
            close_hdr.magic = PROTOCOL_MAGIC;
            close_hdr.msg_type = MSG_WORKER_CLOSE;
            close_hdr.session_id = session_id;
            close_hdr.payload_len = 0;
            send_msg(fd, &close_hdr, NULL);
            break;
        }

        int chunk_idx = -1;
        time_t now = time(NULL);

        /* Find pending chunk or orphaned chunk whose worker died (>15s) */
        for (uint32_t i = 0; i < sess->total_chunks; i++) {
            if (sess->chunks[i].status == CHUNK_PENDING) {
                chunk_idx = (int)i;
                break;
            } else if (sess->chunks[i].status == CHUNK_IN_PROGRESS && (now - sess->chunks[i].assigned_time > 15)) {
                chunk_idx = (int)i;
                break;
            }
        }

        if (chunk_idx < 0) {
            /* All available chunks are in progress by other workers; wait briefly */
            pthread_mutex_unlock(&sess->mutex);
            usleep(20000);
            continue;
        }

        ServerChunkTask *task = &sess->chunks[chunk_idx];
        task->status = CHUNK_IN_PROGRESS;
        task->assigned_time = now;

        /* Make local copy of task details */
        uint32_t chunk_id = task->chunk_id;
        char fname[MAX_FILENAME_LEN];
        strncpy(fname, task->filename, sizeof(fname) - 1);
        fname[sizeof(fname) - 1] = '\0';
        uint64_t file_offset = task->file_offset;
        uint32_t chunk_size = task->chunk_size;

        pthread_mutex_unlock(&sess->mutex);

        /* Read chunk data from disk */
        char fpath[1024];
        snprintf(fpath, sizeof(fpath), "%s/%s", topic_dir, fname);

        int file_fd = open(fpath, O_RDONLY);
        if (file_fd < 0) {
            fprintf(stderr, "[SERVER] Worker cannot open file %s: %s\n", fpath, strerror(errno));
            pthread_mutex_lock(&sess->mutex);
            task->status = CHUNK_PENDING;
            pthread_mutex_unlock(&sess->mutex);
            break;
        }

        char *buffer = malloc(chunk_size);
        if (!buffer) {
            close(file_fd);
            pthread_mutex_lock(&sess->mutex);
            task->status = CHUNK_PENDING;
            pthread_mutex_unlock(&sess->mutex);
            break;
        }

        ssize_t bytes_read = pread(file_fd, buffer, chunk_size, (off_t)file_offset);
        close(file_fd);

        if (bytes_read <= 0) {
            free(buffer);
            pthread_mutex_lock(&sess->mutex);
            task->status = CHUNK_PENDING;
            pthread_mutex_unlock(&sess->mutex);
            break;
        }

        /* Send MSG_DATA to client worker */
        size_t payload_len = sizeof(struct MsgDataPayloadHdr) + bytes_read;
        char *msg_buf = malloc(payload_len);
        if (!msg_buf) {
            free(buffer);
            pthread_mutex_lock(&sess->mutex);
            task->status = CHUNK_PENDING;
            pthread_mutex_unlock(&sess->mutex);
            break;
        }

        struct MsgDataPayloadHdr *d_hdr = (struct MsgDataPayloadHdr *)msg_buf;
        strncpy(d_hdr->filename, fname, sizeof(d_hdr->filename) - 1);
        d_hdr->offset = hton64(file_offset);
        d_hdr->chunk_size = htonl((uint32_t)bytes_read);
        d_hdr->chunk_id = htonl(chunk_id);
        memcpy(msg_buf + sizeof(struct MsgDataPayloadHdr), buffer, bytes_read);
        free(buffer);

        struct MsgHeader data_hdr;
        memset(&data_hdr, 0, sizeof(data_hdr));
        data_hdr.magic = PROTOCOL_MAGIC;
        data_hdr.msg_type = MSG_DATA;
        data_hdr.session_id = session_id;
        data_hdr.transaction_id = chunk_id;
        data_hdr.payload_len = (uint32_t)payload_len;

        if (send_msg(fd, &data_hdr, msg_buf) != 0) {
            free(msg_buf);
            pthread_mutex_lock(&sess->mutex);
            task->status = CHUNK_PENDING;
            pthread_mutex_unlock(&sess->mutex);
            break;
        }
        free(msg_buf);

        /* Await ACK from client worker */
        struct MsgHeader ack_hdr;
        void *ack_payload = NULL;
        int ret = recv_msg(fd, &ack_hdr, &ack_payload);
        if (ret <= 0) {
            /* Connection lost */
            pthread_mutex_lock(&sess->mutex);
            task->status = CHUNK_PENDING;
            pthread_mutex_unlock(&sess->mutex);
            break;
        }

        if (ack_payload) free(ack_payload);

        /* Mark completed */
        pthread_mutex_lock(&sess->mutex);
        task->status = CHUNK_COMPLETED;
        sess->completed_chunks++;
        sess->last_update = time(NULL);
        pthread_mutex_unlock(&sess->mutex);

        /* FAULT INJECTION CHECK on worker channel */
        if (ctx->failure_prob > 0.0) {
            double roll = (double)rand_r(&rand_seed) / (double)RAND_MAX;
            if (roll < ctx->failure_prob) {
                printf("[FAULT INJECTION] Terminating Worker #%u connection for session 0x%016llx (prob=%.4f, roll=%.4f)\n",
                       worker_id, (unsigned long long)session_id, ctx->failure_prob, roll);
                close(fd);
                return; /* Socket closed; other workers or reconnected worker will continue! */
            }
        }
    }

    close(fd);
}

/* Serve Primary Control Channel */
static void handle_primary_channel(ConnContext *ctx, struct MsgHeader *req_hdr, void *req_payload) {
    int fd = ctx->client_fd;
    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &ctx->client_addr.sin_addr, client_ip, sizeof(client_ip));

    struct MsgGetPayload *get_req = (struct MsgGetPayload *)req_payload;
    char topic[MAX_TOPIC_NAME];
    strncpy(topic, get_req->topic, sizeof(topic) - 1);
    topic[sizeof(topic) - 1] = '\0';
    uint64_t req_session_id = ntoh64(get_req->session_id);
    if (req_session_id == 0 && req_hdr->session_id != 0) {
        req_session_id = req_hdr->session_id;
    }
    free(req_payload);

    if (!is_valid_name(topic)) {
        send_error(fd, ERR_PATH_TRAVERSAL, "Invalid topic name", 0);
        close(fd);
        return;
    }

    char topic_dir[1024];
    snprintf(topic_dir, sizeof(topic_dir), "%s/%s", ctx->topics_root, topic);

    struct stat st;
    if (stat(topic_dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
        send_error(fd, ERR_TOPIC_NOT_FOUND, "Topic directory not found", 0);
        close(fd);
        return;
    }

    EnhancedSession *sess = NULL;
    pthread_mutex_lock(&g_global_lock);

    if (req_session_id != 0) {
        for (int i = 0; i < MAX_SESSIONS; i++) {
            if (g_sessions[i].is_active && g_sessions[i].session_id == req_session_id) {
                sess = &g_sessions[i];
                sess->last_update = time(NULL);
                break;
            }
        }
        if (!sess) {
            pthread_mutex_unlock(&g_global_lock);
            send_error(fd, ERR_SESSION_NOT_FOUND, "Session expired", req_session_id);
            close(fd);
            return;
        }
    } else {
        int slot = -1;
        for (int i = 0; i < MAX_SESSIONS; i++) {
            if (!g_sessions[i].is_active) {
                slot = i;
                break;
            }
        }
        if (slot < 0) {
            pthread_mutex_unlock(&g_global_lock);
            send_error(fd, ERR_INTERNAL, "Session table full", 0);
            close(fd);
            return;
        }

        sess = &g_sessions[slot];
        memset(sess, 0, sizeof(EnhancedSession));
        sess->session_id = generate_unique_session_id();
        strncpy(sess->topic, topic, sizeof(sess->topic) - 1);
        pthread_mutex_init(&sess->mutex, NULL);
        sess->is_active = 1;
        sess->last_update = time(NULL);

        /* Enumerate files */
        DIR *d = opendir(topic_dir);
        if (!d) {
            pthread_mutex_destroy(&sess->mutex);
            memset(sess, 0, sizeof(EnhancedSession));
            pthread_mutex_unlock(&g_global_lock);
            send_error(fd, ERR_FILE_IO, "Could not open topic directory", 0);
            close(fd);
            return;
        }

        struct dirent *entry;
        uint32_t chunk_counter = 0;

        while ((entry = readdir(d)) != NULL && sess->total_files < MAX_FILES_PER_TOPIC) {
            if (entry->d_name[0] == '.') continue;
            if (!is_valid_name(entry->d_name)) continue;

            char fpath[1024];
            snprintf(fpath, sizeof(fpath), "%s/%s", topic_dir, entry->d_name);
            struct stat fst;
            if (stat(fpath, &fst) == 0 && S_ISREG(fst.st_mode)) {
                uint32_t f_idx = sess->total_files;
                strncpy(sess->files[f_idx].filename, entry->d_name, MAX_FILENAME_LEN - 1);
                sess->files[f_idx].file_size = (uint64_t)fst.st_size;
                sess->total_files++;

                /* Split file into chunk tasks */
                uint64_t f_off = 0;
                while (f_off < (uint64_t)fst.st_size && chunk_counter < MAX_CHUNKS) {
                    uint32_t c_size = CHUNK_SIZE;
                    if (f_off + c_size > (uint64_t)fst.st_size) {
                        c_size = (uint32_t)((uint64_t)fst.st_size - f_off);
                    }
                    ServerChunkTask *ct = &sess->chunks[chunk_counter];
                    ct->chunk_id = chunk_counter;
                    strncpy(ct->filename, entry->d_name, sizeof(ct->filename) - 1);
                    ct->file_offset = f_off;
                    ct->chunk_size = c_size;
                    ct->status = CHUNK_PENDING;

                    chunk_counter++;
                    f_off += c_size;
                }
            }
        }
        closedir(d);
        sess->total_chunks = chunk_counter;

        printf("[SERVER] Enhanced Session 0x%016llx created: %u files, %u chunks\n",
               (unsigned long long)sess->session_id, sess->total_files, sess->total_chunks);
    }

    uint64_t session_id = sess->session_id;
    uint32_t total_files = sess->total_files;
    pthread_mutex_unlock(&g_global_lock);

    /* Send MSG_OK */
    size_t ok_payload_len = sizeof(struct MsgOkHeader) + total_files * sizeof(struct FileInfoEntry);
    char *ok_buf = malloc(ok_payload_len);
    if (!ok_buf) {
        send_error(fd, ERR_INTERNAL, "Memory allocation error", session_id);
        close(fd);
        return;
    }

    struct MsgOkHeader *ok_hdr = (struct MsgOkHeader *)ok_buf;
    ok_hdr->num_files = htonl(total_files);
    ok_hdr->session_id = hton64(session_id);
    strncpy(ok_hdr->topic, topic, sizeof(ok_hdr->topic) - 1);

    struct FileInfoEntry *entries = (struct FileInfoEntry *)(ok_buf + sizeof(struct MsgOkHeader));
    for (uint32_t i = 0; i < total_files; i++) {
        strncpy(entries[i].filename, sess->files[i].filename, sizeof(entries[i].filename) - 1);
        entries[i].file_size = hton64(sess->files[i].file_size);
        entries[i].start_offset = 0;
    }

    struct MsgHeader resp_hdr;
    memset(&resp_hdr, 0, sizeof(resp_hdr));
    resp_hdr.magic = PROTOCOL_MAGIC;
    resp_hdr.msg_type = MSG_OK;
    resp_hdr.session_id = session_id;
    resp_hdr.payload_len = (uint32_t)ok_payload_len;

    if (send_msg(fd, &resp_hdr, ok_buf) != 0) {
        free(ok_buf);
        close(fd);
        return;
    }
    free(ok_buf);

    /* Expect MSG_START */
    struct MsgHeader start_hdr;
    void *start_payload = NULL;
    int ret = recv_msg(fd, &start_hdr, &start_payload);
    if (ret <= 0) {
        close(fd);
        return;
    }
    if (start_payload) free(start_payload);

    printf("[SERVER] Primary channel ready for parallel transfers (Session 0x%016llx)\n",
           (unsigned long long)session_id);

    /* Primary channel waits until client sends MSG_CLOSE */
    struct MsgHeader close_req;
    void *close_payload = NULL;
    ret = recv_msg(fd, &close_req, &close_payload);
    if (close_payload) free(close_payload);

    /* Mark session complete and respond MSG_CLOSE */
    pthread_mutex_lock(&sess->mutex);
    sess->is_completed = 1;
    pthread_mutex_unlock(&sess->mutex);

    struct MsgClosePayload close_p;
    memset(&close_p, 0, sizeof(close_p));
    strncpy(close_p.topic, topic, sizeof(close_p.topic) - 1);
    close_p.session_id = hton64(session_id);
    close_p.status_code = htonl(0);

    struct MsgHeader close_resp;
    memset(&close_resp, 0, sizeof(close_resp));
    close_resp.magic = PROTOCOL_MAGIC;
    close_resp.msg_type = MSG_CLOSE;
    close_resp.session_id = session_id;
    close_resp.payload_len = sizeof(close_p);

    send_msg(fd, &close_resp, &close_p);
    printf("[SERVER] Enhanced Session 0x%016llx complete! Primary connection closed.\n",
           (unsigned long long)session_id);
    close(fd);
}

/* Master connection router */
static void *conn_dispatcher(void *arg) {
    ConnContext *ctx = (ConnContext *)arg;
    int fd = ctx->client_fd;

    struct MsgHeader hdr;
    void *payload = NULL;

    int ret = recv_msg(fd, &hdr, &payload);
    if (ret <= 0) {
        close(fd);
        free(ctx);
        return NULL;
    }

    if (hdr.msg_type == MSG_GET) {
        handle_primary_channel(ctx, &hdr, payload);
    } else if (hdr.msg_type == MSG_WORKER_JOIN) {
        struct MsgWorkerJoinPayload *join_p = (struct MsgWorkerJoinPayload *)payload;
        uint64_t session_id = ntoh64(join_p->session_id);
        uint32_t worker_id = ntohl(join_p->worker_id);
        free(payload);
        handle_worker_channel(ctx, session_id, worker_id);
    } else {
        fprintf(stderr, "[SERVER] Unknown initial message type: %u\n", hdr.msg_type);
        if (payload) free(payload);
        close(fd);
    }

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
        fprintf(stderr, "Error: Topics root '%s' does not exist.\n", topics_root);
        return 1;
    }

    double failure_prob = atof(argv[3]);
    if (failure_prob < 0.0 || failure_prob > 1.0) {
        fprintf(stderr, "Error: Failure probability must be between 0.0 and 1.0\n");
        return 1;
    }

    printf("====================================================\n");
    printf(" Part 2 Case 2 ENHANCED Server (Parallel TCP Streams)\n");
    printf(" Port: %d | Root: %s | Failure Prob: %.4f\n", port, topics_root, failure_prob);
    printf(" Mode: Multi-threaded with Dynamic Parallel Chunk Queues\n");
    printf("====================================================\n");

    pthread_t reaper_tid;
    pthread_create(&reaper_tid, NULL, reaper_thread_func, NULL);
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

        ConnContext *ctx = malloc(sizeof(ConnContext));
        if (!ctx) {
            close(client_fd);
            continue;
        }
        ctx->client_fd = client_fd;
        ctx->client_addr = client_addr;
        strncpy(ctx->topics_root, topics_root, sizeof(ctx->topics_root) - 1);
        ctx->failure_prob = failure_prob;

        pthread_t tid;
        if (pthread_create(&tid, NULL, conn_dispatcher, ctx) != 0) {
            perror("pthread_create");
            free(ctx);
            close(client_fd);
        } else {
            pthread_detach(tid);
        }
    }

    close(listen_fd);
    return 0;
}
