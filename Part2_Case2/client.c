#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

#include "protocol.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>

int main(int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <server_ip> <server_port> <topic> <output_dir>\n", argv[0]);
        return 1;
    }

    const char *server_ip = argv[1];
    int server_port = atoi(argv[2]);
    const char *topic = argv[3];
    const char *output_dir = argv[4];

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
    printf(" Part 2 Case 2 Client (With Session Management)\n");
    printf(" Target Server: %s:%d\n", server_ip, server_port);
    printf(" Topic: '%s' | Output: '%s'\n", topic, target_dir);
    printf("====================================================\n");

    uint64_t session_id = 0;
    char ckpt_filename[MAX_FILENAME_LEN] = {0};
    uint64_t ckpt_offset = 0;

    int transfer_completed = 0;
    int connection_attempts = 0;
    int resume_count = 0;
    uint64_t total_bytes_transferred = 0;
    uint64_t total_topic_bytes = 0;
    double t_start = get_time_sec();

    while (!transfer_completed) {
        connection_attempts++;

        if (session_id == 0) {
            printf("[CLIENT] Initial connection attempt #%d...\n", connection_attempts);
        } else {
            resume_count++;
            printf("[CLIENT] Resuming session 0x%016llx (Attempt #%d, Resume #%d, File '%s' at offset %llu)...\n",
                   (unsigned long long)session_id, connection_attempts, resume_count,
                   ckpt_filename, (unsigned long long)ckpt_offset);
        }

        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
            perror("socket");
            usleep(200000);
            continue;
        }

        struct sockaddr_in serv_addr;
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(server_port);

        if (inet_pton(AF_INET, server_ip, &serv_addr.sin_addr) <= 0) {
            fprintf(stderr, "[CLIENT] Invalid IP: %s\n", server_ip);
            close(sockfd);
            return 1;
        }

        if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            fprintf(stderr, "[CLIENT] Connect failed: %s. Retrying in 200ms...\n", strerror(errno));
            close(sockfd);
            usleep(200000);
            continue;
        }

        /* Step 1: Send MSG_GET with session_id and checkpoint info */
        struct MsgGetPayload get_payload;
        memset(&get_payload, 0, sizeof(get_payload));
        strncpy(get_payload.topic, topic, sizeof(get_payload.topic) - 1);
        get_payload.session_id = hton64(session_id);
        strncpy(get_payload.checkpoint_file, ckpt_filename, sizeof(get_payload.checkpoint_file) - 1);
        get_payload.checkpoint_offset = hton64(ckpt_offset);

        struct MsgHeader get_hdr;
        memset(&get_hdr, 0, sizeof(get_hdr));
        get_hdr.magic = PROTOCOL_MAGIC;
        get_hdr.msg_type = MSG_GET;
        get_hdr.session_id = session_id;
        get_hdr.payload_len = sizeof(get_payload);

        if (send_msg(sockfd, &get_hdr, &get_payload) != 0) {
            close(sockfd);
            usleep(200000);
            continue;
        }

        /* Step 2: Recv MSG_OK */
        struct MsgHeader resp_hdr;
        void *resp_payload = NULL;
        int ret = recv_msg(sockfd, &resp_hdr, &resp_payload);
        if (ret <= 0) {
            close(sockfd);
            usleep(200000);
            continue;
        }

        if (resp_hdr.msg_type == MSG_ERR) {
            struct MsgErrPayload *err_p = (struct MsgErrPayload *)resp_payload;
            uint32_t err_code = ntohl(err_p->error_code);
            fprintf(stderr, "[CLIENT] Server returned error (%u): %s\n", err_code, err_p->error_msg);
            free(resp_payload);
            close(sockfd);
            if (err_code == ERR_SESSION_NOT_FOUND) {
                /* Session expired on server; reset and start fresh */
                printf("[CLIENT] Session expired on server. Restarting fresh session...\n");
                session_id = 0;
                ckpt_offset = 0;
                ckpt_filename[0] = '\0';
                continue;
            }
            return 1;
        }

        if (resp_hdr.msg_type != MSG_OK) {
            if (resp_payload) free(resp_payload);
            close(sockfd);
            usleep(200000);
            continue;
        }

        struct MsgOkHeader *ok_hdr = (struct MsgOkHeader *)resp_payload;
        uint32_t num_files = ntohl(ok_hdr->num_files);
        uint64_t assigned_session_id = ntoh64(ok_hdr->session_id);
        if (session_id == 0) {
            session_id = assigned_session_id;
            printf("[CLIENT] Assigned Session ID: 0x%016llx (%u files in topic)\n",
                   (unsigned long long)session_id, num_files);

            /* Calculate total expected topic bytes */
            struct FileInfoEntry *entries = (struct FileInfoEntry *)((char *)resp_payload + sizeof(struct MsgOkHeader));
            total_topic_bytes = 0;
            for (uint32_t i = 0; i < num_files; i++) {
                total_topic_bytes += ntoh64(entries[i].file_size);
            }
        }
        free(resp_payload);
        resp_payload = NULL;

        /* Step 3: Send MSG_START */
        struct MsgStartPayload start_p;
        memset(&start_p, 0, sizeof(start_p));
        strncpy(start_p.topic, topic, sizeof(start_p.topic) - 1);
        start_p.session_id = hton64(session_id);

        struct MsgHeader start_hdr;
        memset(&start_hdr, 0, sizeof(start_hdr));
        start_hdr.magic = PROTOCOL_MAGIC;
        start_hdr.msg_type = MSG_START;
        start_hdr.session_id = session_id;
        start_hdr.payload_len = sizeof(start_p);

        if (send_msg(sockfd, &start_hdr, &start_p) != 0) {
            close(sockfd);
            usleep(200000);
            continue;
        }

        /* Step 4: Transfer loop */
        int attempt_interrupted = 0;

        while (1) {
            struct MsgHeader msg_hdr;
            void *msg_payload = NULL;
            ret = recv_msg(sockfd, &msg_hdr, &msg_payload);
            if (ret <= 0) {
                printf("[CLIENT] Interrupted: server connection closed (Fault injected).\n");
                attempt_interrupted = 1;
                break;
            }

            if (msg_hdr.msg_type == MSG_CLOSE) {
                printf("[CLIENT] Received MSG_CLOSE from server. All files received!\n");
                if (msg_payload) free(msg_payload);
                transfer_completed = 1;
                break;
            }

            if (msg_hdr.msg_type != MSG_FILE_HDR) {
                if (msg_payload) free(msg_payload);
                attempt_interrupted = 1;
                break;
            }

            struct MsgFileHdrPayload *fhdr_p = (struct MsgFileHdrPayload *)msg_payload;
            char filename[MAX_FILENAME_LEN];
            strncpy(filename, fhdr_p->filename, sizeof(filename) - 1);
            filename[sizeof(filename) - 1] = '\0';
            uint64_t file_size = ntoh64(fhdr_p->file_size);
            uint64_t start_offset = ntoh64(fhdr_p->start_offset);
            free(msg_payload);
            msg_payload = NULL;

            if (!is_valid_name(filename)) {
                attempt_interrupted = 1;
                break;
            }

            char file_path[1024];
            snprintf(file_path, sizeof(file_path), "%s/%s", target_dir, filename);

            FILE *fp = NULL;
            if (start_offset > 0) {
                /* Resume partial file */
                fp = fopen(file_path, "r+b");
                if (!fp) {
                    /* Create if doesn't exist */
                    fp = fopen(file_path, "wb");
                } else {
                    fseek(fp, (long)start_offset, SEEK_SET);
                }
                printf("[CLIENT] Resuming '%s' from byte offset %llu/%llu...\n",
                       filename, (unsigned long long)start_offset, (unsigned long long)file_size);
            } else {
                fp = fopen(file_path, "wb");
                printf("[CLIENT] Receiving '%s' (%llu bytes)...\n", filename, (unsigned long long)file_size);
            }

            if (!fp) {
                perror("fopen");
                attempt_interrupted = 1;
                break;
            }

            uint64_t file_bytes_recv = start_offset;
            while (file_bytes_recv < file_size) {
                struct MsgHeader data_hdr;
                void *data_payload = NULL;
                ret = recv_msg(sockfd, &data_hdr, &data_payload);
                if (ret <= 0) {
                    printf("[CLIENT] Connection lost while receiving data for '%s'!\n", filename);
                    attempt_interrupted = 1;
                    break;
                }

                if (data_hdr.msg_type != MSG_DATA) {
                    if (data_payload) free(data_payload);
                    attempt_interrupted = 1;
                    break;
                }

                struct MsgDataPayloadHdr *d_hdr = (struct MsgDataPayloadHdr *)data_payload;
                uint32_t chunk_size = ntohl(d_hdr->chunk_size);
                char *chunk_data = (char *)data_payload + sizeof(struct MsgDataPayloadHdr);

                fwrite(chunk_data, 1, chunk_size, fp);
                fflush(fp); /* Ensure flushed to disk for fault tolerance */

                file_bytes_recv += chunk_size;
                total_bytes_transferred += chunk_size;

                /* Save local checkpoint */
                strncpy(ckpt_filename, filename, sizeof(ckpt_filename) - 1);
                ckpt_offset = file_bytes_recv;

                free(data_payload);

                /* Send MSG_ACK to checkpoint progress on server */
                struct MsgAckPayload ack_p;
                memset(&ack_p, 0, sizeof(ack_p));
                strncpy(ack_p.filename, filename, sizeof(ack_p.filename) - 1);
                ack_p.acked_offset = hton64(file_bytes_recv);
                ack_p.chunk_size = htonl(chunk_size);
                ack_p.status = htonl(0);

                struct MsgHeader ack_hdr;
                memset(&ack_hdr, 0, sizeof(ack_hdr));
                ack_hdr.magic = PROTOCOL_MAGIC;
                ack_hdr.msg_type = MSG_ACK;
                ack_hdr.session_id = session_id;
                ack_hdr.payload_len = sizeof(ack_p);

                if (send_msg(sockfd, &ack_hdr, &ack_p) != 0) {
                    attempt_interrupted = 1;
                    break;
                }
            }

            fclose(fp);
            if (attempt_interrupted) break;

            /* File complete */
            ckpt_offset = 0;
            ckpt_filename[0] = '\0';
        }

        close(sockfd);

        if (attempt_interrupted) {
            usleep(150000); /* 150ms backoff before resuming */
        }
    }

    double t_end = get_time_sec();
    double elapsed = t_end - t_start;
    uint64_t redundant_bytes = (total_bytes_transferred > total_topic_bytes) ? (total_bytes_transferred - total_topic_bytes) : 0;
    double overhead_pct = (total_topic_bytes > 0) ? ((double)redundant_bytes / (double)total_topic_bytes * 100.0) : 0.0;
    double mbps = (elapsed > 0) ? ((double)total_topic_bytes / (1024.0 * 1024.0) / elapsed) : 0.0;

    printf("\n====================================================\n");
    printf(" Part 2 Case 2 Transfer Complete (With Session State)\n");
    printf(" Session ID:                 0x%016llx\n", (unsigned long long)session_id);
    printf(" Total Connections Made:     %d\n", connection_attempts);
    printf(" Successful Resumes:         %d\n", resume_count);
    printf(" Useful Bytes Delivered:     %llu\n", (unsigned long long)total_topic_bytes);
    printf(" Redundant Retransmitted:    %llu\n", (unsigned long long)redundant_bytes);
    printf(" Total Bytes Transmitted:    %llu\n", (unsigned long long)total_bytes_transferred);
    printf(" Retransmission Overhead:    %.2f%%\n", overhead_pct);
    printf(" Total Elapsed Time:         %.4f seconds\n", elapsed);
    printf(" Effective Throughput:       %.2f MB/s\n", mbps);
    printf("====================================================\n");

    return 0;
}
