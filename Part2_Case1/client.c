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

    printf("====================================================\n");
    printf(" Part 2 Case 1 Client (No Session Management)\n");
    printf(" Target Server: %s:%d\n", server_ip, server_port);
    printf(" Topic: '%s' | Output: '%s'\n", topic, target_dir);
    printf("====================================================\n");

    int transfer_completed = 0;
    int attempt_count = 0;
    uint64_t total_bytes_transferred = 0;
    uint64_t useful_bytes = 0;
    double t_start = get_time_sec();

    while (!transfer_completed) {
        attempt_count++;
        printf("[CLIENT] --- Starting Transfer Attempt #%d ---\n", attempt_count);

        /* Case 1: Incomplete or prior attempt files must be completely wiped */
        remove_dir_recursive(target_dir);
        if (mkdir_p(target_dir) != 0) {
            fprintf(stderr, "[CLIENT] Error creating output directory '%s': %s\n", target_dir, strerror(errno));
            return 1;
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
            fprintf(stderr, "[CLIENT] Invalid IP address: %s\n", server_ip);
            close(sockfd);
            return 1;
        }

        if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            fprintf(stderr, "[CLIENT] Connect failed: %s. Retrying in 200ms...\n", strerror(errno));
            close(sockfd);
            usleep(200000);
            continue;
        }

        /* Send MSG_GET */
        struct MsgGetPayload get_payload;
        memset(&get_payload, 0, sizeof(get_payload));
        strncpy(get_payload.topic, topic, sizeof(get_payload.topic) - 1);

        struct MsgHeader get_hdr;
        memset(&get_hdr, 0, sizeof(get_hdr));
        get_hdr.magic = PROTOCOL_MAGIC;
        get_hdr.msg_type = MSG_GET;
        get_hdr.payload_len = sizeof(get_payload);

        if (send_msg(sockfd, &get_hdr, &get_payload) != 0) {
            fprintf(stderr, "[CLIENT] Failed to send GET. Retrying...\n");
            close(sockfd);
            usleep(200000);
            continue;
        }

        /* Recv response */
        struct MsgHeader resp_hdr;
        void *resp_payload = NULL;
        int ret = recv_msg(sockfd, &resp_hdr, &resp_payload);
        if (ret <= 0) {
            fprintf(stderr, "[CLIENT] Disconnected during response. Retrying...\n");
            close(sockfd);
            usleep(200000);
            continue;
        }

        if (resp_hdr.msg_type == MSG_ERR) {
            struct MsgErrPayload *err_p = (struct MsgErrPayload *)resp_payload;
            fprintf(stderr, "[CLIENT] Server reported fatal error (%u): %s\n",
                    ntohl(err_p->error_code), err_p->error_msg);
            free(resp_payload);
            close(sockfd);
            return 1;
        }

        if (resp_hdr.msg_type != MSG_OK) {
            fprintf(stderr, "[CLIENT] Protocol error: Expected MSG_OK, got %u\n", resp_hdr.msg_type);
            if (resp_payload) free(resp_payload);
            close(sockfd);
            usleep(200000);
            continue;
        }

        struct MsgOkHeader *ok_hdr = (struct MsgOkHeader *)resp_payload;
        uint32_t num_files = ntohl(ok_hdr->num_files);
        printf("[CLIENT] Server confirmed %u files for topic '%s'.\n", num_files, topic);
        free(resp_payload);
        resp_payload = NULL;

        /* Send MSG_START */
        struct MsgStartPayload start_p;
        memset(&start_p, 0, sizeof(start_p));
        strncpy(start_p.topic, topic, sizeof(start_p.topic) - 1);

        struct MsgHeader start_hdr;
        memset(&start_hdr, 0, sizeof(start_hdr));
        start_hdr.magic = PROTOCOL_MAGIC;
        start_hdr.msg_type = MSG_START;
        start_hdr.payload_len = sizeof(start_p);

        if (send_msg(sockfd, &start_hdr, &start_p) != 0) {
            fprintf(stderr, "[CLIENT] Send START failed. Retrying...\n");
            close(sockfd);
            usleep(200000);
            continue;
        }

        /* Receive files */
        int attempt_failed = 0;
        uint64_t current_attempt_useful = 0;

        while (1) {
            struct MsgHeader msg_hdr;
            void *msg_payload = NULL;
            ret = recv_msg(sockfd, &msg_hdr, &msg_payload);
            if (ret <= 0) {
                printf("[CLIENT] Connection lost during file receive (Failure injected by server).\n");
                attempt_failed = 1;
                break;
            }

            if (msg_hdr.msg_type == MSG_CLOSE) {
                printf("[CLIENT] MSG_CLOSE received. Transfer complete!\n");
                if (msg_payload) free(msg_payload);
                transfer_completed = 1;
                useful_bytes = current_attempt_useful;
                break;
            }

            if (msg_hdr.msg_type != MSG_FILE_HDR) {
                fprintf(stderr, "[CLIENT] Protocol error: Expected FILE_HDR, got %u\n", msg_hdr.msg_type);
                if (msg_payload) free(msg_payload);
                attempt_failed = 1;
                break;
            }

            struct MsgFileHdrPayload *fhdr_p = (struct MsgFileHdrPayload *)msg_payload;
            char filename[MAX_FILENAME_LEN];
            strncpy(filename, fhdr_p->filename, sizeof(filename) - 1);
            filename[sizeof(filename) - 1] = '\0';
            uint64_t file_size = ntoh64(fhdr_p->file_size);
            free(msg_payload);
            msg_payload = NULL;

            if (!is_valid_name(filename)) {
                attempt_failed = 1;
                break;
            }

            char file_path[1024];
            snprintf(file_path, sizeof(file_path), "%s/%s", target_dir, filename);

            FILE *fp = fopen(file_path, "wb");
            if (!fp) {
                perror("fopen");
                attempt_failed = 1;
                break;
            }

            uint64_t file_bytes_recv = 0;
            while (file_bytes_recv < file_size) {
                struct MsgHeader data_hdr;
                void *data_payload = NULL;
                ret = recv_msg(sockfd, &data_hdr, &data_payload);
                if (ret <= 0) {
                    printf("[CLIENT] Connection severed during data chunk of '%s'!\n", filename);
                    attempt_failed = 1;
                    break;
                }

                if (data_hdr.msg_type != MSG_DATA) {
                    if (data_payload) free(data_payload);
                    attempt_failed = 1;
                    break;
                }

                struct MsgDataPayloadHdr *d_hdr = (struct MsgDataPayloadHdr *)data_payload;
                uint32_t chunk_size = ntohl(d_hdr->chunk_size);
                char *chunk_data = (char *)data_payload + sizeof(struct MsgDataPayloadHdr);

                fwrite(chunk_data, 1, chunk_size, fp);
                file_bytes_recv += chunk_size;
                total_bytes_transferred += chunk_size;
                current_attempt_useful += chunk_size;
                free(data_payload);

                /* Send ACK */
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
                ack_hdr.payload_len = sizeof(ack_p);

                if (send_msg(sockfd, &ack_hdr, &ack_p) != 0) {
                    attempt_failed = 1;
                    break;
                }
            }

            fclose(fp);
            if (attempt_failed) break;
        }

        close(sockfd);

        if (attempt_failed) {
            printf("[CLIENT] Attempt #%d failed. Wiping directory and restarting topic from scratch...\n", attempt_count);
            usleep(150000); /* 150ms backoff before next attempt */
        }
    }

    double t_end = get_time_sec();
    double elapsed = t_end - t_start;
    uint64_t redundant_bytes = (total_bytes_transferred > useful_bytes) ? (total_bytes_transferred - useful_bytes) : 0;
    double overhead_pct = (useful_bytes > 0) ? ((double)redundant_bytes / (double)useful_bytes * 100.0) : 0.0;
    double mbps = (elapsed > 0) ? ((double)useful_bytes / (1024.0 * 1024.0) / elapsed) : 0.0;

    printf("\n====================================================\n");
    printf(" Part 2 Case 1 Transfer Complete (No Session State)\n");
    printf(" Total Attempts:             %d\n", attempt_count);
    printf(" Useful Bytes Delivered:     %llu\n", (unsigned long long)useful_bytes);
    printf(" Redundant Retransmitted:    %llu\n", (unsigned long long)redundant_bytes);
    printf(" Total Bytes Transmitted:    %llu\n", (unsigned long long)total_bytes_transferred);
    printf(" Retransmission Overhead:    %.2f%%\n", overhead_pct);
    printf(" Total Elapsed Time:         %.4f seconds\n", elapsed);
    printf(" Effective Throughput:       %.2f MB/s\n", mbps);
    printf("====================================================\n");

    return 0;
}
