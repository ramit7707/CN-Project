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
    /*
     * Prevent the program from terminating if the server closes the
     * connection while the client is trying to send data.
     */
    signal(SIGPIPE, SIG_IGN);

    /*
     * Read and validate command-line arguments:
     * server IP, port, requested topic, and local output directory.
     */
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

    /*
     * Create a separate local folder for the requested topic.
     * Downloaded files will be stored in: output_dir/topic/
     */
    char target_dir[1024];
    snprintf(target_dir, sizeof(target_dir), "%s/%s", output_dir, topic);

    if (mkdir_p(target_dir) != 0) {
        fprintf(stderr, "Error creating output directory '%s': %s\n",
                target_dir, strerror(errno));
        return 1;
    }

    printf("====================================================\n");
    printf(" Part 1 Client\n");
    printf(" Connecting to: %s:%d\n", server_ip, server_port);
    printf(" Topic: '%s' | Output: '%s'\n", topic, target_dir);
    printf("====================================================\n");

    /*
     * Create a TCP socket and establish a connection with the server.
     */
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(server_port);

    if (inet_pton(AF_INET, server_ip, &serv_addr.sin_addr) <= 0) {
        fprintf(stderr, "Invalid server IP address: %s\n", server_ip);
        close(sockfd);
        return 1;
    }

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("connect");
        close(sockfd);
        return 1;
    }

    printf("[CLIENT] Connected to server.\n");

    /*
     * Begin transfer timing and track total received data for final statistics.
     */
    double t_start = get_time_sec();
    uint64_t total_bytes_received = 0;

    /*
     * Phase 1: Request the selected topic from the server using MSG_GET.
     */
    struct MsgGetPayload get_payload;
    memset(&get_payload, 0, sizeof(get_payload));
    strncpy(get_payload.topic, topic, sizeof(get_payload.topic) - 1);

    struct MsgHeader get_hdr;
    memset(&get_hdr, 0, sizeof(get_hdr));
    get_hdr.magic = PROTOCOL_MAGIC;
    get_hdr.msg_type = MSG_GET;
    get_hdr.payload_len = sizeof(get_payload);

    if (send_msg(sockfd, &get_hdr, &get_payload) != 0) {
        fprintf(stderr, "[CLIENT] Failed to send GET request.\n");
        close(sockfd);
        return 1;
    }

    /*
     * Phase 2: Wait for server confirmation. The server either confirms
     * the topic with MSG_OK or rejects the request with MSG_ERR.
     */
    struct MsgHeader resp_hdr;
    void *resp_payload = NULL;

    int ret = recv_msg(sockfd, &resp_hdr, &resp_payload);
    if (ret <= 0) {
        fprintf(stderr, "[CLIENT] Server disconnected before sending response.\n");
        close(sockfd);
        return 1;
    }

    if (resp_hdr.msg_type == MSG_ERR) {
        struct MsgErrPayload *err_p = (struct MsgErrPayload *)resp_payload;

        fprintf(stderr, "[CLIENT] Server reported ERROR (Code %u): %s\n",
                ntohl(err_p->error_code), err_p->error_msg);

        free(resp_payload);
        close(sockfd);
        return 1;
    }

    if (resp_hdr.msg_type != MSG_OK) {
        fprintf(stderr, "[CLIENT] Unexpected message type: %u\n", resp_hdr.msg_type);

        if (resp_payload) {
            free(resp_payload);
        }

        close(sockfd);
        return 1;
    }

    struct MsgOkHeader *ok_hdr = (struct MsgOkHeader *)resp_payload;
    uint32_t num_files = ntohl(ok_hdr->num_files);

    printf("[CLIENT] Topic '%s' confirmed by server. File count: %u\n",
           topic, num_files);

    free(resp_payload);
    resp_payload = NULL;

    /*
     * Phase 3: Notify the server that the client is ready to receive files.
     */
    struct MsgStartPayload start_p;
    memset(&start_p, 0, sizeof(start_p));
    strncpy(start_p.topic, topic, sizeof(start_p.topic) - 1);

    struct MsgHeader start_hdr;
    memset(&start_hdr, 0, sizeof(start_hdr));
    start_hdr.magic = PROTOCOL_MAGIC;
    start_hdr.msg_type = MSG_START;
    start_hdr.payload_len = sizeof(start_p);

    if (send_msg(sockfd, &start_hdr, &start_p) != 0) {
        fprintf(stderr, "[CLIENT] Failed to send START message.\n");
        close(sockfd);
        return 1;
    }

    /*
     * Phase 4: Receive all files. The server sends a file header first,
     * followed by one or more data chunks for that file.
     */
    int transfer_done = 0;

    while (!transfer_done) {
        struct MsgHeader msg_hdr;
        void *msg_payload = NULL;

        ret = recv_msg(sockfd, &msg_hdr, &msg_payload);

        if (ret <= 0) {
            fprintf(stderr, "[CLIENT] Connection closed unexpectedly during transfer.\n");
            break;
        }

        /*
         * MSG_CLOSE means that every file in the requested topic was sent.
         */
        if (msg_hdr.msg_type == MSG_CLOSE) {
            printf("[CLIENT] Received MSG_CLOSE from server. Transfer complete.\n");

            if (msg_payload) {
                free(msg_payload);
            }

            transfer_done = 1;
            break;
        }

        /*
         * Stop safely if the server reports an error during the transfer.
         */

        if (msg_hdr.msg_type == MSG_ERR) {
            struct MsgErrPayload *err_p = (struct MsgErrPayload *)msg_payload;

            fprintf(stderr, "[CLIENT] Server error during transfer: %s\n",
                    err_p->error_msg);

            free(msg_payload);
            break;
        }

        /*
         * A new file must always begin with a MSG_FILE_HDR message.
         */
        if (msg_hdr.msg_type != MSG_FILE_HDR) {
            fprintf(stderr, "[CLIENT] Protocol error: Expected FILE_HDR, got %u\n",
                    msg_hdr.msg_type);

            if (msg_payload) {
                free(msg_payload);
            }

            break;
        }

        /*
         * Read file metadata, validate the filename, and create the
         * corresponding output file on the client machine.
         */
        struct MsgFileHdrPayload *fhdr_p =
            (struct MsgFileHdrPayload *)msg_payload;

        char filename[MAX_FILENAME_LEN];
        strncpy(filename, fhdr_p->filename, sizeof(filename) - 1);
        filename[sizeof(filename) - 1] = '\0';

        uint64_t file_size = ntoh64(fhdr_p->file_size);

        free(msg_payload);
        msg_payload = NULL;

        if (!is_valid_name(filename)) {
            fprintf(stderr, "[CLIENT] Insecure filename skipped: '%s'\n", filename);
            break;
        }

        char file_path[1024];
        snprintf(file_path, sizeof(file_path), "%s/%s", target_dir, filename);

        FILE *fp = fopen(file_path, "wb");
        if (!fp) {
            fprintf(stderr, "[CLIENT] Cannot create file '%s': %s\n",
                    file_path, strerror(errno));
            break;
        }
        
        printf("[CLIENT] Receiving '%s' (%llu bytes)...\n",
               filename, (unsigned long long)file_size);

        uint64_t file_bytes_received = 0;
        int file_error = 0;

        /*
         * Receive, save, and acknowledge each chunk until the complete
         * current file has been downloaded.
         */
        while (file_bytes_received < file_size) {
            struct MsgHeader data_hdr;
            void *data_payload = NULL;

            ret = recv_msg(sockfd, &data_hdr, &data_payload);

            if (ret <= 0) {
                file_error = 1;
                break;
            }

            if (data_hdr.msg_type != MSG_DATA ||
                data_hdr.payload_len < sizeof(struct MsgDataPayloadHdr)) {
                fprintf(stderr, "[CLIENT] Expected MSG_DATA, got %u\n",
                        data_hdr.msg_type);

                if (data_payload) {
                    free(data_payload);
                }

                file_error = 1;
                break;
            }

            struct MsgDataPayloadHdr *d_hdr =
                (struct MsgDataPayloadHdr *)data_payload;

            uint32_t chunk_size = ntohl(d_hdr->chunk_size);

            char *chunk_data =
                (char *)data_payload + sizeof(struct MsgDataPayloadHdr);

            /*
             * Write the received file bytes to local storage.
             */
            size_t written = fwrite(chunk_data, 1, chunk_size, fp);

            if (written != chunk_size) {
                fprintf(stderr, "[CLIENT] Disk write error on file '%s'\n",
                        filename);

                free(data_payload);
                file_error = 1;
                break;
            }

            file_bytes_received += chunk_size;
            total_bytes_received += chunk_size;

            free(data_payload);

            /*
             * Confirm that this chunk was written successfully. The server
             * waits for this MSG_ACK before sending the next chunk.
             */
            struct MsgAckPayload ack_p;
            memset(&ack_p, 0, sizeof(ack_p));

            strncpy(ack_p.filename, filename, sizeof(ack_p.filename) - 1);
            ack_p.acked_offset = hton64(file_bytes_received);
            ack_p.chunk_size = htonl(chunk_size);
            ack_p.status = htonl(0);

            struct MsgHeader ack_hdr;
            memset(&ack_hdr, 0, sizeof(ack_hdr));

            ack_hdr.magic = PROTOCOL_MAGIC;
            ack_hdr.msg_type = MSG_ACK;
            ack_hdr.payload_len = sizeof(ack_p);

            if (send_msg(sockfd, &ack_hdr, &ack_p) != 0) {
                file_error = 1;
                break;
            }
        }

        /*
         * Close the current file before receiving the next one.
         */
        fclose(fp);

        if (file_error) {
            fprintf(stderr, "[CLIENT] Aborting transfer due to file receive error.\n");
            break;
        }
    }

    /*
     * Clean up the network connection and report the final transfer result.
     */
    close(sockfd);

    double t_end = get_time_sec();
    double elapsed = t_end - t_start;

    if (transfer_done) {
        printf("====================================================\n");
        printf(" Transfer Succeeded!\n");

        print_transfer_stats("CLIENT", total_bytes_received, elapsed);

        printf("====================================================\n");
        return 0;
    }

    fprintf(stderr, "[CLIENT] Transfer failed or was incomplete.\n");
    return 1;
}