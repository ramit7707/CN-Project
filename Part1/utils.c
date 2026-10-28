#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

#include "utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <dirent.h>
#include <arpa/inet.h>

#define MAX_REASONABLE_PAYLOAD (16 * 1024 * 1024)

uint64_t hton64(uint64_t val) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return (((uint64_t)htonl((uint32_t)(val & 0xFFFFFFFFULL))) << 32) |
           htonl((uint32_t)(val >> 32));
#else
    return val;
#endif
}

uint64_t ntoh64(uint64_t val) {
    return hton64(val);
}

int send_all(int sockfd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t sent = send(sockfd, p, remaining, 0);
        if (sent < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (sent == 0) return -1;
        p += sent;
        remaining -= (size_t)sent;
    }
    return 0;
}

int recv_all(int sockfd, void *buf, size_t len) {
    char *p = (char *)buf;
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t r = recv(sockfd, p, remaining, 0);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) {
            /* Peer closed connection */
            return (remaining == len) ? 0 : -1;
        }
        p += r;
        remaining -= (size_t)r;
    }
    return (int)len;
}

int send_msg(int sockfd, const struct MsgHeader *hdr, const void *payload) {
    struct MsgHeader net_hdr;
    net_hdr.magic = htonl(hdr->magic);
    net_hdr.msg_type = htons(hdr->msg_type);
    net_hdr.status_flags = htons(hdr->status_flags);
    net_hdr.session_id = hton64(hdr->session_id);
    net_hdr.transaction_id = hton64(hdr->transaction_id);
    net_hdr.payload_len = htonl(hdr->payload_len);

    if (send_all(sockfd, &net_hdr, sizeof(net_hdr)) != 0) {
        return -1;
    }
    if (hdr->payload_len > 0 && payload != NULL) {
        if (send_all(sockfd, payload, hdr->payload_len) != 0) {
            return -1;
        }
    }
    return 0;
}

int recv_msg(int sockfd, struct MsgHeader *hdr, void **payload_out) {
    struct MsgHeader net_hdr;
    int ret = recv_all(sockfd, &net_hdr, sizeof(net_hdr));
    if (ret <= 0) {
        if (payload_out) *payload_out = NULL;
        return ret; /* 0 for EOF, -1 for error */
    }

    hdr->magic = ntohl(net_hdr.magic);
    hdr->msg_type = ntohs(net_hdr.msg_type);
    hdr->status_flags = ntohs(net_hdr.status_flags);
    hdr->session_id = ntoh64(net_hdr.session_id);
    hdr->transaction_id = ntoh64(net_hdr.transaction_id);
    hdr->payload_len = ntohl(net_hdr.payload_len);

    if (hdr->magic != PROTOCOL_MAGIC) {
        fprintf(stderr, "[ERROR] Corrupt message header magic: 0x%08x\n", hdr->magic);
        if (payload_out) *payload_out = NULL;
        return -1;
    }

    if (hdr->payload_len > MAX_REASONABLE_PAYLOAD) {
        fprintf(stderr, "[ERROR] Payload too large: %u bytes\n", hdr->payload_len);
        if (payload_out) *payload_out = NULL;
        return -1;
    }

    if (hdr->payload_len > 0) {
        void *buf = malloc(hdr->payload_len + 1);
        if (!buf) {
            fprintf(stderr, "[ERROR] Out of memory allocating %u bytes\n", hdr->payload_len);
            if (payload_out) *payload_out = NULL;
            return -1;
        }
        int r = recv_all(sockfd, buf, hdr->payload_len);
        if (r <= 0) {
            free(buf);
            if (payload_out) *payload_out = NULL;
            return -1;
        }
        ((char *)buf)[hdr->payload_len] = '\0';
        if (payload_out) *payload_out = buf;
        else free(buf);
    } else {
        if (payload_out) *payload_out = NULL;
    }

    return 1;
}

int is_valid_name(const char *name) {
    if (!name || strlen(name) == 0 || strlen(name) >= 128) return 0;
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) return 0;
    if (name[0] == '.') return 0; /* No hidden files */

    for (size_t i = 0; name[i] != '\0'; i++) {
        char c = name[i];
        if (c == '/' || c == '\\') return 0;
        if (!isalnum((unsigned char)c) && c != '_' && c != '-' && c != '.') {
            return 0;
        }
    }
    return 1;
}

int mkdir_p(const char *path) {
    char tmp[512];
    snprintf(tmp, sizeof(tmp), "%s", path);
    size_t len = strlen(tmp);
    if (len == 0) return 0;
    if (tmp[len - 1] == '/') tmp[len - 1] = '\0';

    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST) {
                return -1;
            }
            *p = '/';
        }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) {
        return -1;
    }
    return 0;
}

int remove_dir_recursive(const char *path) {
    DIR *d = opendir(path);
    if (!d) {
        if (errno == ENOENT) return 0;
        return -1;
    }
    struct dirent *entry;
    while ((entry = readdir(d)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;
        char subpath[1024];
        snprintf(subpath, sizeof(subpath), "%s/%s", path, entry->d_name);
        struct stat st;
        if (stat(subpath, &st) == 0) {
            if (S_ISDIR(st.st_mode)) {
                remove_dir_recursive(subpath);
            } else {
                unlink(subpath);
            }
        }
    }
    closedir(d);
    return rmdir(path);
}

double get_time_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

void print_transfer_stats(const char *label, uint64_t bytes, double elapsed_sec) {
    double mb = (double)bytes / (1024.0 * 1024.0);
    double mbps = (elapsed_sec > 0.0) ? (mb / elapsed_sec) : 0.0;
    printf("[%s] Transferred: %llu bytes (%.2f MB) in %.4f seconds (Throughput: %.2f MB/s)\n",
           label, (unsigned long long)bytes, mb, elapsed_sec, mbps);
}
