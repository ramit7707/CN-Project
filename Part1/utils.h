#ifndef UTILS_H
#define UTILS_H

#include "protocol.h"
#include <stdint.h>
#include <stddef.h>

/* 64-bit Network Byte Order conversion */
uint64_t hton64(uint64_t val);
uint64_t ntoh64(uint64_t val);

/* Network transmission helpers */
int send_all(int sockfd, const void *buf, size_t len);
int recv_all(int sockfd, void *buf, size_t len);

/* Framed message sending & receiving */
int send_msg(int sockfd, const struct MsgHeader *hdr, const void *payload);
int recv_msg(int sockfd, struct MsgHeader *hdr, void **payload_out);

/* Security: path traversal check */
int is_valid_name(const char *name);

/* Filesystem helpers */
int mkdir_p(const char *path);
int remove_dir_recursive(const char *path);

/* High-precision timing */
double get_time_sec(void);

/* Formatted stats printing */
void print_transfer_stats(const char *label, uint64_t bytes, double elapsed_sec);

#endif /* UTILS_H */
