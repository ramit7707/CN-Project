#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <stddef.h>

#define PROTOCOL_MAGIC 0x49543330  /* "IT30" */

/* Message Types */
enum MsgType {
    MSG_GET       = 1,   /* Client -> Server: Request topic */
    MSG_OK        = 2,   /* Server -> Client: Topic metadata / file list */
    MSG_ERR       = 3,   /* Server -> Client: Error report */
    MSG_START     = 4,   /* Client -> Server: Ready to receive data */
    MSG_FILE_HDR  = 5,   /* Server -> Client: Metadata for upcoming file */
    MSG_DATA      = 6,   /* Server -> Client: File data chunk */
    MSG_ACK       = 7,   /* Client -> Server: Acknowledgment */
    MSG_CLOSE     = 8    /* Server -> Client: Topic transfer complete */
};

/* Error Codes */
enum ErrCode {
    ERR_OK                  = 0,
    ERR_TOPIC_NOT_FOUND     = 1,
    ERR_PATH_TRAVERSAL      = 2,
    ERR_INVALID_REQUEST     = 3,
    ERR_INVALID_START       = 4,
    ERR_FILE_IO             = 5,
    ERR_EMPTY_TOPIC         = 6,
    ERR_INTERNAL            = 7
};

#define MAX_TOPIC_NAME  128
#define MAX_FILENAME_LEN 256
#define CHUNK_SIZE      (32 * 1024)  /* 32 KB chunk size */

#pragma pack(push, 1)

/* Common Binary Header (28 bytes) */
struct MsgHeader {
    uint32_t magic;          /* PROTOCOL_MAGIC */
    uint16_t msg_type;       /* enum MsgType */
    uint16_t status_flags;   /* Optional status or flags */
    uint64_t session_id;     /* Session ID (0 for Part 1) */
    uint64_t transaction_id; /* Sequence or transaction counter */
    uint32_t payload_len;    /* Byte count of following payload */
};

/* Payload: MSG_GET */
struct MsgGetPayload {
    char topic[MAX_TOPIC_NAME];
};

/* Payload: Entry inside MSG_OK */
struct FileInfoEntry {
    char filename[MAX_FILENAME_LEN];
    uint64_t file_size;
};

/* Payload: MSG_OK */
struct MsgOkHeader {
    uint32_t num_files;
    char topic[MAX_TOPIC_NAME];
};

/* Payload: MSG_ERR */
struct MsgErrPayload {
    uint32_t error_code;
    char error_msg[256];
};

/* Payload: MSG_START */
struct MsgStartPayload {
    char topic[MAX_TOPIC_NAME];
};

/* Payload: MSG_FILE_HDR */
struct MsgFileHdrPayload {
    char filename[MAX_FILENAME_LEN];
    uint64_t file_size;
    uint64_t start_offset;
    uint32_t mode;
};

/* Payload: MSG_DATA Header */
struct MsgDataPayloadHdr {
    char filename[MAX_FILENAME_LEN];
    uint64_t offset;
    uint32_t chunk_size;
    /* Followed immediately by chunk_size raw bytes */
};

/* Payload: MSG_ACK */
struct MsgAckPayload {
    char filename[MAX_FILENAME_LEN];
    uint64_t acked_offset;
    uint32_t chunk_size;
    uint32_t status;
};

/* Payload: MSG_CLOSE */
struct MsgClosePayload {
    char topic[MAX_TOPIC_NAME];
    uint32_t status_code;
    uint64_t total_bytes_sent;
};

#pragma pack(pop)

#endif /* PROTOCOL_H */
