#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <stddef.h>

#define PROTOCOL_MAGIC 0x49543330  /* "IT30" */

/* Message Types */
enum MsgType {
    MSG_GET          = 1,   /* Client -> Server: Request topic */
    MSG_OK           = 2,   /* Server -> Client: Metadata & session_id */
    MSG_ERR          = 3,   /* Server -> Client: Error report */
    MSG_START        = 4,   /* Client -> Server: Ready */
    MSG_FILE_HDR     = 5,   /* Server -> Client: File metadata */
    MSG_DATA         = 6,   /* Server -> Client: File data chunk */
    MSG_ACK          = 7,   /* Client -> Server: Chunk acknowledgment */
    MSG_CLOSE        = 8,   /* Server -> Client: All transfers complete */
    MSG_WORKER_JOIN  = 9,   /* Client Worker -> Server: Join parallel session */
    MSG_WORKER_CLOSE = 10,  /* Server -> Client Worker: No more chunks */
    MSG_PULL_CHUNK   = 11   /* Client Worker -> Server: Request next available chunk */
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
    ERR_SESSION_NOT_FOUND   = 7,
    ERR_INTERNAL            = 8
};

#define MAX_TOPIC_NAME   128
#define MAX_FILENAME_LEN 256
#define CHUNK_SIZE       (32 * 1024)  /* 32 KB chunk size */
#define DEFAULT_PARALLEL_CONNECTIONS 4

#pragma pack(push, 1)

/* Common Binary Header (28 bytes) */
struct MsgHeader {
    uint32_t magic;          /* PROTOCOL_MAGIC */
    uint16_t msg_type;       /* enum MsgType */
    uint16_t status_flags;   /* Flags */
    uint64_t session_id;     /* Session ID */
    uint64_t transaction_id; /* Transaction / Chunk sequence ID */
    uint32_t payload_len;    /* Payload byte count */
};

/* Payload: MSG_GET */
struct MsgGetPayload {
    char topic[MAX_TOPIC_NAME];
    uint64_t session_id;
};

/* Payload: Entry inside MSG_OK */
struct FileInfoEntry {
    char filename[MAX_FILENAME_LEN];
    uint64_t file_size;
    uint64_t start_offset;
};

/* Payload: MSG_OK */
struct MsgOkHeader {
    uint32_t num_files;
    uint64_t session_id;
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
    uint64_t session_id;
    uint32_t num_workers;
};

/* Payload: MSG_WORKER_JOIN */
struct MsgWorkerJoinPayload {
    uint64_t session_id;
    uint32_t worker_id;
};

/* Payload: MSG_DATA Header */
struct MsgDataPayloadHdr {
    char filename[MAX_FILENAME_LEN];
    uint64_t offset;
    uint32_t chunk_size;
    uint32_t chunk_id;
};

/* Payload: MSG_ACK */
struct MsgAckPayload {
    char filename[MAX_FILENAME_LEN];
    uint64_t acked_offset;
    uint32_t chunk_size;
    uint32_t chunk_id;
    uint32_t status;
};

/* Payload: MSG_CLOSE */
struct MsgClosePayload {
    char topic[MAX_TOPIC_NAME];
    uint64_t session_id;
    uint32_t status_code;
    uint64_t total_bytes_sent;
};

#pragma pack(pop)

#endif /* PROTOCOL_H */
