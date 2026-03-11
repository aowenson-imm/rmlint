#ifndef RM_TRAVERSE_WORKER_H
#define RM_TRAVERSE_WORKER_H

#include <stdbool.h>
#include <stdint.h>
#include <sys/stat.h>

#include <glib.h>

typedef enum RmTravWorkerAction {
    RM_TRAV_WORKER_ACTION_NONE = 0,
    RM_TRAV_WORKER_ACTION_SKIP = 1,
    RM_TRAV_WORKER_ACTION_FOLLOW = 2
} RmTravWorkerAction;

typedef enum RmTravWorkerReadResult {
    RM_TRAV_WORKER_READ_ENTRY = 1,
    RM_TRAV_WORKER_READ_EOF = 2,
    RM_TRAV_WORKER_READ_ERROR = 3,
    RM_TRAV_WORKER_READ_TIMEOUT = 4,
    RM_TRAV_WORKER_READ_IOFAIL = 5
} RmTravWorkerReadResult;

typedef struct RmTravWorkerRespWire {
    uint32_t kind;
    int32_t err;
    int32_t fts_info;
    int32_t fts_level;
    int32_t fts_errno;
    uint64_t fts_dev;
    uint32_t has_stat;
    uint32_t path_len;
    uint32_t name_len;
    struct stat stat_buf;
} RmTravWorkerRespWire;

typedef struct RmTravWorkerResp {
    RmTravWorkerRespWire wire;
    char *path;
    char *name;
} RmTravWorkerResp;

typedef struct RmTravWorker RmTravWorker;

RmTravWorker *rm_trav_worker_get(void);
bool rm_trav_worker_ensure(RmTravWorker *worker);
bool rm_trav_worker_start_path(RmTravWorker *worker, const char *path, int fts_flags);
RmTravWorkerReadResult rm_trav_worker_next(RmTravWorker *worker,
                                           RmTravWorkerAction action,
                                           gint timeout_ms,
                                           RmTravWorkerResp *resp);
void rm_trav_worker_kill(RmTravWorker *worker);
void rm_trav_worker_resp_clear(RmTravWorkerResp *resp);

#endif
