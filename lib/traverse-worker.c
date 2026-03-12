#include "traverse-worker.h"

#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "fts/fts.h"

typedef enum RmTravWorkerCmd {
    RM_TRAV_WORKER_CMD_START = 1,
    RM_TRAV_WORKER_CMD_NEXT = 2,
    RM_TRAV_WORKER_CMD_GET_DEV = 3
} RmTravWorkerCmd;

typedef enum RmTravWorkerRespKind {
    RM_TRAV_WORKER_RESP_ENTRY = 1,
    RM_TRAV_WORKER_RESP_EOF = 2,
    RM_TRAV_WORKER_RESP_ERROR = 3,
    RM_TRAV_WORKER_RESP_DEV = 4
} RmTravWorkerRespKind;

typedef struct RmTravWorkerCmdWire {
    uint32_t type;
    int32_t action;
    int32_t fts_flags;
    uint32_t path_len;
} RmTravWorkerCmdWire;

struct RmTravWorker {
    pid_t pid;
    int to_worker_fd;
    int from_worker_fd;
};

/* Defensive IPC cap: reject absurd string payload sizes in worker messages. */
#define RM_TRAV_WORKER_MAX_WIRE_STRLEN (16u * 1024u * 1024u)

/* Keep one persistent traversal worker per thread to avoid per-directory fork overhead. */
static void rm_trav_worker_destroy(gpointer data);
static GPrivate rm_trav_worker_key = G_PRIVATE_INIT(rm_trav_worker_destroy);

/* Write exactly size bytes unless a hard I/O failure occurs. */
static bool rm_trav_write_full(int fd, const void *buf, size_t size) {
    const uint8_t *data = buf;
    size_t written = 0;
    while(written < size) {
        ssize_t rc = write(fd, data + written, size - written);
        if(rc < 0) {
            if(errno == EINTR) { continue; }
            return false;
        }
        if(rc == 0) { return false; }
        written += (size_t)rc;
    }
    return true;
}

/* Read exactly size bytes unless EOF or a hard I/O failure occurs. */
static bool rm_trav_read_full(int fd, void *buf, size_t size) {
    uint8_t *data = buf;
    size_t read_bytes = 0;
    while(read_bytes < size) {
        ssize_t rc = read(fd, data + read_bytes, size - read_bytes);
        if(rc < 0) {
            if(errno == EINTR) { continue; }
            return false;
        }
        if(rc == 0) { return false; }
        read_bytes += (size_t)rc;
    }
    return true;
}

/* Poll once with an overall timeout budget that survives EINTR retries. */
static int rm_trav_poll_with_budget(int fd, gint timeout_s) {
    gint64 deadline_us = g_get_monotonic_time() + ((gint64)timeout_s*1000000);

    while(true) {
        gint64 now_us = g_get_monotonic_time();
        gint64 remaining_us = deadline_us - now_us;
        if(remaining_us <= 0) {
            return 0;
        }

        int remaining_ms = (int)((remaining_us + 999) / 1000);
        struct pollfd pfd = {.fd = fd, .events = POLLIN, .revents = 0};
        int rc = poll(&pfd, 1, remaining_ms);
        if(rc < 0 && errno == EINTR) {
            continue;
        }
        return rc;
    }
}

/* Release per-response heap fields so response buffers can be safely reused. */
void rm_trav_worker_resp_clear(RmTravWorkerResp *resp) {
    g_free(resp->path);
    g_free(resp->name);
    memset(resp, 0, sizeof(*resp));
}

/* Send one command message, plus optional path payload for START commands. */
static bool rm_trav_worker_send_cmd(RmTravWorker *worker, const RmTravWorkerCmdWire *cmd,
                                    const char *path) {
    if(!rm_trav_write_full(worker->to_worker_fd, cmd, sizeof(*cmd))) {
        return false;
    }
    if(cmd->path_len > 0 && path != NULL) {
        return rm_trav_write_full(worker->to_worker_fd, path, cmd->path_len);
    }
    return true;
}

/* Read one full response message and materialize variable-length strings. */
static bool rm_trav_worker_read_resp(RmTravWorker *worker, RmTravWorkerResp *resp) {
    rm_trav_worker_resp_clear(resp);
    if(!rm_trav_read_full(worker->from_worker_fd, &resp->wire, sizeof(resp->wire))) {
        return false;
    }
    if(resp->wire.path_len > RM_TRAV_WORKER_MAX_WIRE_STRLEN ||
       resp->wire.name_len > RM_TRAV_WORKER_MAX_WIRE_STRLEN) {
        return false;
    }

    if(resp->wire.path_len > 0) {
        resp->path = g_malloc0(resp->wire.path_len);
        if(!rm_trav_read_full(worker->from_worker_fd, resp->path, resp->wire.path_len)) {
            rm_trav_worker_resp_clear(resp);
            return false;
        }
    }

    if(resp->wire.name_len > 0) {
        resp->name = g_malloc0(resp->wire.name_len);
        if(!rm_trav_read_full(worker->from_worker_fd, resp->name, resp->wire.name_len)) {
            rm_trav_worker_resp_clear(resp);
            return false;
        }
    }

    return true;
}

/* Terminate worker process and close both pipe endpoints. */
void rm_trav_worker_kill(RmTravWorker *worker) {
    if(worker->pid > 0) {
        kill(worker->pid, SIGTERM);
        /* Give the worker a short grace window before forcing SIGKILL. */
        usleep(20 * 1000);
        if(waitpid(worker->pid, NULL, WNOHANG) == 0) {
            kill(worker->pid, SIGKILL);
            waitpid(worker->pid, NULL, 0);
        } else {
            waitpid(worker->pid, NULL, 0);
        }
    }

    if(worker->to_worker_fd >= 0) {
        close(worker->to_worker_fd);
    }
    if(worker->from_worker_fd >= 0) {
        close(worker->from_worker_fd);
    }

    worker->pid = -1;
    worker->to_worker_fd = -1;
    worker->from_worker_fd = -1;
}

/* Worker main loop: receive commands, run fts_*, and stream serialized results. */
static void rm_trav_worker_loop(int read_fd, int write_fd) {
    /* Worker side of the drop-in fts_read replacement: execute fts_* and stream entries. */
    FTS *ftsp = NULL;
    FTSENT *last = NULL;

    while(true) {
        RmTravWorkerCmdWire cmd = {0};
        if(!rm_trav_read_full(read_fd, &cmd, sizeof(cmd))) {
            break;
        }

        if(cmd.type == RM_TRAV_WORKER_CMD_START) {
            char *path = NULL;
            if(cmd.path_len > RM_TRAV_WORKER_MAX_WIRE_STRLEN) {
                RmTravWorkerRespWire resp = {.kind = RM_TRAV_WORKER_RESP_ERROR,
                                             .err = EINVAL};
                rm_trav_write_full(write_fd, &resp, sizeof(resp));
                continue;
            }
            if(cmd.path_len > 0) {
                path = g_malloc0(cmd.path_len);
                if(!rm_trav_read_full(read_fd, path, cmd.path_len)) {
                    g_free(path);
                    _exit(0);
                }
            }

            if(ftsp != NULL) {
                fts_close(ftsp);
                ftsp = NULL;
                last = NULL;
            }

            if(path == NULL || cmd.path_len == 0) {
                RmTravWorkerRespWire resp = {.kind = RM_TRAV_WORKER_RESP_ERROR,
                                             .err = EINVAL};
                rm_trav_write_full(write_fd, &resp, sizeof(resp));
                g_free(path);
                continue;
            }

            ftsp = fts_open((const char *const[2]){path, NULL}, cmd.fts_flags, NULL);
            g_free(path);
            if(ftsp == NULL) {
                RmTravWorkerRespWire resp = {.kind = RM_TRAV_WORKER_RESP_ERROR,
                                             .err = errno};
                rm_trav_write_full(write_fd, &resp, sizeof(resp));
            }
            continue;
        }

        if(cmd.type == RM_TRAV_WORKER_CMD_GET_DEV) {
            RmTravWorkerRespWire resp = {0};
            if(ftsp == NULL) {
                resp.kind = RM_TRAV_WORKER_RESP_ERROR;
                resp.err = EINVAL;
            } else {
                errno = 0;
                FTSENT *chp = fts_children(ftsp, 0);
                if(chp == NULL) {
                    resp.kind = RM_TRAV_WORKER_RESP_ERROR;
                    resp.err = (errno != 0) ? errno : ENOENT;
                } else {
                    resp.kind = RM_TRAV_WORKER_RESP_DEV;
                    resp.fts_dev = (uint64_t)chp->fts_dev;
                }
            }
            if(!rm_trav_write_full(write_fd, &resp, sizeof(resp))) {
                _exit(0);
            }
            continue;
        }

        if(cmd.type != RM_TRAV_WORKER_CMD_NEXT || ftsp == NULL) {
            RmTravWorkerRespWire resp = {.kind = RM_TRAV_WORKER_RESP_ERROR, .err = EINVAL};
            rm_trav_write_full(write_fd, &resp, sizeof(resp));
            continue;
        }

        if(last != NULL) {
            /* Mirror parent-side fts_set requests against the last emitted entry. */
            if(cmd.action == RM_TRAV_WORKER_ACTION_SKIP) {
                fts_set(ftsp, last, FTS_SKIP);
            } else if(cmd.action == RM_TRAV_WORKER_ACTION_FOLLOW) {
                fts_set(ftsp, last, FTS_FOLLOW);
            }
        }

        errno = 0;
        FTSENT *ent = fts_read(ftsp);
        if(ent == NULL) {
            RmTravWorkerRespWire resp = {0};
            if(errno != 0) {
                resp.kind = RM_TRAV_WORKER_RESP_ERROR;
                resp.err = errno;
                resp.path_len = (uint32_t)(strlen(ftsp->fts_path) + 1);
            } else {
                resp.kind = RM_TRAV_WORKER_RESP_EOF;
            }

            if(!rm_trav_write_full(write_fd, &resp, sizeof(resp))) {
                _exit(0);
            }
            if(resp.path_len > 0 &&
               !rm_trav_write_full(write_fd, ftsp->fts_path, resp.path_len)) {
                _exit(0);
            }

            fts_close(ftsp);
            ftsp = NULL;
            last = NULL;
            continue;
        }

        RmTravWorkerRespWire resp = {.kind = RM_TRAV_WORKER_RESP_ENTRY,
                                     .fts_info = ent->fts_info,
                                     .fts_level = ent->fts_level,
                                     .fts_errno = ent->fts_errno,
                                     .fts_dev = ent->fts_dev,
                                     .path_len = (uint32_t)(strlen(ent->fts_path) + 1),
                                     .name_len = (uint32_t)(strlen(ent->fts_name) + 1)};

        if(ent->fts_statp != NULL && ent->fts_info != FTS_NS &&
           ent->fts_info != FTS_NSOK) {
            resp.has_stat = 1;
            resp.stat_buf = *ent->fts_statp;
        }

        if(!rm_trav_write_full(write_fd, &resp, sizeof(resp)) ||
           !rm_trav_write_full(write_fd, ent->fts_path, resp.path_len) ||
           !rm_trav_write_full(write_fd, ent->fts_name, resp.name_len)) {
            _exit(0);
        }

        last = ent;
    }

    if(ftsp != NULL) {
        fts_close(ftsp);
    }
    _exit(0);
}

/* Fork a worker process and connect request/response pipes. */
static bool rm_trav_worker_spawn(RmTravWorker *worker) {
    int to_worker[2] = {-1, -1};
    int from_worker[2] = {-1, -1};

    if(pipe(to_worker) != 0 || pipe(from_worker) != 0) {
        if(to_worker[0] >= 0) {
            close(to_worker[0]);
            close(to_worker[1]);
        }
        if(from_worker[0] >= 0) {
            close(from_worker[0]);
            close(from_worker[1]);
        }
        return false;
    }

    pid_t pid = fork();
    if(pid < 0) {
        close(to_worker[0]);
        close(to_worker[1]);
        close(from_worker[0]);
        close(from_worker[1]);
        return false;
    }

    if(pid == 0) {
        close(to_worker[1]);
        close(from_worker[0]);
        rm_trav_worker_loop(to_worker[0], from_worker[1]);
        _exit(0);
    }

    close(to_worker[0]);
    close(from_worker[1]);
    worker->pid = pid;
    worker->to_worker_fd = to_worker[1];
    worker->from_worker_fd = from_worker[0];
    return true;
}

/* Return the current thread's persistent worker handle, creating it if needed. */
RmTravWorker *rm_trav_worker_get(void) {
    RmTravWorker *worker = g_private_get(&rm_trav_worker_key);
    if(worker == NULL) {
        worker = g_new0(RmTravWorker, 1);
        worker->pid = -1;
        worker->to_worker_fd = -1;
        worker->from_worker_fd = -1;
        g_private_replace(&rm_trav_worker_key, worker);
    }
    return worker;
}

/* Reuse a live worker or transparently recreate one that exited. */
bool rm_trav_worker_ensure(RmTravWorker *worker) {
    if(worker->pid > 0) {
        int status = 0;
        pid_t rc = waitpid(worker->pid, &status, WNOHANG);
        if(rc == 0) {
            return true;
        }
        rm_trav_worker_kill(worker);
    }
    return rm_trav_worker_spawn(worker);
}

/* Start traversal of a root path and detect immediate startup errors. */
bool rm_trav_worker_start_path(RmTravWorker *worker, const char *path,
                               int fts_flags) {
    RmTravWorkerCmdWire cmd = {.type = RM_TRAV_WORKER_CMD_START,
                               .fts_flags = fts_flags,
                               .path_len = (uint32_t)(strlen(path) + 1)};
    if(!rm_trav_worker_send_cmd(worker, &cmd, path)) {
        return false;
    }

    /* Probe only for immediate startup failures; keep the common path non-blocking. */
    struct pollfd pfd = {.fd = worker->from_worker_fd, .events = POLLIN, .revents = 0};
    int rc;
    do {
        rc = poll(&pfd, 1, 0);
    } while(rc < 0 && errno == EINTR);

    if(rc == 0) {
        // timeout
        return true;
    }
    if(rc < 0) {
        // error
        return false;
    }

    RmTravWorkerResp resp = {0};
    bool ok = rm_trav_worker_read_resp(worker, &resp) &&
              resp.wire.kind != RM_TRAV_WORKER_RESP_ERROR;
    rm_trav_worker_resp_clear(&resp);
    return ok;
}

/* Query first root child device from worker-side fts state with poll timeout. */
bool rm_trav_worker_get_dev(RmTravWorker *worker, gint timeout, dev_t *dev) {
    RmTravWorkerCmdWire cmd = {.type = RM_TRAV_WORKER_CMD_GET_DEV};
    if(!rm_trav_worker_send_cmd(worker, &cmd, NULL)) {
        return false;
    }

    int rc = rm_trav_poll_with_budget(worker->from_worker_fd, timeout*1000);
    if(rc == 0) {
        errno = ETIMEDOUT;
        return false;
    }
    if(rc < 0) {
        return false;
    }

    RmTravWorkerResp resp = {0};
    bool ok = rm_trav_worker_read_resp(worker, &resp) &&
              resp.wire.kind == RM_TRAV_WORKER_RESP_DEV;
    if(ok) {
        *dev = (dev_t)resp.wire.fts_dev;
    } else if(resp.wire.kind == RM_TRAV_WORKER_RESP_ERROR && resp.wire.err != 0) {
        errno = resp.wire.err;
    }
    rm_trav_worker_resp_clear(&resp);
    return ok;
}

/* Request next traversal entry with timeout while waiting for worker I/O. */
RmTravWorkerReadResult rm_trav_worker_next(RmTravWorker *worker,
                                           RmTravWorkerAction action,
                                           gint timeout_s,
                                           RmTravWorkerResp *resp) {
    RmTravWorkerCmdWire cmd = {.type = RM_TRAV_WORKER_CMD_NEXT, .action = action};
    if(!rm_trav_worker_send_cmd(worker, &cmd, NULL)) {
        return RM_TRAV_WORKER_READ_IOFAIL;
    }

    /* Timeout is applied while waiting for worker I/O, not while processing an entry. */
    int rc = rm_trav_poll_with_budget(worker->from_worker_fd, timeout_s*1000);
    if(rc == 0) {
        return RM_TRAV_WORKER_READ_TIMEOUT;
    }
    if(rc < 0) {
        return RM_TRAV_WORKER_READ_IOFAIL;
    }

    if(!rm_trav_worker_read_resp(worker, resp)) {
        return RM_TRAV_WORKER_READ_IOFAIL;
    }

    if(resp->wire.kind == RM_TRAV_WORKER_RESP_ENTRY) {
        return RM_TRAV_WORKER_READ_ENTRY;
    }
    if(resp->wire.kind == RM_TRAV_WORKER_RESP_EOF) {
        return RM_TRAV_WORKER_READ_EOF;
    }
    return RM_TRAV_WORKER_READ_ERROR;
}

/* GLib thread-local destructor for the per-thread worker handle. */
static void rm_trav_worker_destroy(gpointer data) {
    RmTravWorker *worker = data;
    if(worker != NULL) {
        rm_trav_worker_kill(worker);
        g_free(worker);
    }
}
