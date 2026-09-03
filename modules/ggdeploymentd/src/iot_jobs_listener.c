// aws-greengrass-lite - AWS IoT Greengrass runtime for constrained devices
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
// cspell:ignore dedup enqueuable unsynchronized

#include "iot_jobs_listener.h"
#include "bootstrap_manager.h"
#include "deployment_model.h"
#include "deployment_queue.h"
#include "status_keeper.h"
#include <assert.h>
#include <gg/arena.h>
#include <gg/backoff.h>
#include <gg/buffer.h>
#include <gg/cleanup.h>
#include <gg/error.h>
#include <gg/flags.h>
#include <gg/log.h>
#include <gg/map.h>
#include <gg/object.h>
#include <gg/vector.h>
#include <ggl/aws_iot_call.h>
#include <ggl/core_bus/aws_iot_mqtt.h>
#include <ggl/core_bus/client.h>
#include <ggl/core_bus/constants.h>
#include <ggl/core_bus/gg_config.h>
#include <inttypes.h>
#include <pthread.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdnoreturn.h>

struct timespec;

#define MAX_THING_NAME_LEN 128
#define UPDATE_JOB_RESPONSE_OBJECT_SIZE 1024
#define DESCRIBE_JOB_RESPONSE_OBJECT_SIZE 4096

// Deep ownership preserves the existing decoded-object capacity and may copy
// every response byte from the bounded core-bus dispatch buffer. Update may be
// called while a describe response is live, so these arenas can consume about
// 25 KiB together on the listener call chain.
#define UPDATE_JOB_RESPONSE_ARENA_SIZE \
    (UPDATE_JOB_RESPONSE_OBJECT_SIZE + GGL_COREBUS_MAX_MSG_LEN)
#define DESCRIBE_JOB_RESPONSE_ARENA_SIZE \
    (DESCRIBE_JOB_RESPONSE_OBJECT_SIZE + GGL_COREBUS_MAX_MSG_LEN)

// Re-publish a persisted status at most this often while one is pending, so a
// publish that failed without an MQTT disconnect still recovers.
#define STATUS_FLUSH_RETRY_SECONDS 60

// Bounded delay before the listener thread retries a failed describe or
// subscription, or re-attempts a job that hit a full deployment queue. Keeps
// the listener from tight-spinning on a persistent failure while still letting
// a pending status flush run between attempts.
#define LISTENER_RETRY_DELAY_SECONDS 10

typedef enum QualityOfService {
    QOS_FIRE_AND_FORGET = 0,
    QOS_AT_LEAST_ONCE = 1,
    QOS_EXACTLY_ONCE = 2
} QoS;

typedef enum DeploymentStatusAction {
    DSA_DO_NOTHING = 0,
    DSA_ENQUEUE_JOB = 1,
    DSA_CANCEL_JOB = 2,
} DeploymentStatusAction;

// format strings for greengrass deployment job topic filters
#define THINGS_TOPIC_PREFIX "$aws/things/"
#define JOBS_TOPIC_PREFIX "/jobs/"
#define JOBS_UPDATE_TOPIC "/namespace-aws-gg-deployment/update"
#define JOBS_GET_TOPIC "/namespace-aws-gg-deployment/get"
#define NEXT_JOB_EXECUTION_CHANGED_TOPIC \
    "/jobs/notify-next-namespace-aws-gg-deployment"

#define NEXT_JOB_LITERAL "$next"

static pthread_mutex_t thing_name_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint8_t thing_name_mem[MAX_THING_NAME_LEN];
static GgBuffer thing_name_buf;

// Initialization ordering only. This mutex is never held while acquiring the
// thing-name, primary-status, listener-work, or current-job mutexes.
static pthread_mutex_t listener_ready_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t listener_ready_cond = PTHREAD_COND_INITIALIZER;
static bool listener_ready;

static pthread_mutex_t current_job_id_mutex = PTHREAD_MUTEX_INITIALIZER;
// Serializes the primary-socket pending-status read/publish/finalize
// transaction. Listener callbacks never acquire this mutex, and listener work
// never holds listener_mutex while acquiring it.
static pthread_mutex_t primary_status_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint8_t current_job_id_buf[64];
static GgByteVec current_job_id;
static uint8_t current_deployment_id_buf[64];
static GgByteVec current_deployment_id;
static uint8_t last_queue_job_id_buf[64];
static GgByteVec last_queue_job_id;
static int64_t last_queue_at;
static pthread_cond_t bootstrap_scan_cond = PTHREAD_COND_INITIALIZER;
static bool bootstrap_scan_complete;

static pthread_mutex_t listener_mutex = PTHREAD_MUTEX_INITIALIZER;
// listener_cond uses CLOCK_MONOTONIC so the flush retry timeout is immune to
// wall-clock changes. It is initialized exactly once via listener_cond_once:
// both the listener thread (before waiting) and update_job_to on the
// deployment-handler thread (before signaling) run pthread_once, since either
// may reach the cond first after startup.
static pthread_cond_t listener_cond;
static pthread_once_t listener_cond_once = PTHREAD_ONCE_INIT;
// Bounded work the listener thread owns. Callbacks only set these flags (never
// do the work themselves). Each *_retry_after is a CLOCK_MONOTONIC deadline
// gating a RE-armed item after a failure; a zeroed deadline means "eligible
// now", so a freshly-armed item runs immediately.
static bool needs_describe = false;
// Explicit flush request (an MQTT reconnect). Runs on the next capture pass
// regardless of the periodic deadline below, so a re-established connection
// re-sends a pending status immediately.
static bool needs_flush = false;
static bool needs_jobs_subscribe = false;
static bool needs_connection_subscribe = false;
static struct timespec describe_retry_after;
static struct timespec jobs_subscribe_retry_after;
static struct timespec connection_subscribe_retry_after;
// Absolute CLOCK_MONOTONIC deadline throttling the PERIODIC status-flush retry
// (a publish that failed without an MQTT disconnect). Armed once to
// now + STATUS_FLUSH_RETRY_SECONDS the first time a status is pending, and only
// then does the periodic flush become ready. Unlike the *_retry_after fields
// above, a zeroed deadline here means "not armed" (tracked by
// status_flush_retry_armed), not "eligible now".
static struct timespec status_flush_retry_after;
static bool status_flush_retry_armed = false;

static void init_listener_cond(void) {
    pthread_condattr_t cond_attr;
    pthread_condattr_init(&cond_attr);
    pthread_condattr_setclock(&cond_attr, CLOCK_MONOTONIC);
    pthread_cond_init(&listener_cond, &cond_attr);
    pthread_condattr_destroy(&cond_attr);
}

void ggl_iot_jobs_wait_for_listener_ready(void) {
    GG_MTX_SCOPE_GUARD(&listener_ready_mutex);
    while (!listener_ready) {
        pthread_cond_wait(&listener_ready_cond, &listener_ready_mutex);
    }
}

static void publish_listener_ready(void) {
    GG_MTX_SCOPE_GUARD(&listener_ready_mutex);
    listener_ready = true;
    pthread_cond_broadcast(&listener_ready_cond);
}

// --- Listener scheduling primitives --------------------------------------
// The core-bus dispatch thread never performs job work directly. Response and
// close callbacks only record bounded work flags under listener_mutex and wake
// the listener thread, which owns every describe, enqueue, status flush, and
// subscription call. None of these callbacks sleep, wait on IoT, retry
// indefinitely, or call a bulk-resubscribe routine.

static struct timespec listener_monotonic_now(void) {
    struct timespec ts = { 0 };
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts;
}

// True once `deadline` has been reached at `now`. Pure (no clock read) so the
// periodic-flush gating built on it is deterministically unit-testable. A
// zeroed deadline is always reached.
static bool listener_deadline_reached_at(
    struct timespec now, struct timespec deadline
) {
    if (now.tv_sec != deadline.tv_sec) {
        return now.tv_sec > deadline.tv_sec;
    }
    return now.tv_nsec >= deadline.tv_nsec;
}

// True once `deadline` has been reached. A zeroed deadline is always reached,
// so freshly-armed work runs immediately while re-armed work waits out its
// bounded retry delay.
static bool listener_deadline_reached(struct timespec deadline) {
    return listener_deadline_reached_at(listener_monotonic_now(), deadline);
}

static struct timespec listener_deadline_in(long seconds) {
    struct timespec deadline = listener_monotonic_now();
    deadline.tv_sec += seconds;
    return deadline;
}

// Fold `candidate` into `*earliest`, tracking the soonest deadline to wait for.
static void listener_track_earliest(
    bool *have, struct timespec *earliest, struct timespec candidate
) {
    if (!*have || (candidate.tv_sec < earliest->tv_sec)
        || ((candidate.tv_sec == earliest->tv_sec)
            && (candidate.tv_nsec < earliest->tv_nsec))) {
        *earliest = candidate;
        *have = true;
    }
}

// Schedule a describe on the listener thread as soon as possible (a fresh
// request from a notify-next or reconnect).
static void schedule_describe_now(void) {
    pthread_once(&listener_cond_once, init_listener_cond);
    GG_MTX_SCOPE_GUARD(&listener_mutex);
    needs_describe = true;
    describe_retry_after = (struct timespec) { 0 };
    pthread_cond_signal(&listener_cond);
}

// Close callback for the jobs-topic subscription. Re-arms ONLY the jobs
// subscription; the still-live connection-status subscription is left intact.
static void jobs_subscription_closed(void *ctx, uint32_t handle) {
    (void) ctx;
    (void) handle;
    GG_LOGD("Jobs subscription closed; scheduling jobs resubscribe.");
    pthread_once(&listener_cond_once, init_listener_cond);
    GG_MTX_SCOPE_GUARD(&listener_mutex);
    needs_jobs_subscribe = true;
    jobs_subscribe_retry_after = (struct timespec) { 0 };
    pthread_cond_signal(&listener_cond);
}

// Close callback for the connection-status subscription. Re-arms ONLY the
// connection-status subscription.
static void connection_subscription_closed(void *ctx, uint32_t handle) {
    (void) ctx;
    (void) handle;
    GG_LOGD("Connection-status subscription closed; scheduling resubscribe.");
    pthread_once(&listener_cond_once, init_listener_cond);
    GG_MTX_SCOPE_GUARD(&listener_mutex);
    needs_connection_subscribe = true;
    connection_subscribe_retry_after = (struct timespec) { 0 };
    pthread_cond_signal(&listener_cond);
}

static GgError subscribe_to_next_job_topics(void *ctx);
static GgError subscribe_to_connection_status(void *ctx);

static GgError create_get_next_job_topic(
    GgBuffer thing_name, GgBuffer *job_topic
) {
    GgByteVec job_topic_vec = gg_byte_vec_init(*job_topic);
    GgError err = GG_ERR_OK;
    gg_byte_vec_chain_append(&err, &job_topic_vec, GG_STR(THINGS_TOPIC_PREFIX));
    gg_byte_vec_chain_append(&err, &job_topic_vec, thing_name);
    gg_byte_vec_chain_append(&err, &job_topic_vec, GG_STR(JOBS_TOPIC_PREFIX));
    gg_byte_vec_chain_append(&err, &job_topic_vec, GG_STR(NEXT_JOB_LITERAL));
    gg_byte_vec_chain_append(&err, &job_topic_vec, GG_STR(JOBS_GET_TOPIC));
    if (err == GG_ERR_OK) {
        *job_topic = job_topic_vec.buf;
    }
    return err;
}

static GgError create_update_job_topic(
    GgBuffer thing_name, GgBuffer job_id, GgBuffer *job_topic
) {
    GgByteVec job_topic_vec = gg_byte_vec_init(*job_topic);
    GgError err = GG_ERR_OK;
    gg_byte_vec_chain_append(&err, &job_topic_vec, GG_STR(THINGS_TOPIC_PREFIX));
    gg_byte_vec_chain_append(&err, &job_topic_vec, thing_name);
    gg_byte_vec_chain_append(&err, &job_topic_vec, GG_STR(JOBS_TOPIC_PREFIX));
    gg_byte_vec_chain_append(&err, &job_topic_vec, job_id);
    gg_byte_vec_chain_append(&err, &job_topic_vec, GG_STR(JOBS_UPDATE_TOPIC));
    if (err == GG_ERR_OK) {
        *job_topic = job_topic_vec.buf;
    }
    return err;
}

static GgError create_next_job_execution_changed_topic(
    GgBuffer thing_name, GgBuffer *job_topic
) {
    GgByteVec job_topic_vec = gg_byte_vec_init(*job_topic);
    GgError err = GG_ERR_OK;
    gg_byte_vec_chain_append(&err, &job_topic_vec, GG_STR(THINGS_TOPIC_PREFIX));
    gg_byte_vec_chain_append(&err, &job_topic_vec, thing_name);
    gg_byte_vec_chain_append(
        &err, &job_topic_vec, GG_STR(NEXT_JOB_EXECUTION_CHANGED_TOPIC)
    );
    if (err == GG_ERR_OK) {
        *job_topic = job_topic_vec.buf;
    }
    return err;
}

static GgError update_job(GgBuffer job_id, GgBuffer job_status);

static GgError process_job_execution(GgMap job_execution);

#ifdef GG_SDK_TESTING

static GgError (*thing_name_config_reader)(GgBufList, GgArena *, GgBuffer *)
    = ggl_gg_config_read_str;
static GgError (*iot_call_for_jobs)(GgBuffer, GgBuffer, GgObject, bool, GgArena *, GgObject *)
    = ggl_aws_iot_call;
static GgError (*pending_status_read_for_jobs)(GgArena *, GgBuffer *, GgBuffer *)
    = status_keeper_read;
static GgError (*pending_status_persist_for_jobs)(GgBuffer, GgBuffer)
    = status_keeper_persist;
static GgError (*pending_status_clear_for_jobs)(void) = status_keeper_clear;
static GgError (*save_iot_jobs_id_for_update)(GgBuffer) = save_iot_jobs_id;

#define THING_NAME_CONFIG_READER thing_name_config_reader
#define IOT_CALL_FOR_JOBS iot_call_for_jobs
#define PENDING_STATUS_READ_FOR_JOBS pending_status_read_for_jobs
#define PENDING_STATUS_PERSIST_FOR_JOBS pending_status_persist_for_jobs
#define PENDING_STATUS_CLEAR_FOR_JOBS pending_status_clear_for_jobs
#define SAVE_IOT_JOBS_ID_FOR_UPDATE save_iot_jobs_id_for_update

#else

#define THING_NAME_CONFIG_READER ggl_gg_config_read_str
#define IOT_CALL_FOR_JOBS ggl_aws_iot_call
#define PENDING_STATUS_READ_FOR_JOBS status_keeper_read
#define PENDING_STATUS_PERSIST_FOR_JOBS status_keeper_persist
#define PENDING_STATUS_CLEAR_FOR_JOBS status_keeper_clear
#define SAVE_IOT_JOBS_ID_FOR_UPDATE save_iot_jobs_id

#endif

static GgError copy_thing_name(GgBuffer *thing_name) {
    GG_MTX_SCOPE_GUARD(&thing_name_mutex);
    if (thing_name_buf.len == 0) {
        return GG_ERR_NOENTRY;
    }
    return gg_buf_copy(thing_name_buf, thing_name);
}

static GgError get_thing_name(void *ctx) {
    (void) ctx;
    GG_LOGD("Attempting to retrieve thing name");

    uint8_t read_mem[MAX_THING_NAME_LEN];
    GgArena alloc = gg_arena_init(GG_BUF(read_mem));
    GgBuffer thing_name = { 0 };
    GgError ret = THING_NAME_CONFIG_READER(
        GG_BUF_LIST(GG_STR("system"), GG_STR("thingName")), &alloc, &thing_name
    );
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to read thingName from config.");
        return ret;
    }

    GG_MTX_SCOPE_GUARD(&thing_name_mutex);
    GgBuffer published_name = GG_BUF(thing_name_mem);
    ret = gg_buf_copy(thing_name, &published_name);
    if (ret == GG_ERR_OK) {
        thing_name_buf = published_name;
    }
    return ret;
}

// True if an IoT Jobs UpdateJobExecution rejection won't succeed on retry, per
// the Jobs ErrorResponse "code" contract. InternalError and RequestThrottled
// are transient, and any unrecognized code is treated as transient too, so
// those fall through to the persist/retry path.
static bool reject_is_permanent(GgObject result) {
    if (gg_obj_type(result) != GG_TYPE_MAP) {
        return false;
    }
    GgObject *code_obj;
    if (!gg_map_get(gg_obj_into_map(result), GG_STR("code"), &code_obj)
        || (gg_obj_type(*code_obj) != GG_TYPE_BUF)) {
        return false;
    }
    GgBuffer code = gg_obj_into_buf(*code_obj);
    return gg_buffer_eq(code, GG_STR("InvalidTopic"))
        || gg_buffer_eq(code, GG_STR("InvalidJson"))
        || gg_buffer_eq(code, GG_STR("InvalidRequest"))
        || gg_buffer_eq(code, GG_STR("InvalidStateTransition"))
        || gg_buffer_eq(code, GG_STR("ResourceNotFound"))
        || gg_buffer_eq(code, GG_STR("VersionMismatch"))
        || gg_buffer_eq(code, GG_STR("TerminalStateReached"));
}

// Performs one update operation. When track_pending is true, the caller must
// hold primary_status_mutex across this entire call.
static GgError update_job_to_impl(
    GgBuffer job_id,
    GgBuffer job_status,
    GgBuffer socket_name,
    bool track_pending
) {
    uint8_t thing_name_scratch[MAX_THING_NAME_LEN];
    GgBuffer thing_name = GG_BUF(thing_name_scratch);
    GgError ret = copy_thing_name(&thing_name);
    if (ret != GG_ERR_OK) {
        return ret;
    }

    GgBuffer topic = GG_BUF((uint8_t[256]) { 0 });
    ret = create_update_job_topic(thing_name, job_id, &topic);
    if (ret != GG_ERR_OK) {
        return ret;
    }

    // expectedVersion omitted to avoid VersionMismatch errors on MQTT
    // reconnect races; only one ggdeploymentd updates a given job.
    GgObject payload_object = gg_obj_map(GG_MAP(
        gg_kv(GG_STR("status"), gg_obj_buf(job_status)),
        gg_kv(GG_STR("clientToken"), gg_obj_buf(GG_STR("jobs-nucleus-lite")))
    ));

    // Holds the decoded /accepted or /rejected response. Sized to fit a
    // rejection's executionState payload so the reject code can be classified.
    uint8_t response_scratch[UPDATE_JOB_RESPONSE_ARENA_SIZE];
    GgArena call_alloc = gg_arena_init(GG_BUF(response_scratch));
    GgObject result = { 0 };
    ret = IOT_CALL_FOR_JOBS(
        socket_name, topic, payload_object, false, &call_alloc, &result
    );
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to publish on update job topic.");
        if (track_pending) {
            if ((ret == GG_ERR_REMOTE) && reject_is_permanent(result)) {
                // Cloud permanently rejected this update (e.g. the job already
                // reached a terminal state, or was superseded). Retrying can't
                // succeed, so drop any pending slot instead of re-sending it
                // indefinitely.
                GG_LOGW(
                    "Update permanently rejected; clearing pending status for job %.*s.",
                    (int) job_id.len,
                    job_id.data
                );
                (void) PENDING_STATUS_CLEAR_FOR_JOBS();
            } else {
                // Transport failure / timeout / retryable reject
                // (InternalError, RequestThrottled): persist so the job
                // listener can re-send the status after reconnect or restart,
                // and wake it to start the periodic flush retry even when no
                // MQTT reconnect follows.
                (void) PENDING_STATUS_PERSIST_FOR_JOBS(job_id, job_status);
                pthread_once(&listener_cond_once, init_listener_cond);
                GG_MTX_SCOPE_GUARD(&listener_mutex);
                pthread_cond_signal(&listener_cond);
            }
        }
        return GG_ERR_FAILURE;
    }

    // Publish succeeded: drop any previously-persisted pending status (it is
    // now delivered, or superseded by this newer one). Self-gated in
    // status_keeper, so the happy path issues no config call.
    if (track_pending) {
        (void) PENDING_STATUS_CLEAR_FOR_JOBS();
    }

    // save jobs ID to config in case of bootstrap
    ret = SAVE_IOT_JOBS_ID_FOR_UPDATE(job_id);
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to save job ID to config.");
        return ret;
    }

    return GG_ERR_OK;
}

static GgError update_job_to(
    GgBuffer job_id, GgBuffer job_status, GgBuffer socket_name
) {
    // The pending-status slot is only tracked for the primary MQTT socket.
    // The endpoint-switch path uses a temporary "iotcoreddeploy" socket with
    // its own retry; persisting that would later flush via the wrong account.
    bool track_pending = gg_buffer_eq(socket_name, GG_STR("aws_iot_mqtt"));
    if (!track_pending) {
        return update_job_to_impl(job_id, job_status, socket_name, false);
    }

    GG_MTX_SCOPE_GUARD(&primary_status_mutex);
    return update_job_to_impl(job_id, job_status, socket_name, true);
}

static GgError update_job(GgBuffer job_id, GgBuffer job_status) {
    return update_job_to(job_id, job_status, GG_STR("aws_iot_mqtt"));
}

static GgError describe_next_job(void *ctx) {
    (void) ctx;
    GG_LOGD("Requesting next job information.");

    uint8_t thing_name_scratch[MAX_THING_NAME_LEN];
    GgBuffer thing_name = GG_BUF(thing_name_scratch);
    GgError ret = copy_thing_name(&thing_name);
    if (ret != GG_ERR_OK) {
        return ret;
    }

    uint8_t topic_scratch[512];
    GgBuffer topic = GG_BUF(topic_scratch);
    ret = create_get_next_job_topic(thing_name, &topic);
    if (ret != GG_ERR_OK) {
        return ret;
    }

    // https://docs.aws.amazon.com/iot/latest/developerguide/jobs-mqtt-api.html
    GgObject payload_object = gg_obj_map(GG_MAP(
        gg_kv(GG_STR("jobId"), gg_obj_buf(GG_STR(NEXT_JOB_LITERAL))),
        gg_kv(GG_STR("thingName"), gg_obj_buf(thing_name)),
        gg_kv(GG_STR("includeJobDocument"), gg_obj_bool(true)),
        gg_kv(GG_STR("clientToken"), gg_obj_buf(GG_STR("jobs-nucleus-lite")))
    ));

    uint8_t response_scratch[DESCRIBE_JOB_RESPONSE_ARENA_SIZE];
    GgArena call_alloc = gg_arena_init(GG_BUF(response_scratch));
    GgObject job_description;
    ret = IOT_CALL_FOR_JOBS(
        GG_STR("aws_iot_mqtt"),
        topic,
        payload_object,
        false,
        &call_alloc,
        &job_description
    );
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to publish on describe job topic");
        return ret;
    }

    if (gg_obj_type(job_description) != GG_TYPE_MAP) {
        GG_LOGE("Describe payload not of type Map");
        return GG_ERR_FAILURE;
    }

    GgObject *execution = NULL;
    ret = gg_map_validate(
        gg_obj_into_map(job_description),
        GG_MAP_SCHEMA(
            { GG_STR("execution"), GG_OPTIONAL, GG_TYPE_MAP, &execution }
        )
    );
    if (ret != GG_ERR_OK) {
        return GG_ERR_FAILURE;
    }
    if (execution == NULL) {
        GG_LOGD("No deployment to process.");
        return GG_ERR_OK;
    }
    GG_LOGD("Processing execution.");
    return process_job_execution(gg_obj_into_map(*execution));
}

static void wait_for_bootstrap_scan(void) {
    GG_MTX_SCOPE_GUARD(&current_job_id_mutex);
    while (!bootstrap_scan_complete) {
        pthread_cond_wait(&bootstrap_scan_cond, &current_job_id_mutex);
    }
}

void ggl_iot_jobs_bootstrap_scan_complete(void) {
    GG_MTX_SCOPE_GUARD(&current_job_id_mutex);
    bootstrap_scan_complete = true;
    pthread_cond_broadcast(&bootstrap_scan_cond);
}

#ifdef GG_SDK_TESTING
static GgError (*enqueue_for_job)(
    GgMap, GgByteVec *, GgBuffer, GglDeploymentType
) = ggl_deployment_enqueue;
static GgError (*clear_status_for_job_enqueue)(void) = status_keeper_clear;
static GgError (*report_job_enqueue_failure)(GgBuffer, GgBuffer) = update_job;
#else
// NOLINTBEGIN(readability-identifier-naming)
#define enqueue_for_job ggl_deployment_enqueue
#define clear_status_for_job_enqueue status_keeper_clear
#define report_job_enqueue_failure update_job
// NOLINTEND(readability-identifier-naming)
#endif

static GgError enqueue_job(
    GgMap deployment_doc, GgBuffer job_id, int64_t queued_at
) {
    wait_for_bootstrap_scan();

    {
        GG_MTX_SCOPE_GUARD(&current_job_id_mutex);
        if (gg_buffer_eq(last_queue_job_id.buf, job_id)
            && (queued_at <= last_queue_at)) {
            GG_LOGI(
                "Duplicate job notification for %.*s "
                "(queuedAt=%" PRId64 "). Skipping.",
                (int) job_id.len,
                job_id.data,
                queued_at
            );
            return GG_ERR_OK;
        }
    }

    GgByteVec deployment_id_vec = GG_BYTE_VEC((uint8_t[64]) { 0 });
    GgError ret = enqueue_for_job(
        deployment_doc, &deployment_id_vec, job_id, THING_GROUP_DEPLOYMENT
    );

    if (ret == GG_ERR_BUSY) {
        // The deployment queue is full. Do not sleep on this thread, clear the
        // pending status, report a terminal failure, or advance dedup state --
        // the job must stay retryable. Propagate GG_ERR_BUSY so the caller
        // (describe_next_job -> run_listener_work) owns the bounded delayed
        // re-arm that re-fetches the authoritative job document and
        // re-enqueues, rather than scheduling from this call chain.
        GG_LOGI(
            "Deployment queue full; leaving job %.*s retryable for the listener.",
            (int) job_id.len,
            job_id.data
        );
        return GG_ERR_BUSY;
    }

    // Non-BUSY outcome: the job was accepted, conflicted, or failed terminally.
    // Commit dedup now so a duplicate notification for this same job (with an
    // equal-or-older queuedAt) is skipped from here on.
    {
        GG_MTX_SCOPE_GUARD(&current_job_id_mutex);
        last_queue_job_id = GG_BYTE_VEC(last_queue_job_id_buf);
        GgError append_ret = gg_byte_vec_append(&last_queue_job_id, job_id);
        assert(append_ret == GG_ERR_OK);
        (void) append_ret;
        last_queue_at = queued_at;
    }

    if (ret == GG_ERR_CONFLICT) {
        GG_LOGI("Deployment is already in progress. Skipping duplicate job.");
        return GG_ERR_OK;
    }

    // A new deployment supersedes any status still pending for a previous one.
    // Greengrass nucleus lite tracks a single deployment at a time ($next), so
    // reaching here with a new job_id means the prior deployment has finalized
    // and any held status is stale -- drop it so we never re-publish a
    // superseded job's status. status_keeper_clear() self-gates on the pending
    // hint, so this is a no-op when nothing is pending.
    {
        GG_MTX_SCOPE_GUARD(&primary_status_mutex);
        (void) clear_status_for_job_enqueue();
    }

    if (ret != GG_ERR_OK) {
        (void) report_job_enqueue_failure(job_id, GG_STR("FAILED"));
    }

    return ret;
}

static GgError process_job_execution(GgMap job_execution) {
    GgObject *job_id = NULL;
    GgObject *status = NULL;
    GgObject *deployment_doc = NULL;
    GgObject *queued_at_obj = NULL;
    GgError err = gg_map_validate(
        job_execution,
        GG_MAP_SCHEMA(
            { GG_STR("jobId"), GG_OPTIONAL, GG_TYPE_BUF, &job_id },
            { GG_STR("status"), GG_OPTIONAL, GG_TYPE_BUF, &status },
            { GG_STR("jobDocument"),
              GG_OPTIONAL,
              GG_TYPE_MAP,
              &deployment_doc },
            { GG_STR("queuedAt"), GG_REQUIRED, GG_TYPE_I64, &queued_at_obj }
        )
    );
    if (err != GG_ERR_OK) {
        GG_LOGE("Failed to validate job execution response.");
        return GG_ERR_FAILURE;
    }
    if ((status == NULL) || (job_id == NULL)) {
        return GG_ERR_OK;
    }
    DeploymentStatusAction action;
    {
        GgMap status_action_map = GG_MAP(
            gg_kv(GG_STR("QUEUED"), gg_obj_i64(DSA_ENQUEUE_JOB)),
            gg_kv(GG_STR("IN_PROGRESS"), gg_obj_i64(DSA_ENQUEUE_JOB)),
            gg_kv(GG_STR("SUCCEEDED"), gg_obj_i64(DSA_DO_NOTHING)),
            gg_kv(GG_STR("FAILED"), gg_obj_i64(DSA_DO_NOTHING)),
            gg_kv(GG_STR("TIMED_OUT"), gg_obj_i64(DSA_CANCEL_JOB)),
            gg_kv(GG_STR("REJECTED"), gg_obj_i64(DSA_DO_NOTHING)),
            gg_kv(GG_STR("REMOVED"), gg_obj_i64(DSA_CANCEL_JOB)),
            gg_kv(GG_STR("CANCELED"), gg_obj_i64(DSA_CANCEL_JOB)),
        );
        GgObject *integer = NULL;
        if (!gg_map_get(
                status_action_map, gg_obj_into_buf(*status), &integer
            )) {
            GG_LOGE("Job status not a valid value");
            return GG_ERR_INVALID;
        }
        action = (DeploymentStatusAction) gg_obj_into_i64(*integer);
    }
    switch (action) {
    case DSA_CANCEL_JOB:
        // TODO: cancelation?
        break;

    case DSA_ENQUEUE_JOB: {
        if (deployment_doc == NULL) {
            GG_LOGE(
                "Job status is queued/in progress, but no deployment doc was given."
            );
            return GG_ERR_INVALID;
        }
        // Propagate the enqueue result -- notably GG_ERR_BUSY on a full queue
        // -- so describe_next_job returns it and run_listener_work owns the
        // bounded delayed re-arm/retry.
        return enqueue_job(
            gg_obj_into_map(*deployment_doc),
            gg_obj_into_buf(*job_id),
            gg_obj_into_i64(*queued_at_obj)
        );
    }
    default:
        break;
    }
    return GG_ERR_OK;
}

static GgError next_job_execution_changed_callback(
    void *ctx, uint32_t handle, GgObject data
) {
    (void) ctx;
    (void) handle;
    (void) data;
    // Defer all work to the listener thread. The authoritative next-job
    // document comes from the listener's own describe, so this dispatch-thread
    // callback only schedules a describe and returns promptly. It never parses
    // the notify payload, enqueues, sleeps, or publishes status -- even a
    // payload that would previously fail local JSON parsing still just
    // schedules a describe.
    GG_LOGD("Next job execution changed; scheduling describe.");
    schedule_describe_now();
    return GG_ERR_OK;
}

// Re-send the persisted pending status (if any). The primary transaction mutex
// keeps the slot read, publication, and persist-or-clear finalization atomic
// relative to newer primary-socket updates and superseded-job clearing.
static void flush_pending_status(void) {
    GG_MTX_SCOPE_GUARD(&primary_status_mutex);
    static uint8_t slot_scratch[512];
    GgArena alloc = gg_arena_init(GG_BUF(slot_scratch));
    GgBuffer job_id = { 0 };
    GgBuffer job_status = { 0 };
    GgError ret = PENDING_STATUS_READ_FOR_JOBS(&alloc, &job_id, &job_status);
    if (ret != GG_ERR_OK) {
        // Nothing pending to flush (or the slot was unreadable).
        return;
    }
    GG_LOGI(
        "Re-sending pending deployment status %.*s for job %.*s.",
        (int) job_status.len,
        job_status.data,
        (int) job_id.len,
        job_id.data
    );
    // Calling the implementation directly avoids recursively acquiring
    // primary_status_mutex while preserving the full transaction boundary.
    (void) update_job_to_impl(job_id, job_status, GG_STR("aws_iot_mqtt"), true);
}

typedef struct {
    bool describe;
    bool flush;
    bool jobs_subscribe;
    bool connection_subscribe;
} ListenerWork;

// Seams for what the listener thread runs for each captured work type. The
// listener thread owns all of these calls; dispatch-thread callbacks only set
// flags.
#ifdef GG_SDK_TESTING
static GgError (*describe_for_listener)(void *) = describe_next_job;
static void (*flush_for_listener)(void) = flush_pending_status;
static GgError (*subscribe_jobs_for_listener)(void *)
    = subscribe_to_next_job_topics;
static GgError (*subscribe_connection_for_listener)(void *)
    = subscribe_to_connection_status;
#define DESCRIBE_FOR_LISTENER describe_for_listener
#define FLUSH_FOR_LISTENER flush_for_listener
#define SUBSCRIBE_JOBS_FOR_LISTENER subscribe_jobs_for_listener
#define SUBSCRIBE_CONNECTION_FOR_LISTENER subscribe_connection_for_listener
#else
#define DESCRIBE_FOR_LISTENER describe_next_job
#define FLUSH_FOR_LISTENER flush_pending_status
#define SUBSCRIBE_JOBS_FOR_LISTENER subscribe_to_next_job_topics
#define SUBSCRIBE_CONNECTION_FOR_LISTENER subscribe_to_connection_status
#endif

// Run each captured work item exactly once (a bounded amount of work per
// cycle) and report which items must be re-armed. A failed describe or
// subscribe is re-armed; a pending flush always runs even when describe fails.
static ListenerWork run_listener_work(ListenerWork work) {
    ListenerWork rearm = { 0 };
    if (work.jobs_subscribe
        && (SUBSCRIBE_JOBS_FOR_LISTENER(NULL) != GG_ERR_OK)) {
        rearm.jobs_subscribe = true;
    }
    if (work.connection_subscribe
        && (SUBSCRIBE_CONNECTION_FOR_LISTENER(NULL) != GG_ERR_OK)) {
        rearm.connection_subscribe = true;
    }
    if (work.describe && (DESCRIBE_FOR_LISTENER(NULL) != GG_ERR_OK)) {
        rearm.describe = true;
    }
    if (work.flush) {
        FLUSH_FOR_LISTENER();
    }
    return rearm;
}

// Merge one failed-work re-arm into the shared listener state. MUST be called
// under listener_mutex. If no same-type work is pending, install the bounded
// retry deadline. If a request is already pending, keep the earlier deadline so
// a fresh immediate (zero) describe or reconnect scheduled during the work call
// is not pushed out by this failure's retry delay; only a later pending
// deadline is replaced by the earlier retry deadline.
static void merge_listener_rearm(
    bool *needs, struct timespec *retry_after, struct timespec retry_at
) {
    if (!*needs) {
        *needs = true;
        *retry_after = retry_at;
        return;
    }
    bool have = true;
    listener_track_earliest(&have, retry_after, retry_at);
}

// Re-arm failed work under listener_mutex, each with a bounded retry delay so
// the listener retries later without tight-spinning. Only the failed items are
// touched, so a jobs-subscribe failure never disturbs the connection-status
// subscription and vice versa. Each re-arm merges with any request already
// pending for the same work type, preserving the earlier deadline.
static void apply_listener_rearm(ListenerWork rearm) {
    if (!rearm.describe && !rearm.jobs_subscribe
        && !rearm.connection_subscribe) {
        return;
    }
    struct timespec retry_at
        = listener_deadline_in(LISTENER_RETRY_DELAY_SECONDS);
    GG_MTX_SCOPE_GUARD(&listener_mutex);
    if (rearm.describe) {
        merge_listener_rearm(&needs_describe, &describe_retry_after, retry_at);
    }
    if (rearm.jobs_subscribe) {
        merge_listener_rearm(
            &needs_jobs_subscribe, &jobs_subscribe_retry_after, retry_at
        );
    }
    if (rearm.connection_subscribe) {
        merge_listener_rearm(
            &needs_connection_subscribe,
            &connection_subscribe_retry_after,
            retry_at
        );
    }
}

// Advance the periodic status-flush throttle for one capture pass and report
// whether the periodic flush is due at `now`. Pure state transition (no
// globals, no clock read) so the STATUS_FLUSH_RETRY_SECONDS gating is
// deterministically unit-testable. The absolute deadline is armed exactly once
// when a status first becomes pending; a later pass never pushes it out, so an
// earlier subscribe/describe retry, a signal, or a spurious wake cannot make
// the flush ready before its own deadline. When nothing is pending the window
// is disarmed so a future pending status starts a fresh interval. The explicit
// reconnect flush (needs_flush) is handled by the caller and not modeled here.
static bool status_flush_periodic_due(
    bool pending, struct timespec now, bool *armed, struct timespec *retry_after
) {
    if (!pending) {
        *armed = false;
        *retry_after = (struct timespec) { 0 };
        return false;
    }
    if (!*armed) {
        *retry_after = now;
        retry_after->tv_sec += STATUS_FLUSH_RETRY_SECONDS;
        *armed = true;
    }
    return listener_deadline_reached_at(now, *retry_after);
}

// Block until at least one work item is eligible (respecting retry deadlines),
// then capture and clear the ready items. A pending status is re-sent at most
// once per STATUS_FLUSH_RETRY_SECONDS via the periodic-flush deadline, so a
// flush that failed without an MQTT disconnect still recovers without any
// unrelated wake flushing early.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
static ListenerWork capture_listener_work(void) {
    ListenerWork work = { 0 };
    GG_MTX_SCOPE_GUARD(&listener_mutex);
    while (true) {
        struct timespec now = listener_monotonic_now();
        bool pending = status_keeper_has_pending();
        // Re-evaluated on every pass (i.e. after every wake): arms the periodic
        // deadline once while pending and only reports due once it is reached.
        bool periodic_flush_ready = status_flush_periodic_due(
            pending, now, &status_flush_retry_armed, &status_flush_retry_after
        );

        bool describe_ready
            = needs_describe && listener_deadline_reached(describe_retry_after);
        bool jobs_ready = needs_jobs_subscribe
            && listener_deadline_reached(jobs_subscribe_retry_after);
        bool connection_ready = needs_connection_subscribe
            && listener_deadline_reached(connection_subscribe_retry_after);
        // An explicit reconnect flush runs immediately; the periodic retry runs
        // only once its own deadline is reached.
        bool flush_ready = needs_flush || periodic_flush_ready;

        if (describe_ready || flush_ready || jobs_ready || connection_ready) {
            work.describe = describe_ready;
            work.flush = flush_ready;
            work.jobs_subscribe = jobs_ready;
            work.connection_subscribe = connection_ready;
            if (describe_ready) {
                needs_describe = false;
                describe_retry_after = (struct timespec) { 0 };
            }
            if (flush_ready) {
                needs_flush = false;
                // Restart the periodic window: disarm so the next pass re-arms
                // relative to just after this attempt if the status is still
                // pending, keeping retries STATUS_FLUSH_RETRY_SECONDS apart.
                status_flush_retry_armed = false;
                status_flush_retry_after = (struct timespec) { 0 };
            }
            if (jobs_ready) {
                needs_jobs_subscribe = false;
                jobs_subscribe_retry_after = (struct timespec) { 0 };
            }
            if (connection_ready) {
                needs_connection_subscribe = false;
                connection_subscribe_retry_after = (struct timespec) { 0 };
            }
            return work;
        }

        // Nothing is eligible yet. Wait until the soonest pending retry
        // deadline, or (when a status is pending) the periodic flush deadline.
        bool have_deadline = false;
        struct timespec deadline = { 0 };
        if (needs_describe) {
            listener_track_earliest(
                &have_deadline, &deadline, describe_retry_after
            );
        }
        if (needs_jobs_subscribe) {
            listener_track_earliest(
                &have_deadline, &deadline, jobs_subscribe_retry_after
            );
        }
        if (needs_connection_subscribe) {
            listener_track_earliest(
                &have_deadline, &deadline, connection_subscribe_retry_after
            );
        }
        if (pending && status_flush_retry_armed) {
            listener_track_earliest(
                &have_deadline, &deadline, status_flush_retry_after
            );
        }

        if (have_deadline) {
            (void) pthread_cond_timedwait(
                &listener_cond, &listener_mutex, &deadline
            );
        } else {
            (void) pthread_cond_wait(&listener_cond, &listener_mutex);
        }
        // Readiness is re-evaluated at the top of the loop after every wake, so
        // no wake reason (signal, spurious wake, or an earlier retry deadline)
        // triggers the periodic flush before its own deadline.
    }
}

static GgError synchronize_pending_status(void *ctx) {
    (void) ctx;

    uint8_t slot_scratch[512];
    GgArena slot_alloc = gg_arena_init(GG_BUF(slot_scratch));
    GgError ret;
    {
        GG_MTX_SCOPE_GUARD(&primary_status_mutex);
        ret = PENDING_STATUS_READ_FOR_JOBS(&slot_alloc, NULL, NULL);
    }

    if ((ret == GG_ERR_OK) || (ret == GG_ERR_NOENTRY)) {
        return GG_ERR_OK;
    }

    GG_LOGE(
        "Failed to synchronize pending deployment status before listener readiness: %s. Retrying.",
        gg_strerror(ret)
    );
    return ret;
}

static void initialize_listener_state(void) {
    pthread_once(&listener_cond_once, init_listener_cond);

    (void) gg_backoff(1, 1000, 0, get_thing_name, NULL);

    // Sync the pending-status hint with on-disk state before allowing bootstrap
    // processing to report a terminal status. Otherwise clear can observe an
    // unsynchronized false hint and leave a stale slot to be re-armed later.
    // The callback releases primary_status_mutex before gg_backoff sleeps.
    (void) gg_backoff(1, 1000, 0, synchronize_pending_status, NULL);

    publish_listener_ready();
}

noreturn void *job_listener_thread(void *ctx) {
    (void) ctx;

    initialize_listener_state();

    // Arm the initial subscriptions. The listener thread owns every subscribe
    // call, so a failed attempt is retried here without a callback ever
    // creating a second live subscription.
    {
        GG_MTX_SCOPE_GUARD(&listener_mutex);
        needs_jobs_subscribe = true;
        needs_connection_subscribe = true;
    }

    // coverity[infinite_loop]
    while (true) {
        ListenerWork work = capture_listener_work();
        ListenerWork rearm = run_listener_work(work);
        apply_listener_rearm(rearm);
    }
}

// Seams for the underlying subscribe APIs so tests can confirm each
// subscription registers the matching close callback.
#ifdef GG_SDK_TESTING
static GgError (*mqtt_subscribe_seam)(GgBuffer, GgBufList, uint8_t, bool, GglSubscribeCallback, GglSubscribeCloseCallback, void *, uint32_t *)
    = ggl_aws_iot_mqtt_subscribe;
static GgError (*bus_subscribe_seam)(GgBuffer, GgBuffer, GgMap, GglSubscribeCallback, GglSubscribeCloseCallback, void *, GgError *, uint32_t *)
    = ggl_subscribe;
#define MQTT_SUBSCRIBE_SEAM mqtt_subscribe_seam
#define BUS_SUBSCRIBE_SEAM bus_subscribe_seam
#else
#define MQTT_SUBSCRIBE_SEAM ggl_aws_iot_mqtt_subscribe
#define BUS_SUBSCRIBE_SEAM ggl_subscribe
#endif

static GgError subscribe_to_next_job_topics(void *ctx) {
    (void) ctx;

    uint8_t thing_name_scratch[MAX_THING_NAME_LEN];
    GgBuffer thing_name = GG_BUF(thing_name_scratch);
    GgError err = copy_thing_name(&thing_name);
    if (err != GG_ERR_OK) {
        return err;
    }

    uint8_t topic_scratch[256];
    GgBuffer job_topic = GG_BUF(topic_scratch);
    err = create_next_job_execution_changed_topic(thing_name, &job_topic);
    if (err != GG_ERR_OK) {
        return err;
    }
    return MQTT_SUBSCRIBE_SEAM(
        GG_STR("aws_iot_mqtt"),
        GG_BUF_LIST(job_topic),
        QOS_AT_LEAST_ONCE,
        false,
        next_job_execution_changed_callback,
        jobs_subscription_closed,
        NULL,
        NULL
    );
}

static GgError iot_jobs_on_reconnect(
    void *ctx, uint32_t handle, GgObject data
) {
    (void) ctx;
    (void) handle;
    if (gg_obj_into_bool(data)) {
        GG_LOGD("Reconnected to MQTT; requesting new job query and flush.");
        pthread_once(&listener_cond_once, init_listener_cond);
        GG_MTX_SCOPE_GUARD(&listener_mutex);
        needs_describe = true;
        describe_retry_after = (struct timespec) { 0 };
        needs_flush = true;
        pthread_cond_signal(&listener_cond);
    }
    return GG_ERR_OK;
}

static GgError subscribe_to_connection_status(void *ctx) {
    (void) ctx;
    return BUS_SUBSCRIBE_SEAM(
        GG_STR("aws_iot_mqtt"),
        GG_STR("connection_status"),
        GG_MAP(),
        iot_jobs_on_reconnect,
        connection_subscription_closed,
        NULL,
        NULL,
        NULL
    );
}

GgError update_current_jobs_deployment_to(
    GgBuffer deployment_id, GgBuffer status, GgBuffer socket_name
) {
    GgBuffer job_id = GG_BUF((uint8_t[64]) { 0 });
    {
        GG_MTX_SCOPE_GUARD(&current_job_id_mutex);
        if (!gg_buffer_eq(deployment_id, current_deployment_id.buf)) {
            return GG_ERR_NOENTRY;
        }
        if (current_job_id.buf.len == 0) {
            // Local deployments have no IoT Job — nothing to publish.
            return GG_ERR_OK;
        }
        memcpy(job_id.data, current_job_id.buf.data, current_job_id.buf.len);
        job_id.len = current_job_id.buf.len;
    }

    return update_job_to(job_id, status, socket_name);
}

GgError update_current_jobs_deployment(
    GgBuffer deployment_id, GgBuffer status
) {
    return update_current_jobs_deployment_to(
        deployment_id, status, GG_STR("aws_iot_mqtt")
    );
}

GgError set_jobs_deployment_for_bootstrap(
    GgBuffer job_id, GgBuffer deployment_id
) {
    GG_MTX_SCOPE_GUARD(&current_job_id_mutex);
    if (!gg_buffer_eq(job_id, current_job_id.buf)) {
        if (current_job_id.buf.len != 0) {
            GG_LOGI("Bootstrap deployment was canceled by cloud.");
            return GG_ERR_NOENTRY;
        }
        current_job_id = GG_BYTE_VEC(current_job_id_buf);
        GgError ret = gg_byte_vec_append(&current_job_id, job_id);
        if (ret != GG_ERR_OK) {
            GG_LOGE("Job ID too long.");
            return ret;
        }
        current_deployment_id = GG_BYTE_VEC(current_deployment_id_buf);
        ret = gg_byte_vec_append(&current_deployment_id, deployment_id);
        if (ret != GG_ERR_OK) {
            GG_LOGE("Deployment ID too long.");
            return ret;
        }
    }

    last_queue_job_id = GG_BYTE_VEC(last_queue_job_id_buf);
    GgError ret = gg_byte_vec_append(&last_queue_job_id, job_id);
    assert(ret == GG_ERR_OK);
    (void) ret;
    last_queue_at = INT64_MAX;

    return GG_ERR_OK;
}

void set_current_job(GgBuffer job_id, GgBuffer deployment_id) {
    GG_MTX_SCOPE_GUARD(&current_job_id_mutex);
    current_job_id = GG_BYTE_VEC(current_job_id_buf);
    if (job_id.len > 0) {
        GgError ret = gg_byte_vec_append(&current_job_id, job_id);
        assert(ret == GG_ERR_OK);
        (void) ret;
    }
    current_deployment_id = GG_BYTE_VEC(current_deployment_id_buf);
    GgError ret = gg_byte_vec_append(&current_deployment_id, deployment_id);
    assert(ret == GG_ERR_OK);
    (void) ret;
}

#ifdef GG_SDK_TESTING

#include <errno.h>
#include <gg/test.h>
#include <unity.h>

// Exported (non-static) so the binary-wide Unity tearDown in component_store.c
// can restore listener seams that survive an assertion longjmp. Declared here
// too so the definition has a visible prototype; deliberately kept out of the
// production header.
void iot_jobs_reset_test_seams(void);
void iot_jobs_override_test_seam_for_reset_test(void);
bool iot_jobs_test_seams_are_reset(void);

static GgError enqueue_job_test_results[2];
static size_t enqueue_job_test_result_count;
static size_t enqueue_job_test_enqueue_calls;
static size_t enqueue_job_test_clear_calls;
static size_t enqueue_job_test_failure_calls;
static GgBuffer enqueue_job_test_failure_job_id;
static GgBuffer enqueue_job_test_failure_status;

typedef struct EnqueueJobTestWorkerArgs {
    GgBuffer job_id;
    int64_t queued_at;
} EnqueueJobTestWorkerArgs;

static pthread_mutex_t enqueue_job_test_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t enqueue_job_test_cond = PTHREAD_COND_INITIALIZER;
static bool enqueue_job_test_worker_entered;
static bool enqueue_job_test_worker_done;
static GgError enqueue_job_test_worker_result;

static void *enqueue_job_test_worker(void *ctx) {
    EnqueueJobTestWorkerArgs *args = ctx;
    {
        GG_MTX_SCOPE_GUARD(&enqueue_job_test_mutex);
        enqueue_job_test_worker_entered = true;
        pthread_cond_broadcast(&enqueue_job_test_cond);
    }

    GgError result = enqueue_job(GG_MAP(), args->job_id, args->queued_at);

    {
        GG_MTX_SCOPE_GUARD(&enqueue_job_test_mutex);
        enqueue_job_test_worker_result = result;
        enqueue_job_test_worker_done = true;
        pthread_cond_broadcast(&enqueue_job_test_cond);
    }
    return NULL;
}

static int enqueue_job_test_wait_for(bool *state) {
    struct timespec deadline;
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
        return errno;
    }
    deadline.tv_sec += 1;

    GG_MTX_SCOPE_GUARD(&enqueue_job_test_mutex);
    int ret = 0;
    while (!*state && (ret == 0)) {
        ret = pthread_cond_timedwait(
            &enqueue_job_test_cond, &enqueue_job_test_mutex, &deadline
        );
    }
    return *state ? 0 : ret;
}

static GgError enqueue_job_test_enqueue(
    GgMap deployment_doc,
    GgByteVec *deployment_id,
    GgBuffer job_id,
    GglDeploymentType type
) {
    (void) deployment_doc;
    (void) deployment_id;
    (void) job_id;
    (void) type;
    if (enqueue_job_test_enqueue_calls >= enqueue_job_test_result_count) {
        return GG_ERR_FAILURE;
    }
    return enqueue_job_test_results[enqueue_job_test_enqueue_calls++];
}

static GgError enqueue_job_test_clear_status(void) {
    enqueue_job_test_clear_calls += 1;
    return GG_ERR_OK;
}

static GgError enqueue_job_test_report_failure(
    GgBuffer job_id, GgBuffer status
) {
    enqueue_job_test_failure_calls += 1;
    enqueue_job_test_failure_job_id = job_id;
    enqueue_job_test_failure_status = status;
    return GG_ERR_OK;
}

// Reset the listener work flags and retry deadlines between tests. Acquired on
// its own so no test establishes a listener_mutex/current_job_id_mutex nesting.
static void reset_listener_work_flags_for_test(void) {
    GG_MTX_SCOPE_GUARD(&listener_mutex);
    needs_describe = false;
    needs_flush = false;
    needs_jobs_subscribe = false;
    needs_connection_subscribe = false;
    describe_retry_after = (struct timespec) { 0 };
    jobs_subscribe_retry_after = (struct timespec) { 0 };
    connection_subscribe_retry_after = (struct timespec) { 0 };
    status_flush_retry_after = (struct timespec) { 0 };
    status_flush_retry_armed = false;
}

static void enqueue_job_test_reset(void) {
    memset(enqueue_job_test_results, 0, sizeof(enqueue_job_test_results));
    enqueue_job_test_result_count = 0;
    enqueue_job_test_enqueue_calls = 0;
    enqueue_job_test_clear_calls = 0;
    enqueue_job_test_failure_calls = 0;
    enqueue_job_test_failure_job_id = (GgBuffer) { 0 };
    enqueue_job_test_failure_status = (GgBuffer) { 0 };
    enqueue_for_job = enqueue_job_test_enqueue;
    clear_status_for_job_enqueue = enqueue_job_test_clear_status;
    report_job_enqueue_failure = enqueue_job_test_report_failure;
    {
        GG_MTX_SCOPE_GUARD(&enqueue_job_test_mutex);
        enqueue_job_test_worker_entered = false;
        enqueue_job_test_worker_done = false;
        enqueue_job_test_worker_result = GG_ERR_FAILURE;
    }
    reset_listener_work_flags_for_test();
    GG_MTX_SCOPE_GUARD(&current_job_id_mutex);
    current_job_id = GG_BYTE_VEC(current_job_id_buf);
    current_deployment_id = GG_BYTE_VEC(current_deployment_id_buf);
    last_queue_job_id = GG_BYTE_VEC(last_queue_job_id_buf);
    last_queue_at = 0;
    bootstrap_scan_complete = true;
}

GG_TEST_DEFINE(iot_jobs_in_progress_conflict_is_skipped) {
    enqueue_job_test_reset();
    enqueue_job_test_results[0] = GG_ERR_CONFLICT;
    enqueue_job_test_result_count = 1;

    GG_TEST_ASSERT_OK(enqueue_job(GG_MAP(), GG_STR("job"), 1));
    TEST_ASSERT_EQUAL_size_t(1, enqueue_job_test_enqueue_calls);
    TEST_ASSERT_EQUAL_size_t(0, enqueue_job_test_clear_calls);
    TEST_ASSERT_EQUAL_size_t(0, enqueue_job_test_failure_calls);
}

GG_TEST_DEFINE(iot_jobs_busy_enqueue_stays_retryable_and_defers_scheduling) {
    enqueue_job_test_reset();
    enqueue_job_test_results[0] = GG_ERR_BUSY;
    enqueue_job_test_results[1] = GG_ERR_OK;
    enqueue_job_test_result_count = 2;

    // A full queue must not sleep, clear status, report failure, or advance
    // dedup. enqueue_job returns GG_ERR_BUSY and leaves the job retryable;
    // scheduling the bounded retry is the listener's job (run_listener_work ->
    // apply_listener_rearm on the propagated BUSY), not enqueue_job's.
    TEST_ASSERT_EQUAL_INT(GG_ERR_BUSY, enqueue_job(GG_MAP(), GG_STR("job"), 1));
    TEST_ASSERT_EQUAL_size_t(1, enqueue_job_test_enqueue_calls);
    TEST_ASSERT_EQUAL_size_t(0, enqueue_job_test_clear_calls);
    TEST_ASSERT_EQUAL_size_t(0, enqueue_job_test_failure_calls);
    size_t last_queue_job_id_len;
    {
        // Dedup state was NOT advanced: the same job stays enqueuable.
        GG_MTX_SCOPE_GUARD(&current_job_id_mutex);
        last_queue_job_id_len = last_queue_job_id.buf.len;
    }
    TEST_ASSERT_EQUAL_size_t(0, last_queue_job_id_len);
    bool describe_scheduled;
    {
        // enqueue_job no longer schedules describe itself; the listener owns
        // the re-arm now, so a bare enqueue leaves the work flags untouched.
        GG_MTX_SCOPE_GUARD(&listener_mutex);
        describe_scheduled = needs_describe;
    }
    TEST_ASSERT_FALSE(describe_scheduled);

    // A later attempt at the SAME job still succeeds: it was not deduped, so it
    // now enqueues successfully and clears any pending status.
    TEST_ASSERT_EQUAL_INT(GG_ERR_OK, enqueue_job(GG_MAP(), GG_STR("job"), 1));
    TEST_ASSERT_EQUAL_size_t(2, enqueue_job_test_enqueue_calls);
    TEST_ASSERT_EQUAL_size_t(1, enqueue_job_test_clear_calls);
    TEST_ASSERT_EQUAL_size_t(0, enqueue_job_test_failure_calls);
    bool queued_job_matches;
    int64_t queued_at;
    {
        GG_MTX_SCOPE_GUARD(&current_job_id_mutex);
        queued_job_matches = gg_buffer_eq(last_queue_job_id.buf, GG_STR("job"));
        queued_at = last_queue_at;
    }
    TEST_ASSERT_TRUE(queued_job_matches);
    TEST_ASSERT_EQUAL_INT64(1, queued_at);
}

// A queued execution whose enqueue hits a full deployment queue must surface
// GG_ERR_BUSY to the caller (describe_next_job -> run_listener_work owns the
// bounded delayed retry) without advancing dedup, clearing the pending status,
// or reporting a terminal failure.
GG_TEST_DEFINE(iot_jobs_process_execution_propagates_busy) {
    enqueue_job_test_reset();
    enqueue_job_test_results[0] = GG_ERR_BUSY;
    enqueue_job_test_result_count = 1;

    GgMap execution = GG_MAP(
        gg_kv(GG_STR("jobId"), gg_obj_buf(GG_STR("job"))),
        gg_kv(GG_STR("status"), gg_obj_buf(GG_STR("QUEUED"))),
        gg_kv(GG_STR("jobDocument"), gg_obj_map(GG_MAP())),
        gg_kv(GG_STR("queuedAt"), gg_obj_i64(1))
    );
    TEST_ASSERT_EQUAL_INT(GG_ERR_BUSY, process_job_execution(execution));
    TEST_ASSERT_EQUAL_size_t(1, enqueue_job_test_enqueue_calls);
    TEST_ASSERT_EQUAL_size_t(0, enqueue_job_test_clear_calls);
    TEST_ASSERT_EQUAL_size_t(0, enqueue_job_test_failure_calls);
    size_t last_queue_job_id_len;
    {
        // Dedup state was NOT advanced: the job stays enqueuable on retry.
        GG_MTX_SCOPE_GUARD(&current_job_id_mutex);
        last_queue_job_id_len = last_queue_job_id.buf.len;
    }
    TEST_ASSERT_EQUAL_size_t(0, last_queue_job_id_len);
}

GG_TEST_DEFINE(iot_jobs_enqueue_failure_clears_and_reports_failure) {
    enqueue_job_test_reset();
    enqueue_job_test_results[0] = GG_ERR_PARSE;
    enqueue_job_test_result_count = 1;

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_PARSE, enqueue_job(GG_MAP(), GG_STR("job"), 1)
    );
    TEST_ASSERT_EQUAL_size_t(1, enqueue_job_test_enqueue_calls);
    TEST_ASSERT_EQUAL_size_t(1, enqueue_job_test_clear_calls);
    TEST_ASSERT_EQUAL_size_t(1, enqueue_job_test_failure_calls);
    TEST_ASSERT_TRUE(
        gg_buffer_eq(enqueue_job_test_failure_job_id, GG_STR("job"))
    );
    TEST_ASSERT_TRUE(
        gg_buffer_eq(enqueue_job_test_failure_status, GG_STR("FAILED"))
    );
}

GG_TEST_DEFINE(iot_jobs_recovered_notification_is_deduped_after_wait) {
    enqueue_job_test_reset();
    enqueue_job_test_results[0] = GG_ERR_OK;
    enqueue_job_test_result_count = 1;
    {
        GG_MTX_SCOPE_GUARD(&current_job_id_mutex);
        bootstrap_scan_complete = false;
    }

    EnqueueJobTestWorkerArgs worker_args = {
        .job_id = GG_STR("new-job"),
        .queued_at = 1,
    };
    pthread_t worker;
    TEST_ASSERT_EQUAL_INT(
        0, pthread_create(&worker, NULL, enqueue_job_test_worker, &worker_args)
    );
    int entered_wait
        = enqueue_job_test_wait_for(&enqueue_job_test_worker_entered);
    int blocked_wait = enqueue_job_test_wait_for(&enqueue_job_test_worker_done);
    size_t blocked_enqueue_calls = enqueue_job_test_enqueue_calls;
    size_t blocked_clear_calls = enqueue_job_test_clear_calls;
    size_t blocked_failure_calls = enqueue_job_test_failure_calls;

    ggl_iot_jobs_bootstrap_scan_complete();
    int done_wait = enqueue_job_test_wait_for(&enqueue_job_test_worker_done);
    TEST_ASSERT_EQUAL_INT(0, done_wait);
    TEST_ASSERT_EQUAL_INT(0, pthread_join(worker, NULL));
    TEST_ASSERT_EQUAL_INT(0, entered_wait);
    TEST_ASSERT_EQUAL_INT(ETIMEDOUT, blocked_wait);
    TEST_ASSERT_EQUAL_size_t(0, blocked_enqueue_calls);
    TEST_ASSERT_EQUAL_size_t(0, blocked_clear_calls);
    TEST_ASSERT_EQUAL_size_t(0, blocked_failure_calls);
    TEST_ASSERT_EQUAL_INT(GG_ERR_OK, enqueue_job_test_worker_result);
    TEST_ASSERT_EQUAL_size_t(1, enqueue_job_test_enqueue_calls);
    TEST_ASSERT_EQUAL_size_t(1, enqueue_job_test_clear_calls);
    TEST_ASSERT_EQUAL_size_t(0, enqueue_job_test_failure_calls);

    enqueue_job_test_reset();
    {
        GG_MTX_SCOPE_GUARD(&current_job_id_mutex);
        bootstrap_scan_complete = false;
    }
    worker_args = (EnqueueJobTestWorkerArgs) {
        .job_id = GG_STR("recovered-job"),
        .queued_at = INT64_MAX,
    };
    TEST_ASSERT_EQUAL_INT(
        0, pthread_create(&worker, NULL, enqueue_job_test_worker, &worker_args)
    );
    entered_wait = enqueue_job_test_wait_for(&enqueue_job_test_worker_entered);
    blocked_wait = enqueue_job_test_wait_for(&enqueue_job_test_worker_done);
    blocked_enqueue_calls = enqueue_job_test_enqueue_calls;
    blocked_clear_calls = enqueue_job_test_clear_calls;
    blocked_failure_calls = enqueue_job_test_failure_calls;

    GgError recover_ret = set_jobs_deployment_for_bootstrap(
        GG_STR("recovered-job"), GG_STR("recovered-deployment")
    );
    ggl_iot_jobs_bootstrap_scan_complete();
    done_wait = enqueue_job_test_wait_for(&enqueue_job_test_worker_done);
    TEST_ASSERT_EQUAL_INT(0, done_wait);
    TEST_ASSERT_EQUAL_INT(0, pthread_join(worker, NULL));
    GG_TEST_ASSERT_OK(recover_ret);
    TEST_ASSERT_EQUAL_INT(0, entered_wait);
    TEST_ASSERT_EQUAL_INT(ETIMEDOUT, blocked_wait);
    TEST_ASSERT_EQUAL_size_t(0, blocked_enqueue_calls);
    TEST_ASSERT_EQUAL_size_t(0, blocked_clear_calls);
    TEST_ASSERT_EQUAL_size_t(0, blocked_failure_calls);
    TEST_ASSERT_EQUAL_INT(GG_ERR_OK, enqueue_job_test_worker_result);
    TEST_ASSERT_EQUAL_size_t(0, enqueue_job_test_enqueue_calls);
    TEST_ASSERT_EQUAL_size_t(0, enqueue_job_test_clear_calls);
    TEST_ASSERT_EQUAL_size_t(0, enqueue_job_test_failure_calls);
    TEST_ASSERT_TRUE(
        gg_buffer_eq(last_queue_job_id.buf, GG_STR("recovered-job"))
    );
    TEST_ASSERT_EQUAL_INT64(INT64_MAX, last_queue_at);
}

static GgBuffer thing_name_test_value;

static GgError thing_name_test_reader(
    GgBufList key_path, GgArena *alloc, GgBuffer *result
) {
    (void) key_path;
    (void) alloc;
    *result = thing_name_test_value;
    return GG_ERR_OK;
}

static void reset_thing_name_for_test(void) {
    GG_MTX_SCOPE_GUARD(&thing_name_mutex);
    memset(thing_name_mem, 0, sizeof(thing_name_mem));
    thing_name_buf = (GgBuffer) { 0 };
}

GG_TEST_DEFINE(iot_jobs_thing_name_unpublished_returns_error) {
    reset_thing_name_for_test();
    uint8_t output_mem[MAX_THING_NAME_LEN];
    GgBuffer output = GG_BUF(output_mem);
    TEST_ASSERT_EQUAL_INT(GG_ERR_NOENTRY, copy_thing_name(&output));
}

GG_TEST_DEFINE(iot_jobs_thing_name_is_published_and_read_by_copy) {
    reset_thing_name_for_test();
    uint8_t source_mem[] = "owned-thing";
    thing_name_test_value = (GgBuffer) {
        .data = source_mem,
        .len = sizeof(source_mem) - 1,
    };
    thing_name_config_reader = thing_name_test_reader;

    GG_TEST_ASSERT_OK(get_thing_name(NULL));
    memset(source_mem, 'x', sizeof(source_mem));

    uint8_t first_mem[MAX_THING_NAME_LEN];
    GgBuffer first = GG_BUF(first_mem);
    GG_TEST_ASSERT_OK(copy_thing_name(&first));
    TEST_ASSERT_TRUE(gg_buffer_eq(first, GG_STR("owned-thing")));

    memset(first.data, 'y', first.len);
    uint8_t second_mem[MAX_THING_NAME_LEN];
    GgBuffer second = GG_BUF(second_mem);
    GG_TEST_ASSERT_OK(copy_thing_name(&second));
    TEST_ASSERT_TRUE(gg_buffer_eq(second, GG_STR("owned-thing")));
    TEST_ASSERT_NOT_EQUAL(first.data, second.data);

    thing_name_config_reader = ggl_gg_config_read_str;
}

typedef struct UpdateJobStorageTestArgs {
    GgBuffer job_id;
    GgError result;
} UpdateJobStorageTestArgs;

static pthread_mutex_t update_job_storage_test_mutex
    = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t update_job_storage_test_cond = PTHREAD_COND_INITIALIZER;
static uintptr_t update_job_storage_test_arena_addrs[2];
static size_t update_job_storage_test_calls;

static GgError update_job_storage_test_iot_call(
    GgBuffer socket_name,
    GgBuffer topic,
    GgObject payload,
    bool virtual,
    GgArena *alloc,
    GgObject *result
) {
    (void) socket_name;
    (void) topic;
    (void) payload;
    (void) virtual;
    (void) result;

    GG_MTX_SCOPE_GUARD(&update_job_storage_test_mutex);
    size_t index = update_job_storage_test_calls;
    if (index >= 2) {
        return GG_ERR_RANGE;
    }
    update_job_storage_test_arena_addrs[index] = (uintptr_t) alloc->mem;
    update_job_storage_test_calls += 1;
    pthread_cond_broadcast(&update_job_storage_test_cond);
    while (update_job_storage_test_calls < 2) {
        pthread_cond_wait(
            &update_job_storage_test_cond, &update_job_storage_test_mutex
        );
    }
    return GG_ERR_FAILURE;
}

static void *update_job_storage_test_worker(void *ctx) {
    UpdateJobStorageTestArgs *args = ctx;
    args->result = update_job_to(
        args->job_id, GG_STR("IN_PROGRESS"), GG_STR("test-iotcored")
    );
    return NULL;
}

GG_TEST_DEFINE(iot_jobs_concurrent_updates_use_distinct_response_arenas) {
    reset_thing_name_for_test();
    thing_name_test_value = GG_STR("owned-thing");
    thing_name_config_reader = thing_name_test_reader;
    GG_TEST_ASSERT_OK(get_thing_name(NULL));

    update_job_storage_test_calls = 0;
    memset(
        update_job_storage_test_arena_addrs,
        0,
        sizeof(update_job_storage_test_arena_addrs)
    );
    iot_call_for_jobs = update_job_storage_test_iot_call;

    UpdateJobStorageTestArgs args[2] = {
        { .job_id = GG_STR("job-one"), .result = GG_ERR_OK },
        { .job_id = GG_STR("job-two"), .result = GG_ERR_OK },
    };
    pthread_t workers[2];
    TEST_ASSERT_EQUAL_INT(
        0,
        pthread_create(
            &workers[0], NULL, update_job_storage_test_worker, &args[0]
        )
    );
    TEST_ASSERT_EQUAL_INT(
        0,
        pthread_create(
            &workers[1], NULL, update_job_storage_test_worker, &args[1]
        )
    );
    TEST_ASSERT_EQUAL_INT(0, pthread_join(workers[0], NULL));
    TEST_ASSERT_EQUAL_INT(0, pthread_join(workers[1], NULL));

    TEST_ASSERT_EQUAL_INT(GG_ERR_FAILURE, args[0].result);
    TEST_ASSERT_EQUAL_INT(GG_ERR_FAILURE, args[1].result);
    TEST_ASSERT_NOT_EQUAL_UINT64(0, update_job_storage_test_arena_addrs[0]);
    TEST_ASSERT_NOT_EQUAL_UINT64(0, update_job_storage_test_arena_addrs[1]);
    TEST_ASSERT_NOT_EQUAL_UINT64(
        update_job_storage_test_arena_addrs[0],
        update_job_storage_test_arena_addrs[1]
    );

    iot_call_for_jobs = ggl_aws_iot_call;
    thing_name_config_reader = ggl_gg_config_read_str;
}

typedef struct PrimaryStatusTestUpdateArgs {
    GgError result;
} PrimaryStatusTestUpdateArgs;

static pthread_mutex_t primary_status_test_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t primary_status_test_cond = PTHREAD_COND_INITIALIZER;
static uint8_t primary_status_test_job_id_mem[64];
static uint8_t primary_status_test_status_mem[64];
static GgBuffer primary_status_test_job_id;
static GgBuffer primary_status_test_status;
static bool primary_status_test_pending;
static bool primary_status_test_hint_pending;
static size_t primary_status_test_read_calls;
static size_t primary_status_test_publish_calls;
static size_t primary_status_test_clear_calls;
static GgError primary_status_test_first_read_result;
static bool primary_status_test_block_retry_read;
static bool primary_status_test_retry_read_entered;
static bool primary_status_test_release_retry_read;
static GgError primary_status_test_terminal_result;
static bool primary_status_test_stale_publish_entered;
static bool primary_status_test_release_stale_publish;
static bool primary_status_test_terminal_worker_started;
static bool primary_status_test_terminal_publish_entered;

static GgError primary_status_test_read(
    GgArena *alloc, GgBuffer *job_id, GgBuffer *status
) {
    (void) alloc;
    GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
    primary_status_test_read_calls += 1;
    if ((primary_status_test_read_calls == 1)
        && (primary_status_test_first_read_result != GG_ERR_OK)) {
        pthread_cond_broadcast(&primary_status_test_cond);
        return primary_status_test_first_read_result;
    }
    if (primary_status_test_read_calls > 1) {
        primary_status_test_retry_read_entered = true;
        pthread_cond_broadcast(&primary_status_test_cond);
        while (primary_status_test_block_retry_read
               && !primary_status_test_release_retry_read) {
            pthread_cond_wait(
                &primary_status_test_cond, &primary_status_test_mutex
            );
        }
    }
    if (!primary_status_test_pending) {
        return GG_ERR_NOENTRY;
    }
    primary_status_test_hint_pending = true;
    if (job_id != NULL) {
        *job_id = primary_status_test_job_id;
    }
    if (status != NULL) {
        *status = primary_status_test_status;
    }
    return GG_ERR_OK;
}

static GgError primary_status_test_persist(GgBuffer job_id, GgBuffer status) {
    if ((job_id.len > sizeof(primary_status_test_job_id_mem))
        || (status.len > sizeof(primary_status_test_status_mem))) {
        return GG_ERR_NOMEM;
    }

    GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
    memcpy(primary_status_test_job_id_mem, job_id.data, job_id.len);
    primary_status_test_job_id = (GgBuffer) {
        .data = primary_status_test_job_id_mem,
        .len = job_id.len,
    };
    memcpy(primary_status_test_status_mem, status.data, status.len);
    primary_status_test_status = (GgBuffer) {
        .data = primary_status_test_status_mem,
        .len = status.len,
    };
    primary_status_test_pending = true;
    primary_status_test_hint_pending = true;
    return GG_ERR_OK;
}

static GgError primary_status_test_clear(void) {
    GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
    if (!primary_status_test_hint_pending) {
        return GG_ERR_OK;
    }
    primary_status_test_clear_calls += 1;
    primary_status_test_pending = false;
    primary_status_test_hint_pending = false;
    return GG_ERR_OK;
}

static GgError primary_status_test_save_job_id(GgBuffer job_id) {
    (void) job_id;
    return GG_ERR_OK;
}

static GgError primary_status_test_iot_call(
    GgBuffer socket_name,
    GgBuffer topic,
    GgObject payload,
    bool virtual,
    GgArena *alloc,
    GgObject *result
) {
    (void) socket_name;
    (void) topic;
    (void) virtual;
    (void) alloc;
    (void) result;

    if (gg_obj_type(payload) != GG_TYPE_MAP) {
        return GG_ERR_INVALID;
    }
    GgObject *status_obj = NULL;
    if (!gg_map_get(gg_obj_into_map(payload), GG_STR("status"), &status_obj)
        || (gg_obj_type(*status_obj) != GG_TYPE_BUF)) {
        return GG_ERR_INVALID;
    }

    GgBuffer status = gg_obj_into_buf(*status_obj);
    if (gg_buffer_eq(status, GG_STR("IN_PROGRESS"))) {
        GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
        primary_status_test_publish_calls += 1;
        primary_status_test_stale_publish_entered = true;
        pthread_cond_broadcast(&primary_status_test_cond);
        while (!primary_status_test_release_stale_publish) {
            pthread_cond_wait(
                &primary_status_test_cond, &primary_status_test_mutex
            );
        }
        return GG_ERR_OK;
    }
    if (gg_buffer_eq(status, GG_STR("FAILED"))) {
        GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
        primary_status_test_publish_calls += 1;
        primary_status_test_terminal_publish_entered = true;
        pthread_cond_broadcast(&primary_status_test_cond);
        return primary_status_test_terminal_result;
    }
    return GG_ERR_INVALID;
}

static int primary_status_test_wait_for(bool *state) {
    struct timespec deadline;
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
        return errno;
    }
    deadline.tv_sec += 1;

    GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
    int ret = 0;
    while (!*state && (ret == 0)) {
        ret = pthread_cond_timedwait(
            &primary_status_test_cond, &primary_status_test_mutex, &deadline
        );
    }
    return *state ? 0 : ret;
}

static void primary_status_test_reset(void) {
    GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
    memcpy(primary_status_test_job_id_mem, "job", 3);
    primary_status_test_job_id = (GgBuffer) {
        .data = primary_status_test_job_id_mem,
        .len = 3,
    };
    memcpy(primary_status_test_status_mem, "IN_PROGRESS", 11);
    primary_status_test_status = (GgBuffer) {
        .data = primary_status_test_status_mem,
        .len = 11,
    };
    primary_status_test_pending = true;
    primary_status_test_hint_pending = true;
    primary_status_test_read_calls = 0;
    primary_status_test_publish_calls = 0;
    primary_status_test_clear_calls = 0;
    primary_status_test_first_read_result = GG_ERR_OK;
    primary_status_test_block_retry_read = false;
    primary_status_test_retry_read_entered = false;
    primary_status_test_release_retry_read = false;
    primary_status_test_terminal_result = GG_ERR_NOCONN;
    primary_status_test_stale_publish_entered = false;
    primary_status_test_release_stale_publish = false;
    primary_status_test_terminal_worker_started = false;
    primary_status_test_terminal_publish_entered = false;
}

static void *primary_status_test_flush_worker(void *ctx) {
    (void) ctx;
    flush_pending_status();
    return NULL;
}

static void *primary_status_test_terminal_worker(void *ctx) {
    PrimaryStatusTestUpdateArgs *args = ctx;
    {
        GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
        primary_status_test_terminal_worker_started = true;
        pthread_cond_broadcast(&primary_status_test_cond);
    }
    args->result = update_job_to(
        GG_STR("job"), GG_STR("FAILED"), GG_STR("aws_iot_mqtt")
    );
    return NULL;
}

static void *primary_status_test_ready_terminal_worker(void *ctx) {
    PrimaryStatusTestUpdateArgs *args = ctx;
    {
        GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
        primary_status_test_terminal_worker_started = true;
        pthread_cond_broadcast(&primary_status_test_cond);
    }
    ggl_iot_jobs_wait_for_listener_ready();
    args->result = update_job_to(
        GG_STR("job"), GG_STR("FAILED"), GG_STR("aws_iot_mqtt")
    );
    return NULL;
}

static void *primary_status_test_initialize_worker(void *ctx) {
    (void) ctx;
    initialize_listener_state();
    return NULL;
}

GG_TEST_DEFINE(iot_jobs_newer_terminal_status_survives_stale_flush_overlap) {
    reset_thing_name_for_test();
    thing_name_test_value = GG_STR("owned-thing");
    thing_name_config_reader = thing_name_test_reader;
    GG_TEST_ASSERT_OK(get_thing_name(NULL));

    primary_status_test_reset();
    pending_status_read_for_jobs = primary_status_test_read;
    pending_status_persist_for_jobs = primary_status_test_persist;
    pending_status_clear_for_jobs = primary_status_test_clear;
    save_iot_jobs_id_for_update = primary_status_test_save_job_id;
    iot_call_for_jobs = primary_status_test_iot_call;

    pthread_t flush_worker;
    pthread_t terminal_worker;
    PrimaryStatusTestUpdateArgs terminal_args = { .result = GG_ERR_OK };
    int flush_create_ret = pthread_create(
        &flush_worker, NULL, primary_status_test_flush_worker, NULL
    );
    int stale_entered_ret = EINVAL;
    int terminal_create_ret = EINVAL;
    int terminal_started_ret = EINVAL;
    int terminal_blocked_ret = EINVAL;
    int flush_join_ret = EINVAL;
    int terminal_join_ret = EINVAL;
    bool terminal_entered_before_release = false;

    if (flush_create_ret == 0) {
        stale_entered_ret = primary_status_test_wait_for(
            &primary_status_test_stale_publish_entered
        );
        if (stale_entered_ret == 0) {
            terminal_create_ret = pthread_create(
                &terminal_worker,
                NULL,
                primary_status_test_terminal_worker,
                &terminal_args
            );
            if (terminal_create_ret == 0) {
                terminal_started_ret = primary_status_test_wait_for(
                    &primary_status_test_terminal_worker_started
                );
                terminal_blocked_ret = primary_status_test_wait_for(
                    &primary_status_test_terminal_publish_entered
                );
                {
                    GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
                    terminal_entered_before_release
                        = primary_status_test_terminal_publish_entered;
                }
            }
        }

        {
            GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
            primary_status_test_release_stale_publish = true;
            pthread_cond_broadcast(&primary_status_test_cond);
        }
        flush_join_ret = pthread_join(flush_worker, NULL);
        if (terminal_create_ret == 0) {
            terminal_join_ret = pthread_join(terminal_worker, NULL);
        }
    }

    bool final_pending;
    GgBuffer final_job_id;
    GgBuffer final_status;
    {
        GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
        final_pending = primary_status_test_pending;
        final_job_id = primary_status_test_job_id;
        final_status = primary_status_test_status;
    }

    TEST_ASSERT_EQUAL_INT(0, flush_create_ret);
    TEST_ASSERT_EQUAL_INT(0, stale_entered_ret);
    TEST_ASSERT_EQUAL_INT(0, terminal_create_ret);
    TEST_ASSERT_EQUAL_INT(0, terminal_started_ret);
    TEST_ASSERT_EQUAL_INT(ETIMEDOUT, terminal_blocked_ret);
    TEST_ASSERT_FALSE(terminal_entered_before_release);
    TEST_ASSERT_EQUAL_INT(0, flush_join_ret);
    TEST_ASSERT_EQUAL_INT(0, terminal_join_ret);
    TEST_ASSERT_EQUAL_INT(GG_ERR_FAILURE, terminal_args.result);
    TEST_ASSERT_TRUE(final_pending);
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("job"), final_job_id);
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("FAILED"), final_status);
}

GG_TEST_DEFINE(iot_jobs_bootstrap_terminal_waits_for_listener_initialization) {
    iot_jobs_reset_test_seams();
    reset_thing_name_for_test();
    thing_name_test_value = GG_STR("owned-thing");
    thing_name_config_reader = thing_name_test_reader;

    primary_status_test_reset();
    {
        GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
        // Model restart state: the stale slot exists on disk, but the cached
        // pending hint has not yet been synchronized by the listener.
        primary_status_test_hint_pending = false;
        primary_status_test_first_read_result = GG_ERR_NOCONN;
        primary_status_test_block_retry_read = true;
        primary_status_test_terminal_result = GG_ERR_OK;
    }
    pending_status_read_for_jobs = primary_status_test_read;
    pending_status_persist_for_jobs = primary_status_test_persist;
    pending_status_clear_for_jobs = primary_status_test_clear;
    save_iot_jobs_id_for_update = primary_status_test_save_job_id;
    iot_call_for_jobs = primary_status_test_iot_call;

    PrimaryStatusTestUpdateArgs terminal_args = { .result = GG_ERR_FAILURE };
    pthread_t terminal_worker;
    pthread_t initialize_worker;
    int terminal_create_ret = pthread_create(
        &terminal_worker,
        NULL,
        primary_status_test_ready_terminal_worker,
        &terminal_args
    );
    int started_ret = EINVAL;
    int blocked_before_initialization_ret = EINVAL;
    int initialize_create_ret = EINVAL;
    int retry_read_entered_ret = EINVAL;
    int blocked_during_retry_ret = EINVAL;
    int initialize_join_ret = EINVAL;
    int terminal_entered_ret = EINVAL;
    int terminal_join_ret = EINVAL;
    size_t reads_before_initialization = SIZE_MAX;
    size_t publishes_before_initialization = SIZE_MAX;
    bool slot_before_initialization = false;
    bool hint_before_initialization = true;
    size_t reads_during_retry = SIZE_MAX;
    size_t publishes_during_retry = SIZE_MAX;
    bool slot_during_retry = false;
    bool hint_during_retry = true;
    bool ready_during_retry = true;

    if (terminal_create_ret == 0) {
        started_ret = primary_status_test_wait_for(
            &primary_status_test_terminal_worker_started
        );
        blocked_before_initialization_ret = primary_status_test_wait_for(
            &primary_status_test_terminal_publish_entered
        );
        {
            GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
            reads_before_initialization = primary_status_test_read_calls;
            publishes_before_initialization = primary_status_test_publish_calls;
            slot_before_initialization = primary_status_test_pending;
            hint_before_initialization = primary_status_test_hint_pending;
        }

        initialize_create_ret = pthread_create(
            &initialize_worker,
            NULL,
            primary_status_test_initialize_worker,
            NULL
        );
        if (initialize_create_ret == 0) {
            retry_read_entered_ret = primary_status_test_wait_for(
                &primary_status_test_retry_read_entered
            );
            blocked_during_retry_ret = primary_status_test_wait_for(
                &primary_status_test_terminal_publish_entered
            );
            {
                GG_MTX_SCOPE_GUARD(&listener_ready_mutex);
                ready_during_retry = listener_ready;
            }
            {
                GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
                reads_during_retry = primary_status_test_read_calls;
                publishes_during_retry = primary_status_test_publish_calls;
                slot_during_retry = primary_status_test_pending;
                hint_during_retry = primary_status_test_hint_pending;
                primary_status_test_release_retry_read = true;
                pthread_cond_broadcast(&primary_status_test_cond);
            }
            initialize_join_ret = pthread_join(initialize_worker, NULL);
        } else {
            publish_listener_ready();
        }
        terminal_entered_ret = primary_status_test_wait_for(
            &primary_status_test_terminal_publish_entered
        );
        terminal_join_ret = pthread_join(terminal_worker, NULL);
    }

    size_t reads_after_terminal = SIZE_MAX;
    size_t publishes_after_terminal = SIZE_MAX;
    size_t clears_after_terminal = SIZE_MAX;
    bool slot_after_terminal = true;
    bool hint_after_terminal = true;
    {
        GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
        reads_after_terminal = primary_status_test_read_calls;
        publishes_after_terminal = primary_status_test_publish_calls;
        clears_after_terminal = primary_status_test_clear_calls;
        slot_after_terminal = primary_status_test_pending;
        hint_after_terminal = primary_status_test_hint_pending;
    }

    if (terminal_join_ret == 0) {
        flush_pending_status();
    }

    size_t reads_after_flush = SIZE_MAX;
    size_t publishes_after_flush = SIZE_MAX;
    bool slot_after_flush = true;
    {
        GG_MTX_SCOPE_GUARD(&primary_status_test_mutex);
        reads_after_flush = primary_status_test_read_calls;
        publishes_after_flush = primary_status_test_publish_calls;
        slot_after_flush = primary_status_test_pending;
    }

    TEST_ASSERT_EQUAL_INT(0, terminal_create_ret);
    TEST_ASSERT_EQUAL_INT(0, started_ret);
    TEST_ASSERT_EQUAL_INT(ETIMEDOUT, blocked_before_initialization_ret);
    TEST_ASSERT_EQUAL_size_t(0, reads_before_initialization);
    TEST_ASSERT_EQUAL_size_t(0, publishes_before_initialization);
    TEST_ASSERT_TRUE(slot_before_initialization);
    TEST_ASSERT_FALSE(hint_before_initialization);
    TEST_ASSERT_EQUAL_INT(0, initialize_create_ret);
    TEST_ASSERT_EQUAL_INT(0, retry_read_entered_ret);
    TEST_ASSERT_EQUAL_INT(ETIMEDOUT, blocked_during_retry_ret);
    TEST_ASSERT_EQUAL_size_t(2, reads_during_retry);
    TEST_ASSERT_EQUAL_size_t(0, publishes_during_retry);
    TEST_ASSERT_TRUE(slot_during_retry);
    TEST_ASSERT_FALSE(hint_during_retry);
    TEST_ASSERT_FALSE(ready_during_retry);
    TEST_ASSERT_EQUAL_INT(0, initialize_join_ret);
    TEST_ASSERT_EQUAL_INT(0, terminal_entered_ret);
    TEST_ASSERT_EQUAL_INT(0, terminal_join_ret);
    TEST_ASSERT_EQUAL_INT(GG_ERR_OK, terminal_args.result);
    TEST_ASSERT_EQUAL_size_t(2, reads_after_terminal);
    TEST_ASSERT_EQUAL_size_t(1, publishes_after_terminal);
    TEST_ASSERT_EQUAL_size_t(1, clears_after_terminal);
    TEST_ASSERT_FALSE(slot_after_terminal);
    TEST_ASSERT_FALSE(hint_after_terminal);
    TEST_ASSERT_EQUAL_size_t(3, reads_after_flush);
    TEST_ASSERT_EQUAL_size_t(1, publishes_after_flush);
    TEST_ASSERT_FALSE(slot_after_flush);
}

// --- M8 control-flow regressions -----------------------------------------

GG_TEST_DEFINE(iot_jobs_notify_next_defers_describe_without_side_effects) {
    enqueue_job_test_reset();
    // A payload that would previously fail local JSON parsing must still just
    // schedule a describe and return OK, without parsing, enqueuing, or
    // publishing failure status on the dispatch thread.
    GgObject bogus = gg_obj_buf(GG_STR("not-a-subscribe-response"));
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_OK, next_job_execution_changed_callback(NULL, 0, bogus)
    );
    bool describe_scheduled;
    {
        GG_MTX_SCOPE_GUARD(&listener_mutex);
        describe_scheduled = needs_describe;
    }
    TEST_ASSERT_TRUE(describe_scheduled);
    TEST_ASSERT_EQUAL_size_t(0, enqueue_job_test_enqueue_calls);
    TEST_ASSERT_EQUAL_size_t(0, enqueue_job_test_clear_calls);
    TEST_ASSERT_EQUAL_size_t(0, enqueue_job_test_failure_calls);
}

// Seam stubs for the listener work-dispatch layer.
static GgError listener_work_test_describe_result;
static size_t listener_work_test_describe_calls;
static size_t listener_work_test_flush_calls;
static GgError listener_work_test_jobs_result;
static size_t listener_work_test_jobs_calls;
static GgError listener_work_test_connection_result;
static size_t listener_work_test_connection_calls;

static GgError listener_work_test_describe(void *ctx) {
    (void) ctx;
    listener_work_test_describe_calls += 1;
    return listener_work_test_describe_result;
}

static void listener_work_test_flush(void) {
    listener_work_test_flush_calls += 1;
}

static GgError listener_work_test_jobs_subscribe(void *ctx) {
    (void) ctx;
    listener_work_test_jobs_calls += 1;
    return listener_work_test_jobs_result;
}

static GgError listener_work_test_connection_subscribe(void *ctx) {
    (void) ctx;
    listener_work_test_connection_calls += 1;
    return listener_work_test_connection_result;
}

static void listener_work_test_install(void) {
    listener_work_test_describe_result = GG_ERR_OK;
    listener_work_test_describe_calls = 0;
    listener_work_test_flush_calls = 0;
    listener_work_test_jobs_result = GG_ERR_OK;
    listener_work_test_jobs_calls = 0;
    listener_work_test_connection_result = GG_ERR_OK;
    listener_work_test_connection_calls = 0;
    describe_for_listener = listener_work_test_describe;
    flush_for_listener = listener_work_test_flush;
    subscribe_jobs_for_listener = listener_work_test_jobs_subscribe;
    subscribe_connection_for_listener = listener_work_test_connection_subscribe;
    reset_listener_work_flags_for_test();
}

static void listener_work_test_restore(void) {
    describe_for_listener = describe_next_job;
    flush_for_listener = flush_pending_status;
    subscribe_jobs_for_listener = subscribe_to_next_job_topics;
    subscribe_connection_for_listener = subscribe_to_connection_status;
    reset_listener_work_flags_for_test();
}

GG_TEST_DEFINE(iot_jobs_flush_runs_when_describe_fails) {
    listener_work_test_install();
    listener_work_test_describe_result = GG_ERR_FAILURE;

    ListenerWork rearm
        = run_listener_work((ListenerWork) { .describe = true, .flush = true });

    TEST_ASSERT_EQUAL_size_t(1, listener_work_test_describe_calls);
    // Flush ran in the same cycle even though describe failed.
    TEST_ASSERT_EQUAL_size_t(1, listener_work_test_flush_calls);
    TEST_ASSERT_TRUE(rearm.describe);

    apply_listener_rearm(rearm);
    bool describe_rearmed;
    bool describe_retry_ready;
    {
        GG_MTX_SCOPE_GUARD(&listener_mutex);
        describe_rearmed = needs_describe;
        describe_retry_ready = listener_deadline_reached(describe_retry_after);
    }
    TEST_ASSERT_TRUE(describe_rearmed);
    // Re-armed after a bounded delay (future deadline): no tight spin.
    TEST_ASSERT_FALSE(describe_retry_ready);
    listener_work_test_restore();
}

GG_TEST_DEFINE(iot_jobs_immediate_describe_survives_delayed_rearm) {
    listener_work_test_install();
    // Describe fails, so run_listener_work asks to re-arm describe after a
    // bounded delay.
    listener_work_test_describe_result = GG_ERR_FAILURE;
    ListenerWork rearm = run_listener_work((ListenerWork) { .describe = true });
    TEST_ASSERT_TRUE(rearm.describe);

    // A fresh notify-next / reconnect lands during the work call: an immediate
    // (zero-deadline) describe is scheduled before the failure re-arm applies.
    schedule_describe_now();
    {
        bool describe_scheduled;
        bool describe_ready;
        {
            GG_MTX_SCOPE_GUARD(&listener_mutex);
            describe_scheduled = needs_describe;
            describe_ready = listener_deadline_reached(describe_retry_after);
        }
        TEST_ASSERT_TRUE(describe_scheduled);
        TEST_ASSERT_TRUE(describe_ready);
    }

    // Applying the delayed re-arm must NOT push out the fresh immediate
    // describe: the earlier (zero) deadline is preserved so the listener still
    // runs it now rather than after the bounded retry delay.
    apply_listener_rearm(rearm);
    {
        bool describe_scheduled;
        bool describe_ready;
        {
            GG_MTX_SCOPE_GUARD(&listener_mutex);
            describe_scheduled = needs_describe;
            describe_ready = listener_deadline_reached(describe_retry_after);
        }
        TEST_ASSERT_TRUE(describe_scheduled);
        TEST_ASSERT_TRUE(describe_ready);
    }
    listener_work_test_restore();
}

GG_TEST_DEFINE(iot_jobs_successful_describe_not_rearmed) {
    listener_work_test_install();

    ListenerWork rearm = run_listener_work((ListenerWork) { .describe = true });

    TEST_ASSERT_EQUAL_size_t(1, listener_work_test_describe_calls);
    TEST_ASSERT_FALSE(rearm.describe);
    apply_listener_rearm(rearm);
    bool describe_rearmed;
    {
        GG_MTX_SCOPE_GUARD(&listener_mutex);
        describe_rearmed = needs_describe;
    }
    TEST_ASSERT_FALSE(describe_rearmed);
    listener_work_test_restore();
}

GG_TEST_DEFINE(iot_jobs_failed_jobs_subscribe_rearms_only_jobs) {
    listener_work_test_install();
    listener_work_test_jobs_result = GG_ERR_FAILURE;
    listener_work_test_connection_result = GG_ERR_OK;

    ListenerWork rearm = run_listener_work((ListenerWork
    ) { .jobs_subscribe = true, .connection_subscribe = true });

    TEST_ASSERT_EQUAL_size_t(1, listener_work_test_jobs_calls);
    TEST_ASSERT_EQUAL_size_t(1, listener_work_test_connection_calls);
    TEST_ASSERT_TRUE(rearm.jobs_subscribe);
    TEST_ASSERT_FALSE(rearm.connection_subscribe);

    apply_listener_rearm(rearm);
    bool jobs_rearmed;
    bool connection_rearmed;
    bool jobs_retry_ready;
    {
        GG_MTX_SCOPE_GUARD(&listener_mutex);
        jobs_rearmed = needs_jobs_subscribe;
        connection_rearmed = needs_connection_subscribe;
        jobs_retry_ready
            = listener_deadline_reached(jobs_subscribe_retry_after);
    }
    TEST_ASSERT_TRUE(jobs_rearmed);
    TEST_ASSERT_FALSE(connection_rearmed);
    TEST_ASSERT_FALSE(jobs_retry_ready);
    listener_work_test_restore();
}

GG_TEST_DEFINE(iot_jobs_failed_connection_subscribe_rearms_only_connection) {
    listener_work_test_install();
    listener_work_test_jobs_result = GG_ERR_OK;
    listener_work_test_connection_result = GG_ERR_FAILURE;

    ListenerWork rearm = run_listener_work((ListenerWork
    ) { .jobs_subscribe = true, .connection_subscribe = true });

    TEST_ASSERT_FALSE(rearm.jobs_subscribe);
    TEST_ASSERT_TRUE(rearm.connection_subscribe);

    apply_listener_rearm(rearm);
    bool jobs_rearmed;
    bool connection_rearmed;
    {
        GG_MTX_SCOPE_GUARD(&listener_mutex);
        jobs_rearmed = needs_jobs_subscribe;
        connection_rearmed = needs_connection_subscribe;
    }
    TEST_ASSERT_FALSE(jobs_rearmed);
    TEST_ASSERT_TRUE(connection_rearmed);
    listener_work_test_restore();
}

GG_TEST_DEFINE(iot_jobs_jobs_close_schedules_only_jobs_subscribe) {
    reset_listener_work_flags_for_test();
    jobs_subscription_closed(NULL, 0);
    bool jobs_scheduled;
    bool connection_scheduled;
    bool describe_scheduled;
    {
        GG_MTX_SCOPE_GUARD(&listener_mutex);
        jobs_scheduled = needs_jobs_subscribe;
        connection_scheduled = needs_connection_subscribe;
        describe_scheduled = needs_describe;
    }
    TEST_ASSERT_TRUE(jobs_scheduled);
    TEST_ASSERT_FALSE(connection_scheduled);
    TEST_ASSERT_FALSE(describe_scheduled);
}

GG_TEST_DEFINE(iot_jobs_connection_close_schedules_only_connection_subscribe) {
    reset_listener_work_flags_for_test();
    connection_subscription_closed(NULL, 0);
    bool connection_scheduled;
    bool jobs_scheduled;
    bool describe_scheduled;
    {
        GG_MTX_SCOPE_GUARD(&listener_mutex);
        connection_scheduled = needs_connection_subscribe;
        jobs_scheduled = needs_jobs_subscribe;
        describe_scheduled = needs_describe;
    }
    TEST_ASSERT_TRUE(connection_scheduled);
    TEST_ASSERT_FALSE(jobs_scheduled);
    TEST_ASSERT_FALSE(describe_scheduled);
}

GG_TEST_DEFINE(iot_jobs_repeated_jobs_close_does_not_schedule_connection) {
    reset_listener_work_flags_for_test();
    // Repeated jobs-subscription closes must never touch the connection-status
    // subscription, so the loop never recreates a second live one.
    jobs_subscription_closed(NULL, 0);
    jobs_subscription_closed(NULL, 0);
    jobs_subscription_closed(NULL, 0);
    bool jobs_scheduled;
    bool connection_scheduled;
    {
        GG_MTX_SCOPE_GUARD(&listener_mutex);
        jobs_scheduled = needs_jobs_subscribe;
        connection_scheduled = needs_connection_subscribe;
    }
    TEST_ASSERT_TRUE(jobs_scheduled);
    TEST_ASSERT_FALSE(connection_scheduled);
}

// Seam stubs for the underlying subscribe APIs, capturing the registered
// callbacks so the test can confirm each subscription's close callback.
static GglSubscribeCallback subscribe_reg_test_mqtt_on_response;
static GglSubscribeCloseCallback subscribe_reg_test_mqtt_on_close;
static size_t subscribe_reg_test_mqtt_calls;
static GglSubscribeCallback subscribe_reg_test_bus_on_response;
static GglSubscribeCloseCallback subscribe_reg_test_bus_on_close;
static size_t subscribe_reg_test_bus_calls;

static GgError subscribe_reg_test_mqtt(
    GgBuffer socket_name,
    GgBufList topic_filters,
    uint8_t qos,
    bool virtual,
    GglSubscribeCallback on_response,
    GglSubscribeCloseCallback on_close,
    void *ctx,
    uint32_t *handle
) {
    (void) socket_name;
    (void) topic_filters;
    (void) qos;
    (void) virtual;
    (void) ctx;
    (void) handle;
    subscribe_reg_test_mqtt_on_response = on_response;
    subscribe_reg_test_mqtt_on_close = on_close;
    subscribe_reg_test_mqtt_calls += 1;
    return GG_ERR_OK;
}

static GgError subscribe_reg_test_bus(
    GgBuffer interface,
    GgBuffer method,
    GgMap params,
    GglSubscribeCallback on_response,
    GglSubscribeCloseCallback on_close,
    void *ctx,
    GgError *error,
    uint32_t *handle
) {
    (void) interface;
    (void) method;
    (void) params;
    (void) ctx;
    (void) error;
    (void) handle;
    subscribe_reg_test_bus_on_response = on_response;
    subscribe_reg_test_bus_on_close = on_close;
    subscribe_reg_test_bus_calls += 1;
    return GG_ERR_OK;
}

GG_TEST_DEFINE(iot_jobs_subscriptions_register_matching_close_callbacks) {
    reset_thing_name_for_test();
    thing_name_test_value = GG_STR("owned-thing");
    thing_name_config_reader = thing_name_test_reader;
    GG_TEST_ASSERT_OK(get_thing_name(NULL));

    subscribe_reg_test_mqtt_calls = 0;
    subscribe_reg_test_bus_calls = 0;
    subscribe_reg_test_mqtt_on_response = NULL;
    subscribe_reg_test_mqtt_on_close = NULL;
    subscribe_reg_test_bus_on_response = NULL;
    subscribe_reg_test_bus_on_close = NULL;
    mqtt_subscribe_seam = subscribe_reg_test_mqtt;
    bus_subscribe_seam = subscribe_reg_test_bus;

    GG_TEST_ASSERT_OK(subscribe_to_next_job_topics(NULL));
    TEST_ASSERT_EQUAL_size_t(1, subscribe_reg_test_mqtt_calls);
    TEST_ASSERT_TRUE(
        subscribe_reg_test_mqtt_on_response
        == next_job_execution_changed_callback
    );
    TEST_ASSERT_TRUE(
        subscribe_reg_test_mqtt_on_close == jobs_subscription_closed
    );

    GG_TEST_ASSERT_OK(subscribe_to_connection_status(NULL));
    TEST_ASSERT_EQUAL_size_t(1, subscribe_reg_test_bus_calls);
    TEST_ASSERT_TRUE(
        subscribe_reg_test_bus_on_response == iot_jobs_on_reconnect
    );
    TEST_ASSERT_TRUE(
        subscribe_reg_test_bus_on_close == connection_subscription_closed
    );

    mqtt_subscribe_seam = ggl_aws_iot_mqtt_subscribe;
    bus_subscribe_seam = ggl_subscribe;
    thing_name_config_reader = ggl_gg_config_read_str;
}

// Restore every listener seam to its production function and clear the shared
// listener flags, retry deadlines, and periodic-flush window. Unity's
// binary-wide tearDown (in component_store.c) calls this after every test --
// including when an assertion aborts via longjmp before a test's own restore
// runs -- so a mutable seam installed by one test can never leak into the next.
// Tests therefore re-install any seam they need at their start.
void iot_jobs_reset_test_seams(void) {
    thing_name_config_reader = ggl_gg_config_read_str;
    iot_call_for_jobs = ggl_aws_iot_call;
    pending_status_read_for_jobs = status_keeper_read;
    pending_status_persist_for_jobs = status_keeper_persist;
    pending_status_clear_for_jobs = status_keeper_clear;
    save_iot_jobs_id_for_update = save_iot_jobs_id;
    enqueue_for_job = ggl_deployment_enqueue;
    clear_status_for_job_enqueue = status_keeper_clear;
    report_job_enqueue_failure = update_job;
    describe_for_listener = describe_next_job;
    flush_for_listener = flush_pending_status;
    subscribe_jobs_for_listener = subscribe_to_next_job_topics;
    subscribe_connection_for_listener = subscribe_to_connection_status;
    mqtt_subscribe_seam = ggl_aws_iot_mqtt_subscribe;
    bus_subscribe_seam = ggl_subscribe;
    primary_status_test_reset();
    reset_listener_work_flags_for_test();
    {
        GG_MTX_SCOPE_GUARD(&listener_ready_mutex);
        listener_ready = false;
    }
}

void iot_jobs_override_test_seam_for_reset_test(void) {
    iot_call_for_jobs = NULL;
    GG_MTX_SCOPE_GUARD(&listener_ready_mutex);
    listener_ready = true;
}

bool iot_jobs_test_seams_are_reset(void) {
    bool ready;
    {
        GG_MTX_SCOPE_GUARD(&listener_ready_mutex);
        ready = listener_ready;
    }
    return !ready && (thing_name_config_reader == ggl_gg_config_read_str)
        && (iot_call_for_jobs == ggl_aws_iot_call)
        && (pending_status_read_for_jobs == status_keeper_read)
        && (pending_status_persist_for_jobs == status_keeper_persist)
        && (pending_status_clear_for_jobs == status_keeper_clear)
        && (save_iot_jobs_id_for_update == save_iot_jobs_id)
        && (enqueue_for_job == ggl_deployment_enqueue)
        && (clear_status_for_job_enqueue == status_keeper_clear)
        && (report_job_enqueue_failure == update_job)
        && (describe_for_listener == describe_next_job)
        && (flush_for_listener == flush_pending_status)
        && (subscribe_jobs_for_listener == subscribe_to_next_job_topics)
        && (subscribe_connection_for_listener == subscribe_to_connection_status)
        && (mqtt_subscribe_seam == ggl_aws_iot_mqtt_subscribe)
        && (bus_subscribe_seam == ggl_subscribe);
}

// The periodic status-flush window is armed once and only becomes due at its
// own deadline; an earlier subscribe/describe retry firing or a spurious wake
// (both modeled as passes at times before the deadline) must not make it due.
GG_TEST_DEFINE(iot_jobs_periodic_flush_waits_for_its_own_deadline) {
    bool armed = false;
    struct timespec retry_after = { 0 };

    // First pending pass arms the STATUS_FLUSH_RETRY_SECONDS window; not due.
    struct timespec now = { .tv_sec = 1000, .tv_nsec = 0 };
    TEST_ASSERT_FALSE(status_flush_periodic_due(true, now, &armed, &retry_after)
    );
    TEST_ASSERT_TRUE(armed);
    TEST_ASSERT_EQUAL_INT64(
        (int64_t) (1000 + STATUS_FLUSH_RETRY_SECONDS),
        (int64_t) retry_after.tv_sec
    );

    // An earlier retry deadline firing, a signal, or a spurious wake lands well
    // before the deadline: still not due, and the deadline is not pushed out.
    struct timespec early = { .tv_sec = 1010, .tv_nsec = 0 };
    TEST_ASSERT_FALSE(
        status_flush_periodic_due(true, early, &armed, &retry_after)
    );
    TEST_ASSERT_EQUAL_INT64(
        (int64_t) (1000 + STATUS_FLUSH_RETRY_SECONDS),
        (int64_t) retry_after.tv_sec
    );

    // One nanosecond short of the deadline is still not due.
    struct timespec just_before = {
        .tv_sec = 1000 + STATUS_FLUSH_RETRY_SECONDS - 1,
        .tv_nsec = 999999999,
    };
    TEST_ASSERT_FALSE(
        status_flush_periodic_due(true, just_before, &armed, &retry_after)
    );

    // Reaching the exact deadline makes the periodic flush due.
    struct timespec at_deadline = {
        .tv_sec = 1000 + STATUS_FLUSH_RETRY_SECONDS,
        .tv_nsec = 0,
    };
    TEST_ASSERT_TRUE(
        status_flush_periodic_due(true, at_deadline, &armed, &retry_after)
    );
}

// When nothing is pending the window disarms so a delivered status cannot leave
// a stale deadline; a later pending status re-arms relative to the new time,
// keeping periodic retries STATUS_FLUSH_RETRY_SECONDS apart.
GG_TEST_DEFINE(iot_jobs_periodic_flush_disarms_and_rearms) {
    bool armed = false;
    struct timespec retry_after = { 0 };

    struct timespec now = { .tv_sec = 5000, .tv_nsec = 0 };
    (void) status_flush_periodic_due(true, now, &armed, &retry_after);
    TEST_ASSERT_TRUE(armed);

    // Status delivered: window disarms and the deadline is cleared.
    TEST_ASSERT_FALSE(
        status_flush_periodic_due(false, now, &armed, &retry_after)
    );
    TEST_ASSERT_FALSE(armed);
    TEST_ASSERT_EQUAL_INT64(0, (int64_t) retry_after.tv_sec);
    TEST_ASSERT_EQUAL_INT64(0, (int64_t) retry_after.tv_nsec);

    // A new pending status re-arms relative to the new time and is not due yet.
    struct timespec later = { .tv_sec = 5120, .tv_nsec = 0 };
    TEST_ASSERT_FALSE(
        status_flush_periodic_due(true, later, &armed, &retry_after)
    );
    TEST_ASSERT_TRUE(armed);
    TEST_ASSERT_EQUAL_INT64(
        (int64_t) (5120 + STATUS_FLUSH_RETRY_SECONDS),
        (int64_t) retry_after.tv_sec
    );
}

// A reconnect requests an immediate describe and flush; capture_listener_work
// treats needs_flush as ready on the next pass regardless of the periodic
// deadline, so a re-established connection re-sends a pending status at once.
GG_TEST_DEFINE(iot_jobs_reconnect_requests_immediate_flush) {
    reset_listener_work_flags_for_test();

    GG_TEST_ASSERT_OK(iot_jobs_on_reconnect(NULL, 0, gg_obj_bool(true)));

    bool flush_scheduled;
    bool describe_scheduled;
    bool describe_ready;
    {
        GG_MTX_SCOPE_GUARD(&listener_mutex);
        flush_scheduled = needs_flush;
        describe_scheduled = needs_describe;
        describe_ready = listener_deadline_reached(describe_retry_after);
    }
    TEST_ASSERT_TRUE(flush_scheduled);
    TEST_ASSERT_TRUE(describe_scheduled);
    TEST_ASSERT_TRUE(describe_ready);
}

// Finding 2: seams and work state installed by a test survive an assertion
// longjmp, so the binary-wide tearDown must restore them. Simulate the leaked
// state directly, then confirm iot_jobs_reset_test_seams() (what tearDown
// calls) restores every seam to production and clears the flags/deadlines.
GG_TEST_DEFINE(iot_jobs_reset_test_seams_restores_production) {
    thing_name_config_reader = NULL;
    iot_call_for_jobs = NULL;
    pending_status_read_for_jobs = NULL;
    pending_status_persist_for_jobs = NULL;
    pending_status_clear_for_jobs = NULL;
    save_iot_jobs_id_for_update = NULL;
    enqueue_for_job = NULL;
    clear_status_for_job_enqueue = NULL;
    report_job_enqueue_failure = NULL;
    describe_for_listener = NULL;
    flush_for_listener = NULL;
    subscribe_jobs_for_listener = NULL;
    subscribe_connection_for_listener = NULL;
    mqtt_subscribe_seam = NULL;
    bus_subscribe_seam = NULL;
    {
        GG_MTX_SCOPE_GUARD(&listener_mutex);
        needs_describe = true;
        needs_flush = true;
        needs_jobs_subscribe = true;
        needs_connection_subscribe = true;
        status_flush_retry_armed = true;
        status_flush_retry_after
            = (struct timespec) { .tv_sec = 1, .tv_nsec = 2 };
    }
    {
        GG_MTX_SCOPE_GUARD(&listener_ready_mutex);
        listener_ready = true;
    }

    iot_jobs_reset_test_seams();

    TEST_ASSERT_TRUE(thing_name_config_reader == ggl_gg_config_read_str);
    TEST_ASSERT_TRUE(iot_call_for_jobs == ggl_aws_iot_call);
    TEST_ASSERT_TRUE(pending_status_read_for_jobs == status_keeper_read);
    TEST_ASSERT_TRUE(pending_status_persist_for_jobs == status_keeper_persist);
    TEST_ASSERT_TRUE(pending_status_clear_for_jobs == status_keeper_clear);
    TEST_ASSERT_TRUE(save_iot_jobs_id_for_update == save_iot_jobs_id);
    TEST_ASSERT_TRUE(enqueue_for_job == ggl_deployment_enqueue);
    TEST_ASSERT_TRUE(clear_status_for_job_enqueue == status_keeper_clear);
    TEST_ASSERT_TRUE(report_job_enqueue_failure == update_job);
    TEST_ASSERT_TRUE(describe_for_listener == describe_next_job);
    TEST_ASSERT_TRUE(flush_for_listener == flush_pending_status);
    TEST_ASSERT_TRUE(
        subscribe_jobs_for_listener == subscribe_to_next_job_topics
    );
    TEST_ASSERT_TRUE(
        subscribe_connection_for_listener == subscribe_to_connection_status
    );
    TEST_ASSERT_TRUE(mqtt_subscribe_seam == ggl_aws_iot_mqtt_subscribe);
    TEST_ASSERT_TRUE(bus_subscribe_seam == ggl_subscribe);

    bool describe_pending;
    bool flush_pending;
    bool jobs_subscribe_pending;
    bool connection_subscribe_pending;
    bool flush_retry_armed;
    bool ready_after_reset;

    struct timespec flush_retry_after;
    {
        GG_MTX_SCOPE_GUARD(&listener_ready_mutex);
        ready_after_reset = listener_ready;
    }

    {
        GG_MTX_SCOPE_GUARD(&listener_mutex);
        describe_pending = needs_describe;
        flush_pending = needs_flush;
        jobs_subscribe_pending = needs_jobs_subscribe;
        connection_subscribe_pending = needs_connection_subscribe;
        flush_retry_armed = status_flush_retry_armed;
        flush_retry_after = status_flush_retry_after;
    }

    TEST_ASSERT_FALSE(ready_after_reset);
    TEST_ASSERT_FALSE(describe_pending);
    TEST_ASSERT_FALSE(flush_pending);
    TEST_ASSERT_FALSE(jobs_subscribe_pending);
    TEST_ASSERT_FALSE(connection_subscribe_pending);
    TEST_ASSERT_FALSE(flush_retry_armed);
    TEST_ASSERT_EQUAL_INT64(0, (int64_t) flush_retry_after.tv_sec);
    TEST_ASSERT_EQUAL_INT64(0, (int64_t) flush_retry_after.tv_nsec);
}

#endif
