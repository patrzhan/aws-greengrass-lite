// aws-greengrass-lite - AWS IoT Greengrass runtime for constrained devices
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#include "iotcored_instance.h"
#include <assert.h>
#include <errno.h>
#include <gg/arena.h>
#include <gg/buffer.h>
#include <gg/cleanup.h>
#include <gg/error.h>
#include <gg/log.h>
#include <gg/utils.h>
#include <ggl/core_bus/aws_iot_mqtt.h>
#include <ggl/core_bus/client.h>
#include <ggl/core_bus/gg_config.h>
#include <ggl/process.h>
#include <limits.h>
#include <pthread.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

// TODO: Remove retry by pre-creating the socket before spawn (socket
// activation).
#define SUBSCRIBE_RETRY_INTERVAL_MS 500
#define IOTCORED_INSTANCE_NAME "iotcoreddeploy"
#define MAX_ENDPOINT_LEN 128
#define MAX_THING_NAME_LEN 128
// MQTT client ID: thingName + suffix. Must match IoT policy pattern thingName*
// and stay within 128-byte MQTT client ID limit.
// TODO: Use a dynamic suffix if multiple iotcored instances are needed
// concurrently.
#define MAX_CLIENT_ID_LEN 128
#define CLIENT_ID_SUFFIX "#endpoint-switch"

typedef struct {
    pthread_mutex_t mtx;
    pthread_cond_t cond;
    bool connected;
} ConnectionCtx;

static GgError connection_status_callback(
    void *ctx, uint32_t handle, GgObject data
) {
    (void) handle;
    ConnectionCtx *conn_ctx = ctx;

    bool connected = false;
    GgError ret = ggl_aws_iot_mqtt_connection_status_parse(data, &connected);
    if (ret != GG_ERR_OK) {
        return ret;
    }

    if (connected) {
        GG_MTX_SCOPE_GUARD(&conn_ctx->mtx);
        conn_ctx->connected = true;
        pthread_cond_signal(&conn_ctx->cond);
    }

    return GG_ERR_OK;
}

#ifdef GG_SDK_TESTING

#include <gg/test.h>
#include <unity.h>

static GgError (*instance_config_read_str)(GgBufList, GgArena *, GgBuffer *)
    = ggl_gg_config_read_str;
static GgError (*instance_process_spawn)(const char *const[], const GglProcessSpawnConfig *, GglProcessHandle *)
    = ggl_process_spawn;

#define INSTANCE_CONFIG_READ_STR instance_config_read_str
#define INSTANCE_PROCESS_SPAWN instance_process_spawn

static pthread_mutex_t instance_start_test_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t instance_start_test_cond = PTHREAD_COND_INITIALIZER;
static uintptr_t instance_start_test_arena_addrs[2];
static char instance_start_test_client_ids[2][MAX_CLIENT_ID_LEN + 1];
static size_t instance_start_test_config_calls;
static size_t instance_start_test_spawn_calls;
static bool instance_start_test_abort;

typedef struct InstanceStartTestArgs {
    IotcoredInstance instance;
    GgError result;
} InstanceStartTestArgs;

static GgError instance_start_test_config_read(
    GgBufList key_path, GgArena *alloc, GgBuffer *result
) {
    (void) key_path;

    pthread_mutex_lock(&instance_start_test_mutex);
    size_t index = instance_start_test_config_calls;
    if (index >= 2) {
        pthread_mutex_unlock(&instance_start_test_mutex);
        return GG_ERR_RANGE;
    }

    GgBuffer value = (index == 0) ? GG_STR("thing-one") : GG_STR("thing-two");
    GgError ret = gg_arena_claim_buf(&value, alloc);
    if (ret != GG_ERR_OK) {
        pthread_mutex_unlock(&instance_start_test_mutex);
        return ret;
    }

    instance_start_test_arena_addrs[index] = (uintptr_t) alloc->mem;
    instance_start_test_config_calls += 1;
    pthread_cond_broadcast(&instance_start_test_cond);
    while ((instance_start_test_config_calls < 2) && !instance_start_test_abort
    ) {
        pthread_cond_wait(
            &instance_start_test_cond, &instance_start_test_mutex
        );
    }
    bool aborted = instance_start_test_abort;
    pthread_mutex_unlock(&instance_start_test_mutex);

    if (aborted) {
        return GG_ERR_FAILURE;
    }
    *result = value;
    return GG_ERR_OK;
}

static GgError instance_start_test_spawn(
    const char *const argv[],
    const GglProcessSpawnConfig *config,
    GglProcessHandle *handle
) {
    (void) config;

    pthread_mutex_lock(&instance_start_test_mutex);
    size_t index = instance_start_test_spawn_calls;
    if (index >= 2) {
        pthread_mutex_unlock(&instance_start_test_mutex);
        return GG_ERR_RANGE;
    }
    snprintf(
        instance_start_test_client_ids[index],
        sizeof(instance_start_test_client_ids[index]),
        "%s",
        argv[6]
    );
    instance_start_test_spawn_calls += 1;
    handle->val = (int32_t) instance_start_test_spawn_calls;
    pthread_mutex_unlock(&instance_start_test_mutex);
    return GG_ERR_OK;
}

static void *instance_start_test_worker(void *ctx) {
    InstanceStartTestArgs *args = ctx;
    args->result = iotcored_instance_start(
        &args->instance, GG_STR("/bin/iotcored"), GG_STR("endpoint")
    );
    return NULL;
}

static void instance_start_test_reset(void) {
    pthread_mutex_lock(&instance_start_test_mutex);
    memset(
        instance_start_test_arena_addrs,
        0,
        sizeof(instance_start_test_arena_addrs)
    );
    memset(
        instance_start_test_client_ids,
        0,
        sizeof(instance_start_test_client_ids)
    );
    instance_start_test_config_calls = 0;
    instance_start_test_spawn_calls = 0;
    instance_start_test_abort = false;
    pthread_mutex_unlock(&instance_start_test_mutex);

    instance_config_read_str = instance_start_test_config_read;
    instance_process_spawn = instance_start_test_spawn;
}

static void instance_start_test_restore(void) {
    instance_config_read_str = ggl_gg_config_read_str;
    instance_process_spawn = ggl_process_spawn;
}

GG_TEST_DEFINE(iotcored_instance_concurrent_starts_use_distinct_thing_names) {
    instance_start_test_reset();

    InstanceStartTestArgs args[2] = {
        { .result = GG_ERR_FAILURE },
        { .result = GG_ERR_FAILURE },
    };
    pthread_t workers[2];
    int create_results[2] = { ECANCELED, ECANCELED };
    int join_results[2] = { ECANCELED, ECANCELED };

    create_results[0] = pthread_create(
        &workers[0], NULL, instance_start_test_worker, &args[0]
    );
    if (create_results[0] == 0) {
        create_results[1] = pthread_create(
            &workers[1], NULL, instance_start_test_worker, &args[1]
        );
        if (create_results[1] != 0) {
            pthread_mutex_lock(&instance_start_test_mutex);
            instance_start_test_abort = true;
            pthread_cond_broadcast(&instance_start_test_cond);
            pthread_mutex_unlock(&instance_start_test_mutex);
        }
    }

    if (create_results[0] == 0) {
        join_results[0] = pthread_join(workers[0], NULL);
    }
    if (create_results[1] == 0) {
        join_results[1] = pthread_join(workers[1], NULL);
    }

    instance_start_test_restore();

    bool saw_thing_one = false;
    bool saw_thing_two = false;
    for (size_t i = 0; i < 2; i++) {
        saw_thing_one |= strcmp(
                             instance_start_test_client_ids[i],
                             "thing-one" CLIENT_ID_SUFFIX
                         )
            == 0;
        saw_thing_two |= strcmp(
                             instance_start_test_client_ids[i],
                             "thing-two" CLIENT_ID_SUFFIX
                         )
            == 0;
    }

    TEST_ASSERT_EQUAL_INT(0, create_results[0]);
    TEST_ASSERT_EQUAL_INT(0, create_results[1]);
    TEST_ASSERT_EQUAL_INT(0, join_results[0]);
    TEST_ASSERT_EQUAL_INT(0, join_results[1]);
    TEST_ASSERT_EQUAL_INT(GG_ERR_OK, args[0].result);
    TEST_ASSERT_EQUAL_INT(GG_ERR_OK, args[1].result);
    TEST_ASSERT_EQUAL_size_t(2, instance_start_test_config_calls);
    TEST_ASSERT_EQUAL_size_t(2, instance_start_test_spawn_calls);
    TEST_ASSERT_NOT_EQUAL_UINT64(0, instance_start_test_arena_addrs[0]);
    TEST_ASSERT_NOT_EQUAL_UINT64(0, instance_start_test_arena_addrs[1]);
    TEST_ASSERT_NOT_EQUAL_UINT64(
        instance_start_test_arena_addrs[0], instance_start_test_arena_addrs[1]
    );
    TEST_ASSERT_TRUE(saw_thing_one);
    TEST_ASSERT_TRUE(saw_thing_two);
}

#else

#define INSTANCE_CONFIG_READ_STR ggl_gg_config_read_str
#define INSTANCE_PROCESS_SPAWN ggl_process_spawn

#endif

GgError iotcored_instance_start(
    IotcoredInstance *ctx, GgBuffer iotcored_path, GgBuffer endpoint
) {
    memset(ctx, 0, sizeof(*ctx));
    ctx->handle = (GglProcessHandle) { -1 };

    uint8_t thing_name_mem[MAX_THING_NAME_LEN + 1];
    GgArena alloc = gg_arena_init(
        gg_buffer_substr(GG_BUF(thing_name_mem), 0, MAX_THING_NAME_LEN)
    );
    GgBuffer thing_name = { 0 };
    GgError ret = INSTANCE_CONFIG_READ_STR(
        GG_BUF_LIST(GG_STR("system"), GG_STR("thingName")), &alloc, &thing_name
    );
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to read thingName.");
        return ret;
    }
    char client_id[MAX_CLIENT_ID_LEN + 1];
    int client_id_len = snprintf(
        client_id,
        sizeof(client_id),
        "%.*s" CLIENT_ID_SUFFIX,
        (int) thing_name.len,
        thing_name.data
    );
    if (client_id_len >= (int) sizeof(client_id)) {
        GG_LOGE(
            "MQTT client ID %.*s" CLIENT_ID_SUFFIX " exceeds 128-byte limit.",
            (int) thing_name.len,
            thing_name.data
        );
        return GG_ERR_RANGE;
    }

    char endpoint_buf[MAX_ENDPOINT_LEN + 1];
    if (endpoint.len >= sizeof(endpoint_buf)) {
        GG_LOGE("Endpoint too long: %.*s.", (int) endpoint.len, endpoint.data);
        return GG_ERR_RANGE;
    }
    memcpy(endpoint_buf, endpoint.data, endpoint.len);
    endpoint_buf[endpoint.len] = '\0';

    char path_buf[PATH_MAX];
    if (iotcored_path.len >= sizeof(path_buf)) {
        GG_LOGE(
            "iotcored path too long: %.*s.",
            (int) iotcored_path.len,
            iotcored_path.data
        );
        return GG_ERR_RANGE;
    }
    memcpy(path_buf, iotcored_path.data, iotcored_path.len);
    path_buf[iotcored_path.len] = '\0';

    const char *args[] = {
        path_buf,  "-n", IOTCORED_INSTANCE_NAME, "-e", endpoint_buf, "-i",
        client_id, NULL,
    };

    ret = INSTANCE_PROCESS_SPAWN(args, NULL, &ctx->handle);
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to spawn iotcored instance.");
        ctx->handle = (GglProcessHandle) { -1 };
        return ret;
    }

    GG_LOGD("Spawned iotcored instance (pid=%d).", ctx->handle.val);
    return GG_ERR_OK;
}

#ifdef GG_SDK_TESTING

static GgError (*connection_subscribe)(GgBuffer, GglSubscribeCallback, GglSubscribeCloseCallback, void *, uint32_t *)
    = ggl_aws_iot_mqtt_connection_status;
static void (*connection_sub_close)(uint32_t) = ggl_client_sub_close;
static GgError (*connection_sleep_ms)(int64_t) = gg_sleep_ms;
static int (*connection_clock_gettime)(clockid_t, struct timespec *)
    = clock_gettime;
static int (*connection_mutex_init)(pthread_mutex_t *, const pthread_mutexattr_t *)
    = pthread_mutex_init;
static int (*connection_mutex_destroy)(pthread_mutex_t *)
    = pthread_mutex_destroy;
static int (*connection_cond_init)(pthread_cond_t *, const pthread_condattr_t *)
    = pthread_cond_init;
static int (*connection_cond_destroy)(pthread_cond_t *) = pthread_cond_destroy;
static int (*connection_cond_timedwait)(pthread_cond_t *, pthread_mutex_t *, const struct timespec *)
    = pthread_cond_timedwait;

void iotcored_instance_reset_test_seams(void);
void iotcored_instance_override_test_seam_for_reset_test(void);
bool iotcored_instance_test_seams_are_reset(void);

void iotcored_instance_reset_test_seams(void) {
    instance_config_read_str = ggl_gg_config_read_str;
    instance_process_spawn = ggl_process_spawn;
    connection_subscribe = ggl_aws_iot_mqtt_connection_status;
    connection_sub_close = ggl_client_sub_close;
    connection_sleep_ms = gg_sleep_ms;
    connection_clock_gettime = clock_gettime;
    connection_mutex_init = pthread_mutex_init;
    connection_mutex_destroy = pthread_mutex_destroy;
    connection_cond_init = pthread_cond_init;
    connection_cond_destroy = pthread_cond_destroy;
    connection_cond_timedwait = pthread_cond_timedwait;
}

void iotcored_instance_override_test_seam_for_reset_test(void) {
    instance_config_read_str = instance_start_test_config_read;
}

bool iotcored_instance_test_seams_are_reset(void) {
    return (instance_config_read_str == ggl_gg_config_read_str)
        && (instance_process_spawn == ggl_process_spawn)
        && (connection_subscribe == ggl_aws_iot_mqtt_connection_status)
        && (connection_sub_close == ggl_client_sub_close)
        && (connection_sleep_ms == gg_sleep_ms)
        && (connection_clock_gettime == clock_gettime)
        && (connection_mutex_init == pthread_mutex_init)
        && (connection_mutex_destroy == pthread_mutex_destroy)
        && (connection_cond_init == pthread_cond_init)
        && (connection_cond_destroy == pthread_cond_destroy)
        && (connection_cond_timedwait == pthread_cond_timedwait);
}

#define CONNECTION_SUBSCRIBE connection_subscribe
#define CONNECTION_SUB_CLOSE connection_sub_close
#define CONNECTION_SLEEP_MS connection_sleep_ms
#define CONNECTION_CLOCK_GETTIME connection_clock_gettime
#define CONNECTION_MUTEX_INIT connection_mutex_init
#define CONNECTION_MUTEX_DESTROY connection_mutex_destroy
#define CONNECTION_COND_INIT connection_cond_init
#define CONNECTION_COND_DESTROY connection_cond_destroy
#define CONNECTION_COND_TIMEDWAIT connection_cond_timedwait

#else

#define CONNECTION_SUBSCRIBE ggl_aws_iot_mqtt_connection_status
#define CONNECTION_SUB_CLOSE ggl_client_sub_close
#define CONNECTION_SLEEP_MS gg_sleep_ms
#define CONNECTION_CLOCK_GETTIME clock_gettime
#define CONNECTION_MUTEX_INIT pthread_mutex_init
#define CONNECTION_MUTEX_DESTROY pthread_mutex_destroy
#define CONNECTION_COND_INIT pthread_cond_init
#define CONNECTION_COND_DESTROY pthread_cond_destroy
#define CONNECTION_COND_TIMEDWAIT pthread_cond_timedwait

#endif

static GgError connection_ctx_init(ConnectionCtx *ctx) {
    *ctx = (ConnectionCtx) { 0 };
    if (CONNECTION_MUTEX_INIT(&ctx->mtx, NULL) != 0) {
        return GG_ERR_FAILURE;
    }

    pthread_condattr_t attr;
    if (pthread_condattr_init(&attr) != 0) {
        (void) CONNECTION_MUTEX_DESTROY(&ctx->mtx);
        return GG_ERR_FAILURE;
    }
    int ret = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
    if (ret == 0) {
        ret = CONNECTION_COND_INIT(&ctx->cond, &attr);
    }
    pthread_condattr_destroy(&attr);
    if (ret != 0) {
        (void) CONNECTION_MUTEX_DESTROY(&ctx->mtx);
        return GG_ERR_FAILURE;
    }
    return GG_ERR_OK;
}

static void connection_ctx_destroy(ConnectionCtx *ctx) {
    (void) CONNECTION_COND_DESTROY(&ctx->cond);
    (void) CONNECTION_MUTEX_DESTROY(&ctx->mtx);
}

GgError iotcored_await_connection(GgBuffer socket_name, uint32_t timeout_s) {
    ConnectionCtx ctx;
    GgError result = connection_ctx_init(&ctx);
    if (result != GG_ERR_OK) {
        return result;
    }

    struct timespec deadline;
    CONNECTION_CLOCK_GETTIME(CLOCK_MONOTONIC, &deadline);
    deadline.tv_sec += timeout_s;

    uint32_t sub_handle = 0;
    bool subscribed = false;

    while (true) {
        struct timespec now;
        CONNECTION_CLOCK_GETTIME(CLOCK_MONOTONIC, &now);
        if ((now.tv_sec > deadline.tv_sec)
            || ((now.tv_sec == deadline.tv_sec)
                && (now.tv_nsec >= deadline.tv_nsec))) {
            result = GG_ERR_FAILURE;
            goto cleanup;
        }

        GgError ret = CONNECTION_SUBSCRIBE(
            socket_name, connection_status_callback, NULL, &ctx, &sub_handle
        );
        if (ret == GG_ERR_OK) {
            subscribed = true;
            break;
        }

        (void) CONNECTION_SLEEP_MS(SUBSCRIBE_RETRY_INTERVAL_MS);
    }

    bool timed_out = false;
    {
        GG_MTX_SCOPE_GUARD(&ctx.mtx);

        while (!ctx.connected) {
            int cond_ret
                = CONNECTION_COND_TIMEDWAIT(&ctx.cond, &ctx.mtx, &deadline);
            if ((cond_ret != 0) && (cond_ret != EINTR)) {
                assert(cond_ret == ETIMEDOUT);
                timed_out = true;
                break;
            }
        }
    }

    result = timed_out ? GG_ERR_FAILURE : GG_ERR_OK;

cleanup:
    if (subscribed) {
        CONNECTION_SUB_CLOSE(sub_handle);
    }
    connection_ctx_destroy(&ctx);
    return result;
}

void iotcored_instance_stop(IotcoredInstance *ctx) {
    if (ctx->handle.val > 0) {
        GG_LOGD("Stopping iotcored instance (pid=%d).", ctx->handle.val);
        (void) ggl_process_kill(ctx->handle, 5);
        ctx->handle = (GglProcessHandle) { -1 };
    }
}

#ifdef GG_SDK_TESTING

#include <gg/test.h>
#include <unity.h>

typedef enum ConnectionTestMode {
    CONNECTION_TEST_SIGNAL,
    CONNECTION_TEST_RETRY_SIGNAL,
    CONNECTION_TEST_TIMEOUT,
} ConnectionTestMode;

static ConnectionTestMode connection_test_mode;
static uintptr_t connection_test_callback_ctx_addr;
static uintptr_t connection_test_callback_mutex_addr;
static uintptr_t connection_test_callback_cond_addr;
static uintptr_t connection_test_mutex_init_addr;
static uintptr_t connection_test_mutex_destroy_addr;
static uintptr_t connection_test_cond_init_addr;
static uintptr_t connection_test_cond_destroy_addr;
static size_t connection_test_subscribe_calls;
static size_t connection_test_sleep_calls;
static size_t connection_test_timedwait_calls;
static size_t connection_test_close_calls;
static size_t connection_test_mutex_destroy_calls;
static size_t connection_test_cond_destroy_calls;
static size_t connection_test_sequence;
static size_t connection_test_close_order;
static size_t connection_test_cond_destroy_order;
static size_t connection_test_mutex_destroy_order;

static int connection_test_mutex_init(
    pthread_mutex_t *mutex, const pthread_mutexattr_t *attr
) {
    connection_test_mutex_init_addr = (uintptr_t) mutex;
    return pthread_mutex_init(mutex, attr);
}

static int connection_test_mutex_destroy(pthread_mutex_t *mutex) {
    connection_test_mutex_destroy_addr = (uintptr_t) mutex;
    connection_test_mutex_destroy_calls += 1;
    connection_test_mutex_destroy_order = ++connection_test_sequence;
    return pthread_mutex_destroy(mutex);
}

static int connection_test_cond_init(
    pthread_cond_t *cond, const pthread_condattr_t *attr
) {
    connection_test_cond_init_addr = (uintptr_t) cond;
    return pthread_cond_init(cond, attr);
}

static int connection_test_cond_init_failure(
    pthread_cond_t *cond, const pthread_condattr_t *attr
) {
    (void) attr;
    connection_test_cond_init_addr = (uintptr_t) cond;
    return EINVAL;
}

static int connection_test_cond_destroy(pthread_cond_t *cond) {
    connection_test_cond_destroy_addr = (uintptr_t) cond;
    connection_test_cond_destroy_calls += 1;
    connection_test_cond_destroy_order = ++connection_test_sequence;
    return pthread_cond_destroy(cond);
}

static int connection_test_timedwait(
    pthread_cond_t *cond,
    pthread_mutex_t *mutex,
    const struct timespec *deadline
) {
    (void) cond;
    (void) mutex;
    (void) deadline;
    connection_test_timedwait_calls += 1;
    return ETIMEDOUT;
}

static int connection_test_clock_gettime(
    clockid_t clock_id, struct timespec *time
) {
    TEST_ASSERT_EQUAL_INT(CLOCK_MONOTONIC, clock_id);
    *time = (struct timespec) { .tv_sec = 100, .tv_nsec = 0 };
    return 0;
}

static GgError connection_test_sleep(int64_t ms) {
    TEST_ASSERT_EQUAL_INT64(SUBSCRIBE_RETRY_INTERVAL_MS, ms);
    connection_test_sleep_calls += 1;
    return GG_ERR_OK;
}

static void connection_test_close(uint32_t handle) {
    TEST_ASSERT_EQUAL_UINT32(42, handle);
    connection_test_close_calls += 1;
    connection_test_close_order = ++connection_test_sequence;
}

static GgError connection_test_subscribe(
    GgBuffer socket_name,
    GglSubscribeCallback on_response,
    GglSubscribeCloseCallback on_close,
    void *ctx,
    uint32_t *handle
) {
    (void) socket_name;
    TEST_ASSERT_NULL(on_close);
    connection_test_subscribe_calls += 1;
    ConnectionCtx *callback_ctx = ctx;
    connection_test_callback_ctx_addr = (uintptr_t) callback_ctx;
    connection_test_callback_mutex_addr = (uintptr_t) &callback_ctx->mtx;
    connection_test_callback_cond_addr = (uintptr_t) &callback_ctx->cond;

    if ((connection_test_mode == CONNECTION_TEST_RETRY_SIGNAL)
        && (connection_test_subscribe_calls == 1)) {
        return GG_ERR_FAILURE;
    }

    *handle = 42;
    if (connection_test_mode != CONNECTION_TEST_TIMEOUT) {
        GG_TEST_ASSERT_OK(on_response(ctx, *handle, gg_obj_bool(true)));
    }
    return GG_ERR_OK;
}

static void connection_test_reset(ConnectionTestMode mode) {
    connection_test_mode = mode;
    connection_test_callback_ctx_addr = 0;
    connection_test_callback_mutex_addr = 0;
    connection_test_callback_cond_addr = 0;
    connection_test_mutex_init_addr = 0;
    connection_test_mutex_destroy_addr = 0;
    connection_test_cond_init_addr = 0;
    connection_test_cond_destroy_addr = 0;
    connection_test_subscribe_calls = 0;
    connection_test_sleep_calls = 0;
    connection_test_timedwait_calls = 0;
    connection_test_close_calls = 0;
    connection_test_mutex_destroy_calls = 0;
    connection_test_cond_destroy_calls = 0;
    connection_test_sequence = 0;
    connection_test_close_order = 0;
    connection_test_cond_destroy_order = 0;
    connection_test_mutex_destroy_order = 0;

    connection_subscribe = connection_test_subscribe;
    connection_sub_close = connection_test_close;
    connection_sleep_ms = connection_test_sleep;
    connection_clock_gettime = connection_test_clock_gettime;
    connection_mutex_init = connection_test_mutex_init;
    connection_mutex_destroy = connection_test_mutex_destroy;
    connection_cond_init = connection_test_cond_init;
    connection_cond_destroy = connection_test_cond_destroy;
    connection_cond_timedwait = connection_test_timedwait;
}

static void connection_test_restore(void) {
    connection_subscribe = ggl_aws_iot_mqtt_connection_status;
    connection_sub_close = ggl_client_sub_close;
    connection_sleep_ms = gg_sleep_ms;
    connection_clock_gettime = clock_gettime;
    connection_mutex_init = pthread_mutex_init;
    connection_mutex_destroy = pthread_mutex_destroy;
    connection_cond_init = pthread_cond_init;
    connection_cond_destroy = pthread_cond_destroy;
    connection_cond_timedwait = pthread_cond_timedwait;
}

static void assert_connection_test_lifecycle(void) {
    TEST_ASSERT_NOT_EQUAL_UINT64(0, connection_test_callback_ctx_addr);
    TEST_ASSERT_EQUAL_UINT64(
        connection_test_callback_mutex_addr, connection_test_mutex_init_addr
    );
    TEST_ASSERT_EQUAL_UINT64(
        connection_test_callback_cond_addr, connection_test_cond_init_addr
    );
    TEST_ASSERT_EQUAL_UINT64(
        connection_test_mutex_init_addr, connection_test_mutex_destroy_addr
    );
    TEST_ASSERT_EQUAL_UINT64(
        connection_test_cond_init_addr, connection_test_cond_destroy_addr
    );
    TEST_ASSERT_EQUAL_size_t(1, connection_test_close_calls);
    TEST_ASSERT_EQUAL_size_t(1, connection_test_cond_destroy_calls);
    TEST_ASSERT_EQUAL_size_t(1, connection_test_mutex_destroy_calls);
    TEST_ASSERT_LESS_THAN(
        connection_test_cond_destroy_order, connection_test_close_order
    );
    TEST_ASSERT_LESS_THAN(
        connection_test_mutex_destroy_order, connection_test_cond_destroy_order
    );
}

GG_TEST_DEFINE(iotcored_connection_cond_init_failure_destroys_mutex) {
    connection_test_reset(CONNECTION_TEST_SIGNAL);
    connection_cond_init = connection_test_cond_init_failure;

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_FAILURE, iotcored_await_connection(GG_STR("socket"), 1)
    );
    TEST_ASSERT_EQUAL_UINT64(
        connection_test_mutex_init_addr, connection_test_mutex_destroy_addr
    );
    TEST_ASSERT_NOT_EQUAL_UINT64(0, connection_test_cond_init_addr);
    TEST_ASSERT_EQUAL_size_t(1, connection_test_mutex_destroy_calls);
    TEST_ASSERT_EQUAL_size_t(0, connection_test_cond_destroy_calls);
    TEST_ASSERT_EQUAL_size_t(0, connection_test_subscribe_calls);
    TEST_ASSERT_EQUAL_size_t(0, connection_test_close_calls);

    connection_test_restore();
}

GG_TEST_DEFINE(iotcored_connection_signal_before_wait_uses_final_addresses) {
    connection_test_reset(CONNECTION_TEST_SIGNAL);

    GG_TEST_ASSERT_OK(iotcored_await_connection(GG_STR("socket"), 1));
    TEST_ASSERT_EQUAL_size_t(1, connection_test_subscribe_calls);
    TEST_ASSERT_EQUAL_size_t(0, connection_test_sleep_calls);
    TEST_ASSERT_EQUAL_size_t(0, connection_test_timedwait_calls);
    assert_connection_test_lifecycle();

    connection_test_restore();
}

GG_TEST_DEFINE(iotcored_connection_subscription_retries_without_waiting) {
    connection_test_reset(CONNECTION_TEST_RETRY_SIGNAL);

    GG_TEST_ASSERT_OK(iotcored_await_connection(GG_STR("socket"), 1));
    TEST_ASSERT_EQUAL_size_t(2, connection_test_subscribe_calls);
    TEST_ASSERT_EQUAL_size_t(1, connection_test_sleep_calls);
    TEST_ASSERT_EQUAL_size_t(0, connection_test_timedwait_calls);
    assert_connection_test_lifecycle();

    connection_test_restore();
}

GG_TEST_DEFINE(iotcored_connection_timeout_closes_before_destroy) {
    connection_test_reset(CONNECTION_TEST_TIMEOUT);

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_FAILURE, iotcored_await_connection(GG_STR("socket"), 1)
    );
    TEST_ASSERT_EQUAL_size_t(1, connection_test_subscribe_calls);
    TEST_ASSERT_EQUAL_size_t(0, connection_test_sleep_calls);
    TEST_ASSERT_EQUAL_size_t(1, connection_test_timedwait_calls);
    assert_connection_test_lifecycle();

    connection_test_restore();
}

#endif
