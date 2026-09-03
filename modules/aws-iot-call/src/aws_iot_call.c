// aws-greengrass-lite - AWS IoT Greengrass runtime for constrained devices
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#include <assert.h>
#include <errno.h>
#include <gg/arena.h>
#include <gg/buffer.h>
#include <gg/cleanup.h>
#include <gg/error.h>
#include <gg/json_decode.h>
#include <gg/json_encode.h>
#include <gg/log.h>
#include <gg/map.h>
#include <gg/object.h>
#include <gg/types.h>
#include <gg/vector.h>
#include <ggl/aws_iot_call.h>
#include <ggl/core_bus/aws_iot_mqtt.h>
#include <ggl/core_bus/client.h> // IWYU pragma: keep (cleanup)
#include <pthread.h>
#include <sys/types.h>
#include <time.h>
#include <stdbool.h>
#include <stdint.h>

#define AWS_IOT_MAX_TOPIC_SIZE 256

#define IOT_RESPONSE_TIMEOUT_S 30

#ifndef GGL_MAX_IOT_CORE_API_PAYLOAD_LEN
#define GGL_MAX_IOT_CORE_API_PAYLOAD_LEN 5000
#endif

typedef struct {
    pthread_mutex_t *mtx;
    pthread_cond_t *cond;
    bool ready;
    GgBuffer *client_token;
    GgArena *alloc;
    GgObject *result;
    GgError ret;
} CallbackCtx;

static void cleanup_pthread_cond(pthread_cond_t **cond) {
    pthread_cond_destroy(*cond);
}

static GgError get_client_token(GgObject payload, GgBuffer **client_token) {
    assert(client_token != NULL);
    assert(*client_token != NULL);

    if (gg_obj_type(payload) != GG_TYPE_MAP) {
        *client_token = NULL;
        return GG_ERR_OK;
    }
    GgMap payload_map = gg_obj_into_map(payload);

    GgObject *found;
    if (!gg_map_get(payload_map, GG_STR("clientToken"), &found)) {
        *client_token = NULL;
        return GG_ERR_OK;
    }
    if (gg_obj_type(*found) != GG_TYPE_BUF) {
        GG_LOGE("Invalid clientToken type.");
        return GG_ERR_INVALID;
    }
    **client_token = gg_obj_into_buf(*found);
    return GG_ERR_OK;
}

static bool match_client_token(GgObject payload, GgBuffer *client_token) {
    GgBuffer *payload_client_token = &(GgBuffer) { 0 };

    GgError ret = get_client_token(payload, &payload_client_token);
    if (ret != GG_ERR_OK) {
        return false;
    }

    if ((client_token == NULL) && (payload_client_token == NULL)) {
        return true;
    }

    if ((client_token == NULL) || (payload_client_token == NULL)) {
        return false;
    }

    return gg_buffer_eq(*client_token, *payload_client_token);
}

static GgError subscription_callback(
    void *ctx, uint32_t handle, GgObject data
) {
    (void) handle;
    CallbackCtx *call_ctx = ctx;

    GgBuffer topic;
    GgBuffer payload = { 0 };
    GgError ret = ggl_aws_iot_mqtt_subscribe_parse_resp(data, &topic, &payload);
    if (ret != GG_ERR_OK) {
        return ret;
    }

    bool decoded = true;
    ret = gg_json_decode_destructive(
        payload, call_ctx->alloc, call_ctx->result
    );
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to decode response payload.");
        *(call_ctx->result) = GG_OBJ_NULL;
        decoded = false;
    }

    if (!match_client_token(*call_ctx->result, call_ctx->client_token)) {
        // Skip this message
        return GG_ERR_OK;
    }

    GgError response_ret;
    if (gg_buffer_has_suffix(topic, GG_STR("/accepted"))) {
        if (!decoded) {
            return GG_ERR_INVALID;
        }
        response_ret = GG_ERR_OK;
    } else if (gg_buffer_has_suffix(topic, GG_STR("/rejected"))) {
        GG_LOGE(
            "Received rejected response: %.*s", (int) payload.len, payload.data
        );
        response_ret = GG_ERR_REMOTE;
    } else {
        return GG_ERR_INVALID;
    }

    ret = gg_arena_claim_obj(call_ctx->result, call_ctx->alloc);
    if (ret != GG_ERR_OK) {
        GG_LOGE("Insufficient memory to own response payload.");
        *(call_ctx->result) = GG_OBJ_NULL;
        call_ctx->ret = ret;
        return GG_ERR_EXPECTED;
    }

    call_ctx->ret = response_ret;
    // Err to close subscription
    return GG_ERR_EXPECTED;
}

static void subscription_close_callback(void *ctx, uint32_t handle) {
    (void) handle;
    CallbackCtx *call_ctx = ctx;

    GG_MTX_SCOPE_GUARD(call_ctx->mtx);
    call_ctx->ready = true;
    pthread_cond_signal(call_ctx->cond);
}

GgError ggl_aws_iot_call(
    GgBuffer socket_name,
    GgBuffer topic,
    GgObject payload,
    bool virtual,
    GgArena *alloc,
    GgObject *result
) {
    static pthread_mutex_t mem_mtx = PTHREAD_MUTEX_INITIALIZER;
    GG_MTX_SCOPE_GUARD(&mem_mtx);

    // TODO: Share memory for topic filter and encode
    static uint8_t topic_filter_mem[AWS_IOT_MAX_TOPIC_SIZE];
    static uint8_t json_encode_mem[GGL_MAX_IOT_CORE_API_PAYLOAD_LEN];

    GgByteVec topic_filter = GG_BYTE_VEC(topic_filter_mem);

    GgError ret = gg_byte_vec_append(&topic_filter, topic);
    gg_byte_vec_chain_append(&ret, &topic_filter, GG_STR("/+"));
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to construct response topic filter.");
        return ret;
    }

    GgByteVec payload_vec = GG_BYTE_VEC(json_encode_mem);
    ret = gg_json_encode(payload, gg_byte_vec_writer(&payload_vec));
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to encode JSON payload.");
        return ret;
    }

    pthread_condattr_t notify_condattr;
    pthread_condattr_init(&notify_condattr);
    pthread_condattr_setclock(&notify_condattr, CLOCK_MONOTONIC);
    pthread_cond_t notify_cond;
    pthread_cond_init(&notify_cond, &notify_condattr);
    pthread_condattr_destroy(&notify_condattr);
    GG_CLEANUP(cleanup_pthread_cond, &notify_cond);
    pthread_mutex_t notify_mtx = PTHREAD_MUTEX_INITIALIZER;

    CallbackCtx ctx = {
        .mtx = &notify_mtx,
        .cond = &notify_cond,
        .ready = false,
        .client_token = &(GgBuffer) { 0 },
        .alloc = alloc,
        .result = result,
        .ret = GG_ERR_FAILURE,
    };

    ret = get_client_token(payload, &ctx.client_token);
    if (ret != GG_ERR_OK) {
        return ret;
    }

    uint32_t sub_handle = 0;
    ret = ggl_aws_iot_mqtt_subscribe(
        socket_name,
        GG_BUF_LIST(topic_filter.buf),
        1,
        virtual,
        subscription_callback,
        subscription_close_callback,
        &ctx,
        &sub_handle
    );
    if (ret != GG_ERR_OK) {
        GG_LOGE("Response topic subscription failed.");
        return ret;
    }

    ret = ggl_aws_iot_mqtt_publish(
        socket_name, topic, payload_vec.buf, 1, true
    );
    if (ret != GG_ERR_OK) {
        GG_LOGE("Response topic subscription failed.");
        ggl_client_sub_close(sub_handle);
        return ret;
    }

    struct timespec timeout;
    clock_gettime(CLOCK_MONOTONIC, &timeout);
    timeout.tv_sec += IOT_RESPONSE_TIMEOUT_S;

    bool timed_out = false;

    {
        // Must be unlocked before closing subscription
        // (else subscription response may be blocked, and close would deadlock)
        GG_MTX_SCOPE_GUARD(&notify_mtx);

        while (!ctx.ready) {
            int cond_ret
                = pthread_cond_timedwait(&notify_cond, &notify_mtx, &timeout);
            if ((cond_ret != 0) && (cond_ret != EINTR)) {
                assert(cond_ret == ETIMEDOUT);
                GG_LOGW("Timed out waiting for a response.");
                timed_out = true;
                break;
            }
        }
    }

    if (timed_out) {
        ggl_client_sub_close(sub_handle);
    }

    return ctx.ret;
}

#ifdef GG_SDK_TESTING

#include <gg/test.h>
#include <string.h>
#include <unity.h>

static void assert_response_payload_is_owned(
    GgBuffer topic, GgError expected_result
) {
    uint8_t payload_mem[] = "{\"clientToken\":\"token\",\"key\":\"value\"}";
    GgBuffer payload = {
        .data = payload_mem,
        .len = sizeof(payload_mem) - 1,
    };
    uint8_t result_mem[512];
    GgArena alloc = gg_arena_init(GG_BUF(result_mem));
    GgObject result = GG_OBJ_NULL;
    GgBuffer client_token = GG_STR("token");
    CallbackCtx ctx = {
        .client_token = &client_token,
        .alloc = &alloc,
        .result = &result,
        .ret = GG_ERR_FAILURE,
    };
    GgObject data = gg_obj_map(GG_MAP(
        gg_kv(GG_STR("topic"), gg_obj_buf(topic)),
        gg_kv(GG_STR("payload"), gg_obj_buf(payload))
    ));

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_EXPECTED, subscription_callback(&ctx, 1, data)
    );
    TEST_ASSERT_EQUAL_INT(expected_result, ctx.ret);

    memset(payload_mem, 'x', sizeof(payload_mem));

    TEST_ASSERT_EQUAL_INT(GG_TYPE_MAP, gg_obj_type(result));
    GgMap result_map = gg_obj_into_map(result);
    GgObject *value = NULL;
    TEST_ASSERT_TRUE(gg_map_get(result_map, GG_STR("clientToken"), &value));
    TEST_ASSERT_TRUE(gg_buffer_eq(gg_obj_into_buf(*value), GG_STR("token")));
    TEST_ASSERT_TRUE(gg_map_get(result_map, GG_STR("key"), &value));
    TEST_ASSERT_TRUE(gg_buffer_eq(gg_obj_into_buf(*value), GG_STR("value")));

    for (size_t i = 0; i < result_map.len; i++) {
        TEST_ASSERT_TRUE(
            gg_arena_owns(&alloc, gg_kv_key(result_map.pairs[i]).data)
        );
        GgObject *map_value = gg_kv_val(&result_map.pairs[i]);
        if (gg_obj_type(*map_value) == GG_TYPE_BUF) {
            TEST_ASSERT_TRUE(
                gg_arena_owns(&alloc, gg_obj_into_buf(*map_value).data)
            );
        }
    }
}

GG_TEST_DEFINE(aws_iot_call_accepted_response_is_caller_owned) {
    assert_response_payload_is_owned(GG_STR("topic/accepted"), GG_ERR_OK);
}

GG_TEST_DEFINE(aws_iot_call_rejected_response_is_caller_owned) {
    assert_response_payload_is_owned(GG_STR("topic/rejected"), GG_ERR_REMOTE);
}

GG_TEST_DEFINE(aws_iot_call_returns_nmem_when_response_cannot_be_owned) {
    uint8_t payload_mem[320];
    const GgBuffer prefix = GG_STR("{\"key\":\"");
    memcpy(payload_mem, prefix.data, prefix.len);
    memset(payload_mem + prefix.len, 'a', 300);
    payload_mem[prefix.len + 300] = '"';
    payload_mem[prefix.len + 301] = '}';
    GgBuffer payload = {
        .data = payload_mem,
        .len = prefix.len + 302,
    };

    // Large enough to decode the map structure, but too small to claim the
    // 300-byte value from the transient payload.
    uint8_t result_mem[128];
    GgArena alloc = gg_arena_init(GG_BUF(result_mem));
    GgObject result = GG_OBJ_NULL;
    CallbackCtx ctx = {
        .client_token = NULL,
        .alloc = &alloc,
        .result = &result,
        .ret = GG_ERR_FAILURE,
    };
    GgObject data = gg_obj_map(GG_MAP(
        gg_kv(GG_STR("topic"), gg_obj_buf(GG_STR("topic/accepted"))),
        gg_kv(GG_STR("payload"), gg_obj_buf(payload))
    ));

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_EXPECTED, subscription_callback(&ctx, 1, data)
    );
    TEST_ASSERT_EQUAL_INT(GG_ERR_NOMEM, ctx.ret);
    TEST_ASSERT_EQUAL_INT(GG_TYPE_NULL, gg_obj_type(result));
}

#endif
