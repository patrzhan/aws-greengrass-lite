// aws-greengrass-lite - AWS IoT Greengrass runtime for constrained devices
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#include "credential_endpoint_validation.h"
#include <gg/arena.h>
#include <gg/backoff.h>
#include <gg/buffer.h>
#include <gg/cleanup.h>
#include <gg/error.h>
#include <gg/log.h>
#include <gg/vector.h>
#include <ggl/core_bus/client.h>
#include <ggl/process.h>
#include <limits.h>
#include <string.h>
#include <stdint.h>

#define TESD_INSTANCE_NAME "tesddeploy"
#define MAX_CRED_ENDPOINT_LEN 128
#define MAX_ROLE_ALIAS_LEN 128

typedef struct {
    GglProcessHandle handle;
} TesdInstance;

// Caller/invocation-owned storage for one credential request. The response
// arena is built over caller-owned bytes and passed through the backoff
// callback, so concurrent invocations never share a single static buffer.
typedef struct {
    GgBuffer response_buffer;
    GgObject result;
} CredentialRequestCtx;

// Default seam implementations. The tesd process spawn/kill, the credential
// IPC request, and the retry backoff are each replaceable under GG_SDK_TESTING
// so endpoint validation can be exercised without spawning a process or
// sleeping.
static GgError cred_spawn_default(
    const char *const *argv, GglProcessHandle *handle
) {
    return ggl_process_spawn(argv, NULL, handle);
}

static GgError cred_kill_default(GglProcessHandle handle) {
    return ggl_process_kill(handle, 5);
}

static GgError cred_request_default(CredentialRequestCtx *ctx) {
    GgArena alloc = gg_arena_init(ctx->response_buffer);
    GgMap params = { 0 };
    return ggl_call(
        GG_STR(TESD_INSTANCE_NAME),
        GG_STR("request_credentials"),
        params,
        NULL,
        &alloc,
        &ctx->result
    );
}

static GgError cred_backoff_default(GgError (*fn)(void *), void *ctx) {
    // 500ms base with exponential backoff, 60s max interval, 8 attempts.
    return gg_backoff(500, 60000, 8, fn, ctx);
}

#ifdef GG_SDK_TESTING

static GgError (*cred_spawn)(const char *const *argv, GglProcessHandle *handle)
    = cred_spawn_default;
static GgError (*cred_kill)(GglProcessHandle handle) = cred_kill_default;
static GgError (*cred_request)(CredentialRequestCtx *ctx)
    = cred_request_default;
static GgError (*cred_backoff)(GgError (*fn)(void *), void *ctx)
    = cred_backoff_default;

void credential_endpoint_reset_test_seams(void);
void credential_endpoint_override_test_seam_for_reset_test(void);
bool credential_endpoint_test_seams_are_reset(void);

#else

// NOLINTBEGIN(readability-identifier-naming)
#define cred_spawn cred_spawn_default
#define cred_kill cred_kill_default
#define cred_request cred_request_default
#define cred_backoff cred_backoff_default
// NOLINTEND(readability-identifier-naming)

#endif

// Builds "<bin_path>tesd\0" into the caller-owned byte vector.
static GgError build_tesd_path(const char *bin_path, GgByteVec *tesd_path) {
    GgError ret = gg_byte_vec_append(
        tesd_path, gg_buffer_from_null_term((char *) bin_path)
    );
    gg_byte_vec_chain_append(&ret, tesd_path, GG_STR("tesd"));
    gg_byte_vec_chain_push(&ret, tesd_path, '\0');
    return ret;
}

static GgError try_fetch_credentials(void *ctx) {
    return cred_request((CredentialRequestCtx *) ctx);
}

static GgError tesd_instance_start(
    TesdInstance *ctx,
    const char *tesd_path,
    const char *cred_endpoint,
    const char *role_alias
) {
    ctx->handle = (GglProcessHandle) { -1 };

    const char *args[] = {
        tesd_path,     "-n", TESD_INSTANCE_NAME, "-e",
        cred_endpoint, "-a", role_alias,         NULL,
    };

    GgError ret = cred_spawn(args, &ctx->handle);
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to spawn tesd instance.");
        ctx->handle = (GglProcessHandle) { -1 };
        return ret;
    }

    GG_LOGD("Spawned tesd instance (pid=%d).", ctx->handle.val);
    return GG_ERR_OK;
}

static void tesd_instance_stop(TesdInstance *ctx) {
    if (ctx->handle.val > 0) {
        GG_LOGD("Stopping tesd instance (pid=%d).", ctx->handle.val);
        (void) cred_kill(ctx->handle);
        ctx->handle = (GglProcessHandle) { -1 };
    }
}

GgError check_credential_endpoint(
    GgBuffer cred_endpoint, GgBuffer role_alias, const char *bin_path
) {
    // Caller/invocation-owned tesd path storage (previously a static buffer).
    char tesd_path_buf[PATH_MAX];
    GgByteVec tesd_path = GG_BYTE_VEC(tesd_path_buf);
    GgError ret = build_tesd_path(bin_path, &tesd_path);
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to resolve tesd path.");
        return ret;
    }

    // Null-terminate GgBuffers for argv
    char ep_buf[MAX_CRED_ENDPOINT_LEN + 1];
    if (cred_endpoint.len >= sizeof(ep_buf)) {
        GG_LOGE("Credential endpoint too long.");
        return GG_ERR_RANGE;
    }
    memcpy(ep_buf, cred_endpoint.data, cred_endpoint.len);
    ep_buf[cred_endpoint.len] = '\0';
    char alias_buf[MAX_ROLE_ALIAS_LEN + 1];
    if (role_alias.len >= sizeof(alias_buf)) {
        GG_LOGE("Role alias too long.");
        return GG_ERR_RANGE;
    }
    memcpy(alias_buf, role_alias.data, role_alias.len);
    alias_buf[role_alias.len] = '\0';

    GG_LOGI("Checking credential endpoint %s.", ep_buf);

    TesdInstance instance = { .handle = { -1 } };
    ret = tesd_instance_start(&instance, tesd_path_buf, ep_buf, alias_buf);
    GG_CLEANUP(tesd_instance_stop, instance);
    if (ret != GG_ERR_OK) {
        return ret;
    }

    // Caller/invocation-owned credential response arena passed through the
    // backoff callback (previously a static buffer inside the callback).
    uint8_t cred_mem[1500];
    CredentialRequestCtx request_ctx = { .response_buffer = GG_BUF(cred_mem) };

    ret = cred_backoff(try_fetch_credentials, &request_ctx);
    if (ret != GG_ERR_OK) {
        GG_LOGE("Credential endpoint validation failed for %s.", ep_buf);
    } else {
        GG_LOGI("Credential endpoint validation passed for %s.", ep_buf);
    }
    return ret;
}

#ifdef GG_SDK_TESTING

#include <gg/test.h>
#include <unity.h>

static GgBuffer cred_test_last_buffer;

// argv values copied out of the caller's stack-scoped argv array during spawn
// so tests can inspect them after check_credential_endpoint returns.
static char cred_test_spawn_arg0[PATH_MAX + 1];
static char cred_test_spawn_arg2[PATH_MAX + 1];
static char cred_test_spawn_arg4[PATH_MAX + 1];
static char cred_test_spawn_arg6[PATH_MAX + 1];
static size_t cred_test_spawn_calls;
static GgError cred_test_spawn_result;
static int32_t cred_test_spawn_handle_val;

static size_t cred_test_kill_calls;
static int32_t cred_test_killed_handle_val;

static size_t cred_test_backoff_calls;

static GgError cred_test_record_request(CredentialRequestCtx *ctx) {
    cred_test_last_buffer = ctx->response_buffer;
    return GG_ERR_OK;
}

static void cred_test_copy_arg(char *dst, size_t dst_size, const char *src) {
    size_t len = strlen(src);
    if (len >= dst_size) {
        len = dst_size - 1;
    }
    memcpy(dst, src, len);
    dst[len] = '\0';
}

// Fake spawn: records the argv values and, only on simulated success, hands
// back a test-controlled process handle. No process is created.
static GgError cred_test_record_spawn(
    const char *const *argv, GglProcessHandle *handle
) {
    cred_test_spawn_calls += 1;
    cred_test_copy_arg(
        cred_test_spawn_arg0, sizeof(cred_test_spawn_arg0), argv[0]
    );
    cred_test_copy_arg(
        cred_test_spawn_arg2, sizeof(cred_test_spawn_arg2), argv[2]
    );
    cred_test_copy_arg(
        cred_test_spawn_arg4, sizeof(cred_test_spawn_arg4), argv[4]
    );
    cred_test_copy_arg(
        cred_test_spawn_arg6, sizeof(cred_test_spawn_arg6), argv[6]
    );
    if (cred_test_spawn_result == GG_ERR_OK) {
        *handle = (GglProcessHandle) { cred_test_spawn_handle_val };
    }
    return cred_test_spawn_result;
}

// Fake kill: records how many times it ran and which handle it targeted.
static GgError cred_test_record_kill(GglProcessHandle handle) {
    cred_test_kill_calls += 1;
    cred_test_killed_handle_val = handle.val;
    return GG_ERR_OK;
}

// Fake backoff: invokes the callback exactly once (no sleeping) and returns
// its result, so the credential request path runs without real retries.
static GgError cred_test_backoff_invoke_once(GgError (*fn)(void *), void *ctx) {
    cred_test_backoff_calls += 1;
    return fn(ctx);
}

void credential_endpoint_reset_test_seams(void) {
    cred_spawn = cred_spawn_default;
    cred_kill = cred_kill_default;
    cred_request = cred_request_default;
    cred_backoff = cred_backoff_default;
    cred_test_last_buffer = (GgBuffer) { 0 };
    memset(cred_test_spawn_arg0, 0, sizeof(cred_test_spawn_arg0));
    memset(cred_test_spawn_arg2, 0, sizeof(cred_test_spawn_arg2));
    memset(cred_test_spawn_arg4, 0, sizeof(cred_test_spawn_arg4));
    memset(cred_test_spawn_arg6, 0, sizeof(cred_test_spawn_arg6));
    cred_test_spawn_calls = 0;
    cred_test_spawn_result = GG_ERR_OK;
    cred_test_spawn_handle_val = 0;
    cred_test_kill_calls = 0;
    cred_test_killed_handle_val = 0;
    cred_test_backoff_calls = 0;
}

void credential_endpoint_override_test_seam_for_reset_test(void) {
    cred_spawn = cred_test_record_spawn;
    cred_kill = cred_test_record_kill;
    cred_request = cred_test_record_request;
    cred_backoff = cred_test_backoff_invoke_once;
}

bool credential_endpoint_test_seams_are_reset(void) {
    return (cred_spawn == cred_spawn_default)
        && (cred_kill == cred_kill_default)
        && (cred_request == cred_request_default)
        && (cred_backoff == cred_backoff_default);
}

GG_TEST_DEFINE(credential_request_uses_separate_caller_owned_storage) {
    credential_endpoint_reset_test_seams();
    cred_request = cred_test_record_request;

    uint8_t mem_a[1500];
    uint8_t mem_b[1500];
    CredentialRequestCtx ctx_a = { .response_buffer = GG_BUF(mem_a) };
    CredentialRequestCtx ctx_b = { .response_buffer = GG_BUF(mem_b) };

    // Each simultaneously-live invocation carries its own caller-owned buffer;
    // there is no shared static response storage.
    GG_TEST_ASSERT_OK(try_fetch_credentials(&ctx_a));
    TEST_ASSERT_EQUAL_PTR(mem_a, cred_test_last_buffer.data);
    GG_TEST_ASSERT_OK(try_fetch_credentials(&ctx_b));
    TEST_ASSERT_EQUAL_PTR(mem_b, cred_test_last_buffer.data);
    TEST_ASSERT_TRUE(ctx_a.response_buffer.data != ctx_b.response_buffer.data);

    credential_endpoint_reset_test_seams();
}

GG_TEST_DEFINE(build_tesd_path_uses_caller_owned_storage) {
    char buf_a[PATH_MAX];
    char buf_b[PATH_MAX];
    GgByteVec path_a = GG_BYTE_VEC(buf_a);
    GgByteVec path_b = GG_BYTE_VEC(buf_b);

    // Two calls with different bin paths write into their own caller buffers;
    // building path_b must not disturb path_a, proving no shared storage.
    GG_TEST_ASSERT_OK(build_tesd_path("/usr/bin/", &path_a));
    GG_TEST_ASSERT_OK(build_tesd_path("/opt/gg/", &path_b));

    TEST_ASSERT_TRUE(
        gg_buffer_eq(gg_buffer_from_null_term(buf_a), GG_STR("/usr/bin/tesd"))
    );
    TEST_ASSERT_TRUE(
        gg_buffer_eq(gg_buffer_from_null_term(buf_b), GG_STR("/opt/gg/tesd"))
    );
}

GG_TEST_DEFINE(credential_endpoint_length_boundary_enforced_before_spawn) {
    uint8_t ep_mem[MAX_CRED_ENDPOINT_LEN + 1];
    memset(ep_mem, 'e', sizeof(ep_mem));

    // A 128-byte endpoint is accepted and reaches spawn.
    credential_endpoint_reset_test_seams();
    cred_spawn = cred_test_record_spawn;
    cred_kill = cred_test_record_kill;
    cred_backoff = cred_test_backoff_invoke_once;
    cred_request = cred_test_record_request;
    cred_test_spawn_result = GG_ERR_OK;
    cred_test_spawn_handle_val = 7;
    GgBuffer ep_128 = { .data = ep_mem, .len = MAX_CRED_ENDPOINT_LEN };
    GG_TEST_ASSERT_OK(
        check_credential_endpoint(ep_128, GG_STR("alias"), "/usr/bin/")
    );
    TEST_ASSERT_EQUAL_size_t(1, cred_test_spawn_calls);

    // A 129-byte endpoint is rejected with GG_ERR_RANGE before any spawn.
    credential_endpoint_reset_test_seams();
    cred_spawn = cred_test_record_spawn;
    GgBuffer ep_129 = { .data = ep_mem, .len = MAX_CRED_ENDPOINT_LEN + 1 };
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_RANGE,
        check_credential_endpoint(ep_129, GG_STR("alias"), "/usr/bin/")
    );
    TEST_ASSERT_EQUAL_size_t(0, cred_test_spawn_calls);

    credential_endpoint_reset_test_seams();
}

GG_TEST_DEFINE(credential_role_alias_length_boundary_enforced_before_spawn) {
    uint8_t alias_mem[MAX_ROLE_ALIAS_LEN + 1];
    memset(alias_mem, 'a', sizeof(alias_mem));

    // A 128-byte role alias is accepted and reaches spawn.
    credential_endpoint_reset_test_seams();
    cred_spawn = cred_test_record_spawn;
    cred_kill = cred_test_record_kill;
    cred_backoff = cred_test_backoff_invoke_once;
    cred_request = cred_test_record_request;
    cred_test_spawn_result = GG_ERR_OK;
    cred_test_spawn_handle_val = 7;
    GgBuffer alias_128 = { .data = alias_mem, .len = MAX_ROLE_ALIAS_LEN };
    GG_TEST_ASSERT_OK(
        check_credential_endpoint(GG_STR("ep"), alias_128, "/usr/bin/")
    );
    TEST_ASSERT_EQUAL_size_t(1, cred_test_spawn_calls);

    // A 129-byte role alias is rejected with GG_ERR_RANGE before any spawn.
    credential_endpoint_reset_test_seams();
    cred_spawn = cred_test_record_spawn;
    GgBuffer alias_129 = { .data = alias_mem, .len = MAX_ROLE_ALIAS_LEN + 1 };
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_RANGE,
        check_credential_endpoint(GG_STR("ep"), alias_129, "/usr/bin/")
    );
    TEST_ASSERT_EQUAL_size_t(0, cred_test_spawn_calls);

    credential_endpoint_reset_test_seams();
}

GG_TEST_DEFINE(credential_tesd_path_overflow_rejected_before_spawn) {
    credential_endpoint_reset_test_seams();
    cred_spawn = cred_test_record_spawn;

    // A bin_path that cannot fit "<bin_path>tesd\0" into PATH_MAX overflows the
    // caller-owned tesd path vector, so build_tesd_path fails before any spawn.
    static char long_bin[PATH_MAX + 16];
    memset(long_bin, 'x', sizeof(long_bin) - 1);
    long_bin[sizeof(long_bin) - 1] = '\0';

    TEST_ASSERT_NOT_EQUAL(
        GG_ERR_OK,
        check_credential_endpoint(GG_STR("ep"), GG_STR("alias"), long_bin)
    );
    TEST_ASSERT_EQUAL_size_t(0, cred_test_spawn_calls);

    credential_endpoint_reset_test_seams();
}

GG_TEST_DEFINE(credential_spawn_failure_skips_backoff_and_kill) {
    credential_endpoint_reset_test_seams();
    cred_spawn = cred_test_record_spawn;
    cred_kill = cred_test_record_kill;
    cred_backoff = cred_test_backoff_invoke_once;
    cred_test_spawn_result = GG_ERR_FAILURE;

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_FAILURE,
        check_credential_endpoint(
            GG_STR("data.example.com"), GG_STR("MyRoleAlias"), "/usr/bin/"
        )
    );
    TEST_ASSERT_EQUAL_size_t(1, cred_test_spawn_calls);
    // A failed spawn must neither retry credentials nor kill a missing process.
    TEST_ASSERT_EQUAL_size_t(0, cred_test_backoff_calls);
    TEST_ASSERT_EQUAL_size_t(0, cred_test_kill_calls);

    credential_endpoint_reset_test_seams();
}

GG_TEST_DEFINE(credential_success_backs_off_once_and_kills_once) {
    credential_endpoint_reset_test_seams();
    cred_spawn = cred_test_record_spawn;
    cred_kill = cred_test_record_kill;
    cred_backoff = cred_test_backoff_invoke_once;
    cred_request = cred_test_record_request;
    cred_test_spawn_result = GG_ERR_OK;
    cred_test_spawn_handle_val = 42;

    GG_TEST_ASSERT_OK(check_credential_endpoint(
        GG_STR("data.example.com"), GG_STR("MyRoleAlias"), "/usr/bin/"
    ));

    // Spawned once, backed off once, and killed exactly once for that handle.
    TEST_ASSERT_EQUAL_size_t(1, cred_test_spawn_calls);
    TEST_ASSERT_EQUAL_size_t(1, cred_test_backoff_calls);
    TEST_ASSERT_EQUAL_size_t(1, cred_test_kill_calls);
    TEST_ASSERT_EQUAL_INT32(42, cred_test_killed_handle_val);

    // The exact argv values are preserved end to end.
    TEST_ASSERT_TRUE(gg_buffer_eq(
        gg_buffer_from_null_term(cred_test_spawn_arg0), GG_STR("/usr/bin/tesd")
    ));
    TEST_ASSERT_TRUE(gg_buffer_eq(
        gg_buffer_from_null_term(cred_test_spawn_arg2),
        GG_STR(TESD_INSTANCE_NAME)
    ));
    TEST_ASSERT_TRUE(gg_buffer_eq(
        gg_buffer_from_null_term(cred_test_spawn_arg4),
        GG_STR("data.example.com")
    ));
    TEST_ASSERT_TRUE(gg_buffer_eq(
        gg_buffer_from_null_term(cred_test_spawn_arg6), GG_STR("MyRoleAlias")
    ));

    credential_endpoint_reset_test_seams();
}

#endif
