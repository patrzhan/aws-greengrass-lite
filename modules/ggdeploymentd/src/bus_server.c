// aws-greengrass-lite - AWS IoT Greengrass runtime for constrained devices
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#include "bus_server.h"
#include "deployment_model.h"
#include "deployment_queue.h"
#include <gg/buffer.h>
#include <gg/error.h>
#include <gg/log.h>
#include <gg/object.h>
#include <gg/types.h>
#include <gg/vector.h>
#include <ggl/core_bus/server.h>
#include <systemd/sd-daemon.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

static GgError create_local_deployment(
    void *ctx, GgMap params, uint32_t handle
) {
    (void) ctx;

    GG_LOGT("Received create_local_deployment from core bus.");

    GgByteVec id = GG_BYTE_VEC((uint8_t[36]) { 0 });

    GgError ret = ggl_deployment_enqueue(
        params, &id, (GgBuffer) { 0 }, LOCAL_DEPLOYMENT
    );
    if (ret != GG_ERR_OK) {
        return ret;
    }

    ggl_respond(handle, gg_obj_buf(id.buf));
    return GG_ERR_OK;
}

// Test seams for the systemd readiness notification and the blocking core-bus
// listen call, so startup semantics can be exercised without systemd/sockets.
#ifdef GG_SDK_TESTING

static int (*server_sd_notify)(int unset_environment, const char *state)
    = sd_notify;
static GgError (*server_listen)(
    GgBuffer interface, GglRpcMethodDesc *handlers, size_t handlers_len
) = ggl_listen;

void bus_server_reset_test_seams(void);
void bus_server_override_test_seam_for_reset_test(void);
bool bus_server_test_seams_are_reset(void);

#else

// NOLINTBEGIN(readability-identifier-naming)
#define server_sd_notify sd_notify
#define server_listen ggl_listen
// NOLINTEND(readability-identifier-naming)

#endif

GgError ggdeploymentd_start_server(void) {
    GG_LOGI("Starting ggdeploymentd core bus server.");

    // F-MISC-05 (READY-after-bind) is a verified upstream blocker, not resolved
    // here. Ideally "READY=1" would be sent only after the core-bus socket is
    // bound and listening. But ggl_listen exposes only
    // (interface, handlers, handlers_len): it builds the socket path and then
    // immediately enters the blocking ggl_socket_server_listen
    // (modules/core-bus/src/server.c) and returns only on exit. Neither API
    // surfaces a post-bind callback to ggdeploymentd, so the readiness
    // notification cannot be ordered after the bind from within this module.
    // Closing the race requires an upstream core-bus readiness hook; M9 must
    // not add a polling workaround, so READY=1 is still sent before listen.
    int notify_ret = server_sd_notify(0, "READY=1");
    if (notify_ret < 0) {
        GG_LOGE("Failed to send sd_notify (errno=%d).", -notify_ret);
        return GG_ERR_FAILURE;
    }
    // A zero result means NOTIFY_SOCKET is unset, which is not an error; still
    // start listening.

    GglRpcMethodDesc handlers[] = { { GG_STR("create_local_deployment"),
                                      false,
                                      create_local_deployment,
                                      NULL } };
    size_t handlers_len = sizeof(handlers) / sizeof(handlers[0]);

    GgError ret
        = server_listen(GG_STR("gg_deployment"), handlers, handlers_len);

    GG_LOGE("Exiting with error %u.", (unsigned) ret);
    return ret;
}

#ifdef GG_SDK_TESTING

#include <gg/test.h>
#include <unity.h>

static int server_test_notify_result;
static size_t server_test_notify_calls;
static GgError server_test_listen_result;
static size_t server_test_listen_calls;

static int server_test_record_notify(int unset_environment, const char *state) {
    (void) unset_environment;
    (void) state;
    server_test_notify_calls += 1;
    return server_test_notify_result;
}

static GgError server_test_record_listen(
    GgBuffer interface, GglRpcMethodDesc *handlers, size_t handlers_len
) {
    (void) interface;
    (void) handlers;
    (void) handlers_len;
    server_test_listen_calls += 1;
    return server_test_listen_result;
}

void bus_server_reset_test_seams(void) {
    server_sd_notify = sd_notify;
    server_listen = ggl_listen;
    server_test_notify_result = 0;
    server_test_notify_calls = 0;
    server_test_listen_result = GG_ERR_OK;
    server_test_listen_calls = 0;
}

void bus_server_override_test_seam_for_reset_test(void) {
    server_sd_notify = server_test_record_notify;
    server_listen = server_test_record_listen;
}

bool bus_server_test_seams_are_reset(void) {
    return (server_sd_notify == sd_notify) && (server_listen == ggl_listen);
}

static void bus_server_install_test_seams(void) {
    bus_server_reset_test_seams();
    server_sd_notify = server_test_record_notify;
    server_listen = server_test_record_listen;
}

GG_TEST_DEFINE(server_negative_notify_fails_without_listen) {
    bus_server_install_test_seams();
    server_test_notify_result = -1;

    TEST_ASSERT_NOT_EQUAL(GG_ERR_OK, ggdeploymentd_start_server());
    TEST_ASSERT_EQUAL_size_t(1, server_test_notify_calls);
    TEST_ASSERT_EQUAL_size_t(0, server_test_listen_calls);
}

GG_TEST_DEFINE(server_zero_notify_still_listens) {
    bus_server_install_test_seams();
    // Zero means NOTIFY_SOCKET is unset; listen must still run.
    server_test_notify_result = 0;
    server_test_listen_result = GG_ERR_OK;

    GG_TEST_ASSERT_OK(ggdeploymentd_start_server());
    TEST_ASSERT_EQUAL_size_t(1, server_test_listen_calls);
}

GG_TEST_DEFINE(server_propagates_listen_error) {
    bus_server_install_test_seams();
    server_test_notify_result = 1;
    server_test_listen_result = GG_ERR_FAILURE;

    TEST_ASSERT_EQUAL_INT(GG_ERR_FAILURE, ggdeploymentd_start_server());
    TEST_ASSERT_EQUAL_size_t(1, server_test_listen_calls);
}

#endif
