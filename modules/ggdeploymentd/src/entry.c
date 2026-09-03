// aws-greengrass-lite - AWS IoT Greengrass runtime for constrained devices
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#include "bus_server.h"
#include "component_status_listener.h"
#include "deployment_handler.h"
#include "iot_jobs_listener.h"
#include <errno.h>
#include <fcntl.h>
#include <gg/arena.h>
#include <gg/buffer.h>
#include <gg/error.h>
#include <gg/file.h>
#include <gg/log.h>
#include <gg/types.h>
#include <ggdeploymentd.h>
#include <ggl/core_bus/gg_config.h>
#include <ggl/proxy/environment.h>
#include <limits.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

// Narrow seams over the two pthread primitives used to launch a worker. They
// let a test drive start_worker_default's create/detach ordering -- in
// particular that a failed pthread_create never detaches an indeterminate
// thread ID -- without spawning a real thread.
static int entry_pthread_create_default(
    pthread_t *thread_id, void *(*start_routine)(void *), void *arg
) {
    return pthread_create(thread_id, NULL, start_routine, arg);
}

static int entry_pthread_detach_default(pthread_t thread_id) {
    return pthread_detach(thread_id);
}

#ifdef GG_SDK_TESTING

static int (*entry_pthread_create)(
    pthread_t *thread_id, void *(*start_routine)(void *), void *arg
) = entry_pthread_create_default;
static int (*entry_pthread_detach)(pthread_t thread_id)
    = entry_pthread_detach_default;

#else

// NOLINTBEGIN(readability-identifier-naming)
#define entry_pthread_create entry_pthread_create_default
#define entry_pthread_detach entry_pthread_detach_default
// NOLINTEND(readability-identifier-naming)

#endif

// Creates a worker thread and detaches it only after successful creation, so a
// failed pthread_create never leaves an indeterminate thread ID to detach.
static GgError start_worker_default(
    pthread_t *thread_id, void *(*start_routine)(void *), void *arg
) {
    int err = entry_pthread_create(thread_id, start_routine, arg);
    if (err != 0) {
        GG_LOGE("Failed to create worker thread (errno=%d).", err);
        return GG_ERR_FAILURE;
    }
    err = entry_pthread_detach(*thread_id);
    if (err != 0) {
        GG_LOGE("Failed to detach worker thread (pthread error=%d).", err);
        return GG_ERR_FAILURE;
    }
    return GG_ERR_OK;
}

// Test seams for the worker/server coordinator. Early process initialization is
// intentionally left out of the seam surface; only the worker start, status
// listener, and server start are replaceable so startup ordering and error
// propagation can be exercised without real threads or sockets.
#ifdef GG_SDK_TESTING

static GgError (*entry_start_worker)(
    pthread_t *thread_id, void *(*start_routine)(void *), void *arg
) = start_worker_default;
static void (*entry_start_status_listener)(void)
    = ggl_start_component_status_listener;
static GgError (*entry_start_server)(void) = ggdeploymentd_start_server;

void entry_reset_test_seams(void);
void entry_override_test_seam_for_reset_test(void);
bool entry_test_seams_are_reset(void);

#else

// NOLINTBEGIN(readability-identifier-naming)
#define entry_start_worker start_worker_default
#define entry_start_status_listener ggl_start_component_status_listener
#define entry_start_server ggdeploymentd_start_server
// NOLINTEND(readability-identifier-naming)

#endif

// Starts the two worker threads, then the status listener, then the core-bus
// server. A worker that fails to start aborts startup before the status
// listener or server run, and the server result is returned to the caller.
static GgError run_workers_and_server(GglDeploymentHandlerThreadArgs *args) {
    pthread_t ptid_jobs;
    GgError ret = entry_start_worker(&ptid_jobs, &job_listener_thread, args);
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to start the IoT Jobs listener thread.");
        return ret;
    }

    pthread_t ptid_handler;
    ret = entry_start_worker(
        &ptid_handler, &ggl_deployment_handler_thread, args
    );
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to start the deployment handler thread.");
        return ret;
    }

    // Forward gghealthd component lifecycle state changes to the fleet status
    // service when no deployment is in progress.
    entry_start_status_listener();

    return entry_start_server();
}

// Populates the worker arguments in file-scoped static storage and returns a
// pointer to it. The detached worker threads read these arguments after
// run_ggdeploymentd returns, so the storage must outlive the call; returning
// the address of static storage lets a test prove the populated context
// survives the initializer's return.
static GglDeploymentHandlerThreadArgs *init_worker_args(
    int root_path_fd, GgBuffer root_path, const char *bin_path
) {
    static GglDeploymentHandlerThreadArgs args;
    args = (GglDeploymentHandlerThreadArgs) { .root_path_fd = root_path_fd,
                                              .root_path = root_path,
                                              .bin_path = bin_path };
    return &args;
}

GgError run_ggdeploymentd(const char *bin_path) {
    GG_LOGI("Started ggdeploymentd process.");

    GgError ret = ggl_proxy_set_environment();
    if (ret != GG_ERR_OK) {
        return ret;
    }

    umask(0002);

    static uint8_t root_path_mem[PATH_MAX] = { 0 };
    GgArena alloc = gg_arena_init(
        gg_buffer_substr(GG_BUF(root_path_mem), 0, sizeof(root_path_mem) - 1)
    );
    GgBuffer root_path;
    ret = ggl_gg_config_read_str(
        GG_BUF_LIST(GG_STR("system"), GG_STR("rootPath")), &alloc, &root_path
    );
    if (ret != GG_ERR_OK) {
        GG_LOGW("Failed to get root path from config.");
        return ret;
    }

    int root_path_fd;
    ret = gg_dir_open(root_path, O_PATH, false, &root_path_fd);
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to open rootPath.");
        return ret;
    }

    int sys_ret = fchdir(root_path_fd);
    if (sys_ret != 0) {
        GG_LOGE("Failed to enter rootPath: %d.", errno);
        (void) gg_close(root_path_fd);
        return GG_ERR_FAILURE;
    }

    // Worker arguments must outlive this function because the detached worker
    // threads read them; init_worker_args backs them with static storage.
    return run_workers_and_server(
        init_worker_args(root_path_fd, root_path, bin_path)
    );
}

#ifdef GG_SDK_TESTING

#include <gg/test.h>
#include <unity.h>

static GgError entry_test_worker_results[2];
static size_t entry_test_worker_calls;
static void *entry_test_worker_args[2];
static size_t entry_test_status_listener_calls;
static GgError entry_test_server_result;
static size_t entry_test_server_calls;

static int entry_test_create_result;
static size_t entry_test_create_calls;
static pthread_t entry_test_created_thread_id;
static int entry_test_detach_result;
static size_t entry_test_detach_calls;
static pthread_t entry_test_detached_thread_id;

static void *entry_test_noop_routine(void *arg) {
    (void) arg;
    return NULL;
}

// Fake pthread_create: records the call and, only on simulated success, writes
// the caller's thread ID slot with a test-controlled value.
static int entry_test_record_create(
    pthread_t *thread_id, void *(*start_routine)(void *), void *arg
) {
    (void) start_routine;
    (void) arg;
    entry_test_create_calls += 1;
    if (entry_test_create_result == 0) {
        *thread_id = entry_test_created_thread_id;
    }
    return entry_test_create_result;
}

// Fake pthread_detach: records how many times it ran and with which thread ID.
static int entry_test_record_detach(pthread_t thread_id) {
    entry_test_detach_calls += 1;
    entry_test_detached_thread_id = thread_id;
    return entry_test_detach_result;
}

static GgError entry_test_record_worker(
    pthread_t *thread_id, void *(*start_routine)(void *), void *arg
) {
    (void) thread_id;
    (void) start_routine;
    GgError result = GG_ERR_OK;
    if (entry_test_worker_calls < 2) {
        entry_test_worker_args[entry_test_worker_calls] = arg;
        result = entry_test_worker_results[entry_test_worker_calls];
    }
    entry_test_worker_calls += 1;
    return result;
}

static void entry_test_record_status_listener(void) {
    entry_test_status_listener_calls += 1;
}

static GgError entry_test_record_server(void) {
    entry_test_server_calls += 1;
    return entry_test_server_result;
}

void entry_reset_test_seams(void) {
    entry_start_worker = start_worker_default;
    entry_start_status_listener = ggl_start_component_status_listener;
    entry_start_server = ggdeploymentd_start_server;
    entry_pthread_create = entry_pthread_create_default;
    entry_pthread_detach = entry_pthread_detach_default;
    entry_test_worker_results[0] = GG_ERR_OK;
    entry_test_worker_results[1] = GG_ERR_OK;
    entry_test_worker_calls = 0;
    entry_test_worker_args[0] = NULL;
    entry_test_worker_args[1] = NULL;
    entry_test_status_listener_calls = 0;
    entry_test_server_result = GG_ERR_OK;
    entry_test_server_calls = 0;
    entry_test_create_result = 0;
    entry_test_create_calls = 0;
    entry_test_created_thread_id = (pthread_t) { 0 };
    entry_test_detach_result = 0;
    entry_test_detach_calls = 0;
    entry_test_detached_thread_id = (pthread_t) { 0 };
}

void entry_override_test_seam_for_reset_test(void) {
    entry_start_worker = entry_test_record_worker;
    entry_start_status_listener = entry_test_record_status_listener;
    entry_start_server = entry_test_record_server;
    entry_pthread_create = entry_test_record_create;
    entry_pthread_detach = entry_test_record_detach;
}

bool entry_test_seams_are_reset(void) {
    return (entry_start_worker == start_worker_default)
        && (entry_start_status_listener == ggl_start_component_status_listener)
        && (entry_start_server == ggdeploymentd_start_server)
        && (entry_pthread_create == entry_pthread_create_default)
        && (entry_pthread_detach == entry_pthread_detach_default);
}

static void entry_install_test_seams(void) {
    entry_reset_test_seams();
    entry_start_worker = entry_test_record_worker;
    entry_start_status_listener = entry_test_record_status_listener;
    entry_start_server = entry_test_record_server;
}

GG_TEST_DEFINE(entry_first_worker_failure_aborts_startup) {
    entry_install_test_seams();
    entry_test_worker_results[0] = GG_ERR_FAILURE;
    GglDeploymentHandlerThreadArgs args = { .root_path_fd = 7 };

    TEST_ASSERT_NOT_EQUAL(GG_ERR_OK, run_workers_and_server(&args));
    TEST_ASSERT_EQUAL_size_t(1, entry_test_worker_calls);
    TEST_ASSERT_EQUAL_size_t(0, entry_test_status_listener_calls);
    TEST_ASSERT_EQUAL_size_t(0, entry_test_server_calls);
    // Worker context is passed through and remains valid after return.
    TEST_ASSERT_EQUAL_PTR(&args, entry_test_worker_args[0]);
    TEST_ASSERT_EQUAL_INT(7, args.root_path_fd);
}

GG_TEST_DEFINE(entry_second_worker_failure_aborts_startup) {
    entry_install_test_seams();
    entry_test_worker_results[0] = GG_ERR_OK;
    entry_test_worker_results[1] = GG_ERR_FAILURE;
    GglDeploymentHandlerThreadArgs args = { .root_path_fd = 7 };

    TEST_ASSERT_NOT_EQUAL(GG_ERR_OK, run_workers_and_server(&args));
    TEST_ASSERT_EQUAL_size_t(2, entry_test_worker_calls);
    TEST_ASSERT_EQUAL_size_t(0, entry_test_status_listener_calls);
    TEST_ASSERT_EQUAL_size_t(0, entry_test_server_calls);
    TEST_ASSERT_EQUAL_PTR(&args, entry_test_worker_args[0]);
    TEST_ASSERT_EQUAL_PTR(&args, entry_test_worker_args[1]);
}

GG_TEST_DEFINE(entry_server_error_propagates_after_workers_start) {
    entry_install_test_seams();
    entry_test_server_result = GG_ERR_FAILURE;
    GglDeploymentHandlerThreadArgs args = { .root_path_fd = 7 };

    TEST_ASSERT_EQUAL_INT(GG_ERR_FAILURE, run_workers_and_server(&args));
    TEST_ASSERT_EQUAL_size_t(2, entry_test_worker_calls);
    TEST_ASSERT_EQUAL_size_t(1, entry_test_status_listener_calls);
    TEST_ASSERT_EQUAL_size_t(1, entry_test_server_calls);
}

GG_TEST_DEFINE(entry_successful_startup_returns_server_result) {
    entry_install_test_seams();
    entry_test_server_result = GG_ERR_OK;
    GglDeploymentHandlerThreadArgs args = { .root_path_fd = 7 };

    GG_TEST_ASSERT_OK(run_workers_and_server(&args));
    TEST_ASSERT_EQUAL_size_t(2, entry_test_worker_calls);
    TEST_ASSERT_EQUAL_size_t(1, entry_test_status_listener_calls);
    TEST_ASSERT_EQUAL_size_t(1, entry_test_server_calls);
}

GG_TEST_DEFINE(entry_worker_create_failure_skips_detach) {
    entry_reset_test_seams();
    entry_pthread_create = entry_test_record_create;
    entry_pthread_detach = entry_test_record_detach;
    entry_test_create_result = EAGAIN;

    pthread_t thread_id;
    TEST_ASSERT_NOT_EQUAL(
        GG_ERR_OK,
        start_worker_default(&thread_id, entry_test_noop_routine, NULL)
    );
    TEST_ASSERT_EQUAL_size_t(1, entry_test_create_calls);
    // A failed pthread_create must never detach an indeterminate thread ID.
    TEST_ASSERT_EQUAL_size_t(0, entry_test_detach_calls);

    entry_reset_test_seams();
}

GG_TEST_DEFINE(entry_worker_create_success_detaches_created_thread_once) {
    entry_reset_test_seams();
    entry_pthread_create = entry_test_record_create;
    entry_pthread_detach = entry_test_record_detach;
    entry_test_create_result = 0;
    // A real, comparable thread ID for pthread_create to hand back.
    entry_test_created_thread_id = pthread_self();

    pthread_t thread_id;
    GG_TEST_ASSERT_OK(
        start_worker_default(&thread_id, entry_test_noop_routine, NULL)
    );
    TEST_ASSERT_EQUAL_size_t(1, entry_test_create_calls);
    TEST_ASSERT_EQUAL_size_t(1, entry_test_detach_calls);
    // The exact thread ID produced by pthread_create is the one detached.
    TEST_ASSERT_TRUE(pthread_equal(entry_test_created_thread_id, thread_id));
    TEST_ASSERT_TRUE(pthread_equal(thread_id, entry_test_detached_thread_id));

    entry_reset_test_seams();
}

GG_TEST_DEFINE(entry_worker_detach_failure_propagates_for_created_thread) {
    entry_reset_test_seams();
    entry_pthread_create = entry_test_record_create;
    entry_pthread_detach = entry_test_record_detach;
    entry_test_created_thread_id = pthread_self();
    entry_test_detach_result = EINVAL;

    pthread_t thread_id;
    TEST_ASSERT_NOT_EQUAL(
        GG_ERR_OK,
        start_worker_default(&thread_id, entry_test_noop_routine, NULL)
    );
    TEST_ASSERT_EQUAL_size_t(1, entry_test_create_calls);
    TEST_ASSERT_EQUAL_size_t(1, entry_test_detach_calls);
    TEST_ASSERT_TRUE(pthread_equal(entry_test_created_thread_id, thread_id));
    TEST_ASSERT_TRUE(pthread_equal(thread_id, entry_test_detached_thread_id));

    entry_reset_test_seams();
}

GG_TEST_DEFINE(entry_worker_detach_failure_aborts_coordinator_startup) {
    entry_reset_test_seams();
    entry_pthread_create = entry_test_record_create;
    entry_pthread_detach = entry_test_record_detach;
    entry_start_status_listener = entry_test_record_status_listener;
    entry_start_server = entry_test_record_server;
    entry_test_created_thread_id = pthread_self();
    entry_test_detach_result = EINVAL;
    GglDeploymentHandlerThreadArgs args = { .root_path_fd = 7 };

    TEST_ASSERT_NOT_EQUAL(GG_ERR_OK, run_workers_and_server(&args));
    // Only the first worker is created; later startup does not run.
    TEST_ASSERT_EQUAL_size_t(1, entry_test_create_calls);
    TEST_ASSERT_EQUAL_size_t(1, entry_test_detach_calls);
    TEST_ASSERT_TRUE(pthread_equal(
        entry_test_created_thread_id, entry_test_detached_thread_id
    ));
    TEST_ASSERT_EQUAL_size_t(0, entry_test_status_listener_calls);
    TEST_ASSERT_EQUAL_size_t(0, entry_test_server_calls);

    entry_reset_test_seams();
}

GG_TEST_DEFINE(entry_worker_args_persist_after_initializer_returns) {
    int expected_fd = 7;
    GgBuffer expected_root = GG_STR("/var/lib/greengrass");
    const char *expected_bin = "/usr/bin/";

    GglDeploymentHandlerThreadArgs *first
        = init_worker_args(expected_fd, expected_root, expected_bin);
    // The populated context is fully readable after the initializer returns.
    TEST_ASSERT_NOT_NULL(first);
    TEST_ASSERT_EQUAL_INT(expected_fd, first->root_path_fd);
    TEST_ASSERT_EQUAL_PTR(expected_bin, first->bin_path);
    TEST_ASSERT_TRUE(gg_buffer_eq(first->root_path, expected_root));

    // The storage is static, so the pointer handed to the detached workers is
    // stable across calls and outlives run_ggdeploymentd.
    GglDeploymentHandlerThreadArgs *second
        = init_worker_args(expected_fd, expected_root, expected_bin);
    TEST_ASSERT_EQUAL_PTR(first, second);
}

#endif
