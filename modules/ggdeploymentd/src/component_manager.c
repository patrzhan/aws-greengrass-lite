// aws-greengrass-lite - AWS IoT Greengrass runtime for constrained devices
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#include "component_manager.h"
#include "component_store.h"
#include "config_access.h"
#include <assert.h>
#include <gg/arena.h>
#include <gg/buffer.h>
#include <gg/error.h>
#include <gg/log.h>
#include <gg/vector.h>
#include <ggl/core_bus/gg_healthd.h>
#include <ggl/semver.h>
#include <limits.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef GG_SDK_TESTING

static GgError (*component_status_reader)(
    GgBuffer component, GgArena *alloc, GgBuffer *component_status
) = ggl_gghealthd_retrieve_component_status;

void component_manager_reset_test_seams(void);
void component_manager_override_test_seam_for_reset_test(void);
bool component_manager_test_seams_are_reset(void);

static GgError component_manager_reset_test_override(
    GgBuffer component, GgArena *alloc, GgBuffer *component_status
) {
    (void) component;
    (void) alloc;
    (void) component_status;
    return GG_ERR_FAILURE;
}

void component_manager_reset_test_seams(void) {
    component_status_reader = ggl_gghealthd_retrieve_component_status;
}

void component_manager_override_test_seam_for_reset_test(void) {
    component_status_reader = component_manager_reset_test_override;
}

bool component_manager_test_seams_are_reset(void) {
    return component_status_reader == ggl_gghealthd_retrieve_component_status;
}

#else

// NOLINTNEXTLINE(readability-identifier-naming)
#define component_status_reader ggl_gghealthd_retrieve_component_status

#endif

static GgError find_active_version(
    GgBuffer package_name, GgBuffer version_requirement, GgBuffer *version
) {
    // check the config to see if the provided package name is already a running
    // service

    // find the version of the active running component
    uint8_t version_scratch[128];
    uint8_t version_storage[128];
    GgByteVec version_destination = GG_BYTE_VEC(version_storage);
    GgError ret = ggl_deployment_config_read_string(
        GG_BUF_LIST(GG_STR("services"), package_name, GG_STR("version")),
        GG_BUF(version_scratch),
        &version_destination
    );

    if (ret != GG_ERR_OK) {
        GG_LOGI(
            "Unable to retrieve version of %.*s. Assuming no active version found.",
            (int) package_name.len,
            package_name.data
        );
        return GG_ERR_NOENTRY;
    }

    GgBuffer version_resp = version_destination.buf;

    // active component found, update the version if it is a valid version
    if (!is_in_range(version_resp, version_requirement)) {
        return GG_ERR_NOENTRY;
    }

    // Check that the component is actually running (or finished)
    uint8_t component_status_buf[NAME_MAX];
    GgArena alloc = gg_arena_init(GG_BUF(component_status_buf));
    GgBuffer component_status;
    ret = component_status_reader(package_name, &alloc, &component_status);

    if (ret != GG_ERR_OK) {
        GG_LOGI(
            "Component status not found for component %.*s despite finding active version. Not using this version.",
            (int) package_name.len,
            package_name.data
        );
        return GG_ERR_INVALID;
    }

    if (!gg_buffer_eq(component_status, GG_STR("RUNNING"))
        && !gg_buffer_eq(component_status, GG_STR("FINISHED"))) {
        GG_LOGI(
            "Component %.*s is not in the RUNNING or FINISHED states. Not using the active version.",
            (int) package_name.len,
            package_name.data
        );
        return GG_ERR_INVALID;
    }

    ret = ggl_deployment_copy_buffer(version_resp, version);
    if (ret != GG_ERR_OK) {
        return ret;
    }
    return GG_ERR_OK;
}

static GgError find_best_candidate_locally(
    GgBuffer component_name, GgBuffer version_requirement, GgBuffer *version
) {
    GG_LOGD("Searching for the best local candidate on the device.");

    GgError ret
        = find_active_version(component_name, version_requirement, version);

    if (ret == GG_ERR_OK) {
        GG_LOGI("Found running component which meets the version requirements."
        );
        return GG_ERR_OK;
    }
    GG_LOGI(
        "No running component satisfies the version requirements. Searching in the local component store."
    );

    return find_available_component(
        component_name, version_requirement, version
    );
}

bool resolve_component_version(
    GgBuffer component_name,
    GgBuffer version_requirement,
    GgBuffer *resolved_version
) {
    GG_LOGD("Resolving component version.");

    // find best local candidate
    uint8_t local_version_arr[NAME_MAX];
    GgBuffer local_version = GG_BUF(local_version_arr);
    GgError ret = find_best_candidate_locally(
        component_name, version_requirement, &local_version
    );

    if (ret != GG_ERR_OK) {
        GG_LOGI(
            "Failed to find a local candidate that satisfies the requrement."
        );
        return false;
    }

    // TODO: also check that the component region matches the expected region
    // (component store functionality)
    GG_LOGI(
        "Found local candidate for %.*s that satisfies version requirements. Using the local candidate as the resolved version without negotiating with the cloud.",
        (int) component_name.len,
        (char *) component_name.data
    );

    assert(local_version.len <= NAME_MAX);
    memcpy(resolved_version->data, local_version.data, local_version.len);
    resolved_version->len = local_version.len;
    return true;
}

#ifdef GG_SDK_TESTING

#include <gg/test.h>
#include <unity.h>

static GgError component_manager_config_error;
static GgObject component_manager_config_value;

static GgError component_manager_fake_config_reader(
    GgBufList key_path, GgArena *alloc, GgObject *result
) {
    (void) key_path;
    (void) alloc;
    if (component_manager_config_error != GG_ERR_OK) {
        return component_manager_config_error;
    }
    *result = component_manager_config_value;
    return GG_ERR_OK;
}

static GgError component_manager_health_error;
static GgBuffer component_manager_health_status;

static GgError component_manager_fake_status_reader(
    GgBuffer component, GgArena *alloc, GgBuffer *component_status
) {
    (void) component;
    (void) alloc;
    if (component_manager_health_error != GG_ERR_OK) {
        return component_manager_health_error;
    }
    *component_status = component_manager_health_status;
    return GG_ERR_OK;
}

static void assert_active_version_is_caller_owned(GgBuffer health_status) {
    uint8_t output_mem[16] = { 0 };
    GgBuffer output = GG_BUF(output_mem);
    component_manager_config_error = GG_ERR_OK;
    component_manager_config_value = gg_obj_buf(GG_STR("1.2.3"));
    ggl_deployment_config_set_reader_for_test(
        component_manager_fake_config_reader
    );
    component_manager_health_error = GG_ERR_OK;
    component_manager_health_status = health_status;
    component_status_reader = component_manager_fake_status_reader;

    GG_TEST_ASSERT_OK(
        find_active_version(GG_STR("component"), GG_STR(">=1.0.0"), &output)
    );
    TEST_ASSERT_EQUAL_PTR(output_mem, output.data);
    TEST_ASSERT_TRUE(gg_buffer_eq(output, GG_STR("1.2.3")));

    uint8_t next_output_mem[16] = { 0 };
    GgBuffer next_output = GG_BUF(next_output_mem);
    component_manager_config_value = gg_obj_buf(GG_STR("9.9.9"));
    GG_TEST_ASSERT_OK(find_active_version(
        GG_STR("component"), GG_STR(">=1.0.0"), &next_output
    ));
    TEST_ASSERT_TRUE(gg_buffer_eq(next_output, GG_STR("9.9.9")));
    TEST_ASSERT_TRUE(gg_buffer_eq(output, GG_STR("1.2.3")));
    TEST_ASSERT_EQUAL_MEMORY("1.2.3", output_mem, 5);
}

GG_TEST_DEFINE(active_version_running_is_copied_to_caller_storage) {
    assert_active_version_is_caller_owned(GG_STR("RUNNING"));
}

GG_TEST_DEFINE(active_version_finished_is_copied_to_caller_storage) {
    assert_active_version_is_caller_owned(GG_STR("FINISHED"));
}

GG_TEST_DEFINE(active_version_missing_or_unusable_remains_not_found) {
    uint8_t output_mem[8] = { 'k', 'e', 'e', 'p', 0, 0, 0, 0 };
    GgBuffer output = GG_BUF(output_mem);
    ggl_deployment_config_set_reader_for_test(
        component_manager_fake_config_reader
    );

    component_manager_config_error = GG_ERR_NOENTRY;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOENTRY,
        find_active_version(GG_STR("component"), GG_STR(">=1.0.0"), &output)
    );
    TEST_ASSERT_EQUAL_size_t(sizeof(output_mem), output.len);
    TEST_ASSERT_EQUAL_MEMORY("keep", output_mem, 4);

    component_manager_config_error = GG_ERR_OK;
    component_manager_config_value = gg_obj_i64(1);
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOENTRY,
        find_active_version(GG_STR("component"), GG_STR(">=1.0.0"), &output)
    );
    TEST_ASSERT_EQUAL_size_t(sizeof(output_mem), output.len);
    TEST_ASSERT_EQUAL_MEMORY("keep", output_mem, 4);

    uint8_t oversized_version[128];
    memset(oversized_version, '1', sizeof(oversized_version));
    component_manager_config_value = gg_obj_buf(GG_BUF(oversized_version));
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOENTRY,
        find_active_version(GG_STR("component"), GG_STR(">=1.0.0"), &output)
    );
    TEST_ASSERT_EQUAL_size_t(sizeof(output_mem), output.len);
    TEST_ASSERT_EQUAL_MEMORY("keep", output_mem, 4);
}

#endif
