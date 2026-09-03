// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#include "config_access.h"
#include <gg/arena.h>
#include <gg/error.h>
#include <gg/log.h>
#include <gg/object.h>
#include <gg/types.h>
#include <gg/vector.h>
#include <ggl/core_bus/gg_config.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef GG_SDK_TESTING

static GglDeploymentConfigReader config_reader = ggl_gg_config_read;

void ggl_deployment_config_set_reader_for_test(GglDeploymentConfigReader reader
) {
    config_reader = (reader == NULL) ? ggl_gg_config_read : reader;
}

#else

// NOLINTNEXTLINE(readability-identifier-naming)
#define config_reader ggl_gg_config_read

#endif

static bool buffers_overlap(GgBuffer first, GgBuffer second) {
    if ((first.len == 0) || (second.len == 0)) {
        return false;
    }

    uintptr_t first_start = (uintptr_t) first.data;
    uintptr_t second_start = (uintptr_t) second.data;
    if ((first.len > (UINTPTR_MAX - first_start))
        || (second.len > (UINTPTR_MAX - second_start))) {
        return true;
    }

    uintptr_t first_end = first_start + first.len;
    uintptr_t second_end = second_start + second.len;
    return (first_start < second_end) && (second_start < first_end);
}

GgError ggl_deployment_config_read_object(
    GgBufList key_path,
    GgArena *scratch_alloc,
    GgObjectType expected_type,
    GgObject *result
) {
    if ((scratch_alloc == NULL) || (result == NULL)) {
        return GG_ERR_INVALID;
    }

    GgObject value;
    GgError ret = config_reader(key_path, scratch_alloc, &value);
    if (ret != GG_ERR_OK) {
        return ret;
    }
    if (gg_obj_type(value) != expected_type) {
        GG_LOGE(
            "Configuration value has type %d, expected %d.",
            (int) gg_obj_type(value),
            (int) expected_type
        );
        return GG_ERR_CONFIG;
    }

    *result = value;
    return GG_ERR_OK;
}

GgError ggl_deployment_config_read_string(
    GgBufList key_path, GgBuffer scratch, GgByteVec *destination
) {
    if ((scratch.data == NULL) || (destination == NULL)
        || (destination->buf.data == NULL)) {
        return GG_ERR_INVALID;
    }

    GgBuffer destination_storage = {
        .data = destination->buf.data,
        .len = destination->capacity,
    };
    if (buffers_overlap(scratch, destination_storage)) {
        return GG_ERR_INVALID;
    }

    GgArena scratch_alloc = gg_arena_init(scratch);
    GgObject value;
    GgError ret = ggl_deployment_config_read_object(
        key_path, &scratch_alloc, GG_TYPE_BUF, &value
    );
    if (ret != GG_ERR_OK) {
        return ret;
    }

    GgBuffer string = gg_obj_into_buf(value);
    if (string.len >= destination->capacity) {
        return GG_ERR_NOMEM;
    }

    if (string.len > 0) {
        memcpy(destination->buf.data, string.data, string.len);
    }
    destination->buf.data[string.len] = '\0';
    destination->buf.len = string.len;
    return GG_ERR_OK;
}

GgError ggl_deployment_copy_buffer(GgBuffer source, GgBuffer *destination) {
    if ((destination == NULL) || ((source.len > 0) && (source.data == NULL))
        || ((destination->len > 0) && (destination->data == NULL))) {
        return GG_ERR_INVALID;
    }
    if (source.len > destination->len) {
        return GG_ERR_RANGE;
    }

    if (source.len > 0) {
        memmove(destination->data, source.data, source.len);
    }
    destination->len = source.len;
    return GG_ERR_OK;
}

#ifdef GG_SDK_TESTING

#include <gg/test.h>
#include <unity.h>

static GgError fake_read_error;
static GgObject fake_read_value;

static GgError fake_config_reader(
    GgBufList key_path, GgArena *alloc, GgObject *result
) {
    (void) key_path;
    (void) alloc;
    if (fake_read_error != GG_ERR_OK) {
        return fake_read_error;
    }
    *result = fake_read_value;
    return GG_ERR_OK;
}

static void use_fake_config_value(GgObject value) {
    fake_read_error = GG_ERR_OK;
    fake_read_value = value;
    ggl_deployment_config_set_reader_for_test(fake_config_reader);
}

GG_TEST_DEFINE(config_object_rejects_null_outputs) {
    GgArena alloc = gg_arena_init(GG_BUF((uint8_t[8]) { 0 }));
    GgObject result;

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID,
        ggl_deployment_config_read_object(
            GG_BUF_LIST(GG_STR("key")), NULL, GG_TYPE_BUF, &result
        )
    );
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID,
        ggl_deployment_config_read_object(
            GG_BUF_LIST(GG_STR("key")), &alloc, GG_TYPE_BUF, NULL
        )
    );
}

GG_TEST_DEFINE(config_object_preserves_read_errors_and_output) {
    fake_read_error = GG_ERR_NOENTRY;
    ggl_deployment_config_set_reader_for_test(fake_config_reader);
    GgArena alloc = gg_arena_init(GG_BUF((uint8_t[8]) { 0 }));
    GgObject result = gg_obj_i64(42);

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOENTRY,
        ggl_deployment_config_read_object(
            GG_BUF_LIST(GG_STR("key")), &alloc, GG_TYPE_I64, &result
        )
    );
    TEST_ASSERT_EQUAL_INT(GG_TYPE_I64, gg_obj_type(result));
    TEST_ASSERT_EQUAL_INT64(42, gg_obj_into_i64(result));

    fake_read_error = GG_ERR_NOCONN;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOCONN,
        ggl_deployment_config_read_object(
            GG_BUF_LIST(GG_STR("key")), &alloc, GG_TYPE_I64, &result
        )
    );
    TEST_ASSERT_EQUAL_INT64(42, gg_obj_into_i64(result));
}

GG_TEST_DEFINE(config_object_rejects_wrong_type_without_output_mutation) {
    use_fake_config_value(gg_obj_buf(GG_STR("not-a-list")));
    GgArena alloc = gg_arena_init(GG_BUF((uint8_t[8]) { 0 }));
    GgObject result = gg_obj_i64(7);

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_CONFIG,
        ggl_deployment_config_read_object(
            GG_BUF_LIST(GG_STR("key")), &alloc, GG_TYPE_LIST, &result
        )
    );
    TEST_ASSERT_EQUAL_INT64(7, gg_obj_into_i64(result));
}

GG_TEST_DEFINE(config_string_long_to_short_is_nul_terminated) {
    uint8_t scratch[32];
    uint8_t storage[16] = { 0 };
    GgByteVec destination = GG_BYTE_VEC(storage);

    use_fake_config_value(gg_obj_buf(GG_STR("long-value")));
    GG_TEST_ASSERT_OK(ggl_deployment_config_read_string(
        GG_BUF_LIST(GG_STR("key")), GG_BUF(scratch), &destination
    ));
    TEST_ASSERT_EQUAL_size_t(10, destination.buf.len);
    TEST_ASSERT_EQUAL_UINT8('\0', storage[10]);

    destination = GG_BYTE_VEC(storage);
    use_fake_config_value(gg_obj_buf(GG_STR("x")));
    GG_TEST_ASSERT_OK(ggl_deployment_config_read_string(
        GG_BUF_LIST(GG_STR("key")), GG_BUF(scratch), &destination
    ));
    TEST_ASSERT_EQUAL_size_t(1, destination.buf.len);
    TEST_ASSERT_EQUAL_UINT8('\0', storage[1]);
    TEST_ASSERT_TRUE(gg_buffer_eq(destination.buf, GG_STR("x")));
    TEST_ASSERT_TRUE(
        gg_buffer_eq(gg_buffer_from_null_term((char *) storage), GG_STR("x"))
    );
}

GG_TEST_DEFINE(config_string_failure_does_not_mutate_destination) {
    uint8_t scratch[32];
    uint8_t storage[5] = { 'k', 'e', 'e', 'p', '\0' };
    GgByteVec destination = GG_BYTE_VEC(storage);
    destination.buf.len = 4;

    use_fake_config_value(gg_obj_i64(1));
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_CONFIG,
        ggl_deployment_config_read_string(
            GG_BUF_LIST(GG_STR("key")), GG_BUF(scratch), &destination
        )
    );
    TEST_ASSERT_EQUAL_size_t(4, destination.buf.len);
    TEST_ASSERT_EQUAL_MEMORY("keep", storage, 4);

    use_fake_config_value(gg_obj_buf(GG_STR("too-long")));
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOMEM,
        ggl_deployment_config_read_string(
            GG_BUF_LIST(GG_STR("key")), GG_BUF(scratch), &destination
        )
    );
    TEST_ASSERT_EQUAL_size_t(4, destination.buf.len);
    TEST_ASSERT_EQUAL_MEMORY("keep", storage, 4);
}

GG_TEST_DEFINE(config_string_requires_separate_valid_storage) {
    uint8_t storage[8];
    GgByteVec destination = GG_BYTE_VEC(storage);

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID,
        ggl_deployment_config_read_string(
            GG_BUF_LIST(GG_STR("key")), GG_BUF(storage), &destination
        )
    );
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID,
        ggl_deployment_config_read_string(
            GG_BUF_LIST(GG_STR("key")), GG_BUF(storage), NULL
        )
    );
}

GG_TEST_DEFINE(config_copy_supports_exact_capacity) {
    uint8_t source_mem[64];
    memset(source_mem, 'a', sizeof(source_mem));
    uint8_t destination_mem[64] = { 0 };
    GgBuffer destination = GG_BUF(destination_mem);

    GG_TEST_ASSERT_OK(
        ggl_deployment_copy_buffer(GG_BUF(source_mem), &destination)
    );
    TEST_ASSERT_EQUAL_size_t(sizeof(source_mem), destination.len);
    TEST_ASSERT_EQUAL_MEMORY(source_mem, destination_mem, sizeof(source_mem));
}

GG_TEST_DEFINE(config_copy_rejects_over_capacity_without_mutation) {
    uint8_t destination_mem[4] = { 'k', 'e', 'e', 'p' };
    GgBuffer destination = GG_BUF(destination_mem);

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_RANGE, ggl_deployment_copy_buffer(GG_STR("large"), &destination)
    );
    TEST_ASSERT_EQUAL_size_t(sizeof(destination_mem), destination.len);
    TEST_ASSERT_EQUAL_MEMORY("keep", destination_mem, 4);
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID, ggl_deployment_copy_buffer(GG_STR("x"), NULL)
    );
}

#endif
