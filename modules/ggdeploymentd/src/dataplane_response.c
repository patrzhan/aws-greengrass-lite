// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#include "dataplane_response.h"
#include <gg/arena.h>
#include <gg/json_decode.h>
#include <gg/map.h>
#include <gg/object.h>
#include <stddef.h>

GgError ggl_dataplane_response_get_typed_field(
    GgBuffer response,
    GgArena *alloc,
    GgBuffer field_name,
    GgObjectType expected_type,
    GgObject **result
) {
    if ((alloc == NULL) || (result == NULL)
        || ((response.len > 0) && (response.data == NULL))
        || ((field_name.len > 0) && (field_name.data == NULL))
        || (expected_type < GG_TYPE_NULL) || (expected_type > GG_TYPE_MAP)) {
        return GG_ERR_INVALID;
    }

    GgObject response_obj;
    GgError ret = gg_json_decode_destructive(response, alloc, &response_obj);
    if (ret != GG_ERR_OK) {
        return ret;
    }
    if (gg_obj_type(response_obj) != GG_TYPE_MAP) {
        return GG_ERR_PARSE;
    }

    GgObject *field;
    if (!gg_map_get(gg_obj_into_map(response_obj), field_name, &field)
        || (gg_obj_type(*field) != expected_type)) {
        return GG_ERR_PARSE;
    }

    *result = field;
    return GG_ERR_OK;
}

#ifdef GG_SDK_TESTING

#include <gg/test.h>
#include <unity.h>
#include <stdint.h>

GG_TEST_DEFINE(dataplane_response_returns_valid_typed_field) {
    uint8_t response[] = "{\"items\":[\"first\",\"second\"]}";
    uint8_t arena_mem[128];
    GgArena alloc = gg_arena_init(GG_BUF(arena_mem));
    GgObject *result = NULL;

    GG_TEST_ASSERT_OK(ggl_dataplane_response_get_typed_field(
        (GgBuffer) { .data = response, .len = sizeof(response) - 1 },
        &alloc,
        GG_STR("items"),
        GG_TYPE_LIST,
        &result
    ));
    TEST_ASSERT_NOT_NULL(result);
    TEST_ASSERT_EQUAL_INT(GG_TYPE_LIST, gg_obj_type(*result));
    TEST_ASSERT_EQUAL_size_t(2, gg_obj_into_list(*result).len);
}

GG_TEST_DEFINE(dataplane_response_preserves_decoder_error_and_output) {
    uint8_t response[] = "{\"items\":";
    uint8_t arena_mem[128];
    GgArena alloc = gg_arena_init(GG_BUF(arena_mem));
    GgObject sentinel = gg_obj_i64(42);
    GgObject *result = &sentinel;

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_PARSE,
        ggl_dataplane_response_get_typed_field(
            (GgBuffer) { .data = response, .len = sizeof(response) - 1 },
            &alloc,
            GG_STR("items"),
            GG_TYPE_LIST,
            &result
        )
    );
    TEST_ASSERT_EQUAL_PTR(&sentinel, result);
}

GG_TEST_DEFINE(dataplane_response_rejects_non_map_without_output) {
    uint8_t response[] = "[]";
    uint8_t arena_mem[64];
    GgArena alloc = gg_arena_init(GG_BUF(arena_mem));
    GgObject sentinel = gg_obj_i64(42);
    GgObject *result = &sentinel;

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_PARSE,
        ggl_dataplane_response_get_typed_field(
            (GgBuffer) { .data = response, .len = sizeof(response) - 1 },
            &alloc,
            GG_STR("items"),
            GG_TYPE_LIST,
            &result
        )
    );
    TEST_ASSERT_EQUAL_PTR(&sentinel, result);
}

GG_TEST_DEFINE(dataplane_response_rejects_missing_field_without_output) {
    uint8_t response[] = "{}";
    uint8_t arena_mem[64];
    GgArena alloc = gg_arena_init(GG_BUF(arena_mem));
    GgObject sentinel = gg_obj_i64(42);
    GgObject *result = &sentinel;

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_PARSE,
        ggl_dataplane_response_get_typed_field(
            (GgBuffer) { .data = response, .len = sizeof(response) - 1 },
            &alloc,
            GG_STR("items"),
            GG_TYPE_LIST,
            &result
        )
    );
    TEST_ASSERT_EQUAL_PTR(&sentinel, result);
}

GG_TEST_DEFINE(dataplane_response_rejects_wrong_type_without_output) {
    uint8_t response[] = "{\"items\":{}}";
    uint8_t arena_mem[64];
    GgArena alloc = gg_arena_init(GG_BUF(arena_mem));
    GgObject sentinel = gg_obj_i64(42);
    GgObject *result = &sentinel;

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_PARSE,
        ggl_dataplane_response_get_typed_field(
            (GgBuffer) { .data = response, .len = sizeof(response) - 1 },
            &alloc,
            GG_STR("items"),
            GG_TYPE_LIST,
            &result
        )
    );
    TEST_ASSERT_EQUAL_PTR(&sentinel, result);
}

GG_TEST_DEFINE(dataplane_response_rejects_invalid_required_arguments) {
    uint8_t response[] = "{\"items\":[]}";
    uint8_t arena_mem[64];
    GgArena alloc = gg_arena_init(GG_BUF(arena_mem));
    GgObject sentinel = gg_obj_i64(42);
    GgObject *result = &sentinel;
    GgBuffer valid_response = { .data = response, .len = sizeof(response) - 1 };

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID,
        ggl_dataplane_response_get_typed_field(
            valid_response, NULL, GG_STR("items"), GG_TYPE_LIST, &result
        )
    );
    TEST_ASSERT_EQUAL_PTR(&sentinel, result);
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID,
        ggl_dataplane_response_get_typed_field(
            valid_response, &alloc, GG_STR("items"), GG_TYPE_LIST, NULL
        )
    );
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID,
        ggl_dataplane_response_get_typed_field(
            (GgBuffer) { .data = NULL, .len = 1 },
            &alloc,
            GG_STR("items"),
            GG_TYPE_LIST,
            &result
        )
    );
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID,
        ggl_dataplane_response_get_typed_field(
            valid_response,
            &alloc,
            (GgBuffer) { .data = NULL, .len = 1 },
            GG_TYPE_LIST,
            &result
        )
    );
    TEST_ASSERT_EQUAL_PTR(&sentinel, result);
}

#endif
