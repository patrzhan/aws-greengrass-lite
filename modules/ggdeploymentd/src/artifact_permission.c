// aws-greengrass-lite - AWS IoT Greengrass runtime for constrained devices
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#include "artifact_permission.h"
#include <gg/buffer.h>
#include <gg/error.h>
#include <gg/map.h>
#include <gg/object.h>
#include <gg/types.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdbool.h>
#include <stddef.h>

static bool is_valid_permission_value(GgBuffer value) {
    return gg_buffer_eq(value, GG_STR("OWNER"))
        || gg_buffer_eq(value, GG_STR("ALL"))
        || gg_buffer_eq(value, GG_STR("NONE"));
}

// Matches Greengrass Nucleus permission logic from:
// https://github.com/aws-greengrass/aws-greengrass-nucleus/blob/main/src/main/java/com/aws/greengrass/componentmanager/models/Permission.java
//
// Greengrass Nucleus treats group == owner and execute implies group read:
//   ownerRead:    always true
//   ownerExecute: execute == OWNER || execute == ALL
//   groupRead:    read != NONE || execute != NONE
//   groupExecute: execute == OWNER || execute == ALL
//   otherRead:    read == ALL || execute == ALL
//   otherExecute: execute == ALL
GgError artifact_permission_to_mode(GgMap permission_map, mode_t *mode) {
    if (mode == NULL) {
        return GG_ERR_INVALID;
    }

    GgObject *read_obj = NULL;
    GgObject *execute_obj = NULL;

    gg_map_get(permission_map, GG_STR("Read"), &read_obj);
    gg_map_get(permission_map, GG_STR("Execute"), &execute_obj);

    GgBuffer read = GG_STR("OWNER");
    if (read_obj != NULL) {
        if (gg_obj_type(*read_obj) != GG_TYPE_BUF) {
            return GG_ERR_PARSE;
        }
        read = gg_obj_into_buf(*read_obj);
        if (!is_valid_permission_value(read)) {
            return GG_ERR_PARSE;
        }
    }

    GgBuffer execute = GG_STR("NONE");
    if (execute_obj != NULL) {
        if (gg_obj_type(*execute_obj) != GG_TYPE_BUF) {
            return GG_ERR_PARSE;
        }
        execute = gg_obj_into_buf(*execute_obj);
        if (!is_valid_permission_value(execute)) {
            return GG_ERR_PARSE;
        }
    }

    bool read_all = gg_buffer_eq(read, GG_STR("ALL"));
    bool read_none = gg_buffer_eq(read, GG_STR("NONE"));
    bool exec_owner = gg_buffer_eq(execute, GG_STR("OWNER"));
    bool exec_all = gg_buffer_eq(execute, GG_STR("ALL"));

    mode_t result = S_IRUSR;
    if (exec_owner || exec_all) {
        result |= S_IXUSR;
    }
    if (!read_none || exec_owner || exec_all) {
        result |= S_IRGRP;
    }
    if (exec_owner || exec_all) {
        result |= S_IXGRP;
    }
    if (read_all || exec_all) {
        result |= S_IROTH;
    }
    if (exec_all) {
        result |= S_IXOTH;
    }

    *mode = result;
    return GG_ERR_OK;
}

#ifdef GG_SDK_TESTING

#include <gg/test.h>
#include <unity.h>

static void assert_permission_mode(mode_t expected, GgMap permission_map) {
    mode_t mode = 0;
    GG_TEST_ASSERT_OK(artifact_permission_to_mode(permission_map, &mode));
    TEST_ASSERT_EQUAL_HEX16(expected, mode);
}

GG_TEST_DEFINE(permission_read_owner_exec_none) {
    assert_permission_mode(
        0440,
        GG_MAP(
            gg_kv(GG_STR("Read"), gg_obj_buf(GG_STR("OWNER"))),
            gg_kv(GG_STR("Execute"), gg_obj_buf(GG_STR("NONE")))
        )
    );
}

GG_TEST_DEFINE(permission_read_all_exec_none) {
    assert_permission_mode(
        0444,
        GG_MAP(
            gg_kv(GG_STR("Read"), gg_obj_buf(GG_STR("ALL"))),
            gg_kv(GG_STR("Execute"), gg_obj_buf(GG_STR("NONE")))
        )
    );
}

GG_TEST_DEFINE(permission_read_owner_exec_owner) {
    assert_permission_mode(
        0550,
        GG_MAP(
            gg_kv(GG_STR("Read"), gg_obj_buf(GG_STR("OWNER"))),
            gg_kv(GG_STR("Execute"), gg_obj_buf(GG_STR("OWNER")))
        )
    );
}

GG_TEST_DEFINE(permission_read_owner_exec_all) {
    assert_permission_mode(
        0555,
        GG_MAP(
            gg_kv(GG_STR("Read"), gg_obj_buf(GG_STR("OWNER"))),
            gg_kv(GG_STR("Execute"), gg_obj_buf(GG_STR("ALL")))
        )
    );
}

GG_TEST_DEFINE(permission_read_all_exec_all) {
    assert_permission_mode(
        0555,
        GG_MAP(
            gg_kv(GG_STR("Read"), gg_obj_buf(GG_STR("ALL"))),
            gg_kv(GG_STR("Execute"), gg_obj_buf(GG_STR("ALL")))
        )
    );
}

GG_TEST_DEFINE(permission_read_none_exec_none) {
    assert_permission_mode(
        0400,
        GG_MAP(
            gg_kv(GG_STR("Read"), gg_obj_buf(GG_STR("NONE"))),
            gg_kv(GG_STR("Execute"), gg_obj_buf(GG_STR("NONE")))
        )
    );
}

GG_TEST_DEFINE(permission_read_none_exec_owner) {
    assert_permission_mode(
        0550,
        GG_MAP(
            gg_kv(GG_STR("Read"), gg_obj_buf(GG_STR("NONE"))),
            gg_kv(GG_STR("Execute"), gg_obj_buf(GG_STR("OWNER")))
        )
    );
}

GG_TEST_DEFINE(permission_read_all_exec_owner) {
    assert_permission_mode(
        0554,
        GG_MAP(
            gg_kv(GG_STR("Read"), gg_obj_buf(GG_STR("ALL"))),
            gg_kv(GG_STR("Execute"), gg_obj_buf(GG_STR("OWNER")))
        )
    );
}

GG_TEST_DEFINE(permission_read_none_exec_all) {
    assert_permission_mode(
        0555,
        GG_MAP(
            gg_kv(GG_STR("Read"), gg_obj_buf(GG_STR("NONE"))),
            gg_kv(GG_STR("Execute"), gg_obj_buf(GG_STR("ALL")))
        )
    );
}

GG_TEST_DEFINE(permission_empty_map) {
    assert_permission_mode(0440, (GgMap) { 0 });
}

GG_TEST_DEFINE(permission_read_wrong_type) {
    mode_t mode = 0;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_PARSE,
        artifact_permission_to_mode(
            GG_MAP(gg_kv(GG_STR("Read"), gg_obj_i64(1))), &mode
        )
    );
}

GG_TEST_DEFINE(permission_execute_wrong_type) {
    mode_t mode = 0;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_PARSE,
        artifact_permission_to_mode(
            GG_MAP(gg_kv(GG_STR("Execute"), gg_obj_i64(1))), &mode
        )
    );
}

GG_TEST_DEFINE(permission_read_invalid_value) {
    mode_t mode = 0;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_PARSE,
        artifact_permission_to_mode(
            GG_MAP(gg_kv(GG_STR("Read"), gg_obj_buf(GG_STR("INVALID")))), &mode
        )
    );
}

GG_TEST_DEFINE(permission_execute_invalid_value) {
    mode_t mode = 0;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_PARSE,
        artifact_permission_to_mode(
            GG_MAP(gg_kv(GG_STR("Execute"), gg_obj_buf(GG_STR("INVALID")))),
            &mode
        )
    );
}

#endif
