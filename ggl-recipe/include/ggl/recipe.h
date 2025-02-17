// aws-greengrass-lite - AWS IoT Greengrass runtime for constrained devices
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#ifndef GGL_RECIPE_H
#define GGL_RECIPE_H

//! Greengrass recipe utils

#include "stdbool.h"
#include <ggl/alloc.h>
#include <ggl/buffer.h>
#include <ggl/error.h>
#include <ggl/object.h>

GglError ggl_recipe_get_from_file(
    int root_path_fd,
    GglBuffer component_name,
    GglBuffer component_version,
    GglAlloc *alloc,
    GglObject *recipe
);

GglError fetch_script_section(
    GglMap selected_lifecycle,
    GglBuffer selected_phase,
    bool *is_root,
    GglBuffer *out_selected_script_as_buf,
    GglMap *out_set_env_as_map
);

GglError select_linux_manifest(
    GglMap recipe_map, GglMap *out_selected_lifecycle_map
);

#endif
