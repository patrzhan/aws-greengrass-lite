// aws-greengrass-lite - AWS IoT Greengrass runtime for constrained devices
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#ifndef GGDEPLOYMENTD_COMPONENT_STORE_H
#define GGDEPLOYMENTD_COMPONENT_STORE_H

#include <dirent.h>
#include <gg/error.h>
#include <gg/types.h>

GgError get_recipe_dir_fd(int *recipe_fd);

/// Returns the next valid component recipe in @p dir. Malformed directory
/// entries and entries that do not match the supported recipe filename format
/// are skipped, so they are invisible to stale cleanup. The writable storage
/// referenced by @p component_name_buffer and @p version must each have
/// NAME_MAX bytes of capacity. Outputs are changed only when GG_ERR_OK is
/// returned, and GG_ERR_OK guarantees that @p entry points to a non-NULL
/// directory entry. GG_ERR_NOENTRY indicates clean directory exhaustion; any
/// other error indicates that the directory scan failed.
///
/// The @p entry output and this public signature are deliberately retained in
/// M7 for compatibility with the existing stale-cleanup API.
GgError iterate_over_components(
    DIR *dir,
    GgBuffer *component_name_buffer,
    GgBuffer *version,
    struct dirent **entry
);

GgError find_available_component(
    GgBuffer component_name, GgBuffer requirement, GgBuffer *version
);

#endif
