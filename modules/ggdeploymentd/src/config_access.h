// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#ifndef GGDEPLOYMENTD_CONFIG_ACCESS_H
#define GGDEPLOYMENTD_CONFIG_ACCESS_H

#include <gg/arena.h>
#include <gg/error.h>
#include <gg/types.h>
#include <gg/vector.h>

/// Read a config value and validate its type before publishing it. The result
/// aliases memory owned by scratch_alloc. Read and type errors leave result
/// unchanged.
GgError ggl_deployment_config_read_object(
    GgBufList key_path,
    GgArena *scratch_alloc,
    GgObjectType expected_type,
    GgObject *result
);

/// Read a string through separate scratch storage and overwrite destination.
/// On success destination contains a physical NUL terminator which is excluded
/// from destination->buf.len. Any failure leaves destination unchanged.
GgError ggl_deployment_config_read_string(
    GgBufList key_path, GgBuffer scratch, GgByteVec *destination
);

/// Copy source into destination using destination->len as input capacity. On
/// success destination->len is the copied logical length. Any failure leaves
/// destination and its bytes unchanged. Exact-capacity copies are supported.
GgError ggl_deployment_copy_buffer(GgBuffer source, GgBuffer *destination);

#ifdef GG_SDK_TESTING

typedef GgError (*GglDeploymentConfigReader)(
    GgBufList key_path, GgArena *alloc, GgObject *result
);

void ggl_deployment_config_set_reader_for_test(GglDeploymentConfigReader reader
);

#endif

#endif
