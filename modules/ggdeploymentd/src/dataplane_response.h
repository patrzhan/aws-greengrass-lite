// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#ifndef GGDEPLOYMENTD_DATAPLANE_RESPONSE_H
#define GGDEPLOYMENTD_DATAPLANE_RESPONSE_H

#include <gg/arena.h>
#include <gg/error.h>
#include <gg/types.h>

/// Destructively decode response into alloc and retrieve a required top-level
/// field of expected_type. Decoder errors are returned unchanged. JSON shape
/// mismatches return GG_ERR_PARSE. Any failure leaves result unchanged.
GgError ggl_dataplane_response_get_typed_field(
    GgBuffer response,
    GgArena *alloc,
    GgBuffer field_name,
    GgObjectType expected_type,
    GgObject **result
);

#endif
