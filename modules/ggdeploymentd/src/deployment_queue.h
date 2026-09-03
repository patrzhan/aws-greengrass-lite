// aws-greengrass-lite - AWS IoT Greengrass runtime for constrained devices
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#ifndef GGDEPLOYMENTD_QUEUE_H
#define GGDEPLOYMENTD_QUEUE_H

#include "deployment_model.h"
#include <gg/arena.h>
#include <gg/error.h>
#include <gg/types.h>
#include <gg/vector.h>
#include <stdint.h>

/// Opaque capability for releasing one successfully dequeued deployment.
///
/// Callers must zero-initialize this type and must not inspect its contents.
/// Only ggl_deployment_dequeue publishes a valid token.
typedef struct {
    uint8_t _opaque[3 * sizeof(uint64_t)];
} GglDeploymentQueueToken;

/// Attempts to add a deployment into the queue.
///
/// If the deployment ID does not exist already in the queue, then add the
/// deployment to the end of the queue. If there is an existing deployment in
/// the queue with the same ID, then replace it if the deployment is in a
/// replaceable state. Return GG_ERR_CONFLICT if that deployment is already in
/// progress, or GG_ERR_BUSY if a new deployment cannot fit in the queue.
GgError ggl_deployment_enqueue(
    GgMap deployment_doc,
    GgByteVec *id,
    GgBuffer iot_job_id,
    GglDeploymentType type
);

/// Get the next deployment and its release token.
///
/// Blocks until a deployment is available if the queue is empty. On failure,
/// both outputs are unchanged.
GgError ggl_deployment_dequeue(
    GglDeployment **deployment, GglDeploymentQueueToken *token
);

/// Release a successfully dequeued deployment.
///
/// Invalid, stale, duplicate, or non-head tokens do not mutate the queue. On
/// success the token is invalidated.
GgError ggl_deployment_release(GglDeploymentQueueToken *token);

GgError deep_copy_deployment(GglDeployment *deployment, GgArena *alloc);

/// Validate a component name.
///
/// Names must match `[a-zA-Z0-9._-]+` and be 1-128 characters long
GgError ggl_validate_component_name(GgBuffer name);

#ifdef GG_SDK_TESTING

void ggl_deployment_queue_reset_for_test(void);
size_t ggl_deployment_queue_count_for_test(void);
size_t ggl_deployment_queue_index_for_test(void);
const GglDeployment *ggl_deployment_queue_slot_for_test(size_t slot);
const uint8_t *ggl_deployment_queue_storage_for_test(size_t slot);
const uint8_t *ggl_deployment_queue_scratch_for_test(void);
uint64_t ggl_deployment_queue_generation_for_test(size_t slot);
uint64_t ggl_deployment_queue_serial_for_test(size_t slot);
GglDeploymentQueueToken ggl_deployment_queue_token_for_test(
    size_t slot, uint64_t generation, uint64_t serial
);

#endif

#endif
