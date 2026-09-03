// aws-greengrass-lite - AWS IoT Greengrass runtime for constrained devices
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#include "deployment_queue.h"
#include "config_access.h"
#include "deployment_model.h"
#include <assert.h>
#include <gg/arena.h>
#include <gg/buffer.h>
#include <gg/cleanup.h>
#include <gg/error.h>
#include <gg/flags.h>
#include <gg/log.h>
#include <gg/map.h>
#include <gg/object.h>
#include <gg/vector.h>
#include <pthread.h>
#include <string.h>
#include <sys/types.h>
#include <uuid/uuid.h>
#include <stdbool.h>
#include <stdint.h>

#ifndef DEPLOYMENT_QUEUE_SIZE
#define DEPLOYMENT_QUEUE_SIZE 10
#endif

#ifndef DEPLOYMENT_MEM_SIZE
#define DEPLOYMENT_MEM_SIZE 5000
#endif

#ifndef MAX_LOCAL_COMPONENTS
#define MAX_LOCAL_COMPONENTS 64
#endif

static GglDeployment deployments[DEPLOYMENT_QUEUE_SIZE];
static uint8_t deployment_mem[DEPLOYMENT_QUEUE_SIZE + 1][DEPLOYMENT_MEM_SIZE];
static uint8_t *deployment_storage[DEPLOYMENT_QUEUE_SIZE];
static uint8_t *scratch_storage;
static uint64_t deployment_generations[DEPLOYMENT_QUEUE_SIZE];
static uint64_t deployment_token_serials[DEPLOYMENT_QUEUE_SIZE];
static uint64_t next_token_serial;
static size_t queue_index = 0;
static size_t queue_count = 0;
static bool storage_initialized;

static pthread_mutex_t queue_mtx = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t notify_cond = PTHREAD_COND_INITIALIZER;

static void initialize_storage_mapping(void) {
    if (storage_initialized) {
        return;
    }
    for (size_t i = 0; i < DEPLOYMENT_QUEUE_SIZE; i++) {
        deployment_storage[i] = deployment_mem[i];
    }
    scratch_storage = deployment_mem[DEPLOYMENT_QUEUE_SIZE];
    storage_initialized = true;
}

static bool get_matching_deployment(GgBuffer deployment_id, size_t *index) {
    for (size_t i = 0; i < queue_count; i++) {
        size_t index_i = (queue_index + i) % DEPLOYMENT_QUEUE_SIZE;
        if (gg_buffer_eq(deployment_id, deployments[index_i].deployment_id)) {
            *index = index_i;
            return true;
        }
    }
    return false;
}

static GgError null_terminate_buffer(GgBuffer *buf, GgArena *alloc) {
    if (buf->len == 0) {
        *buf = GG_STR("");
        return GG_ERR_OK;
    }

    uint8_t *mem = GG_ARENA_ALLOCN(alloc, uint8_t, buf->len + 1);
    if (mem == NULL) {
        GG_LOGE("Failed to allocate memory for copying buffer.");
        return GG_ERR_NOMEM;
    }

    memcpy(mem, buf->data, buf->len);
    mem[buf->len] = '\0';
    buf->data = mem;
    return GG_ERR_OK;
}

GgError deep_copy_deployment(GglDeployment *deployment, GgArena *alloc) {
    assert(deployment != NULL);

    GgObject obj = gg_obj_buf(deployment->deployment_id);
    GgError ret = gg_arena_claim_obj(&obj, alloc);
    if (ret != GG_ERR_OK) {
        return ret;
    }
    deployment->deployment_id = gg_obj_into_buf(obj);

    if (deployment->iot_job_id.len > 0) {
        obj = gg_obj_buf(deployment->iot_job_id);
        ret = gg_arena_claim_obj(&obj, alloc);
        if (ret != GG_ERR_OK) {
            return ret;
        }
        deployment->iot_job_id = gg_obj_into_buf(obj);
    }

    ret = null_terminate_buffer(&deployment->recipe_directory_path, alloc);
    if (ret != GG_ERR_OK) {
        return ret;
    }

    ret = null_terminate_buffer(&deployment->artifacts_directory_path, alloc);
    if (ret != GG_ERR_OK) {
        return ret;
    }

    obj = gg_obj_map(deployment->components);
    ret = gg_arena_claim_obj(&obj, alloc);
    if (ret != GG_ERR_OK) {
        return ret;
    }
    deployment->components = gg_obj_into_map(obj);

    obj = gg_obj_buf(deployment->configuration_arn);
    ret = gg_arena_claim_obj(&obj, alloc);
    if (ret != GG_ERR_OK) {
        return ret;
    }
    deployment->configuration_arn = gg_obj_into_buf(obj);

    obj = gg_obj_buf(deployment->thing_group);
    ret = gg_arena_claim_obj(&obj, alloc);
    if (ret != GG_ERR_OK) {
        return ret;
    }
    deployment->thing_group = gg_obj_into_buf(obj);

    if (deployment->component_to_configuration.len > 0) {
        obj = gg_obj_map(deployment->component_to_configuration);
        ret = gg_arena_claim_obj(&obj, alloc);
        if (ret != GG_ERR_OK) {
            return ret;
        }
        deployment->component_to_configuration = gg_obj_into_map(obj);
    }

    return GG_ERR_OK;
}

/// Locate the '/' that starts the thing-group segment and the trailing ':'
/// that ends it in a thing-group configuration ARN of the form
/// "...:<thing-group>:<version>". The scan runs right to left, so it selects
/// the rightmost ':' (the version delimiter) and the rightmost '/'.
///
/// Returns GG_ERR_INVALID for a missing '/', a missing trailing ':', reversed
/// delimiters (the ':' at or before the '/'), or an empty thing-group segment.
/// On success the thing group is arn[*slash_index + 1, *last_colon_index) and
/// this function does not publish any deployment field, so a malformed ARN
/// aborts the enqueue before the queue is mutated.
static GgError get_slash_and_colon_locations_from_arn(
    GgBuffer arn, size_t *slash_index, size_t *last_colon_index
) {
    assert(*slash_index == 0);
    assert(*last_colon_index == 0);

    bool slash_found = false;
    bool colon_found = false;
    for (size_t i = arn.len; i > 0; i--) {
        if ((arn.data[i - 1] == ':') && !colon_found) {
            *last_colon_index = i - 1;
            colon_found = true;
        }
        if (arn.data[i - 1] == '/') {
            *slash_index = i - 1;
            slash_found = true;
        }
        if (slash_found && colon_found) {
            break;
        }
    }

    if (!slash_found || !colon_found) {
        GG_LOGE("Configuration ARN is missing a '/' or trailing ':' delimiter."
        );
        return GG_ERR_INVALID;
    }
    // The trailing ':' must fall after the '/' with at least one byte of
    // thing-group name between them. This also rejects reversed delimiters.
    if (*last_colon_index <= (*slash_index + 1)) {
        GG_LOGE(
            "Configuration ARN thing-group segment is empty or its delimiters are reversed."
        );
        return GG_ERR_INVALID;
    }

    return GG_ERR_OK;
}

static bool is_in_removal_list(
    GgBuffer component_name, GgObject *removal_list
) {
    if (removal_list == NULL) {
        return false;
    }
    GgList list = gg_obj_into_list(*removal_list);
    for (size_t i = 0; i < list.len; i++) {
        if (gg_obj_type(list.items[i]) == GG_TYPE_BUF
            && gg_buffer_eq(gg_obj_into_buf(list.items[i]), component_name)) {
            return true;
        }
    }
    return false;
}

static GgError push_component_version(
    GgKVVec *vec, GgBuffer name, GgObject version, GgArena *alloc
) {
    GgKV *info_mem = GG_ARENA_ALLOC(alloc, GgKV);
    if (info_mem == NULL) {
        GG_LOGE(
            "No memory when allocating memory while enqueuing local deployment."
        );
        return GG_ERR_NOMEM;
    }
    *info_mem = gg_kv(GG_STR("version"), version);
    GgMap info_map = (GgMap) { .pairs = info_mem, .len = 1 };
    return gg_kv_vec_push(vec, gg_kv(name, gg_obj_map(info_map)));
}

// Component names must be `[a-zA-Z0-9-_.]+`, length 1-128
GgError ggl_validate_component_name(GgBuffer name) {
    if ((name.len < 1) || (name.len > 128)) {
        GG_LOGE(
            "Component name has invalid length %zu (must be 1-128 characters).",
            name.len
        );
        return GG_ERR_INVALID;
    }
    for (size_t i = 0; i < name.len; i++) {
        uint8_t c = name.data[i];
        bool allowed = ((c >= 'A') && (c <= 'Z')) || ((c >= 'a') && (c <= 'z'))
            || ((c >= '0') && (c <= '9')) || (c == '.') || (c == '_')
            || (c == '-');
        if (!allowed) {
            GG_LOGE("Component name contains an invalid character.");
            return GG_ERR_INVALID;
        }
    }
    return GG_ERR_OK;
}

static GgError parse_local_deployment_components(
    GgObject *root_component_versions_to_add,
    GgObject *root_component_versions_to_remove,
    GglDeployment *doc,
    GgArena *alloc,
    GgKVVec *local_components_kv_vec
) {
    GgError ret;

    GgObject local_deployment_root_components_read_value;
    ret = ggl_deployment_config_read_object(
        GG_BUF_LIST(
            GG_STR("services"),
            GG_STR("DeploymentService"),
            GG_STR("thingGroupsToRootComponents"),
            doc->thing_group
        ),
        alloc,
        GG_TYPE_MAP,
        &local_deployment_root_components_read_value
    );
    if (ret == GG_ERR_NOENTRY) {
        GG_LOGI(
            "No info found in config for root components for local deployments, assuming no components have been deployed locally yet."
        );
        if (root_component_versions_to_add == NULL) {
            GG_LOGI("No root_component_versions_to_add provided.");
            doc->components = local_components_kv_vec->map;
            return GG_ERR_OK;
        }
        GG_MAP_FOREACH (
            component_pair, gg_obj_into_map(*root_component_versions_to_add)
        ) {
            if (gg_obj_type(*gg_kv_val(component_pair)) != GG_TYPE_BUF) {
                GG_LOGE(
                    "Local deployment component version read incorrectly from the deployment doc."
                );
                return GG_ERR_INVALID;
            }
            // TODO: Add configurationUpdate and runWith
            ret = push_component_version(
                local_components_kv_vec,
                gg_kv_key(*component_pair),
                *gg_kv_val(component_pair),
                alloc
            );
            if (ret != GG_ERR_OK) {
                return ret;
            }
        }
        doc->components = local_components_kv_vec->map;
        return GG_ERR_OK;
    }

    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to read local deployment root components from config.");
        return ret;
    }

    // Pre-populate with existing components, filtering removals
    GG_MAP_FOREACH (
        old_component_pair,
        gg_obj_into_map(local_deployment_root_components_read_value)
    ) {
        if (gg_obj_type(*gg_kv_val(old_component_pair)) != GG_TYPE_BUF) {
            GG_LOGE(
                "Local deployment component version read incorrectly from the config."
            );
            return GG_ERR_INVALID;
        }

        GG_LOGD(
            "Found existing local component %.*s as part of local deployments group.",
            (int) gg_kv_key(*old_component_pair).len,
            gg_kv_key(*old_component_pair).data
        );

        if (is_in_removal_list(
                gg_kv_key(*old_component_pair),
                root_component_versions_to_remove
            )) {
            GG_LOGI(
                "Removing component %.*s from local deployments group.",
                (int) gg_kv_key(*old_component_pair).len,
                gg_kv_key(*old_component_pair).data
            );
            continue;
        }

        ret = push_component_version(
            local_components_kv_vec,
            gg_kv_key(*old_component_pair),
            *gg_kv_val(old_component_pair),
            alloc
        );
        if (ret != GG_ERR_OK) {
            return ret;
        }
    }

    // Add or update components
    if (root_component_versions_to_add != NULL) {
        GG_MAP_FOREACH (
            component_pair, gg_obj_into_map(*root_component_versions_to_add)
        ) {
            if (gg_obj_type(*gg_kv_val(component_pair)) != GG_TYPE_BUF) {
                GG_LOGE(
                    "Local deployment component version read incorrectly from the deployment doc."
                );
                return GG_ERR_INVALID;
            }

            GgObject *existing_component_data;
            if (!gg_map_get(
                    local_components_kv_vec->map,
                    gg_kv_key(*component_pair),
                    &existing_component_data
                )) {
                GG_LOGD(
                    "Locally deployed component not previously deployed, adding it to the list of local components."
                );
                // TODO: Add configurationUpdate and runWith
                ret = push_component_version(
                    local_components_kv_vec,
                    gg_kv_key(*component_pair),
                    *gg_kv_val(component_pair),
                    alloc
                );
                if (ret != GG_ERR_OK) {
                    return ret;
                }
            } else {
                GgKV *info_mem = GG_ARENA_ALLOC(alloc, GgKV);
                if (info_mem == NULL) {
                    return GG_ERR_NOMEM;
                }
                *info_mem
                    = gg_kv(GG_STR("version"), *gg_kv_val(component_pair));
                GgMap info_map = (GgMap) { .pairs = info_mem, .len = 1 };
                *existing_component_data = gg_obj_map(info_map);
            }
        }
    }

    doc->components = local_components_kv_vec->map;
    return GG_ERR_OK;
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
static GgError parse_deployment_obj(
    GgMap args,
    GglDeployment *doc,
    GglDeploymentType type,
    GgArena *alloc,
    GgKVVec *local_components_kv_vec
) {
    *doc = (GglDeployment) { 0 };

    GgObject *recipe_directory_path;
    GgObject *artifacts_directory_path;
    GgObject *root_component_versions_to_add;
    GgObject *root_component_versions_to_remove;
    GgObject *cloud_components;
    GgObject *deployment_id;
    GgObject *configuration_arn_obj;
    GgObject *group_name;
    GgObject *component_to_configuration;

    GgError ret = gg_map_validate(
        args,
        GG_MAP_SCHEMA(
            { GG_STR("recipe_directory_path"),
              GG_OPTIONAL,
              GG_TYPE_BUF,
              &recipe_directory_path },
            { GG_STR("artifacts_directory_path"),
              GG_OPTIONAL,
              GG_TYPE_BUF,
              &artifacts_directory_path },
            { GG_STR("root_component_versions_to_add"),
              GG_OPTIONAL,
              GG_TYPE_MAP,
              &root_component_versions_to_add },
            { GG_STR("root_component_versions_to_remove"),
              GG_OPTIONAL,
              GG_TYPE_LIST,
              &root_component_versions_to_remove },
            { GG_STR("components"),
              GG_OPTIONAL,
              GG_TYPE_MAP,
              &cloud_components },
            { GG_STR("deploymentId"),
              GG_OPTIONAL,
              GG_TYPE_BUF,
              &deployment_id },
            { GG_STR("configurationArn"),
              GG_OPTIONAL,
              GG_TYPE_BUF,
              &configuration_arn_obj },
            { GG_STR("group_name"), GG_OPTIONAL, GG_TYPE_BUF, &group_name },
            { GG_STR("component_to_configuration"),
              GG_OPTIONAL,
              GG_TYPE_MAP,
              &component_to_configuration },
        )
    );
    if (ret != GG_ERR_OK) {
        GG_LOGE("Received invalid argument.");
        return GG_ERR_INVALID;
    }

    if (root_component_versions_to_add != NULL) {
        GG_MAP_FOREACH (
            pair, gg_obj_into_map(*root_component_versions_to_add)
        ) {
            ret = ggl_validate_component_name(gg_kv_key(*pair));
            if (ret != GG_ERR_OK) {
                return ret;
            }
        }
    }
    if (root_component_versions_to_remove != NULL) {
        GgList component_list
            = gg_obj_into_list(*root_component_versions_to_remove);
        for (size_t i = 0; i < component_list.len; i++) {
            if (gg_obj_type(component_list.items[i]) != GG_TYPE_BUF) {
                GG_LOGE("Component removal list entry is not a string.");
                return GG_ERR_INVALID;
            }
            ret = ggl_validate_component_name(
                gg_obj_into_buf(component_list.items[i])
            );
            if (ret != GG_ERR_OK) {
                return ret;
            }
        }
    }
    if (cloud_components != NULL) {
        GG_MAP_FOREACH (pair, gg_obj_into_map(*cloud_components)) {
            ret = ggl_validate_component_name(gg_kv_key(*pair));
            if (ret != GG_ERR_OK) {
                return ret;
            }
        }
    }
    if (component_to_configuration != NULL) {
        GG_MAP_FOREACH (pair, gg_obj_into_map(*component_to_configuration)) {
            ret = ggl_validate_component_name(gg_kv_key(*pair));
            if (ret != GG_ERR_OK) {
                return ret;
            }
        }
    }

    if (recipe_directory_path != NULL) {
        doc->recipe_directory_path = gg_obj_into_buf(*recipe_directory_path);
    }

    if (artifacts_directory_path != NULL) {
        doc->artifacts_directory_path
            = gg_obj_into_buf(*artifacts_directory_path);
    }

    if (deployment_id != NULL) {
        doc->deployment_id = gg_obj_into_buf(*deployment_id);
    } else {
        static uint8_t uuid_mem[37];
        uuid_t binuuid;
        uuid_generate_random(binuuid);
        uuid_unparse(binuuid, (char *) uuid_mem);
        doc->deployment_id = (GgBuffer) { .data = uuid_mem, .len = 36 };
    }

    if (type == THING_GROUP_DEPLOYMENT) {
        if (cloud_components != NULL) {
            doc->components = gg_obj_into_map(*cloud_components);
        } else {
            GG_LOGW(
                "Deployment is of type thing group deployment but does not have component information."
            );
        }

        if (configuration_arn_obj != NULL) {
            // Assume that the arn has a version at the end, we want to discard
            // the version for the arn.
            GgBuffer configuration_arn
                = gg_obj_into_buf(*configuration_arn_obj);
            size_t last_colon_index = 0;
            size_t slash_index = 0;
            ret = get_slash_and_colon_locations_from_arn(
                configuration_arn, &slash_index, &last_colon_index
            );
            if (ret != GG_ERR_OK) {
                GG_LOGE("Received an invalid configuration ARN.");
                return ret;
            }
            doc->configuration_arn = configuration_arn;
            doc->thing_group = gg_buffer_substr(
                configuration_arn, slash_index + 1, last_colon_index
            );
        }
    }

    if (type == LOCAL_DEPLOYMENT) {
        if (group_name != NULL && gg_obj_into_buf(*group_name).len > 0) {
            doc->thing_group = gg_obj_into_buf(*group_name);
        } else {
            doc->thing_group = GG_STR("LOCAL_DEPLOYMENTS");
        }
        doc->configuration_arn = doc->deployment_id;

        if (component_to_configuration != NULL) {
            doc->component_to_configuration
                = gg_obj_into_map(*component_to_configuration);
            // Validate canonical form: each per-component value must be a
            // map (with optional "merge" and "reset" keys).
            GG_MAP_FOREACH (comp_entry, doc->component_to_configuration) {
                if (gg_obj_type(*gg_kv_val(comp_entry)) != GG_TYPE_MAP) {
                    GG_LOGE(
                        "component_to_configuration entry for %.*s is not a map.",
                        (int) gg_kv_key(*comp_entry).len,
                        gg_kv_key(*comp_entry).data
                    );
                    return GG_ERR_INVALID;
                }
                GgMap comp_map = gg_obj_into_map(*gg_kv_val(comp_entry));
                GgObject *merge_val = NULL;
                GgObject *reset_val = NULL;
                if (gg_map_get(comp_map, GG_STR("merge"), &merge_val)
                    && gg_obj_type(*merge_val) != GG_TYPE_MAP) {
                    GG_LOGE(
                        "componentToConfiguration[%.*s].merge must be a map, got type %d.",
                        (int) gg_kv_key(*comp_entry).len,
                        gg_kv_key(*comp_entry).data,
                        (int) gg_obj_type(*merge_val)
                    );
                    return GG_ERR_INVALID;
                }
                if (gg_map_get(comp_map, GG_STR("reset"), &reset_val)
                    && gg_obj_type(*reset_val) != GG_TYPE_LIST) {
                    GG_LOGE(
                        "componentToConfiguration[%.*s].reset must be a list, got type %d.",
                        (int) gg_kv_key(*comp_entry).len,
                        gg_kv_key(*comp_entry).data,
                        (int) gg_obj_type(*reset_val)
                    );
                    return GG_ERR_INVALID;
                }
            }
        }

        ret = parse_local_deployment_components(
            root_component_versions_to_add,
            root_component_versions_to_remove,
            doc,
            alloc,
            local_components_kv_vec
        );
        if (ret != GG_ERR_OK) {
            return ret;
        }
    }

    return GG_ERR_OK;
}

static GgError get_next_generation(size_t index, uint64_t *generation) {
    if (deployment_generations[index] == UINT64_MAX) {
        return GG_ERR_RANGE;
    }
    *generation = deployment_generations[index] + 1;
    return GG_ERR_OK;
}

static GglDeploymentQueueToken make_queue_token(
    size_t index, uint64_t generation, uint64_t serial
) {
    GglDeploymentQueueToken token = { 0 };
    uint64_t token_index = (uint64_t) index + 1;
    memcpy(&token._opaque[0], &token_index, sizeof(token_index));
    memcpy(
        &token._opaque[sizeof(token_index)], &generation, sizeof(generation)
    );
    memcpy(
        &token._opaque[sizeof(token_index) + sizeof(generation)],
        &serial,
        sizeof(serial)
    );
    return token;
}

GgError ggl_deployment_enqueue(
    GgMap deployment_doc,
    GgByteVec *id,
    GgBuffer iot_job_id,
    GglDeploymentType type
) {
    GG_MTX_SCOPE_GUARD(&queue_mtx);
    initialize_storage_mapping();

    // We are reading a map that may contain MAX_LOCAL_COMPONENTS names to
    // version mappings. This mem is limited to this function call but we deep
    // copy into static memory later in this function.
    uint8_t local_deployment_shortlived_balloc_buf
        [(1 + 2 * MAX_LOCAL_COMPONENTS) * sizeof(GgObject)];
    GgArena shortlived_alloc
        = gg_arena_init(GG_BUF(local_deployment_shortlived_balloc_buf));
    GglDeployment new = { 0 };
    GgKVVec local_components_kv_vec
        = GG_KV_VEC((GgKV[MAX_LOCAL_COMPONENTS]) { 0 });
    GgError ret = parse_deployment_obj(
        deployment_doc, &new, type, &shortlived_alloc, &local_components_kv_vec
    );
    if (ret != GG_ERR_OK) {
        return ret;
    }

    new.type = type;
    new.iot_job_id = iot_job_id;
    new.state = GGL_DEPLOYMENT_QUEUED;

    size_t index;
    bool exists = get_matching_deployment(new.deployment_id, &index);
    if (exists) {
        if (deployments[index].state != GGL_DEPLOYMENT_QUEUED) {
            GG_LOGI("Existing deployment is already in progress.");
            return GG_ERR_CONFLICT;
        }
        GG_LOGI("Replacing existing deployment in queue.");
    } else {
        if (queue_count >= DEPLOYMENT_QUEUE_SIZE) {
            return GG_ERR_BUSY;
        }

        GG_LOGD("Adding a new deployment to the queue.");
        index = (queue_index + queue_count) % DEPLOYMENT_QUEUE_SIZE;
    }

    uint64_t generation;
    ret = get_next_generation(index, &generation);
    if (ret != GG_ERR_OK) {
        return ret;
    }

    GgArena alloc = gg_arena_init((GgBuffer) { .data = scratch_storage,
                                               .len = DEPLOYMENT_MEM_SIZE });
    ret = deep_copy_deployment(&new, &alloc);
    if (ret != GG_ERR_OK) {
        return ret;
    }

    if (id != NULL) {
        ret = gg_byte_vec_append(id, new.deployment_id);
        if (ret != GG_ERR_OK) {
            GG_LOGE("insufficient id length");
            return ret;
        }
    }

    uint8_t *old_storage = deployment_storage[index];
    deployment_storage[index] = scratch_storage;
    scratch_storage = old_storage;
    deployments[index] = new;
    deployment_generations[index] = generation;
    deployment_token_serials[index] = 0;
    if (!exists) {
        queue_count += 1;
    }

    pthread_cond_signal(&notify_cond);

    return GG_ERR_OK;
}

GgError ggl_deployment_dequeue(
    GglDeployment **deployment, GglDeploymentQueueToken *token
) {
    if ((deployment == NULL) || (token == NULL)) {
        return GG_ERR_INVALID;
    }

    GG_MTX_SCOPE_GUARD(&queue_mtx);
    initialize_storage_mapping();

    while (queue_count == 0) {
        pthread_cond_wait(&notify_cond, &queue_mtx);
    }

    if (deployments[queue_index].state != GGL_DEPLOYMENT_QUEUED) {
        return GG_ERR_BUSY;
    }
    if ((deployment_generations[queue_index] == 0)
        || (next_token_serial == UINT64_MAX)) {
        return GG_ERR_FAILURE;
    }

    next_token_serial += 1;
    GglDeploymentQueueToken new_token = make_queue_token(
        queue_index, deployment_generations[queue_index], next_token_serial
    );

    deployments[queue_index].state = GGL_DEPLOYMENT_IN_PROGRESS;
    deployment_token_serials[queue_index] = next_token_serial;
    *deployment = &deployments[queue_index];
    *token = new_token;

    GG_LOGD("Set a deployment to in progress.");

    return GG_ERR_OK;
}

static GgError validate_queue_token(
    const GglDeploymentQueueToken *token, size_t *index
) {
    if (token == NULL) {
        return GG_ERR_INVALID;
    }

    uint64_t token_index;
    uint64_t token_generation;
    uint64_t token_serial;
    memcpy(&token_index, &token->_opaque[0], sizeof(token_index));
    memcpy(
        &token_generation,
        &token->_opaque[sizeof(token_index)],
        sizeof(token_generation)
    );
    memcpy(
        &token_serial,
        &token->_opaque[sizeof(token_index) + sizeof(token_generation)],
        sizeof(token_serial)
    );
    if ((token_index == 0) || (token_index > DEPLOYMENT_QUEUE_SIZE)
        || (token_generation == 0) || (token_serial == 0)) {
        return GG_ERR_INVALID;
    }

    size_t decoded_index = (size_t) (token_index - 1);
    if (queue_count == 0) {
        return GG_ERR_NOENTRY;
    }
    if (decoded_index != queue_index) {
        return GG_ERR_BUSY;
    }
    if (deployments[decoded_index].state != GGL_DEPLOYMENT_IN_PROGRESS) {
        return GG_ERR_NOENTRY;
    }
    if ((token_generation != deployment_generations[decoded_index])
        || (token_serial != deployment_token_serials[decoded_index])) {
        return GG_ERR_NOENTRY;
    }

    *index = decoded_index;
    return GG_ERR_OK;
}

GgError ggl_deployment_release(GglDeploymentQueueToken *token) {
    GG_MTX_SCOPE_GUARD(&queue_mtx);
    initialize_storage_mapping();

    size_t index;
    GgError ret = validate_queue_token(token, &index);
    if (ret != GG_ERR_OK) {
        return ret;
    }

    GG_LOGD("Removing deployment from queue.");

    deployments[index] = (GglDeployment) { 0 };
    deployment_token_serials[index] = 0;
    queue_count -= 1;
    queue_index = (queue_index + 1) % DEPLOYMENT_QUEUE_SIZE;
    *token = (GglDeploymentQueueToken) { 0 };
    return GG_ERR_OK;
}

#ifdef GG_SDK_TESTING

#include <gg/test.h>
#include <unity.h>

void ggl_deployment_queue_reset_for_test(void) {
    GG_MTX_SCOPE_GUARD(&queue_mtx);

    memset(deployments, 0, sizeof(deployments));
    memset(deployment_mem, 0, sizeof(deployment_mem));
    memset(deployment_generations, 0, sizeof(deployment_generations));
    memset(deployment_token_serials, 0, sizeof(deployment_token_serials));
    memset(deployment_storage, 0, sizeof(deployment_storage));
    scratch_storage = NULL;
    next_token_serial = 0;
    queue_index = 0;
    queue_count = 0;
    storage_initialized = false;
    initialize_storage_mapping();
}

size_t ggl_deployment_queue_count_for_test(void) {
    GG_MTX_SCOPE_GUARD(&queue_mtx);
    return queue_count;
}

size_t ggl_deployment_queue_index_for_test(void) {
    GG_MTX_SCOPE_GUARD(&queue_mtx);
    return queue_index;
}

const GglDeployment *ggl_deployment_queue_slot_for_test(size_t slot) {
    GG_MTX_SCOPE_GUARD(&queue_mtx);
    if (slot >= DEPLOYMENT_QUEUE_SIZE) {
        return NULL;
    }
    return &deployments[slot];
}

const uint8_t *ggl_deployment_queue_storage_for_test(size_t slot) {
    GG_MTX_SCOPE_GUARD(&queue_mtx);
    initialize_storage_mapping();
    if (slot >= DEPLOYMENT_QUEUE_SIZE) {
        return NULL;
    }
    return deployment_storage[slot];
}

const uint8_t *ggl_deployment_queue_scratch_for_test(void) {
    GG_MTX_SCOPE_GUARD(&queue_mtx);
    initialize_storage_mapping();
    return scratch_storage;
}

uint64_t ggl_deployment_queue_generation_for_test(size_t slot) {
    GG_MTX_SCOPE_GUARD(&queue_mtx);
    if (slot >= DEPLOYMENT_QUEUE_SIZE) {
        return 0;
    }
    return deployment_generations[slot];
}

uint64_t ggl_deployment_queue_serial_for_test(size_t slot) {
    GG_MTX_SCOPE_GUARD(&queue_mtx);
    if (slot >= DEPLOYMENT_QUEUE_SIZE) {
        return 0;
    }
    return deployment_token_serials[slot];
}

GglDeploymentQueueToken ggl_deployment_queue_token_for_test(
    size_t slot, uint64_t generation, uint64_t serial
) {
    return make_queue_token(slot, generation, serial);
}

typedef struct {
    GgKV component_pairs[1];
    GgKV document_pairs[4];
} QueueTestDeploymentDoc;

static GgMap queue_test_deployment_doc(
    QueueTestDeploymentDoc *storage,
    GgBuffer deployment_id,
    GgBuffer configuration_arn,
    GgBuffer recipe_path,
    GgBuffer component_name,
    GgBuffer component_value
) {
    storage->component_pairs[0]
        = gg_kv(component_name, gg_obj_buf(component_value));
    storage->document_pairs[0]
        = gg_kv(GG_STR("deploymentId"), gg_obj_buf(deployment_id));
    storage->document_pairs[1]
        = gg_kv(GG_STR("configurationArn"), gg_obj_buf(configuration_arn));
    storage->document_pairs[2]
        = gg_kv(GG_STR("recipe_directory_path"), gg_obj_buf(recipe_path));
    storage->document_pairs[3] = gg_kv(
        GG_STR("components"),
        gg_obj_map((GgMap) { .pairs = storage->component_pairs, .len = 1 })
    );
    return (GgMap) { .pairs = storage->document_pairs, .len = 4 };
}

static GgError queue_test_enqueue(
    QueueTestDeploymentDoc *storage,
    GgBuffer deployment_id,
    GgBuffer recipe_path,
    GgBuffer component_value
) {
    return ggl_deployment_enqueue(
        queue_test_deployment_doc(
            storage,
            deployment_id,
            GG_STR("arn:aws:greengrass:test:1:configuration:thinggroup/group:1"
            ),
            recipe_path,
            GG_STR("Test.Component"),
            component_value
        ),
        NULL,
        GG_STR("job"),
        THING_GROUP_DEPLOYMENT
    );
}

static bool queue_test_token_is_zero(GglDeploymentQueueToken token) {
    const GglDeploymentQueueToken zero = { 0 };
    return memcmp(&token, &zero, sizeof(token)) == 0;
}

static void queue_test_reset(void) {
    ggl_deployment_queue_reset_for_test();
    ggl_deployment_config_set_reader_for_test(NULL);
}

static void queue_test_assert_deployment(
    const GglDeployment *deployment,
    GgBuffer deployment_id,
    GgBuffer recipe_path,
    GgBuffer component_value
) {
    TEST_ASSERT_TRUE(gg_buffer_eq(deployment->deployment_id, deployment_id));
    TEST_ASSERT_TRUE(
        gg_buffer_eq(deployment->recipe_directory_path, recipe_path)
    );
    GgObject *value = NULL;
    TEST_ASSERT_TRUE(
        gg_map_get(deployment->components, GG_STR("Test.Component"), &value)
    );
    TEST_ASSERT_EQUAL_INT(GG_TYPE_BUF, gg_obj_type(*value));
    TEST_ASSERT_TRUE(gg_buffer_eq(gg_obj_into_buf(*value), component_value));
}

static void queue_test_dequeue_and_release(GgBuffer expected_id) {
    GglDeployment *deployment = NULL;
    GglDeploymentQueueToken token = { 0 };
    GG_TEST_ASSERT_OK(ggl_deployment_dequeue(&deployment, &token));
    TEST_ASSERT_TRUE(gg_buffer_eq(deployment->deployment_id, expected_id));
    GG_TEST_ASSERT_OK(ggl_deployment_release(&token));
    TEST_ASSERT_TRUE(queue_test_token_is_zero(token));
}

static void queue_test_assert_rejected_release(GglDeploymentQueueToken *token) {
    GglDeploymentQueueToken original_token = *token;
    size_t original_count = queue_count;
    size_t original_index = queue_index;
    GglDeployment original_deployments[DEPLOYMENT_QUEUE_SIZE];
    memcpy(original_deployments, deployments, sizeof(deployments));
    uint8_t *original_storage[DEPLOYMENT_QUEUE_SIZE];
    memcpy(original_storage, deployment_storage, sizeof(deployment_storage));
    uint8_t *original_scratch = scratch_storage;
    uint64_t original_generations[DEPLOYMENT_QUEUE_SIZE];
    memcpy(
        original_generations,
        deployment_generations,
        sizeof(deployment_generations)
    );
    uint64_t original_serials[DEPLOYMENT_QUEUE_SIZE];
    memcpy(
        original_serials,
        deployment_token_serials,
        sizeof(deployment_token_serials)
    );

    TEST_ASSERT_NOT_EQUAL(GG_ERR_OK, ggl_deployment_release(token));
    TEST_ASSERT_EQUAL_MEMORY(&original_token, token, sizeof(*token));
    TEST_ASSERT_EQUAL_size_t(original_count, queue_count);
    TEST_ASSERT_EQUAL_size_t(original_index, queue_index);
    TEST_ASSERT_EQUAL_MEMORY(
        original_deployments, deployments, sizeof(deployments)
    );
    TEST_ASSERT_EQUAL_MEMORY(
        original_storage, deployment_storage, sizeof(deployment_storage)
    );
    TEST_ASSERT_EQUAL_PTR(original_scratch, scratch_storage);
    TEST_ASSERT_EQUAL_MEMORY(
        original_generations,
        deployment_generations,
        sizeof(deployment_generations)
    );
    TEST_ASSERT_EQUAL_MEMORY(
        original_serials,
        deployment_token_serials,
        sizeof(deployment_token_serials)
    );
}

static GgError queue_config_read_error;
static GgObject queue_config_read_value;

static GgError queue_fake_config_reader(
    GgBufList key_path, GgArena *alloc, GgObject *result
) {
    (void) key_path;
    (void) alloc;
    if (queue_config_read_error != GG_ERR_OK) {
        return queue_config_read_error;
    }
    *result = queue_config_read_value;
    return GG_ERR_OK;
}

GG_TEST_DEFINE(local_components_noentry_defaults_to_empty) {
    queue_test_reset();
    queue_config_read_error = GG_ERR_NOENTRY;
    ggl_deployment_config_set_reader_for_test(queue_fake_config_reader);
    GglDeployment deployment = { .thing_group = GG_STR("LOCAL_DEPLOYMENTS") };
    GgArena alloc = gg_arena_init(GG_BUF((uint8_t[256]) { 0 }));
    GgKVVec components = GG_KV_VEC((GgKV[4]) { 0 });

    GG_TEST_ASSERT_OK(parse_local_deployment_components(
        NULL, NULL, &deployment, &alloc, &components
    ));
    TEST_ASSERT_EQUAL_size_t(0, deployment.components.len);
}

GG_TEST_DEFINE(local_components_propagates_config_read_failures) {
    queue_test_reset();
    ggl_deployment_config_set_reader_for_test(queue_fake_config_reader);
    GgMap original_components
        = GG_MAP(gg_kv(GG_STR("existing"), gg_obj_map(GG_MAP())));
    GglDeployment deployment = {
        .components = original_components,
        .thing_group = GG_STR("LOCAL_DEPLOYMENTS"),
    };
    GgArena alloc = gg_arena_init(GG_BUF((uint8_t[256]) { 0 }));
    GgKVVec components = GG_KV_VEC((GgKV[4]) { 0 });

    queue_config_read_error = GG_ERR_NOMEM;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOMEM,
        parse_local_deployment_components(
            NULL, NULL, &deployment, &alloc, &components
        )
    );
    TEST_ASSERT_EQUAL_size_t(1, deployment.components.len);

    queue_config_read_error = GG_ERR_NOCONN;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOCONN,
        parse_local_deployment_components(
            NULL, NULL, &deployment, &alloc, &components
        )
    );
    TEST_ASSERT_EQUAL_size_t(1, deployment.components.len);
}

GG_TEST_DEFINE(local_component_wrong_type_propagates_config_error) {
    queue_test_reset();
    ggl_deployment_config_set_reader_for_test(queue_fake_config_reader);
    queue_config_read_error = GG_ERR_OK;
    queue_config_read_value = gg_obj_i64(1);
    GgMap original_components
        = GG_MAP(gg_kv(GG_STR("existing"), gg_obj_map(GG_MAP())));
    GglDeployment deployment = {
        .components = original_components,
        .thing_group = GG_STR("LOCAL_DEPLOYMENTS"),
    };
    GgArena alloc = gg_arena_init(GG_BUF((uint8_t[256]) { 0 }));
    GgKVVec components = GG_KV_VEC((GgKV[4]) { 0 });

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_CONFIG,
        parse_local_deployment_components(
            NULL, NULL, &deployment, &alloc, &components
        )
    );
    TEST_ASSERT_EQUAL_size_t(1, deployment.components.len);

    size_t original_queue_count = queue_count;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_CONFIG,
        ggl_deployment_enqueue(
            (GgMap) { 0 }, NULL, (GgBuffer) { 0 }, LOCAL_DEPLOYMENT
        )
    );
    TEST_ASSERT_EQUAL_size_t(original_queue_count, queue_count);
}

GG_TEST_DEFINE(local_component_read_failure_prevents_enqueue) {
    queue_test_reset();
    ggl_deployment_config_set_reader_for_test(queue_fake_config_reader);
    size_t original_queue_count = queue_count;

    queue_config_read_error = GG_ERR_NOMEM;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOMEM,
        ggl_deployment_enqueue(
            (GgMap) { 0 }, NULL, (GgBuffer) { 0 }, LOCAL_DEPLOYMENT
        )
    );
    TEST_ASSERT_EQUAL_size_t(original_queue_count, queue_count);
}

GG_TEST_DEFINE(queue_oversized_new_enqueue_is_transactional) {
    queue_test_reset();
    uint8_t oversized[DEPLOYMENT_MEM_SIZE];
    memset(oversized, 'x', sizeof(oversized));
    uint8_t id_mem[] = { 'k', 'e', 'e', 'p' };
    GgByteVec id = { .buf = GG_BUF(id_mem), .capacity = sizeof(id_mem) };
    uint8_t *original_storage[DEPLOYMENT_QUEUE_SIZE];
    uint64_t original_generations[DEPLOYMENT_QUEUE_SIZE];
    for (size_t i = 0; i < DEPLOYMENT_QUEUE_SIZE; i++) {
        original_storage[i] = deployment_storage[i];
        original_generations[i] = deployment_generations[i];
    }
    uint8_t *original_scratch = scratch_storage;
    QueueTestDeploymentDoc doc;

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOMEM,
        ggl_deployment_enqueue(
            queue_test_deployment_doc(
                &doc,
                GG_STR("oversized"),
                GG_STR(
                    "arn:aws:greengrass:test:1:configuration:thinggroup/group:1"
                ),
                GG_STR("recipe"),
                GG_STR("Test.Component"),
                GG_BUF(oversized)
            ),
            &id,
            GG_STR("job"),
            THING_GROUP_DEPLOYMENT
        )
    );
    TEST_ASSERT_EQUAL_size_t(sizeof(id_mem), id.buf.len);
    TEST_ASSERT_EQUAL_MEMORY("keep", id_mem, sizeof(id_mem));
    TEST_ASSERT_EQUAL_size_t(0, queue_count);
    TEST_ASSERT_EQUAL_size_t(0, queue_index);
    TEST_ASSERT_EQUAL_PTR(original_scratch, scratch_storage);
    for (size_t i = 0; i < DEPLOYMENT_QUEUE_SIZE; i++) {
        TEST_ASSERT_EQUAL_PTR(original_storage[i], deployment_storage[i]);
        TEST_ASSERT_EQUAL_UINT64(
            original_generations[i], deployment_generations[i]
        );
    }

    GG_TEST_ASSERT_OK(queue_test_enqueue(
        &doc, GG_STR("ok"), GG_STR("recipe"), GG_STR("1.0.0")
    ));
    queue_test_dequeue_and_release(GG_STR("ok"));
}

GG_TEST_DEFINE(queue_oversized_replacement_preserves_original) {
    queue_test_reset();
    QueueTestDeploymentDoc doc;
    GG_TEST_ASSERT_OK(queue_test_enqueue(
        &doc, GG_STR("same"), GG_STR("old-recipe"), GG_STR("old-value")
    ));
    GglDeployment original = deployments[0];
    uint8_t original_bytes[DEPLOYMENT_MEM_SIZE];
    memcpy(original_bytes, deployment_storage[0], sizeof(original_bytes));
    uint8_t *original_storage = deployment_storage[0];
    uint8_t *original_scratch = scratch_storage;
    uint64_t original_generation = deployment_generations[0];
    uint8_t oversized[DEPLOYMENT_MEM_SIZE];
    memset(oversized, 'x', sizeof(oversized));

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOMEM,
        queue_test_enqueue(
            &doc, GG_STR("same"), GG_STR("new-recipe"), GG_BUF(oversized)
        )
    );
    TEST_ASSERT_EQUAL_size_t(1, queue_count);
    TEST_ASSERT_EQUAL_size_t(0, queue_index);
    TEST_ASSERT_EQUAL_UINT64(original_generation, deployment_generations[0]);
    TEST_ASSERT_EQUAL_PTR(original_storage, deployment_storage[0]);
    TEST_ASSERT_EQUAL_PTR(original_scratch, scratch_storage);
    TEST_ASSERT_EQUAL_PTR(
        original.deployment_id.data, deployments[0].deployment_id.data
    );
    TEST_ASSERT_EQUAL_PTR(
        original.recipe_directory_path.data,
        deployments[0].recipe_directory_path.data
    );
    TEST_ASSERT_EQUAL_PTR(
        original.components.pairs, deployments[0].components.pairs
    );
    TEST_ASSERT_EQUAL_MEMORY(
        original_bytes, deployment_storage[0], sizeof(original_bytes)
    );

    GglDeployment *deployment = NULL;
    GglDeploymentQueueToken token = { 0 };
    GG_TEST_ASSERT_OK(ggl_deployment_dequeue(&deployment, &token));
    queue_test_assert_deployment(
        deployment, GG_STR("same"), GG_STR("old-recipe"), GG_STR("old-value")
    );
    GG_TEST_ASSERT_OK(ggl_deployment_release(&token));
}

GG_TEST_DEFINE(queue_replacement_preserves_order_and_deep_copies) {
    queue_test_reset();
    QueueTestDeploymentDoc doc;
    uint8_t id[] = { 'f', 'i', 'r', 's', 't' };
    uint8_t recipe[] = { 'o', 'l', 'd' };
    uint8_t value[] = { '1', '.', '0' };
    GG_TEST_ASSERT_OK(queue_test_enqueue(
        &doc,
        (GgBuffer) { .data = id, .len = sizeof(id) },
        (GgBuffer) { .data = recipe, .len = sizeof(recipe) },
        (GgBuffer) { .data = value, .len = sizeof(value) }
    ));
    memset(id, 'x', sizeof(id));
    memset(recipe, 'x', sizeof(recipe));
    memset(value, 'x', sizeof(value));
    GG_TEST_ASSERT_OK(queue_test_enqueue(
        &doc, GG_STR("second"), GG_STR("second-recipe"), GG_STR("2.0")
    ));
    uint64_t original_generation = deployment_generations[0];
    uint8_t replacement_value[] = { '3', '.', '0' };
    GG_TEST_ASSERT_OK(queue_test_enqueue(
        &doc,
        GG_STR("first"),
        GG_STR("new-recipe"),
        (GgBuffer) { .data = replacement_value,
                     .len = sizeof(replacement_value) }
    ));
    memset(replacement_value, 'x', sizeof(replacement_value));

    TEST_ASSERT_EQUAL_size_t(2, queue_count);
    TEST_ASSERT_EQUAL_size_t(0, queue_index);
    TEST_ASSERT_GREATER_THAN_UINT64(
        original_generation, deployment_generations[0]
    );
    GglDeployment *deployment = NULL;
    GglDeploymentQueueToken token = { 0 };
    GG_TEST_ASSERT_OK(ggl_deployment_dequeue(&deployment, &token));
    queue_test_assert_deployment(
        deployment, GG_STR("first"), GG_STR("new-recipe"), GG_STR("3.0")
    );
    GG_TEST_ASSERT_OK(ggl_deployment_release(&token));
    GG_TEST_ASSERT_OK(ggl_deployment_dequeue(&deployment, &token));
    queue_test_assert_deployment(
        deployment, GG_STR("second"), GG_STR("second-recipe"), GG_STR("2.0")
    );
    GG_TEST_ASSERT_OK(ggl_deployment_release(&token));
}

GG_TEST_DEFINE(queue_single_valid_release_returns_to_zero) {
    queue_test_reset();
    QueueTestDeploymentDoc doc;
    GG_TEST_ASSERT_OK(
        queue_test_enqueue(&doc, GG_STR("only"), GG_STR("recipe"), GG_STR("1"))
    );
    GglDeployment *deployment = NULL;
    GglDeploymentQueueToken token = { 0 };
    GG_TEST_ASSERT_OK(ggl_deployment_dequeue(&deployment, &token));
    TEST_ASSERT_EQUAL_size_t(1, queue_count);
    TEST_ASSERT_EQUAL_size_t(0, queue_index);
    GG_TEST_ASSERT_OK(ggl_deployment_release(&token));
    TEST_ASSERT_TRUE(queue_test_token_is_zero(token));
    TEST_ASSERT_EQUAL_size_t(0, queue_count);
    TEST_ASSERT_EQUAL_size_t(1, queue_index);
    queue_test_assert_rejected_release(&token);
}

GG_TEST_DEFINE(queue_two_entry_fifo_uses_distinct_tokens) {
    queue_test_reset();
    QueueTestDeploymentDoc doc;
    GG_TEST_ASSERT_OK(
        queue_test_enqueue(&doc, GG_STR("first"), GG_STR("recipe"), GG_STR("1"))
    );
    GG_TEST_ASSERT_OK(queue_test_enqueue(
        &doc, GG_STR("second"), GG_STR("recipe"), GG_STR("2")
    ));
    GglDeployment *deployment = NULL;
    GglDeploymentQueueToken first_token = { 0 };
    GG_TEST_ASSERT_OK(ggl_deployment_dequeue(&deployment, &first_token));
    TEST_ASSERT_TRUE(gg_buffer_eq(deployment->deployment_id, GG_STR("first")));
    GglDeploymentQueueToken published_first_token = first_token;
    GG_TEST_ASSERT_OK(ggl_deployment_release(&first_token));
    TEST_ASSERT_EQUAL_size_t(1, queue_count);

    GglDeploymentQueueToken second_token = { 0 };
    GG_TEST_ASSERT_OK(ggl_deployment_dequeue(&deployment, &second_token));
    TEST_ASSERT_TRUE(gg_buffer_eq(deployment->deployment_id, GG_STR("second")));
    TEST_ASSERT_FALSE(queue_test_token_is_zero(second_token));
    TEST_ASSERT_NOT_EQUAL(
        0, memcmp(&published_first_token, &second_token, sizeof(second_token))
    );
    GG_TEST_ASSERT_OK(ggl_deployment_release(&second_token));
    TEST_ASSERT_EQUAL_size_t(0, queue_count);
}

GG_TEST_DEFINE(queue_rejects_invalid_tokens_without_mutation) {
    queue_test_reset();
    QueueTestDeploymentDoc doc;
    GG_TEST_ASSERT_OK(
        queue_test_enqueue(&doc, GG_STR("first"), GG_STR("recipe"), GG_STR("1"))
    );
    GG_TEST_ASSERT_OK(queue_test_enqueue(
        &doc, GG_STR("second"), GG_STR("recipe"), GG_STR("2")
    ));
    GglDeployment *deployment = NULL;
    GglDeploymentQueueToken valid = { 0 };
    GG_TEST_ASSERT_OK(ggl_deployment_dequeue(&deployment, &valid));

    GglDeploymentQueueToken zero = { 0 };
    queue_test_assert_rejected_release(&zero);
    GglDeploymentQueueToken out_of_range = make_queue_token(
        DEPLOYMENT_QUEUE_SIZE,
        deployment_generations[0],
        deployment_token_serials[0]
    );
    queue_test_assert_rejected_release(&out_of_range);
    GglDeploymentQueueToken fabricated = valid;
    fabricated._opaque[sizeof(fabricated._opaque) - 1] ^= 1;
    queue_test_assert_rejected_release(&fabricated);
    GglDeploymentQueueToken wrong_head = make_queue_token(
        1, deployment_generations[1], deployment_token_serials[0]
    );
    queue_test_assert_rejected_release(&wrong_head);
    GglDeploymentQueueToken wrong_generation = make_queue_token(
        0, deployment_generations[0] + 1, deployment_token_serials[0]
    );
    queue_test_assert_rejected_release(&wrong_generation);

    GglDeploymentQueueToken duplicate = valid;
    GG_TEST_ASSERT_OK(ggl_deployment_release(&valid));
    TEST_ASSERT_TRUE(queue_test_token_is_zero(valid));
    queue_test_assert_rejected_release(&duplicate);
    queue_test_dequeue_and_release(GG_STR("second"));
    GG_TEST_ASSERT_OK(
        queue_test_enqueue(&doc, GG_STR("later"), GG_STR("recipe"), GG_STR("3"))
    );
    queue_test_dequeue_and_release(GG_STR("later"));
}

GG_TEST_DEFINE(queue_full_then_reuses_released_slot_and_rejects_stale_token) {
    queue_test_reset();
    QueueTestDeploymentDoc doc;
    uint8_t ids[DEPLOYMENT_QUEUE_SIZE];
    for (size_t i = 0; i < DEPLOYMENT_QUEUE_SIZE; i++) {
        ids[i] = (uint8_t) ('a' + i);
        GG_TEST_ASSERT_OK(queue_test_enqueue(
            &doc,
            (GgBuffer) { .data = &ids[i], .len = 1 },
            GG_STR("recipe"),
            GG_STR("1")
        ));
    }
    TEST_ASSERT_EQUAL_size_t(DEPLOYMENT_QUEUE_SIZE, queue_count);
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_BUSY,
        queue_test_enqueue(
            &doc, GG_STR("overflow"), GG_STR("recipe"), GG_STR("1")
        )
    );

    GglDeployment *deployment = NULL;
    GglDeploymentQueueToken token = { 0 };
    GG_TEST_ASSERT_OK(ggl_deployment_dequeue(&deployment, &token));
    GglDeploymentQueueToken stale = token;
    GG_TEST_ASSERT_OK(ggl_deployment_release(&token));
    GG_TEST_ASSERT_OK(queue_test_enqueue(
        &doc, GG_STR("reused"), GG_STR("recipe"), GG_STR("2")
    ));
    for (size_t i = 1; i < DEPLOYMENT_QUEUE_SIZE; i++) {
        queue_test_dequeue_and_release((GgBuffer) { .data = &ids[i], .len = 1 }
        );
    }
    TEST_ASSERT_EQUAL_size_t(1, queue_count);
    TEST_ASSERT_EQUAL_size_t(0, queue_index);
    queue_test_assert_rejected_release(&stale);
    queue_test_dequeue_and_release(GG_STR("reused"));
}

GG_TEST_DEFINE(queue_same_id_in_progress_is_conflict) {
    queue_test_reset();
    QueueTestDeploymentDoc doc;
    GG_TEST_ASSERT_OK(queue_test_enqueue(
        &doc, GG_STR("same"), GG_STR("old-recipe"), GG_STR("old")
    ));
    GglDeployment *deployment = NULL;
    GglDeploymentQueueToken token = { 0 };
    GG_TEST_ASSERT_OK(ggl_deployment_dequeue(&deployment, &token));
    uint8_t *original_storage = deployment_storage[0];
    uint64_t original_generation = deployment_generations[0];
    uint64_t original_serial = deployment_token_serials[0];

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_CONFLICT,
        queue_test_enqueue(
            &doc, GG_STR("same"), GG_STR("new-recipe"), GG_STR("new")
        )
    );
    TEST_ASSERT_EQUAL_size_t(1, queue_count);
    TEST_ASSERT_EQUAL_PTR(original_storage, deployment_storage[0]);
    TEST_ASSERT_EQUAL_UINT64(original_generation, deployment_generations[0]);
    TEST_ASSERT_EQUAL_UINT64(original_serial, deployment_token_serials[0]);
    queue_test_assert_deployment(
        deployment, GG_STR("same"), GG_STR("old-recipe"), GG_STR("old")
    );
    GG_TEST_ASSERT_OK(ggl_deployment_release(&token));
}

GG_TEST_DEFINE(queue_second_dequeue_preserves_outputs_and_state) {
    queue_test_reset();
    QueueTestDeploymentDoc doc;
    GG_TEST_ASSERT_OK(queue_test_enqueue(
        &doc, GG_STR("queued"), GG_STR("recipe"), GG_STR("1")
    ));
    GglDeployment *deployment = NULL;
    GglDeploymentQueueToken release_token = { 0 };
    GG_TEST_ASSERT_OK(ggl_deployment_dequeue(&deployment, &release_token));

    GglDeployment standalone = { .deployment_id = GG_STR("unchanged") };
    GglDeployment *output = &standalone;
    GglDeploymentQueueToken output_token = make_queue_token(0, 7, 9);
    GglDeploymentQueueToken original_output_token = output_token;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_BUSY, ggl_deployment_dequeue(&output, &output_token)
    );
    TEST_ASSERT_EQUAL_PTR(&standalone, output);
    TEST_ASSERT_EQUAL_MEMORY(
        &original_output_token, &output_token, sizeof(output_token)
    );
    TEST_ASSERT_EQUAL_size_t(1, queue_count);
    TEST_ASSERT_EQUAL_size_t(0, queue_index);
    TEST_ASSERT_EQUAL_INT(GGL_DEPLOYMENT_IN_PROGRESS, deployments[0].state);
    GG_TEST_ASSERT_OK(ggl_deployment_release(&release_token));
}

GG_TEST_DEFINE(queue_dequeue_invalid_arguments_preserve_outputs) {
    queue_test_reset();
    QueueTestDeploymentDoc doc;
    GG_TEST_ASSERT_OK(queue_test_enqueue(
        &doc, GG_STR("queued"), GG_STR("recipe"), GG_STR("1")
    ));
    GglDeployment standalone = { .deployment_id = GG_STR("standalone") };
    GglDeployment *output = &standalone;
    GglDeploymentQueueToken token = make_queue_token(0, 7, 9);
    GglDeploymentQueueToken original_token = token;

    TEST_ASSERT_EQUAL_INT(GG_ERR_INVALID, ggl_deployment_dequeue(NULL, &token));
    TEST_ASSERT_EQUAL_MEMORY(&original_token, &token, sizeof(token));
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID, ggl_deployment_dequeue(&output, NULL)
    );
    TEST_ASSERT_EQUAL_PTR(&standalone, output);
    TEST_ASSERT_EQUAL_size_t(1, queue_count);
    queue_test_dequeue_and_release(GG_STR("queued"));
}

GG_TEST_DEFINE(queue_standalone_deployment_cannot_release_queue) {
    queue_test_reset();
    GglDeploymentQueueToken zero = { 0 };
    queue_test_assert_rejected_release(&zero);
    GglDeploymentQueueToken fabricated = make_queue_token(0, 1, 1);
    queue_test_assert_rejected_release(&fabricated);
    TEST_ASSERT_EQUAL_size_t(0, queue_count);
    TEST_ASSERT_EQUAL_size_t(0, queue_index);

    QueueTestDeploymentDoc doc;
    GG_TEST_ASSERT_OK(queue_test_enqueue(
        &doc, GG_STR("normal"), GG_STR("recipe"), GG_STR("1")
    ));
    queue_test_dequeue_and_release(GG_STR("normal"));
}

GG_TEST_DEFINE(queue_caller_id_failure_has_no_side_effects) {
    queue_test_reset();
    QueueTestDeploymentDoc doc;
    uint8_t id_mem[] = { 'k', 'e', 'e', 'p' };
    GgByteVec id = { .buf = GG_BUF(id_mem), .capacity = sizeof(id_mem) };
    uint8_t *original_scratch = scratch_storage;

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOMEM,
        ggl_deployment_enqueue(
            queue_test_deployment_doc(
                &doc,
                GG_STR("too-long"),
                GG_STR(
                    "arn:aws:greengrass:test:1:configuration:thinggroup/group:1"
                ),
                GG_STR("recipe"),
                GG_STR("Test.Component"),
                GG_STR("1")
            ),
            &id,
            GG_STR("job"),
            THING_GROUP_DEPLOYMENT
        )
    );
    TEST_ASSERT_EQUAL_size_t(sizeof(id_mem), id.buf.len);
    TEST_ASSERT_EQUAL_MEMORY("keep", id_mem, sizeof(id_mem));
    TEST_ASSERT_EQUAL_size_t(0, queue_count);
    TEST_ASSERT_EQUAL_size_t(0, queue_index);
    TEST_ASSERT_EQUAL_UINT64(0, deployment_generations[0]);
    TEST_ASSERT_EQUAL_PTR(original_scratch, scratch_storage);
}

GG_TEST_DEFINE(queue_generation_and_serial_do_not_wrap_to_zero) {
    queue_test_reset();
    QueueTestDeploymentDoc doc;
    deployment_generations[0] = UINT64_MAX;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_RANGE,
        queue_test_enqueue(
            &doc, GG_STR("generation"), GG_STR("recipe"), GG_STR("1")
        )
    );
    TEST_ASSERT_EQUAL_size_t(0, queue_count);

    queue_test_reset();
    GG_TEST_ASSERT_OK(queue_test_enqueue(
        &doc, GG_STR("serial"), GG_STR("recipe"), GG_STR("1")
    ));
    next_token_serial = UINT64_MAX;
    GglDeployment standalone = { .deployment_id = GG_STR("unchanged") };
    GglDeployment *output = &standalone;
    GglDeploymentQueueToken token = make_queue_token(0, 7, 9);
    GglDeploymentQueueToken original_token = token;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_FAILURE, ggl_deployment_dequeue(&output, &token)
    );
    TEST_ASSERT_EQUAL_PTR(&standalone, output);
    TEST_ASSERT_EQUAL_MEMORY(&original_token, &token, sizeof(token));
    TEST_ASSERT_EQUAL_INT(GGL_DEPLOYMENT_QUEUED, deployments[0].state);
    TEST_ASSERT_EQUAL_UINT64(0, deployment_token_serials[0]);
}

GG_TEST_DEFINE(arn_delimiter_parser_extracts_group_and_rejects_malformed) {
    // Valid ARN: the group is the segment between the rightmost '/' and the
    // rightmost (version) ':'.
    GgBuffer valid
        = GG_STR("arn:aws:greengrass:test:1:configuration:thinggroup/group:1");
    size_t slash = 0;
    size_t colon = 0;
    GG_TEST_ASSERT_OK(
        get_slash_and_colon_locations_from_arn(valid, &slash, &colon)
    );
    TEST_ASSERT_TRUE(
        gg_buffer_eq(gg_buffer_substr(valid, slash + 1, colon), GG_STR("group"))
    );

    // Missing '/': no thing-group segment start.
    slash = 0;
    colon = 0;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID,
        get_slash_and_colon_locations_from_arn(
            GG_STR("arn:aws:greengrass:test:1:configuration:thinggroup-group:1"
            ),
            &slash,
            &colon
        )
    );

    // Missing trailing ':' after the group: no version delimiter.
    slash = 0;
    colon = 0;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID,
        get_slash_and_colon_locations_from_arn(
            GG_STR("arn:aws:greengrass:test:1:configuration:thinggroup/group"),
            &slash,
            &colon
        )
    );

    // Reversed delimiters: the last ':' falls before the '/'.
    slash = 0;
    colon = 0;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID,
        get_slash_and_colon_locations_from_arn(
            GG_STR("arn:aws:greengrass:group:configuration/thinggroup"),
            &slash,
            &colon
        )
    );

    // Empty thing-group segment: '/' immediately followed by ':'.
    slash = 0;
    colon = 0;
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID,
        get_slash_and_colon_locations_from_arn(
            GG_STR("arn:aws:greengrass:test:1:configuration:thinggroup/:1"),
            &slash,
            &colon
        )
    );
}

GG_TEST_DEFINE(thing_group_malformed_arn_does_not_enqueue) {
    queue_test_reset();
    QueueTestDeploymentDoc doc;

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID,
        ggl_deployment_enqueue(
            queue_test_deployment_doc(
                &doc,
                GG_STR("deployment"),
                // No '/' delimiter: malformed thing-group ARN.
                GG_STR("arn:aws:greengrass:test:1:configuration:group:1"),
                GG_STR("recipe"),
                GG_STR("Test.Component"),
                GG_STR("1")
            ),
            NULL,
            GG_STR("job"),
            THING_GROUP_DEPLOYMENT
        )
    );
    TEST_ASSERT_EQUAL_size_t(0, queue_count);
    TEST_ASSERT_EQUAL_size_t(0, queue_index);
}

GG_TEST_DEFINE(thing_group_valid_arn_extracts_group_and_enqueues) {
    queue_test_reset();
    QueueTestDeploymentDoc doc;

    GG_TEST_ASSERT_OK(ggl_deployment_enqueue(
        queue_test_deployment_doc(
            &doc,
            GG_STR("deployment"),
            GG_STR("arn:aws:greengrass:test:1:configuration:thinggroup/group:5"
            ),
            GG_STR("recipe"),
            GG_STR("Test.Component"),
            GG_STR("1")
        ),
        NULL,
        GG_STR("job"),
        THING_GROUP_DEPLOYMENT
    ));
    TEST_ASSERT_EQUAL_size_t(1, queue_count);
    const GglDeployment *slot = ggl_deployment_queue_slot_for_test(0);
    TEST_ASSERT_NOT_NULL(slot);
    TEST_ASSERT_TRUE(gg_buffer_eq(slot->thing_group, GG_STR("group")));
    TEST_ASSERT_TRUE(gg_buffer_eq(
        slot->configuration_arn,
        GG_STR("arn:aws:greengrass:test:1:configuration:thinggroup/group:5")
    ));
}

#endif
