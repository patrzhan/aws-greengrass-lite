// aws-greengrass-lite - AWS IoT Greengrass runtime for constrained devices
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#include "bootstrap_manager.h"
#include "config_access.h"
#include "deployment_model.h"
#include "deployment_queue.h"
#include "stale_component.h"
#include <fcntl.h>
#include <gg/arena.h>
#include <gg/buffer.h>
#include <gg/cleanup.h>
#include <gg/error.h>
#include <gg/file.h>
#include <gg/flags.h>
#include <gg/log.h>
#include <gg/map.h>
#include <gg/object.h>
#include <gg/vector.h>
#include <ggl/core_bus/gg_config.h>
#include <ggl/process.h>
#include <limits.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

// Test seams for process execution and config persistence. Production calls
// ggl_process_call()/ggl_gg_config_write() directly; tests replace these to
// force systemctl link/start outcomes and to observe argv boundaries and the
// order and count of persistence writes without touching systemd or config.
static GgError bootstrap_process_call_default(
    const char *const argv[], const GglProcessSpawnConfig *config
) {
    return ggl_process_call(argv, config);
}

#ifdef GG_SDK_TESTING

static GgError (*bootstrap_process_call)(
    const char *const argv[], const GglProcessSpawnConfig *config
) = bootstrap_process_call_default;
static GgError (*bootstrap_config_write)(
    GgBufList key_path, GgObject value, const int64_t *timestamp
) = ggl_gg_config_write;

void bootstrap_manager_reset_test_seams(void);
void bootstrap_manager_override_test_seam_for_reset_test(void);
bool bootstrap_manager_test_seams_are_reset(void);

#else

// NOLINTBEGIN(readability-identifier-naming)
#define bootstrap_process_call bootstrap_process_call_default
#define bootstrap_config_write ggl_gg_config_write
// NOLINTEND(readability-identifier-naming)

#endif

bool component_bootstrap_phase_completed(GgBuffer component_name) {
    // check config to see if component bootstrap steps have already been
    // completed
    uint8_t resp_mem[128] = { 0 };
    GgArena alloc = gg_arena_init(GG_BUF(resp_mem));
    GgBuffer resp;
    GgError ret = ggl_gg_config_read_str(
        GG_BUF_LIST(
            GG_STR("services"),
            GG_STR("DeploymentService"),
            GG_STR("deploymentState"),
            GG_STR("bootstrapComponents"),
            component_name
        ),
        &alloc,
        &resp
    );
    if (ret == GG_ERR_OK) {
        GG_LOGD(
            "Bootstrap steps have already been run for %.*s.",
            (int) component_name.len,
            component_name.data
        );
        return true;
    }
    return false;
}

GgError save_component_info(
    GgBuffer component_name, GgBuffer component_version, GgBuffer type
) {
    GG_LOGD(
        "Saving component name and version for %.*s as type %.*s to the config to track deployment state.",
        (int) component_name.len,
        component_name.data,
        (int) type.len,
        type.data
    );

    if (gg_buffer_eq(type, GG_STR("completed"))) {
        GgError ret = bootstrap_config_write(
            GG_BUF_LIST(
                GG_STR("services"),
                GG_STR("DeploymentService"),
                GG_STR("deploymentState"),
                GG_STR("components"),
                component_name
            ),
            gg_obj_buf(component_version),
            &(int64_t) { 3 }
        );
        if (ret != GG_ERR_OK) {
            GG_LOGE(
                "Failed to write component info for %.*s to config.",
                (int) component_name.len,
                component_name.data
            );
            return ret;
        }
    } else if (gg_buffer_eq(type, GG_STR("bootstrap"))) {
        GgError ret = bootstrap_config_write(
            GG_BUF_LIST(
                GG_STR("services"),
                GG_STR("DeploymentService"),
                GG_STR("deploymentState"),
                GG_STR("bootstrapComponents"),
                component_name
            ),
            gg_obj_buf(component_version),
            &(int64_t) { 3 }
        );
        if (ret != GG_ERR_OK) {
            GG_LOGE(
                "Failed to write component info for %.*s to config.",
                (int) component_name.len,
                component_name.data
            );
            return ret;
        }
    } else {
        GG_LOGE(
            "Invalid component type of %.*s received. Expected type 'bootstrap' or 'completed'.",
            (int) type.len,
            type.data
        );
        return GG_ERR_INVALID;
    }

    return GG_ERR_OK;
}

GgError save_iot_jobs_id(GgBuffer jobs_id) {
    GG_LOGD(
        "Saving IoT Jobs ID %.*s in case of bootstrap.",
        (int) jobs_id.len,
        jobs_id.data
    );

    GgError ret = bootstrap_config_write(
        GG_BUF_LIST(
            GG_STR("services"),
            GG_STR("DeploymentService"),
            GG_STR("deploymentState"),
            GG_STR("jobsID")
        ),
        gg_obj_buf(jobs_id),
        &(int64_t) { 3 }
    );
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to write IoT Jobs ID to config.");
        return ret;
    }
    return GG_ERR_OK;
}

GgError save_deployment_info(
    GglDeployment *deployment, GgBuffer source_iot_data_endpoint
) {
    GG_LOGD("Saving deployment state to config.");

    // Resolve and validate the deployment type before any persistence write so
    // an unrecognized type cannot leave a half-written deployment state (and no
    // longer serializes an all-zero type buffer).
    GgBuffer deployment_type;
    if (deployment->type == LOCAL_DEPLOYMENT) {
        deployment_type = GG_STR("LOCAL_DEPLOYMENT");
    } else if (deployment->type == THING_GROUP_DEPLOYMENT) {
        deployment_type = GG_STR("THING_GROUP_DEPLOYMENT");
    } else {
        GG_LOGE(
            "Refusing to save deployment with unrecognized type %d.",
            (int) deployment->type
        );
        return GG_ERR_INVALID;
    }

    GgObject deployment_doc = gg_obj_map(GG_MAP(
        gg_kv(GG_STR("deployment_id"), gg_obj_buf(deployment->deployment_id)),
        gg_kv(
            GG_STR("recipe_directory_path"),
            gg_obj_buf(deployment->recipe_directory_path)
        ),
        gg_kv(
            GG_STR("artifacts_directory_path"),
            gg_obj_buf(deployment->artifacts_directory_path)
        ),
        gg_kv(
            GG_STR("configuration_arn"),
            gg_obj_buf(deployment->configuration_arn)
        ),
        gg_kv(GG_STR("thing_group"), gg_obj_buf(deployment->thing_group)),
        gg_kv(GG_STR("components"), gg_obj_map(deployment->components))
    ));

    GgError ret = bootstrap_config_write(
        GG_BUF_LIST(
            GG_STR("services"),
            GG_STR("DeploymentService"),
            GG_STR("deploymentState"),
            GG_STR("deploymentDoc")
        ),
        deployment_doc,
        &(int64_t) { 3 }
    );

    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to write deployment document to config.");
        return ret;
    }

    ret = bootstrap_config_write(
        GG_BUF_LIST(
            GG_STR("services"),
            GG_STR("DeploymentService"),
            GG_STR("deploymentState"),
            GG_STR("deploymentType")
        ),
        gg_obj_buf(deployment_type),
        &(int64_t) { 3 }
    );

    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to write deployment type to config.");
        return ret;
    }

    if (source_iot_data_endpoint.len > 0) {
        ret = bootstrap_config_write(
            GG_BUF_LIST(
                GG_STR("services"),
                GG_STR("DeploymentService"),
                GG_STR("deploymentState"),
                GG_STR("sourceIotDataEndpoint")
            ),
            gg_obj_buf(source_iot_data_endpoint),
            &(int64_t) { 3 }
        );
        if (ret != GG_ERR_OK) {
            GG_LOGE("Failed to write source IoT data endpoint to config.");
            return ret;
        }
    }

    return GG_ERR_OK;
}

static GgError copy_jobs_id(GgMap deployment_config, GgBuffer *jobs_id) {
    GgObject *jobs_id_obj;
    GgError ret = gg_map_validate(
        deployment_config,
        GG_MAP_SCHEMA(
            { GG_STR("jobsID"), GG_REQUIRED, GG_TYPE_BUF, &jobs_id_obj }
        )
    );
    if (ret != GG_ERR_OK) {
        return ret;
    }

    return ggl_deployment_copy_buffer(gg_obj_into_buf(*jobs_id_obj), jobs_id);
}

typedef struct {
    uint8_t deployment_mem[5000];
    uint8_t source_endpoint_mem[128];
} RetrievedDeploymentStorage;

// Stage each retrieval in the inactive slot and publish it only on success so
// a failed retrieval cannot invalidate pointers returned by the prior success.
static RetrievedDeploymentStorage retrieved_deployment_storage[2];
static size_t active_retrieved_deployment_storage;

GgError retrieve_in_progress_deployment(
    GglDeployment *deployment, GgBuffer *jobs_id, DeploymentContext *ctx
) {
    GG_LOGD("Searching config for any in progress deployment.");

    if ((deployment == NULL) || (jobs_id == NULL) || (ctx == NULL)
        || ((jobs_id->len > 0) && (jobs_id->data == NULL))) {
        return GG_ERR_INVALID;
    }

    GgBuffer config_mem = GG_BUF((uint8_t[2500]) { 0 });
    GgArena alloc = gg_arena_init(config_mem);
    GgObject deployment_config;

    GgError ret = ggl_deployment_config_read_object(
        GG_BUF_LIST(
            GG_STR("services"),
            GG_STR("DeploymentService"),
            GG_STR("deploymentState")
        ),
        &alloc,
        GG_TYPE_MAP,
        &deployment_config
    );
    if (ret != GG_ERR_OK) {
        return ret;
    }

    GgMap deployment_config_map = gg_obj_into_map(deployment_config);
    uint8_t retrieved_jobs_id_mem[64];
    GgBuffer retrieved_jobs_id = GG_BUF(retrieved_jobs_id_mem);
    ret = copy_jobs_id(deployment_config_map, &retrieved_jobs_id);
    if (ret != GG_ERR_OK) {
        return ret;
    }
    if (retrieved_jobs_id.len > jobs_id->len) {
        return GG_ERR_RANGE;
    }

    GglDeployment retrieved_deployment = { 0 };
    DeploymentContext retrieved_ctx = {
        .is_bootstrap = true,
        .removed_names_storage = ctx->removed_names_storage,
        .removed_components = ctx->removed_components,
    };

    GgObject *deployment_type;
    ret = gg_map_validate(
        deployment_config_map,
        GG_MAP_SCHEMA({ GG_STR("deploymentType"),
                        GG_REQUIRED,
                        GG_TYPE_BUF,
                        &deployment_type })
    );
    if (ret != GG_ERR_OK) {
        return ret;
    }

    GgBuffer deployment_type_value = gg_obj_into_buf(*deployment_type);
    if (gg_buffer_eq(deployment_type_value, GG_STR("LOCAL_DEPLOYMENT"))) {
        retrieved_deployment.type = LOCAL_DEPLOYMENT;
    } else if (gg_buffer_eq(
                   deployment_type_value, GG_STR("THING_GROUP_DEPLOYMENT")
               )) {
        retrieved_deployment.type = THING_GROUP_DEPLOYMENT;
    } else {
        GG_LOGE(
            "Unrecognized persisted deployment type %.*s.",
            (int) deployment_type_value.len,
            deployment_type_value.data
        );
        return GG_ERR_CONFIG;
    }

    GgObject *deployment_doc;
    ret = gg_map_validate(
        deployment_config_map,
        GG_MAP_SCHEMA({ GG_STR("deploymentDoc"),
                        GG_REQUIRED,
                        GG_TYPE_MAP,
                        &deployment_doc })
    );
    if (ret != GG_ERR_OK) {
        return ret;
    }

    GgMap deployment_doc_map = gg_obj_into_map(*deployment_doc);
    GgObject *deployment_id;
    ret = gg_map_validate(
        deployment_doc_map,
        GG_MAP_SCHEMA({ GG_STR("deployment_id"),
                        GG_REQUIRED,
                        GG_TYPE_BUF,
                        &deployment_id })
    );
    if (ret != GG_ERR_OK) {
        return ret;
    }
    retrieved_deployment.deployment_id = gg_obj_into_buf(*deployment_id);

    GgObject *recipe_directory_path;
    ret = gg_map_validate(
        deployment_doc_map,
        GG_MAP_SCHEMA({ GG_STR("recipe_directory_path"),
                        GG_REQUIRED,
                        GG_TYPE_BUF,
                        &recipe_directory_path })
    );
    if (ret != GG_ERR_OK) {
        return ret;
    }
    retrieved_deployment.recipe_directory_path
        = gg_obj_into_buf(*recipe_directory_path);

    GgObject *artifacts_directory_path;
    ret = gg_map_validate(
        deployment_doc_map,
        GG_MAP_SCHEMA({ GG_STR("artifacts_directory_path"),
                        GG_REQUIRED,
                        GG_TYPE_BUF,
                        &artifacts_directory_path })
    );
    if (ret != GG_ERR_OK) {
        return ret;
    }
    retrieved_deployment.artifacts_directory_path
        = gg_obj_into_buf(*artifacts_directory_path);

    GgObject *configuration_arn;
    ret = gg_map_validate(
        deployment_doc_map,
        GG_MAP_SCHEMA({ GG_STR("configuration_arn"),
                        GG_REQUIRED,
                        GG_TYPE_BUF,
                        &configuration_arn })
    );
    if (ret != GG_ERR_OK) {
        return ret;
    }
    retrieved_deployment.configuration_arn
        = gg_obj_into_buf(*configuration_arn);

    GgObject *thing_group;
    ret = gg_map_validate(
        deployment_doc_map,
        GG_MAP_SCHEMA(
            { GG_STR("thing_group"), GG_REQUIRED, GG_TYPE_BUF, &thing_group }
        )
    );
    if (ret != GG_ERR_OK) {
        return ret;
    }
    retrieved_deployment.thing_group = gg_obj_into_buf(*thing_group);

    GgObject *components;
    ret = gg_map_validate(
        deployment_doc_map,
        GG_MAP_SCHEMA(
            { GG_STR("components"), GG_REQUIRED, GG_TYPE_MAP, &components }
        )
    );
    if (ret != GG_ERR_OK) {
        return ret;
    }
    retrieved_deployment.components = gg_obj_into_map(*components);

    GgObject *source_endpoint = NULL;
    ret = gg_map_validate(
        deployment_config_map,
        GG_MAP_SCHEMA({ GG_STR("sourceIotDataEndpoint"),
                        GG_OPTIONAL,
                        GG_TYPE_BUF,
                        &source_endpoint })
    );
    if (ret != GG_ERR_OK) {
        return ret;
    }

    GgBuffer source_endpoint_value = { 0 };
    if (source_endpoint != NULL) {
        source_endpoint_value = gg_obj_into_buf(*source_endpoint);
        if (source_endpoint_value.len
            > sizeof(retrieved_deployment_storage[0].source_endpoint_mem)) {
            return GG_ERR_RANGE;
        }
    }

    size_t staging_slot = active_retrieved_deployment_storage ^ 1U;
    RetrievedDeploymentStorage *staging
        = &retrieved_deployment_storage[staging_slot];
    GgArena deployment_alloc = gg_arena_init(GG_BUF(staging->deployment_mem));
    ret = deep_copy_deployment(&retrieved_deployment, &deployment_alloc);
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to deep copy deployment.");
        return ret;
    }

    if (source_endpoint_value.len > 0) {
        memcpy(
            staging->source_endpoint_mem,
            source_endpoint_value.data,
            source_endpoint_value.len
        );
        retrieved_ctx.source_iot_data_endpoint = (GgBuffer) {
            .data = staging->source_endpoint_mem,
            .len = source_endpoint_value.len,
        };
    }

    if (retrieved_jobs_id.len > 0) {
        memmove(jobs_id->data, retrieved_jobs_id.data, retrieved_jobs_id.len);
    }
    jobs_id->len = retrieved_jobs_id.len;
    *deployment = retrieved_deployment;
    *ctx = retrieved_ctx;
    active_retrieved_deployment_storage = staging_slot;
    return GG_ERR_OK;
}

GgError delete_saved_deployment_from_config(void) {
    GG_LOGD("Deleting previously saved deployment from config.");

    GgError ret = ggl_gg_config_delete(GG_BUF_LIST(
        GG_STR("services"),
        GG_STR("DeploymentService"),
        GG_STR("deploymentState")
    ));

    if (ret != GG_ERR_OK) {
        GG_LOGE(
            "Failed to delete previously saved deployment state from config."
        );
        return ret;
    }

    return GG_ERR_OK;
}

// Links and starts a component's bootstrap unit, then persists the component's
// bootstrap completion only after the unit has started. The service path and
// unit name remain single argv elements, so their bytes are never interpreted
// by a shell.
static GgError link_and_start_bootstrap_service(
    GgBuffer component_name,
    GgBuffer component_version,
    GgBuffer bootstrap_service_file_path
) {
    char service_path[PATH_MAX];
    GgByteVec service_path_vec = GG_BYTE_VEC(service_path);
    GgError ret
        = gg_byte_vec_append(&service_path_vec, bootstrap_service_file_path);
    gg_byte_vec_chain_push(&ret, &service_path_vec, '\0');
    if (ret != GG_ERR_OK) {
        GG_LOGE(
            "Failed to terminate bootstrap service path for %.*s.",
            (int) bootstrap_service_file_path.len,
            bootstrap_service_file_path.data
        );
        return ret;
    }

    const char *link_argv[] = { "systemctl", "link", service_path, NULL };
    ret = bootstrap_process_call(link_argv, NULL);
    if (ret != GG_ERR_OK) {
        GG_LOGE(
            "systemctl link failed for %.*s (error=%d).",
            (int) bootstrap_service_file_path.len,
            bootstrap_service_file_path.data,
            (int) ret
        );
        return ret;
    }
    GG_LOGI(
        "systemctl link succeeded for %.*s.",
        (int) bootstrap_service_file_path.len,
        bootstrap_service_file_path.data
    );

    char unit_name[NAME_MAX];
    GgByteVec unit_name_vec = GG_BYTE_VEC(unit_name);
    ret = gg_byte_vec_append(&unit_name_vec, GG_STR("ggl."));
    gg_byte_vec_chain_append(&ret, &unit_name_vec, component_name);
    gg_byte_vec_chain_append(
        &ret, &unit_name_vec, GG_STR(".bootstrap.service")
    );
    gg_byte_vec_chain_push(&ret, &unit_name_vec, '\0');
    if (ret != GG_ERR_OK) {
        GG_LOGE(
            "Failed to create bootstrap unit name for %.*s.",
            (int) component_name.len,
            component_name.data
        );
        return ret;
    }

    const char *start_argv[] = { "systemctl", "start", unit_name, NULL };
    ret = bootstrap_process_call(start_argv, NULL);
    if (ret != GG_ERR_OK) {
        GG_LOGE(
            "systemctl start failed for %.*s (error=%d).",
            (int) component_name.len,
            component_name.data,
            (int) ret
        );
        return ret;
    }
    GG_LOGI(
        "systemctl start succeeded for %.*s.",
        (int) component_name.len,
        component_name.data
    );

    ret = save_component_info(
        component_name, component_version, GG_STR("bootstrap")
    );
    if (ret != GG_ERR_OK) {
        GG_LOGE(
            "Failed to save component info to config after starting the bootstrap service."
        );
        return ret;
    }
    return GG_ERR_OK;
}

GgError process_bootstrap_phase(
    GgMap components,
    GgBuffer root_path,
    GgBufVec *bootstrap_comp_name_buf_vec,
    GglDeployment *deployment
) {
    int bootstrap_component_count = 0;
    GG_MAP_FOREACH (component, components) {
        GgBuffer component_name = gg_kv_key(*component);

        // check config to see if component bootstrap steps have already been
        // completed
        if (component_bootstrap_phase_completed(component_name)) {
            GG_LOGD("Bootstrap processed. Skipping component.");
            continue;
        }

        static uint8_t bootstrap_service_file_path_buf[PATH_MAX];
        GgByteVec bootstrap_service_file_path_vec
            = GG_BYTE_VEC(bootstrap_service_file_path_buf);
        GgError ret
            = gg_byte_vec_append(&bootstrap_service_file_path_vec, root_path);
        gg_byte_vec_chain_append(
            &ret, &bootstrap_service_file_path_vec, GG_STR("/")
        );
        gg_byte_vec_chain_append(
            &ret, &bootstrap_service_file_path_vec, GG_STR("ggl.")
        );
        gg_byte_vec_chain_append(
            &ret, &bootstrap_service_file_path_vec, component_name
        );
        gg_byte_vec_chain_append(
            &ret, &bootstrap_service_file_path_vec, GG_STR(".bootstrap.service")
        );
        if (ret == GG_ERR_OK) {
            // check if the current component name has relevant bootstrap
            // service file created
            int fd = -1;
            ret = gg_file_open(
                bootstrap_service_file_path_vec.buf, O_RDONLY, 0, &fd
            );
            if (ret != GG_ERR_OK) {
                GG_LOGD(
                    "Component %.*s does not have the relevant bootstrap service file",
                    (int) component_name.len,
                    component_name.data
                );
            } else { // relevant bootstrap service file exists
                GG_CLEANUP(cleanup_close, fd);
                ret = disable_and_unlink_service(&component_name, BOOTSTRAP);
                if (ret != GG_ERR_OK) {
                    return ret;
                }
                GG_LOGI(
                    "Found bootstrap service file for %.*s. Processing.",
                    (int) component_name.len,
                    component_name.data
                );

                // add relevant component name into the vector
                ret = gg_buf_vec_push(
                    bootstrap_comp_name_buf_vec, component_name
                );
                if (ret != GG_ERR_OK) {
                    GG_LOGE(
                        "Failed to add the bootstrap component name into vector"
                    );
                    return ret;
                }
                bootstrap_component_count++;

                // Link and start the bootstrap unit, persisting completion
                // only after the unit starts. A failed link or start is now a
                // real error (these previously returned GG_ERR_OK), and
                // completion is no longer persisted before the unit runs.
                ret = link_and_start_bootstrap_service(
                    component_name,
                    gg_obj_into_buf(*gg_kv_val(component)),
                    bootstrap_service_file_path_vec.buf
                );
                if (ret != GG_ERR_OK) {
                    return ret;
                }
            }
        }
    }

    if (bootstrap_component_count > 0) {
        // save deployment state and restart
        GgError ret = save_deployment_info(deployment, (GgBuffer) { 0 });
        if (ret != GG_ERR_OK) {
            GG_LOGE("Failed to save deployment state for bootstrap.");
            return ret;
        }

        GG_LOGI("Rebooting device for bootstrap.");
        // NOLINTNEXTLINE(concurrency-mt-unsafe)
        int system_ret = system("systemctl reboot");
        if (WIFEXITED(system_ret)) {
            if (WEXITSTATUS(system_ret) != 0) {
                GG_LOGE("systemctl reboot failed");
            }
            GG_LOGI(
                "systemctl reboot exited with child status %d\n",
                WEXITSTATUS(system_ret)
            );
        } else {
            GG_LOGE("systemctl reboot did not exit normally");
        }
    }

    return GG_ERR_OK;
}

#ifdef GG_SDK_TESTING

#include <gg/test.h>
#include <unity.h>

static GgObject bootstrap_config_read_value;

static GgError bootstrap_fake_config_reader(
    GgBufList key_path, GgArena *alloc, GgObject *result
) {
    (void) key_path;
    (void) alloc;
    *result = bootstrap_config_read_value;
    return GG_ERR_OK;
}

static GgError bootstrap_retrieve_test_state(
    GgBuffer stored_jobs_id,
    GgBuffer stored_deployment_id,
    GglDeployment *deployment,
    GgBuffer *jobs_id,
    DeploymentContext *ctx
) {
    GgMap deployment_doc = GG_MAP(
        gg_kv(GG_STR("deployment_id"), gg_obj_buf(stored_deployment_id)),
        gg_kv(GG_STR("recipe_directory_path"), gg_obj_buf(GG_STR("/recipes"))),
        gg_kv(
            GG_STR("artifacts_directory_path"), gg_obj_buf(GG_STR("/artifacts"))
        ),
        gg_kv(GG_STR("configuration_arn"), gg_obj_buf(GG_STR("config-arn"))),
        gg_kv(GG_STR("thing_group"), gg_obj_buf(GG_STR("thing-group"))),
        gg_kv(GG_STR("components"), gg_obj_map((GgMap) { 0 }))
    );
    GgMap deployment_state = GG_MAP(
        gg_kv(GG_STR("jobsID"), gg_obj_buf(stored_jobs_id)),
        gg_kv(GG_STR("deploymentType"), gg_obj_buf(GG_STR("LOCAL_DEPLOYMENT"))),
        gg_kv(GG_STR("deploymentDoc"), gg_obj_map(deployment_doc)),
        gg_kv(
            GG_STR("sourceIotDataEndpoint"),
            gg_obj_buf(GG_STR("source-endpoint"))
        )
    );
    bootstrap_config_read_value = gg_obj_map(deployment_state);
    ggl_deployment_config_set_reader_for_test(bootstrap_fake_config_reader);
    return retrieve_in_progress_deployment(deployment, jobs_id, ctx);
}

GG_TEST_DEFINE(retrieve_exact_capacity_publishes_complete_state) {
    uint8_t stored_jobs_id[64];
    memset(stored_jobs_id, 'j', sizeof(stored_jobs_id));
    uint8_t jobs_id_mem[64] = { 0 };
    GgBuffer jobs_id = GG_BUF(jobs_id_mem);
    GglDeployment deployment = { 0 };
    DeploymentContext ctx = { 0 };

    GG_TEST_ASSERT_OK(bootstrap_retrieve_test_state(
        GG_BUF(stored_jobs_id),
        GG_STR("deployment-id"),
        &deployment,
        &jobs_id,
        &ctx
    ));

    TEST_ASSERT_EQUAL_size_t(sizeof(stored_jobs_id), jobs_id.len);
    TEST_ASSERT_EQUAL_MEMORY(
        stored_jobs_id, jobs_id.data, sizeof(stored_jobs_id)
    );
    TEST_ASSERT_TRUE(
        gg_buffer_eq(deployment.deployment_id, GG_STR("deployment-id"))
    );
    TEST_ASSERT_TRUE(
        gg_buffer_eq(deployment.recipe_directory_path, GG_STR("/recipes"))
    );
    TEST_ASSERT_TRUE(
        gg_buffer_eq(deployment.artifacts_directory_path, GG_STR("/artifacts"))
    );
    TEST_ASSERT_TRUE(
        gg_buffer_eq(deployment.configuration_arn, GG_STR("config-arn"))
    );
    TEST_ASSERT_TRUE(gg_buffer_eq(deployment.thing_group, GG_STR("thing-group"))
    );
    TEST_ASSERT_EQUAL_size_t(0, deployment.components.len);
    TEST_ASSERT_EQUAL_INT(LOCAL_DEPLOYMENT, deployment.type);
    TEST_ASSERT_TRUE(ctx.is_bootstrap);
    TEST_ASSERT_TRUE(
        gg_buffer_eq(ctx.source_iot_data_endpoint, GG_STR("source-endpoint"))
    );
}

GG_TEST_DEFINE(retrieve_over_capacity_does_not_publish_state) {
    uint8_t stored_jobs_id[65];
    memset(stored_jobs_id, 'j', sizeof(stored_jobs_id));
    uint8_t jobs_id_mem[64];
    uint8_t expected_jobs_id[64];
    memset(jobs_id_mem, 'k', sizeof(jobs_id_mem));
    memset(expected_jobs_id, 'k', sizeof(expected_jobs_id));
    GgBuffer jobs_id = GG_BUF(jobs_id_mem);
    GgMap original_components
        = GG_MAP(gg_kv(GG_STR("original"), gg_obj_map((GgMap) { 0 })));
    GglDeployment deployment = {
        .deployment_id = GG_STR("original-id"),
        .recipe_directory_path = GG_STR("original-recipes"),
        .artifacts_directory_path = GG_STR("original-artifacts"),
        .configuration_arn = GG_STR("original-arn"),
        .thing_group = GG_STR("original-group"),
        .components = original_components,
        .type = THING_GROUP_DEPLOYMENT,
    };
    uint8_t removed_name_mem[8];
    GgByteVec removed_names = GG_BYTE_VEC(removed_name_mem);
    GgBuffer removed_component_mem[1];
    GgBufVec removed_components = GG_BUF_VEC(removed_component_mem);
    DeploymentContext ctx = {
        .is_bootstrap = false,
        .source_iot_data_endpoint = GG_STR("original-endpoint"),
        .removed_names_storage = &removed_names,
        .removed_components = &removed_components,
    };

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_RANGE,
        bootstrap_retrieve_test_state(
            GG_BUF(stored_jobs_id),
            GG_STR("deployment-id"),
            &deployment,
            &jobs_id,
            &ctx
        )
    );

    TEST_ASSERT_EQUAL_size_t(sizeof(jobs_id_mem), jobs_id.len);
    TEST_ASSERT_EQUAL_MEMORY(
        expected_jobs_id, jobs_id.data, sizeof(expected_jobs_id)
    );
    TEST_ASSERT_TRUE(
        gg_buffer_eq(deployment.deployment_id, GG_STR("original-id"))
    );
    TEST_ASSERT_TRUE(gg_buffer_eq(
        deployment.recipe_directory_path, GG_STR("original-recipes")
    ));
    TEST_ASSERT_TRUE(gg_buffer_eq(
        deployment.artifacts_directory_path, GG_STR("original-artifacts")
    ));
    TEST_ASSERT_TRUE(
        gg_buffer_eq(deployment.configuration_arn, GG_STR("original-arn"))
    );
    TEST_ASSERT_TRUE(
        gg_buffer_eq(deployment.thing_group, GG_STR("original-group"))
    );
    TEST_ASSERT_EQUAL_size_t(1, deployment.components.len);
    TEST_ASSERT_EQUAL_INT(THING_GROUP_DEPLOYMENT, deployment.type);
    TEST_ASSERT_FALSE(ctx.is_bootstrap);
    TEST_ASSERT_TRUE(
        gg_buffer_eq(ctx.source_iot_data_endpoint, GG_STR("original-endpoint"))
    );
    TEST_ASSERT_EQUAL_PTR(&removed_names, ctx.removed_names_storage);
    TEST_ASSERT_EQUAL_PTR(&removed_components, ctx.removed_components);
}

GG_TEST_DEFINE(retrieve_late_failure_does_not_publish_state) {
    uint8_t stored_jobs_id[64];
    memset(stored_jobs_id, 'j', sizeof(stored_jobs_id));
    uint8_t oversized_deployment_id[5001];
    memset(oversized_deployment_id, 'd', sizeof(oversized_deployment_id));
    uint8_t jobs_id_mem[64];
    uint8_t expected_jobs_id[64];
    memset(jobs_id_mem, 'k', sizeof(jobs_id_mem));
    memset(expected_jobs_id, 'k', sizeof(expected_jobs_id));
    GgBuffer jobs_id = GG_BUF(jobs_id_mem);
    GgMap original_components
        = GG_MAP(gg_kv(GG_STR("original"), gg_obj_map((GgMap) { 0 })));
    GglDeployment deployment = {
        .deployment_id = GG_STR("original-id"),
        .recipe_directory_path = GG_STR("original-recipes"),
        .artifacts_directory_path = GG_STR("original-artifacts"),
        .configuration_arn = GG_STR("original-arn"),
        .thing_group = GG_STR("original-group"),
        .components = original_components,
        .type = THING_GROUP_DEPLOYMENT,
    };
    uint8_t removed_name_mem[8];
    GgByteVec removed_names = GG_BYTE_VEC(removed_name_mem);
    GgBuffer removed_component_mem[1];
    GgBufVec removed_components = GG_BUF_VEC(removed_component_mem);
    DeploymentContext ctx = {
        .is_bootstrap = false,
        .source_iot_data_endpoint = GG_STR("original-endpoint"),
        .removed_names_storage = &removed_names,
        .removed_components = &removed_components,
    };

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOMEM,
        bootstrap_retrieve_test_state(
            GG_BUF(stored_jobs_id),
            GG_BUF(oversized_deployment_id),
            &deployment,
            &jobs_id,
            &ctx
        )
    );

    TEST_ASSERT_EQUAL_size_t(sizeof(jobs_id_mem), jobs_id.len);
    TEST_ASSERT_EQUAL_MEMORY(
        expected_jobs_id, jobs_id.data, sizeof(expected_jobs_id)
    );
    TEST_ASSERT_TRUE(
        gg_buffer_eq(deployment.deployment_id, GG_STR("original-id"))
    );
    TEST_ASSERT_TRUE(gg_buffer_eq(
        deployment.recipe_directory_path, GG_STR("original-recipes")
    ));
    TEST_ASSERT_TRUE(gg_buffer_eq(
        deployment.artifacts_directory_path, GG_STR("original-artifacts")
    ));
    TEST_ASSERT_TRUE(
        gg_buffer_eq(deployment.configuration_arn, GG_STR("original-arn"))
    );
    TEST_ASSERT_TRUE(
        gg_buffer_eq(deployment.thing_group, GG_STR("original-group"))
    );
    TEST_ASSERT_EQUAL_size_t(1, deployment.components.len);
    TEST_ASSERT_EQUAL_INT(THING_GROUP_DEPLOYMENT, deployment.type);
    TEST_ASSERT_FALSE(ctx.is_bootstrap);
    TEST_ASSERT_TRUE(
        gg_buffer_eq(ctx.source_iot_data_endpoint, GG_STR("original-endpoint"))
    );
    TEST_ASSERT_EQUAL_PTR(&removed_names, ctx.removed_names_storage);
    TEST_ASSERT_EQUAL_PTR(&removed_components, ctx.removed_components);
}

static GgError bootstrap_retrieve_configured_state(
    GgBuffer stored_jobs_id,
    GgBuffer stored_deployment_type,
    GgBuffer stored_deployment_id,
    GgBuffer stored_recipe_directory_path,
    GgObject *source_endpoint,
    GglDeployment *deployment,
    GgBuffer *jobs_id,
    DeploymentContext *ctx
) {
    GgKV deployment_doc_pairs[] = {
        gg_kv(GG_STR("deployment_id"), gg_obj_buf(stored_deployment_id)),
        gg_kv(
            GG_STR("recipe_directory_path"),
            gg_obj_buf(stored_recipe_directory_path)
        ),
        gg_kv(
            GG_STR("artifacts_directory_path"), gg_obj_buf(GG_STR("/artifacts"))
        ),
        gg_kv(GG_STR("configuration_arn"), gg_obj_buf(GG_STR("config-arn"))),
        gg_kv(GG_STR("thing_group"), gg_obj_buf(GG_STR("thing-group"))),
        gg_kv(GG_STR("components"), gg_obj_map((GgMap) { 0 })),
    };
    GgMap deployment_doc = {
        .pairs = deployment_doc_pairs,
        .len = sizeof(deployment_doc_pairs) / sizeof(deployment_doc_pairs[0]),
    };
    GgKV deployment_state_pairs[4] = {
        gg_kv(GG_STR("jobsID"), gg_obj_buf(stored_jobs_id)),
        gg_kv(GG_STR("deploymentType"), gg_obj_buf(stored_deployment_type)),
        gg_kv(GG_STR("deploymentDoc"), gg_obj_map(deployment_doc)),
    };
    GgMap deployment_state = {
        .pairs = deployment_state_pairs,
        .len = 3,
    };
    if (source_endpoint != NULL) {
        deployment_state_pairs[deployment_state.len]
            = gg_kv(GG_STR("sourceIotDataEndpoint"), *source_endpoint);
        deployment_state.len += 1;
    }

    bootstrap_config_read_value = gg_obj_map(deployment_state);
    ggl_deployment_config_set_reader_for_test(bootstrap_fake_config_reader);
    return retrieve_in_progress_deployment(deployment, jobs_id, ctx);
}

typedef struct {
    uint8_t jobs_id_mem[64];
    size_t jobs_id_capacity;
    GgBuffer jobs_id;
    GgKV original_component;
    GglDeployment deployment;
    uint8_t removed_name_mem[8];
    GgByteVec removed_names;
    GgBuffer removed_component_mem[1];
    GgBufVec removed_components;
    DeploymentContext ctx;
} UnchangedRetrieveOutputs;

static void init_unchanged_retrieve_outputs(
    UnchangedRetrieveOutputs *outputs, size_t jobs_id_capacity
) {
    memset(outputs, 0, sizeof(*outputs));
    memset(outputs->jobs_id_mem, 'k', sizeof(outputs->jobs_id_mem));
    outputs->jobs_id_capacity = jobs_id_capacity;
    outputs->jobs_id = (GgBuffer) {
        .data = outputs->jobs_id_mem,
        .len = jobs_id_capacity,
    };
    outputs->original_component
        = gg_kv(GG_STR("original"), gg_obj_map((GgMap) { 0 }));
    outputs->deployment = (GglDeployment) {
        .deployment_id = GG_STR("original-id"),
        .recipe_directory_path = GG_STR("original-recipes"),
        .artifacts_directory_path = GG_STR("original-artifacts"),
        .configuration_arn = GG_STR("original-arn"),
        .thing_group = GG_STR("original-group"),
        .components = (GgMap) {
            .pairs = &outputs->original_component,
            .len = 1,
        },
        .type = THING_GROUP_DEPLOYMENT,
    };
    outputs->removed_names = GG_BYTE_VEC(outputs->removed_name_mem);
    outputs->removed_components = GG_BUF_VEC(outputs->removed_component_mem);
    outputs->ctx = (DeploymentContext) {
        .is_bootstrap = false,
        .source_iot_data_endpoint = GG_STR("original-endpoint"),
        .removed_names_storage = &outputs->removed_names,
        .removed_components = &outputs->removed_components,
    };
}

static void assert_retrieve_outputs_unchanged(
    const UnchangedRetrieveOutputs *outputs
) {
    TEST_ASSERT_EQUAL_size_t(outputs->jobs_id_capacity, outputs->jobs_id.len);
    for (size_t i = 0; i < sizeof(outputs->jobs_id_mem); i++) {
        TEST_ASSERT_EQUAL_UINT8('k', outputs->jobs_id_mem[i]);
    }
    TEST_ASSERT_TRUE(
        gg_buffer_eq(outputs->deployment.deployment_id, GG_STR("original-id"))
    );
    TEST_ASSERT_TRUE(gg_buffer_eq(
        outputs->deployment.recipe_directory_path, GG_STR("original-recipes")
    ));
    TEST_ASSERT_TRUE(gg_buffer_eq(
        outputs->deployment.artifacts_directory_path,
        GG_STR("original-artifacts")
    ));
    TEST_ASSERT_TRUE(gg_buffer_eq(
        outputs->deployment.configuration_arn, GG_STR("original-arn")
    ));
    TEST_ASSERT_TRUE(
        gg_buffer_eq(outputs->deployment.thing_group, GG_STR("original-group"))
    );
    TEST_ASSERT_EQUAL_size_t(1, outputs->deployment.components.len);
    TEST_ASSERT_EQUAL_INT(THING_GROUP_DEPLOYMENT, outputs->deployment.type);
    TEST_ASSERT_FALSE(outputs->ctx.is_bootstrap);
    TEST_ASSERT_TRUE(gg_buffer_eq(
        outputs->ctx.source_iot_data_endpoint, GG_STR("original-endpoint")
    ));
    TEST_ASSERT_EQUAL_PTR(
        &outputs->removed_names, outputs->ctx.removed_names_storage
    );
    TEST_ASSERT_EQUAL_PTR(
        &outputs->removed_components, outputs->ctx.removed_components
    );
}

static void assert_successful_retrieval(
    const GglDeployment *deployment,
    const DeploymentContext *ctx,
    GgBuffer expected_deployment_id,
    GgBuffer expected_recipe_directory_path,
    GgBuffer expected_source_endpoint
) {
    TEST_ASSERT_TRUE(
        gg_buffer_eq(deployment->deployment_id, expected_deployment_id)
    );
    TEST_ASSERT_TRUE(gg_buffer_eq(
        deployment->recipe_directory_path, expected_recipe_directory_path
    ));
    TEST_ASSERT_TRUE(
        gg_buffer_eq(deployment->artifacts_directory_path, GG_STR("/artifacts"))
    );
    TEST_ASSERT_TRUE(
        gg_buffer_eq(deployment->configuration_arn, GG_STR("config-arn"))
    );
    TEST_ASSERT_TRUE(
        gg_buffer_eq(deployment->thing_group, GG_STR("thing-group"))
    );
    TEST_ASSERT_EQUAL_size_t(0, deployment->components.len);
    TEST_ASSERT_EQUAL_INT(LOCAL_DEPLOYMENT, deployment->type);
    TEST_ASSERT_TRUE(ctx->is_bootstrap);
    TEST_ASSERT_TRUE(
        gg_buffer_eq(ctx->source_iot_data_endpoint, expected_source_endpoint)
    );
}

GG_TEST_DEFINE(retrieve_wrong_type_source_endpoint_does_not_publish) {
    UnchangedRetrieveOutputs outputs;
    init_unchanged_retrieve_outputs(&outputs, sizeof(outputs.jobs_id_mem));
    GgObject source_endpoint = gg_obj_i64(1);

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_PARSE,
        bootstrap_retrieve_configured_state(
            GG_STR("job-id"),
            GG_STR("LOCAL_DEPLOYMENT"),
            GG_STR("deployment-id"),
            GG_STR("/recipes"),
            &source_endpoint,
            &outputs.deployment,
            &outputs.jobs_id,
            &outputs.ctx
        )
    );
    assert_retrieve_outputs_unchanged(&outputs);
}

GG_TEST_DEFINE(retrieve_128_byte_endpoint_is_persistent_and_absence_clears) {
    uint8_t source_endpoint_mem[128];
    uint8_t expected_endpoint_mem[128];
    memset(source_endpoint_mem, 'e', sizeof(source_endpoint_mem));
    memset(expected_endpoint_mem, 'e', sizeof(expected_endpoint_mem));
    GgObject source_endpoint = gg_obj_buf(GG_BUF(source_endpoint_mem));
    uint8_t first_jobs_id_mem[64] = { 0 };
    GgBuffer first_jobs_id = GG_BUF(first_jobs_id_mem);
    GglDeployment first_deployment = { 0 };
    uint8_t first_removed_name_mem[8];
    GgByteVec first_removed_names = GG_BYTE_VEC(first_removed_name_mem);
    GgBuffer first_removed_component_mem[1];
    GgBufVec first_removed_components = GG_BUF_VEC(first_removed_component_mem);
    DeploymentContext first_ctx = {
        .removed_names_storage = &first_removed_names,
        .removed_components = &first_removed_components,
    };

    GG_TEST_ASSERT_OK(bootstrap_retrieve_configured_state(
        GG_STR("first-job"),
        GG_STR("LOCAL_DEPLOYMENT"),
        GG_STR("first-id"),
        GG_STR("/first-recipes"),
        &source_endpoint,
        &first_deployment,
        &first_jobs_id,
        &first_ctx
    ));
    TEST_ASSERT_NOT_EQUAL(
        source_endpoint_mem, first_ctx.source_iot_data_endpoint.data
    );
    TEST_ASSERT_EQUAL_size_t(
        sizeof(source_endpoint_mem), first_ctx.source_iot_data_endpoint.len
    );
    TEST_ASSERT_EQUAL_MEMORY(
        expected_endpoint_mem,
        first_ctx.source_iot_data_endpoint.data,
        sizeof(expected_endpoint_mem)
    );
    TEST_ASSERT_EQUAL_PTR(
        &first_removed_names, first_ctx.removed_names_storage
    );
    TEST_ASSERT_EQUAL_PTR(
        &first_removed_components, first_ctx.removed_components
    );

    memset(source_endpoint_mem, 'x', sizeof(source_endpoint_mem));
    UnchangedRetrieveOutputs absent_outputs;
    init_unchanged_retrieve_outputs(
        &absent_outputs, sizeof(absent_outputs.jobs_id_mem)
    );
    GG_TEST_ASSERT_OK(bootstrap_retrieve_configured_state(
        GG_STR("second-job"),
        GG_STR("LOCAL_DEPLOYMENT"),
        GG_STR("second-id"),
        GG_STR("/second-recipes"),
        NULL,
        &absent_outputs.deployment,
        &absent_outputs.jobs_id,
        &absent_outputs.ctx
    ));

    TEST_ASSERT_NULL(absent_outputs.ctx.source_iot_data_endpoint.data);
    TEST_ASSERT_EQUAL_size_t(
        0, absent_outputs.ctx.source_iot_data_endpoint.len
    );
    TEST_ASSERT_EQUAL_PTR(
        &absent_outputs.removed_names, absent_outputs.ctx.removed_names_storage
    );
    TEST_ASSERT_EQUAL_PTR(
        &absent_outputs.removed_components,
        absent_outputs.ctx.removed_components
    );
    assert_successful_retrieval(
        &first_deployment,
        &first_ctx,
        GG_STR("first-id"),
        GG_STR("/first-recipes"),
        GG_BUF(expected_endpoint_mem)
    );
}

GG_TEST_DEFINE(retrieve_129_byte_source_endpoint_does_not_publish) {
    uint8_t source_endpoint_mem[129];
    memset(source_endpoint_mem, 'e', sizeof(source_endpoint_mem));
    GgObject source_endpoint = gg_obj_buf(GG_BUF(source_endpoint_mem));
    UnchangedRetrieveOutputs outputs;
    init_unchanged_retrieve_outputs(&outputs, sizeof(outputs.jobs_id_mem));

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_RANGE,
        bootstrap_retrieve_configured_state(
            GG_STR("job-id"),
            GG_STR("LOCAL_DEPLOYMENT"),
            GG_STR("deployment-id"),
            GG_STR("/recipes"),
            &source_endpoint,
            &outputs.deployment,
            &outputs.jobs_id,
            &outputs.ctx
        )
    );
    assert_retrieve_outputs_unchanged(&outputs);
}

GG_TEST_DEFINE(retrieve_unknown_deployment_type_does_not_publish) {
    GgObject source_endpoint = gg_obj_buf(GG_STR("source-endpoint"));
    UnchangedRetrieveOutputs outputs;
    init_unchanged_retrieve_outputs(&outputs, sizeof(outputs.jobs_id_mem));

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_CONFIG,
        bootstrap_retrieve_configured_state(
            GG_STR("job-id"),
            GG_STR("UNKNOWN_DEPLOYMENT"),
            GG_STR("deployment-id"),
            GG_STR("/recipes"),
            &source_endpoint,
            &outputs.deployment,
            &outputs.jobs_id,
            &outputs.ctx
        )
    );
    assert_retrieve_outputs_unchanged(&outputs);
}

GG_TEST_DEFINE(retrieve_late_copy_failure_preserves_prior_success_and_outputs) {
    GgObject first_source_endpoint = gg_obj_buf(GG_STR("first-endpoint"));
    uint8_t first_jobs_id_mem[64] = { 0 };
    GgBuffer first_jobs_id = GG_BUF(first_jobs_id_mem);
    GglDeployment first_deployment = { 0 };
    DeploymentContext first_ctx = { 0 };
    GG_TEST_ASSERT_OK(bootstrap_retrieve_configured_state(
        GG_STR("first-job"),
        GG_STR("LOCAL_DEPLOYMENT"),
        GG_STR("first-id"),
        GG_STR("/first-recipes"),
        &first_source_endpoint,
        &first_deployment,
        &first_jobs_id,
        &first_ctx
    ));

    uint8_t oversized_recipe_path[5001];
    memset(oversized_recipe_path, 'r', sizeof(oversized_recipe_path));
    GgObject second_source_endpoint = gg_obj_buf(GG_STR("second-endpoint"));
    UnchangedRetrieveOutputs failed_outputs;
    init_unchanged_retrieve_outputs(
        &failed_outputs, sizeof(failed_outputs.jobs_id_mem)
    );
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOMEM,
        bootstrap_retrieve_configured_state(
            GG_STR("second-job"),
            GG_STR("LOCAL_DEPLOYMENT"),
            GG_STR("different-id"),
            GG_BUF(oversized_recipe_path),
            &second_source_endpoint,
            &failed_outputs.deployment,
            &failed_outputs.jobs_id,
            &failed_outputs.ctx
        )
    );

    assert_retrieve_outputs_unchanged(&failed_outputs);
    TEST_ASSERT_TRUE(gg_buffer_eq(first_jobs_id, GG_STR("first-job")));
    assert_successful_retrieval(
        &first_deployment,
        &first_ctx,
        GG_STR("first-id"),
        GG_STR("/first-recipes"),
        GG_STR("first-endpoint")
    );
}

GG_TEST_DEFINE(retrieve_small_jobs_destination_preserves_prior_success) {
    GgObject first_source_endpoint = gg_obj_buf(GG_STR("first-endpoint"));
    uint8_t first_jobs_id_mem[64] = { 0 };
    GgBuffer first_jobs_id = GG_BUF(first_jobs_id_mem);
    GglDeployment first_deployment = { 0 };
    DeploymentContext first_ctx = { 0 };
    GG_TEST_ASSERT_OK(bootstrap_retrieve_configured_state(
        GG_STR("first-job"),
        GG_STR("LOCAL_DEPLOYMENT"),
        GG_STR("first-id"),
        GG_STR("/first-recipes"),
        &first_source_endpoint,
        &first_deployment,
        &first_jobs_id,
        &first_ctx
    ));

    GgObject second_source_endpoint = gg_obj_buf(GG_STR("second-endpoint"));
    UnchangedRetrieveOutputs failed_outputs;
    init_unchanged_retrieve_outputs(&failed_outputs, 4);
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_RANGE,
        bootstrap_retrieve_configured_state(
            GG_STR("second-job"),
            GG_STR("LOCAL_DEPLOYMENT"),
            GG_STR("different-id"),
            GG_STR("/different-recipes"),
            &second_source_endpoint,
            &failed_outputs.deployment,
            &failed_outputs.jobs_id,
            &failed_outputs.ctx
        )
    );

    assert_retrieve_outputs_unchanged(&failed_outputs);
    TEST_ASSERT_TRUE(gg_buffer_eq(first_jobs_id, GG_STR("first-job")));
    assert_successful_retrieval(
        &first_deployment,
        &first_ctx,
        GG_STR("first-id"),
        GG_STR("/first-recipes"),
        GG_STR("first-endpoint")
    );
}

GG_TEST_DEFINE(jobs_id_exact_capacity_is_copied) {
    uint8_t source[64];
    memset(source, 'j', sizeof(source));
    GgMap deployment_config
        = GG_MAP(gg_kv(GG_STR("jobsID"), gg_obj_buf(GG_BUF(source))));
    uint8_t destination_mem[64] = { 0 };
    GgBuffer destination = GG_BUF(destination_mem);

    GG_TEST_ASSERT_OK(copy_jobs_id(deployment_config, &destination));
    TEST_ASSERT_EQUAL_size_t(sizeof(source), destination.len);
    TEST_ASSERT_EQUAL_MEMORY(source, destination_mem, sizeof(source));
}

GG_TEST_DEFINE(jobs_id_over_capacity_does_not_copy) {
    uint8_t source[65];
    memset(source, 'j', sizeof(source));
    GgMap deployment_config
        = GG_MAP(gg_kv(GG_STR("jobsID"), gg_obj_buf(GG_BUF(source))));
    uint8_t destination_mem[64];
    uint8_t expected[64];
    memset(destination_mem, 'k', sizeof(destination_mem));
    memset(expected, 'k', sizeof(expected));
    GgBuffer destination = GG_BUF(destination_mem);

    TEST_ASSERT_EQUAL_INT(
        GG_ERR_RANGE, copy_jobs_id(deployment_config, &destination)
    );
    TEST_ASSERT_EQUAL_size_t(sizeof(destination_mem), destination.len);
    TEST_ASSERT_EQUAL_MEMORY(expected, destination_mem, sizeof(expected));
}

GG_TEST_DEFINE(jobs_id_wrong_type_does_not_copy) {
    GgMap deployment_config = GG_MAP(gg_kv(GG_STR("jobsID"), gg_obj_i64(1)));
    uint8_t destination_mem[4] = { 'k', 'e', 'e', 'p' };
    GgBuffer destination = GG_BUF(destination_mem);

    TEST_ASSERT_NOT_EQUAL(
        GG_ERR_OK, copy_jobs_id(deployment_config, &destination)
    );
    TEST_ASSERT_EQUAL_size_t(sizeof(destination_mem), destination.len);
    TEST_ASSERT_EQUAL_MEMORY("keep", destination_mem, sizeof(destination_mem));
}

GG_TEST_DEFINE(jobs_id_missing_does_not_copy) {
    uint8_t destination_mem[4] = { 'k', 'e', 'e', 'p' };
    GgBuffer destination = GG_BUF(destination_mem);

    TEST_ASSERT_NOT_EQUAL(GG_ERR_OK, copy_jobs_id((GgMap) { 0 }, &destination));
    TEST_ASSERT_EQUAL_size_t(sizeof(destination_mem), destination.len);
    TEST_ASSERT_EQUAL_MEMORY("keep", destination_mem, sizeof(destination_mem));
}

// ---- F-BS-BOOT-01/02 process-exec and config-write seam test doubles ----

#define BOOTSTRAP_TEST_MAX_CALLS 2
#define BOOTSTRAP_TEST_MAX_ARGS 4

static char bootstrap_event_log[16];
static size_t bootstrap_event_len;
static GgError bootstrap_link_result;
static GgError bootstrap_start_result;
static GgError bootstrap_write_result;
static size_t bootstrap_write_calls;
static char bootstrap_argv_log[BOOTSTRAP_TEST_MAX_CALLS]
                              [BOOTSTRAP_TEST_MAX_ARGS][PATH_MAX];
static size_t bootstrap_argc_log[BOOTSTRAP_TEST_MAX_CALLS];
static size_t bootstrap_process_calls;

static GgError bootstrap_test_record_process(
    const char *const argv[], const GglProcessSpawnConfig *config
) {
    (void) config;
    bool is_link = strcmp(argv[1], "link") == 0;
    if (bootstrap_event_len < (sizeof(bootstrap_event_log) - 1)) {
        bootstrap_event_log[bootstrap_event_len] = is_link ? 'L' : 'S';
        bootstrap_event_len += 1;
    }

    if (bootstrap_process_calls < BOOTSTRAP_TEST_MAX_CALLS) {
        size_t argc = 0;
        while ((argv[argc] != NULL) && (argc < BOOTSTRAP_TEST_MAX_ARGS)) {
            size_t len = strlen(argv[argc]);
            if (len >= PATH_MAX) {
                len = PATH_MAX - 1;
            }
            memcpy(
                bootstrap_argv_log[bootstrap_process_calls][argc],
                argv[argc],
                len
            );
            bootstrap_argv_log[bootstrap_process_calls][argc][len] = '\0';
            argc += 1;
        }
        bootstrap_argc_log[bootstrap_process_calls] = argc;
    }
    bootstrap_process_calls += 1;
    return is_link ? bootstrap_link_result : bootstrap_start_result;
}

// Records 'P' for each persistence write so ordering against link/start can be
// asserted, and counts writes for the zero-write regression.
static GgError bootstrap_test_record_write(
    GgBufList key_path, GgObject value, const int64_t *timestamp
) {
    (void) key_path;
    (void) value;
    (void) timestamp;
    bootstrap_write_calls += 1;
    if (bootstrap_event_len < (sizeof(bootstrap_event_log) - 1)) {
        bootstrap_event_log[bootstrap_event_len] = 'P';
        bootstrap_event_len += 1;
    }
    return bootstrap_write_result;
}

void bootstrap_manager_reset_test_seams(void) {
    bootstrap_process_call = bootstrap_process_call_default;
    bootstrap_config_write = ggl_gg_config_write;
    memset(bootstrap_event_log, 0, sizeof(bootstrap_event_log));
    bootstrap_event_len = 0;
    bootstrap_link_result = GG_ERR_OK;
    bootstrap_start_result = GG_ERR_OK;
    bootstrap_write_result = GG_ERR_OK;
    bootstrap_write_calls = 0;
    memset(bootstrap_argv_log, 0, sizeof(bootstrap_argv_log));
    memset(bootstrap_argc_log, 0, sizeof(bootstrap_argc_log));
    bootstrap_process_calls = 0;
}

void bootstrap_manager_override_test_seam_for_reset_test(void) {
    bootstrap_process_call = bootstrap_test_record_process;
    bootstrap_config_write = bootstrap_test_record_write;
}

bool bootstrap_manager_test_seams_are_reset(void) {
    return (bootstrap_process_call == bootstrap_process_call_default)
        && (bootstrap_config_write == ggl_gg_config_write);
}

#define BOOTSTRAP_EXIT_OK GG_ERR_OK
#define BOOTSTRAP_EXIT_NONZERO GG_ERR_FAILURE
#define BOOTSTRAP_EXIT_ABNORMAL GG_ERR_FATAL

static GgError bootstrap_run_service_and_capture_at_path(
    GgError link_result, GgError start_result, GgBuffer service_path
) {
    bootstrap_manager_reset_test_seams();
    bootstrap_process_call = bootstrap_test_record_process;
    bootstrap_config_write = bootstrap_test_record_write;
    bootstrap_link_result = link_result;
    bootstrap_start_result = start_result;
    return link_and_start_bootstrap_service(
        GG_STR("Test.Component"), GG_STR("1.0.0"), service_path
    );
}

static GgError bootstrap_run_service_and_capture(
    GgError link_result, GgError start_result
) {
    return bootstrap_run_service_and_capture_at_path(
        link_result,
        start_result,
        GG_STR("/var/lib/greengrass/ggl.Test.Component.bootstrap.service")
    );
}

GG_TEST_DEFINE(save_deployment_info_invalid_type_writes_nothing) {
    bootstrap_manager_reset_test_seams();
    bootstrap_config_write = bootstrap_test_record_write;

    GglDeployment deployment = { .type = (GglDeploymentType) 42 };
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID, save_deployment_info(&deployment, (GgBuffer) { 0 })
    );
    TEST_ASSERT_EQUAL_size_t(0, bootstrap_write_calls);

    // A recognized type writes the deployment doc and type (endpoint absent).
    bootstrap_write_calls = 0;
    deployment.type = LOCAL_DEPLOYMENT;
    GG_TEST_ASSERT_OK(save_deployment_info(&deployment, (GgBuffer) { 0 }));
    TEST_ASSERT_EQUAL_size_t(2, bootstrap_write_calls);
}

GG_TEST_DEFINE(bootstrap_link_failure_is_error_without_start_or_persist) {
    // Nonzero link exit is a real error; start and persist never run.
    TEST_ASSERT_NOT_EQUAL(
        GG_ERR_OK,
        bootstrap_run_service_and_capture(
            BOOTSTRAP_EXIT_NONZERO, BOOTSTRAP_EXIT_OK
        )
    );
    TEST_ASSERT_EQUAL_STRING("L", bootstrap_event_log);

    // Abnormal link exit is likewise a real error.
    TEST_ASSERT_NOT_EQUAL(
        GG_ERR_OK,
        bootstrap_run_service_and_capture(
            BOOTSTRAP_EXIT_ABNORMAL, BOOTSTRAP_EXIT_OK
        )
    );
    TEST_ASSERT_EQUAL_STRING("L", bootstrap_event_log);
}

GG_TEST_DEFINE(bootstrap_start_failure_is_error_without_persist) {
    // Nonzero start exit: link and start ran, completion is not persisted.
    TEST_ASSERT_NOT_EQUAL(
        GG_ERR_OK,
        bootstrap_run_service_and_capture(
            BOOTSTRAP_EXIT_OK, BOOTSTRAP_EXIT_NONZERO
        )
    );
    TEST_ASSERT_EQUAL_STRING("LS", bootstrap_event_log);

    // Abnormal start exit is likewise a real error with no persist.
    TEST_ASSERT_NOT_EQUAL(
        GG_ERR_OK,
        bootstrap_run_service_and_capture(
            BOOTSTRAP_EXIT_OK, BOOTSTRAP_EXIT_ABNORMAL
        )
    );
    TEST_ASSERT_EQUAL_STRING("LS", bootstrap_event_log);
}

GG_TEST_DEFINE(bootstrap_process_arguments_preserve_boundaries) {
    GgBuffer service_path = GG_STR(
        "/var/lib/green grass;touch $HOME/ggl.Test.Component.bootstrap.service"
    );
    GG_TEST_ASSERT_OK(bootstrap_run_service_and_capture_at_path(
        BOOTSTRAP_EXIT_OK, BOOTSTRAP_EXIT_OK, service_path
    ));

    TEST_ASSERT_EQUAL_size_t(2, bootstrap_process_calls);
    TEST_ASSERT_EQUAL_size_t(3, bootstrap_argc_log[0]);
    TEST_ASSERT_EQUAL_STRING("systemctl", bootstrap_argv_log[0][0]);
    TEST_ASSERT_EQUAL_STRING("link", bootstrap_argv_log[0][1]);
    TEST_ASSERT_EQUAL_STRING(
        "/var/lib/green grass;touch $HOME/ggl.Test.Component.bootstrap.service",
        bootstrap_argv_log[0][2]
    );
    TEST_ASSERT_EQUAL_size_t(3, bootstrap_argc_log[1]);
    TEST_ASSERT_EQUAL_STRING("systemctl", bootstrap_argv_log[1][0]);
    TEST_ASSERT_EQUAL_STRING("start", bootstrap_argv_log[1][1]);
    TEST_ASSERT_EQUAL_STRING(
        "ggl.Test.Component.bootstrap.service", bootstrap_argv_log[1][2]
    );
    TEST_ASSERT_EQUAL_STRING("LSP", bootstrap_event_log);
}

GG_TEST_DEFINE(bootstrap_success_persists_completion_after_start) {
    GG_TEST_ASSERT_OK(
        bootstrap_run_service_and_capture(BOOTSTRAP_EXIT_OK, BOOTSTRAP_EXIT_OK)
    );
    // Persist ('P') occurs only after link ('L') and start ('S') both succeed.
    TEST_ASSERT_EQUAL_STRING("LSP", bootstrap_event_log);
}

#endif
