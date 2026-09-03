
#include "stale_component.h"
#include "component_store.h"
#include "deployment_model.h"
#include "deployment_queue.h"
#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <ftw.h>
#include <gg/arena.h>
#include <gg/buffer.h>
#include <gg/cleanup.h>
#include <gg/error.h>
#include <gg/file.h>
#include <gg/log.h>
#include <gg/map.h>
#include <gg/object.h>
#include <gg/vector.h>
#include <ggl/core_bus/gg_config.h>
#include <ggl/docker_artifact_cleanup.h>
#include <ggl/process.h>
#include <limits.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

// Forward declare structure for use in the function below.
struct stat;

// Test seams for argv-based process execution and direct file removal. Tests
// capture exact arguments and paths and can force failures without touching
// systemd or the filesystem.
static GgError stale_process_call_default(
    const char *const argv[], const GglProcessSpawnConfig *config
) {
    return ggl_process_call(argv, config);
}

#ifdef GG_SDK_TESTING

static GgError (*stale_process_call)(
    const char *const argv[], const GglProcessSpawnConfig *config
) = stale_process_call_default;
static int (*stale_remove)(const char *path) = remove;

void stale_component_reset_test_seams(void);
void stale_component_override_test_seam_for_reset_test(void);
bool stale_component_test_seams_are_reset(void);

#else

// NOLINTNEXTLINE(readability-identifier-naming)
#define stale_process_call stale_process_call_default
// NOLINTNEXTLINE(readability-identifier-naming)
#define stale_remove remove

#endif

// Classifies a remove() outcome. Returns true when no warning is warranted:
// the removal succeeded (result == 0) or the file was already absent (ENOENT).
// Any other failure returns false. Pure and directly unit-testable; this is
// the classification the previous code got wrong by comparing remove()'s
// return value against errno constants.
static bool remove_result_is_ignorable(int remove_result, int remove_errno) {
    return (remove_result == 0) || (remove_errno == ENOENT);
}

// Best-effort deletion of a single file. errno is inspected only after
// remove() reports failure; a successful remove never reads errno (where it is
// indeterminate). A missing file (ENOENT) is treated as success, and any other
// failure is logged without altering the caller's cleanup control flow.
static void delete_optional_file(const char *path) {
    int result = stale_remove(path);
    if (result == 0) {
        return;
    }
    int remove_errno = errno;
    if (!remove_result_is_ignorable(result, remove_errno)) {
        GG_LOGW("Failed to delete the file %s (errno=%d).", path, remove_errno);
    }
}

// Builds the canonical systemd unit name for a component lifecycle phase into
// @p unit_name:
//   RUN_STARTUP -> "ggl.<component>.service"
//   INSTALL     -> "ggl.<component>.install.service"
//   BOOTSTRAP   -> "ggl.<component>.bootstrap.service"
// The stop, disable, and both unlink commands all use this one canonical name
// so every cleanup step targets the same unit for a given phase.
static GgError build_service_unit_name(
    GgBuffer component_name, PhaseSelection phase, GgByteVec *unit_name
) {
    GgError ret = gg_byte_vec_append(unit_name, GG_STR("ggl."));
    gg_byte_vec_chain_append(&ret, unit_name, component_name);
    if (phase == INSTALL) {
        gg_byte_vec_chain_append(&ret, unit_name, GG_STR(".install"));
    } else if (phase == BOOTSTRAP) {
        gg_byte_vec_chain_append(&ret, unit_name, GG_STR(".bootstrap"));
    } else {
        // Startup/run appends no phase suffix.
        assert(phase == RUN_STARTUP);
    }
    gg_byte_vec_chain_append(&ret, unit_name, GG_STR(".service"));
    return ret;
}

// Runs a best-effort cleanup process and logs its result. Process failures are
// nonfatal so the remaining cleanup operations still run.
static void run_cleanup_process(
    const char *const argv[], GgBuffer description
) {
    GgError ret = stale_process_call(argv, NULL);
    if (ret == GG_ERR_OK) {
        GG_LOGI("%.*s succeeded.", (int) description.len, description.data);
    } else {
        GG_LOGW(
            "%.*s failed (error=%d).",
            (int) description.len,
            description.data,
            (int) ret
        );
    }
}

static int unlink_cb(
    const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf
) {
    (void) sb;
    (void) typeflag;
    (void) ftwbuf;

    int rv = remove(fpath);

    if (rv) {
        GG_LOGW("Failed to remove file %s.", fpath);
    }

    // Ignore the return code and keep deleting other files.
    return 0;
}

static int remove_all_files(char *path) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    return nftw(path, unlink_cb, 64, FTW_DEPTH | FTW_PHYS);
}

static GgError delete_component_artifact(
    GgBuffer component_name,
    GgBuffer version_number,
    GgByteVec *root_path,
    bool delete_all_versions
) {
    const size_t INDEX_BEFORE_ADDITION = root_path->buf.len;

    // Delete Docker artifacts
    int root_path_fd = -1;
    if (gg_dir_open(root_path->buf, 0, false, &root_path_fd) == GG_ERR_OK) {
        GG_LOGT("Attempting docker artifact removal");
        ggl_docker_artifact_cleanup(
            root_path_fd, component_name, version_number
        );
        (void) gg_close(root_path_fd);
    }

    // Delete artifacts.
    GgError err = gg_byte_vec_append(root_path, GG_STR("/packages/artifacts/"));
    gg_byte_vec_chain_append(&err, root_path, component_name);

    if (delete_all_versions == false) {
        gg_byte_vec_chain_append(&err, root_path, GG_STR("/"));
        gg_byte_vec_chain_append(&err, root_path, version_number);
        gg_byte_vec_chain_append(&err, root_path, GG_STR("\0"));
    } else {
        gg_byte_vec_chain_append(&err, root_path, GG_STR("\0"));
    }

    if (err != GG_ERR_OK) {
        GG_LOGE("Failed to create a delete-artifact path string.");
        return err;
    }

    (void) remove_all_files((char *) root_path->buf.data);

    // We should reset the index regardless of the error code in case caller
    // does not exit.
    root_path->buf.len = INDEX_BEFORE_ADDITION;
    memset(
        &(root_path->buf.data[INDEX_BEFORE_ADDITION]),
        0,
        root_path->capacity - INDEX_BEFORE_ADDITION
    );

    // Delete unarchived artifacts.
    err = gg_byte_vec_append(
        root_path, GG_STR("/packages/artifacts-unarchived/")
    );
    gg_byte_vec_chain_append(&err, root_path, component_name);

    if (delete_all_versions == false) {
        gg_byte_vec_chain_append(&err, root_path, GG_STR("/"));
        gg_byte_vec_chain_append(&err, root_path, version_number);
        gg_byte_vec_chain_append(&err, root_path, GG_STR("\0"));
    } else {
        gg_byte_vec_chain_append(&err, root_path, GG_STR("\0"));
    }

    if (err != GG_ERR_OK) {
        GG_LOGE("Failed to create a delete-artifact path string.");
        return err;
    }

    (void) remove_all_files((char *) root_path->buf.data);

    // We should reset the index regardless of the error code in case caller
    // does not exit.
    root_path->buf.len = INDEX_BEFORE_ADDITION;
    memset(
        &(root_path->buf.data[INDEX_BEFORE_ADDITION]),
        0,
        root_path->capacity - INDEX_BEFORE_ADDITION
    );

    return err;
}

static GgError delete_component_recipe(
    GgBuffer component_name, GgBuffer version_number, GgByteVec *root_path
) {
    const size_t INDEX_BEFORE_ADDITION = root_path->buf.len;
    GgError err = gg_byte_vec_append(root_path, GG_STR("/packages/recipes/"));
    gg_byte_vec_chain_append(&err, root_path, component_name);
    gg_byte_vec_chain_append(&err, root_path, GG_STR("-"));
    gg_byte_vec_chain_append(&err, root_path, version_number);

    // Store index so that we can restore the vector to this state.
    const size_t INDEX_BEFORE_FILE_EXTENTION = root_path->buf.len;
    const char *extentions[] = { ".json", ".yaml", ".yml" };

    for (size_t i = 0; i < (sizeof(extentions) / sizeof(char *)); i++) {
        GgBuffer buf = { .data = (uint8_t *) extentions[i],
                         .len = strlen(extentions[i]) };
        gg_byte_vec_chain_append(&err, root_path, buf);
        gg_byte_vec_chain_push(&err, root_path, '\0');

        if (err != GG_ERR_OK) {
            GG_LOGE("Failed to create a delete-recipe path string.");
            break;
        }

        delete_optional_file((char *) root_path->buf.data);

        // Restore vector state with only the component name added.
        root_path->buf.len = INDEX_BEFORE_FILE_EXTENTION;
        memset(
            &root_path->buf.data[INDEX_BEFORE_FILE_EXTENTION],
            0,
            root_path->capacity - INDEX_BEFORE_FILE_EXTENTION
        );
    }
    // We should reset the index regardless of the error code in case caller
    // does not exit.
    root_path->buf.len = INDEX_BEFORE_ADDITION;
    memset(
        &(root_path->buf.data[INDEX_BEFORE_ADDITION]),
        0,
        root_path->capacity - INDEX_BEFORE_ADDITION
    );

    return err;
}

static GgError delete_component(
    GgBuffer component_name, GgBuffer version_number, bool delete_all_versions
) {
    // TODO: Remove docker image artifacts before deleting recipe if this
    // component is the only one to require this artifact.

    GG_LOGD(
        "Removing component %.*s with version %.*s as it is marked as stale",
        (int) component_name.len,
        component_name.data,
        (int) version_number.len,
        version_number.data
    );
    GgError ret;

    // Remove component from config as we use that as source of truth for active
    // running components
    if (delete_all_versions) {
        ret = ggl_gg_config_delete(
            GG_BUF_LIST(GG_STR("services"), component_name)
        );
        if (ret != GG_ERR_OK) {
            GG_LOGE(
                "Failed to delete component information from the configuration."
            );
            return ret;
        }
        GG_LOGD(
            "Removed configuration of stale component %.*s",
            (int) component_name.len,
            component_name.data
        );
    }

    static uint8_t root_path_mem[PATH_MAX];
    memset(root_path_mem, 0, sizeof(root_path_mem));

    GgArena alloc = gg_arena_init(GG_BUF(root_path_mem));
    GgBuffer root_path_buffer;

    ret = ggl_gg_config_read_str(
        GG_BUF_LIST(GG_STR("system"), GG_STR("rootPath")),
        &alloc,
        &root_path_buffer
    );
    if (ret != GG_ERR_OK) {
        GG_LOGW("Failed to get root path from config.");
        return ret;
    }

    // Remove the trailing slash.
    if ((root_path_buffer.len != 0)
        && (root_path_buffer.data[root_path_buffer.len - 1] == '/')) {
        root_path_buffer.len--;
    }

    GgByteVec root_path = { .buf = { .data = root_path_buffer.data,
                                     .len = root_path_buffer.len },
                            .capacity = sizeof(root_path_mem) };

    GgError err = delete_component_artifact(
        component_name, version_number, &root_path, delete_all_versions
    );

    if (err != GG_ERR_OK) {
        return err;
    }

    err = delete_component_recipe(component_name, version_number, &root_path);

    return err;
}

static GgError delete_recipe_script_and_service_files_at_root(
    GgBuffer component_name, GgBuffer root_path
) {
    GgError ret = ggl_validate_component_name(component_name);
    if (ret != GG_ERR_OK) {
        return ret;
    }

    uint8_t path_mem[PATH_MAX];
    GgByteVec path = GG_BYTE_VEC(path_mem);
    ret = gg_byte_vec_append(&path, root_path);
    gg_byte_vec_chain_append(&ret, &path, GG_STR("/ggl."));
    gg_byte_vec_chain_append(&ret, &path, component_name);
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to create base path for service file deletion.");
        return ret;
    }

    size_t base_len = path.buf.len;
    const char *extensions[]
        = { ".bootstrap.service", ".install.service", ".service" };
    for (size_t i = 0; i < (sizeof(extensions) / sizeof(extensions[0])); i++) {
        GgBuffer extension = gg_buffer_from_null_term((char *) extensions[i]);
        ret = gg_byte_vec_append(&path, extension);
        gg_byte_vec_chain_push(&ret, &path, '\0');
        if (ret != GG_ERR_OK) {
            GG_LOGE("Failed to create path for service file deletion.");
            return ret;
        }

        delete_optional_file((char *) path.buf.data);
        path.buf.len = base_len;
    }
    return GG_ERR_OK;
}

static GgError delete_recipe_script_and_service_files(GgBuffer *component_name
) {
    static uint8_t root_path_mem[PATH_MAX];
    memset(root_path_mem, 0, sizeof(root_path_mem));

    GgArena alloc = gg_arena_init(GG_BUF(root_path_mem));
    GgBuffer root_path;
    GgError ret = ggl_gg_config_read_str(
        GG_BUF_LIST(GG_STR("system"), GG_STR("rootPath")), &alloc, &root_path
    );
    if (ret != GG_ERR_OK) {
        GG_LOGW("Failed to get root path from config.");
        return ret;
    }

    return delete_recipe_script_and_service_files_at_root(
        *component_name, root_path
    );
}

GgError disable_and_unlink_service(
    GgBuffer *component_name, PhaseSelection phase
) {
    GgError ret = ggl_validate_component_name(*component_name);
    if (ret != GG_ERR_OK) {
        return ret;
    }

    uint8_t unit_name_array[NAME_MAX];
    GgByteVec unit_name_vec = GG_BYTE_VEC(unit_name_array);
    ret = build_service_unit_name(*component_name, phase, &unit_name_vec);
    gg_byte_vec_chain_push(&ret, &unit_name_vec, '\0');
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to build the service unit name.");
        return ret;
    }
    GgBuffer unit_name
        = gg_buffer_substr(unit_name_vec.buf, 0, unit_name_vec.buf.len - 1);

    const char *stop_argv[]
        = { "systemctl", "stop", (char *) unit_name_array, NULL };
    run_cleanup_process(stop_argv, GG_STR("systemctl stop"));

    const char *disable_argv[]
        = { "systemctl", "disable", (char *) unit_name_array, NULL };
    run_cleanup_process(disable_argv, GG_STR("systemctl disable"));

    uint8_t etc_path_mem[PATH_MAX];
    GgByteVec etc_path = GG_BYTE_VEC(etc_path_mem);
    ret = gg_byte_vec_append(&etc_path, GG_STR("/etc/systemd/system/"));
    gg_byte_vec_chain_append(&ret, &etc_path, unit_name);
    gg_byte_vec_chain_push(&ret, &etc_path, '\0');
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to create /etc systemd unit path.");
        return ret;
    }
    delete_optional_file((char *) etc_path.buf.data);

    uint8_t usr_path_mem[PATH_MAX];
    GgByteVec usr_path = GG_BYTE_VEC(usr_path_mem);
    ret = gg_byte_vec_append(&usr_path, GG_STR("/usr/lib/systemd/system/"));
    gg_byte_vec_chain_append(&ret, &usr_path, unit_name);
    gg_byte_vec_chain_push(&ret, &usr_path, '\0');
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to create /usr systemd unit path.");
        return ret;
    }
    delete_optional_file((char *) usr_path.buf.data);

    const char *reload_argv[] = { "systemctl", "daemon-reload", NULL };
    run_cleanup_process(reload_argv, GG_STR("systemctl daemon-reload"));

    const char *reset_argv[] = { "systemctl", "reset-failed", NULL };
    run_cleanup_process(reset_argv, GG_STR("systemctl reset-failed"));

    return GG_ERR_OK;
}

// Records @p component_name in @p removed_components_out (with bytes copied
// into @p removed_names_storage), deduplicating against names already
// recorded. A no-op if either output pointer is NULL. On overflow of either
// storage, logs a warning and rolls back the partial byte copy without
// aborting the surrounding cleanup pass.
static void record_removed_component(
    GgBuffer component_name,
    GgByteVec *removed_names_storage,
    GgBufVec *removed_components_out
) {
    if ((removed_names_storage == NULL) || (removed_components_out == NULL)) {
        return;
    }

    for (size_t i = 0; i < removed_components_out->buf_list.len; i++) {
        if (gg_buffer_eq(
                component_name, removed_components_out->buf_list.bufs[i]
            )) {
            return;
        }
    }

    size_t name_offset = removed_names_storage->buf.len;
    GgError ret = gg_byte_vec_append(removed_names_storage, component_name);
    if (ret == GG_ERR_OK) {
        GgBuffer slice
            = { .data = removed_names_storage->buf.data + name_offset,
                .len = component_name.len };
        ret = gg_buf_vec_push(removed_components_out, slice);
    }
    if (ret != GG_ERR_OK) {
        // Roll back the partial copy so the storage offset stays consistent
        // with the bufvec.
        removed_names_storage->buf.len = name_offset;
        GG_LOGW(
            "Could not record removed component %.*s for fleet status update (ret=%d).",
            (int) component_name.len,
            component_name.data,
            (int) ret
        );
    }
}

GgError cleanup_stale_versions(
    GgMap latest_components_map,
    GgByteVec *removed_names_storage,
    GgBufVec *removed_components_out
) {
    // Both must be NULL or both must be non-NULL: the bufvec stores slices
    // into the byte vec, so they share a lifetime.
    assert((removed_names_storage == NULL) == (removed_components_out == NULL));

    int recipe_dir_fd;
    GgError ret = get_recipe_dir_fd(&recipe_dir_fd);
    if (ret != GG_ERR_OK) {
        return ret;
    }

    // iterate through recipes in the directory
    DIR *dir = fdopendir(recipe_dir_fd);
    if (dir == NULL) {
        GG_LOGE("Failed to open recipe directory.");
        (void) gg_close(recipe_dir_fd);
        return GG_ERR_FAILURE;
    }
    GG_CLEANUP(cleanup_closedir, dir);

    struct dirent *entry = NULL;
    uint8_t component_name_array[NAME_MAX];
    GgBuffer component_name_buffer_iterator
        = { .data = component_name_array, .len = 0 };

    uint8_t version_array[NAME_MAX];
    GgBuffer version_buffer_iterator = { .data = version_array, .len = 0 };

    while (true) {
        ret = iterate_over_components(
            dir,
            &component_name_buffer_iterator,
            &version_buffer_iterator,
            &entry
        );

        if (ret == GG_ERR_NOENTRY) {
            // No more entries to go over.
            break;
        }

        if (ret != GG_ERR_OK) {
            return ret;
        }
        assert(entry != NULL);

        if (ggl_validate_component_name(component_name_buffer_iterator)
            != GG_ERR_OK) {
            GG_LOGW("Skipping stale cleanup for invalid recipe component name."
            );
            continue;
        }

        // NucleusLite has no deployed systemd unit — skip stale cleanup.
        if (gg_buffer_has_prefix(
                component_name_buffer_iterator,
                GG_STR("aws.greengrass.NucleusLite")
            )) {
            continue;
        }

        // Try to find this component in the map.
        GgObject *component_version = NULL;
        if (gg_map_get(
                latest_components_map,
                component_name_buffer_iterator,
                &component_version
            )) {
            if (gg_buffer_eq(
                    version_buffer_iterator, gg_obj_into_buf(*component_version)
                )) {
                // The component name and version matches. Skip over it.
                continue;
            }

            // The component name matches but the version number doesn't
            // match. Delete it!
            (void) delete_component(
                component_name_buffer_iterator, version_buffer_iterator, false
            );
        } else {
            // Cannot find this component at all. Delete it!
            (void) delete_component(
                component_name_buffer_iterator, version_buffer_iterator, true
            );

            // Record the removal so the caller can report UNINSTALLED to the
            // cloud. Multiple installed versions of the same removed
            // component reach this branch once each, so the helper
            // deduplicates against names already recorded.
            record_removed_component(
                component_name_buffer_iterator,
                removed_names_storage,
                removed_components_out
            );

            // Also stop any running service for this component.
            (void) disable_and_unlink_service(
                &component_name_buffer_iterator, RUN_STARTUP
            );
            (void) disable_and_unlink_service(
                &component_name_buffer_iterator, INSTALL
            );
            (void) disable_and_unlink_service(
                &component_name_buffer_iterator, BOOTSTRAP
            );

            // Also delete the .script.install and .script.run and .service
            // files.
            (void) delete_recipe_script_and_service_files(
                &component_name_buffer_iterator
            );
        }
    }

    return GG_ERR_OK;
}

#ifdef GG_SDK_TESTING

#include <gg/test.h>
#include <unity.h>

#define STALE_TEST_MAX_PROCESS_CALLS 4
#define STALE_TEST_MAX_ARGS 4
#define STALE_TEST_MAX_REMOVES 8

static char stale_test_argv_log[STALE_TEST_MAX_PROCESS_CALLS]
                               [STALE_TEST_MAX_ARGS][PATH_MAX];
static size_t stale_test_argc_log[STALE_TEST_MAX_PROCESS_CALLS];
static size_t stale_test_process_count;
static GgError stale_test_process_result;
static char stale_test_remove_log[STALE_TEST_MAX_REMOVES][PATH_MAX];
static int stale_test_remove_result;
static int stale_test_remove_errno;
static size_t stale_test_remove_count;
static char stale_test_event_log[16];
static size_t stale_test_event_count;

static void stale_test_record_event(char event) {
    if (stale_test_event_count < (sizeof(stale_test_event_log) - 1)) {
        stale_test_event_log[stale_test_event_count] = event;
        stale_test_event_count += 1;
    }
}

static GgError stale_test_record_process(
    const char *const argv[], const GglProcessSpawnConfig *config
) {
    (void) config;
    if (stale_test_process_count < STALE_TEST_MAX_PROCESS_CALLS) {
        size_t argc = 0;
        while ((argv[argc] != NULL) && (argc < STALE_TEST_MAX_ARGS)) {
            size_t len = strlen(argv[argc]);
            if (len >= PATH_MAX) {
                len = PATH_MAX - 1;
            }
            memcpy(
                stale_test_argv_log[stale_test_process_count][argc],
                argv[argc],
                len
            );
            stale_test_argv_log[stale_test_process_count][argc][len] = '\0';
            argc += 1;
        }
        stale_test_argc_log[stale_test_process_count] = argc;
    }

    if (strcmp(argv[1], "stop") == 0) {
        stale_test_record_event('S');
    } else if (strcmp(argv[1], "disable") == 0) {
        stale_test_record_event('D');
    } else if (strcmp(argv[1], "daemon-reload") == 0) {
        stale_test_record_event('L');
    } else if (strcmp(argv[1], "reset-failed") == 0) {
        stale_test_record_event('F');
    } else {
        stale_test_record_event('U');
    }
    stale_test_process_count += 1;
    return stale_test_process_result;
}

static int stale_test_record_remove(const char *path) {
    if (stale_test_remove_count < STALE_TEST_MAX_REMOVES) {
        size_t len = strlen(path);
        if (len >= PATH_MAX) {
            len = PATH_MAX - 1;
        }
        memcpy(stale_test_remove_log[stale_test_remove_count], path, len);
        stale_test_remove_log[stale_test_remove_count][len] = '\0';
    }
    stale_test_remove_count += 1;
    stale_test_record_event('R');
    errno = stale_test_remove_errno;
    return stale_test_remove_result;
}

void stale_component_reset_test_seams(void) {
    stale_process_call = stale_process_call_default;
    stale_remove = remove;
    memset(stale_test_argv_log, 0, sizeof(stale_test_argv_log));
    memset(stale_test_argc_log, 0, sizeof(stale_test_argc_log));
    stale_test_process_count = 0;
    stale_test_process_result = GG_ERR_OK;
    memset(stale_test_remove_log, 0, sizeof(stale_test_remove_log));
    stale_test_remove_result = 0;
    stale_test_remove_errno = 0;
    stale_test_remove_count = 0;
    memset(stale_test_event_log, 0, sizeof(stale_test_event_log));
    stale_test_event_count = 0;
}

void stale_component_override_test_seam_for_reset_test(void) {
    stale_process_call = stale_test_record_process;
    stale_remove = stale_test_record_remove;
}

bool stale_component_test_seams_are_reset(void) {
    return (stale_process_call == stale_process_call_default)
        && (stale_remove == remove);
}

static void stale_assert_process_call(
    size_t index, size_t argc, const char *action, const char *target
) {
    TEST_ASSERT_EQUAL_size_t(argc, stale_test_argc_log[index]);
    TEST_ASSERT_EQUAL_STRING("systemctl", stale_test_argv_log[index][0]);
    TEST_ASSERT_EQUAL_STRING(action, stale_test_argv_log[index][1]);
    if (target != NULL) {
        TEST_ASSERT_EQUAL_STRING(target, stale_test_argv_log[index][2]);
    }
}

static void stale_assert_remove_path(
    size_t index, GgBuffer prefix, GgBuffer unit
) {
    uint8_t expected_mem[PATH_MAX];
    GgByteVec expected = GG_BYTE_VEC(expected_mem);
    GgError ret = gg_byte_vec_append(&expected, prefix);
    gg_byte_vec_chain_append(&ret, &expected, unit);
    gg_byte_vec_chain_push(&ret, &expected, '\0');
    TEST_ASSERT_EQUAL_INT(GG_ERR_OK, ret);
    TEST_ASSERT_EQUAL_STRING(
        (char *) expected.buf.data, stale_test_remove_log[index]
    );
}

// cspell:ignore SDRRLF
static void stale_assert_service_transcript(
    GgBuffer component_name, PhaseSelection phase, GgBuffer unit
) {
    stale_component_reset_test_seams();
    stale_process_call = stale_test_record_process;
    stale_remove = stale_test_record_remove;

    GgBuffer name = component_name;
    GG_TEST_ASSERT_OK(disable_and_unlink_service(&name, phase));

    TEST_ASSERT_EQUAL_size_t(4, stale_test_process_count);
    TEST_ASSERT_EQUAL_size_t(2, stale_test_remove_count);
    char unit_str[NAME_MAX];
    TEST_ASSERT_TRUE(unit.len < sizeof(unit_str));
    memcpy(unit_str, unit.data, unit.len);
    unit_str[unit.len] = '\0';
    stale_assert_process_call(0, 3, "stop", unit_str);
    stale_assert_process_call(1, 3, "disable", unit_str);
    stale_assert_remove_path(0, GG_STR("/etc/systemd/system/"), unit);
    stale_assert_remove_path(1, GG_STR("/usr/lib/systemd/system/"), unit);
    stale_assert_process_call(2, 2, "daemon-reload", NULL);
    stale_assert_process_call(3, 2, "reset-failed", NULL);
    TEST_ASSERT_EQUAL_STRING("SDRRLF", stale_test_event_log);
}

GG_TEST_DEFINE(stale_service_cleanup_uses_one_phase_aware_unit_name) {
    stale_assert_service_transcript(
        GG_STR("Test.Component"),
        RUN_STARTUP,
        GG_STR("ggl.Test.Component.service")
    );
    stale_assert_service_transcript(
        GG_STR("Test.Component"),
        INSTALL,
        GG_STR("ggl.Test.Component.install.service")
    );
    stale_assert_service_transcript(
        GG_STR("Test.Component"),
        BOOTSTRAP,
        GG_STR("ggl.Test.Component.bootstrap.service")
    );
}

GG_TEST_DEFINE(stale_service_cleanup_failures_remain_nonfatal_and_ordered) {
    stale_component_reset_test_seams();
    stale_process_call = stale_test_record_process;
    stale_remove = stale_test_record_remove;
    stale_test_process_result = GG_ERR_FAILURE;
    stale_test_remove_result = -1;
    stale_test_remove_errno = EACCES;

    GgBuffer name = GG_STR("Test.Component");
    GG_TEST_ASSERT_OK(disable_and_unlink_service(&name, RUN_STARTUP));
    TEST_ASSERT_EQUAL_size_t(4, stale_test_process_count);
    TEST_ASSERT_EQUAL_size_t(2, stale_test_remove_count);
    TEST_ASSERT_EQUAL_STRING("SDRRLF", stale_test_event_log);
}

GG_TEST_DEFINE(remove_result_classification_uses_errno_not_return_value) {
    // Success ignores errno entirely.
    TEST_ASSERT_TRUE(remove_result_is_ignorable(0, 0));
    TEST_ASSERT_TRUE(remove_result_is_ignorable(0, EACCES));
    // A missing file is acceptable.
    TEST_ASSERT_TRUE(remove_result_is_ignorable(-1, ENOENT));
    // Any other failure must be reported. The previous code compared the
    // remove() return value (-1) against these errno constants and never
    // matched, so genuine failures were silently ignored.
    TEST_ASSERT_FALSE(remove_result_is_ignorable(-1, EACCES));
    TEST_ASSERT_FALSE(remove_result_is_ignorable(-1, EPERM));
}

GG_TEST_DEFINE(delete_optional_file_success_does_not_depend_on_errno) {
    // A successful remove() must be treated as success without consulting
    // errno. Force remove() to succeed while poisoning errno with a value that
    // would warn if it were inspected on the success path, and confirm the
    // file is still removed exactly once with no failure handling.
    stale_component_reset_test_seams();
    stale_remove = stale_test_record_remove;
    stale_test_remove_result = 0;
    stale_test_remove_errno = EACCES;

    delete_optional_file("/does/not/matter");
    TEST_ASSERT_EQUAL_size_t(1, stale_test_remove_count);

    stale_component_reset_test_seams();
}

GG_TEST_DEFINE(invalid_component_name_has_no_cleanup_side_effects) {
    stale_component_reset_test_seams();
    stale_process_call = stale_test_record_process;
    stale_remove = stale_test_record_remove;

    GgBuffer invalid_name = GG_STR("Bad;Component");
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID, disable_and_unlink_service(&invalid_name, RUN_STARTUP)
    );
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_INVALID,
        delete_recipe_script_and_service_files_at_root(
            invalid_name, GG_STR("/var/lib/greengrass")
        )
    );
    TEST_ASSERT_EQUAL_size_t(0, stale_test_process_count);
    TEST_ASSERT_EQUAL_size_t(0, stale_test_remove_count);
}

static void stale_assert_service_files_attempted(int remove_errno) {
    stale_component_reset_test_seams();
    stale_remove = stale_test_record_remove;
    stale_test_remove_result = -1;
    stale_test_remove_errno = remove_errno;

    GG_TEST_ASSERT_OK(delete_recipe_script_and_service_files_at_root(
        GG_STR("Test.Component"), GG_STR("/var/lib/green grass;$HOME")
    ));
    TEST_ASSERT_EQUAL_size_t(3, stale_test_remove_count);
    TEST_ASSERT_EQUAL_STRING(
        "/var/lib/green grass;$HOME/ggl.Test.Component.bootstrap.service",
        stale_test_remove_log[0]
    );
    TEST_ASSERT_EQUAL_STRING(
        "/var/lib/green grass;$HOME/ggl.Test.Component.install.service",
        stale_test_remove_log[1]
    );
    TEST_ASSERT_EQUAL_STRING(
        "/var/lib/green grass;$HOME/ggl.Test.Component.service",
        stale_test_remove_log[2]
    );
}

GG_TEST_DEFINE(service_file_deletion_attempts_all_paths_for_failures) {
    stale_assert_service_files_attempted(EACCES);
    stale_assert_service_files_attempted(ENOENT);
}

static void stale_assert_recipe_paths_attempted(int remove_errno) {
    uint8_t root_mem[PATH_MAX];

    stale_component_reset_test_seams();
    stale_remove = stale_test_record_remove;
    stale_test_remove_result = -1;
    stale_test_remove_errno = remove_errno;

    // Poison spare capacity so each path requires the explicit NUL appended by
    // delete_component_recipe. Keep one final NUL to bound the test recorder
    // if that production terminator regresses.
    memset(root_mem, 0xA5, sizeof(root_mem));
    root_mem[sizeof(root_mem) - 1] = '\0';
    GgByteVec root_path = GG_BYTE_VEC(root_mem);
    GG_TEST_ASSERT_OK(
        gg_byte_vec_append(&root_path, GG_STR("/var/lib/greengrass"))
    );
    GG_TEST_ASSERT_OK(delete_component_recipe(
        GG_STR("Test.Component"), GG_STR("1.0.0"), &root_path
    ));

    TEST_ASSERT_EQUAL_size_t(3, stale_test_remove_count);
    TEST_ASSERT_EQUAL_STRING(
        "/var/lib/greengrass/packages/recipes/Test.Component-1.0.0.json",
        stale_test_remove_log[0]
    );
    TEST_ASSERT_EQUAL_STRING(
        "/var/lib/greengrass/packages/recipes/Test.Component-1.0.0.yaml",
        stale_test_remove_log[1]
    );
    TEST_ASSERT_EQUAL_STRING(
        "/var/lib/greengrass/packages/recipes/Test.Component-1.0.0.yml",
        stale_test_remove_log[2]
    );
}

GG_TEST_DEFINE(recipe_deletion_is_nonfatal_and_attempts_all_extensions) {
    // A non-ENOENT failure on every remove() stays nonfatal and still attempts
    // all three exact recipe extension paths.
    stale_assert_recipe_paths_attempted(EACCES);

    // ENOENT is suppressed and equally nonfatal.
    stale_assert_recipe_paths_attempted(ENOENT);
}

#endif
