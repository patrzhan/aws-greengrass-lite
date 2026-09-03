// aws-greengrass-lite - AWS IoT Greengrass runtime for constrained devices
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#include "component_store.h"
#include "config_access.h"
#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <gg/buffer.h>
#include <gg/cleanup.h>
#include <gg/error.h>
#include <gg/file.h>
#include <gg/log.h>
#include <gg/vector.h>
#include <ggl/semver.h>
#include <limits.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>

#define MAX_PATH_LENGTH 128

static GgBuffer root_path = GG_STR("/var/lib/greengrass");
static uint8_t root_path_mem[MAX_PATH_LENGTH];
static GgByteVec root_path_destination = {
    .buf = { .data = root_path_mem, .len = 0 },
    .capacity = sizeof(root_path_mem),
};

static GgError update_root_path(void) {
    static uint8_t response_scratch[MAX_PATH_LENGTH];
    GgError ret = ggl_deployment_config_read_string(
        GG_BUF_LIST(GG_STR("system"), GG_STR("rootPath")),
        GG_BUF(response_scratch),
        &root_path_destination
    );

    if (ret != GG_ERR_OK) {
        GG_LOGW("Failed to get root path from config.");
        if ((ret == GG_ERR_NOMEM) || (ret == GG_ERR_FATAL)) {
            return ret;
        }
        return GG_ERR_OK;
    }

    root_path = root_path_destination.buf;
    return GG_ERR_OK;
}

GgError get_recipe_dir_fd(int *recipe_fd) {
    GgError ret = update_root_path();
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to retrieve root path.");
        return GG_ERR_FAILURE;
    }

    int root_path_fd;
    ret = gg_dir_open(root_path, O_PATH, false, &root_path_fd);
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to open root_path.");
        return GG_ERR_FAILURE;
    }
    GG_CLEANUP(cleanup_close, root_path_fd);

    int recipe_dir_fd;
    ret = gg_dir_openat(
        root_path_fd,
        GG_STR("packages/recipes"),
        O_RDONLY,
        false,
        &recipe_dir_fd
    );
    if (ret != GG_ERR_OK) {
        GG_LOGE("Failed to open recipe subdirectory.");
        return GG_ERR_FAILURE;
    }
    *recipe_fd = recipe_dir_fd;
    return GG_ERR_OK;
}

/// Parses a recipe file name of the form
/// "<component_name>-<version>.<extension>" into borrowed slices of @p
/// filename. On success writes the component name to @p component_name and the
/// version to @p version and returns true; on any malformed input the outputs
/// are left untouched and false is returned.
///
/// A valid file name requires a nonempty component name, a nonempty extension
/// (the bytes after the final '.'), and a nonempty version accepted by
/// is_valid_semver(). Because both the component name and a semantic-version
/// prerelease may contain '-', candidate separators in the stem are tested
/// from right to left and the first (rightmost) split whose suffix is a valid
/// semantic version wins. This keeps the previous last-'-' interpretation when
/// more than one split could parse, while no longer folding a prerelease
/// suffix into the component name.
static bool parse_recipe_filename(
    GgBuffer filename, GgBuffer *component_name, GgBuffer *version
) {
    // Locate the final '.'; the extension after it must be nonempty.
    size_t extension_dot = filename.len;
    for (size_t i = filename.len; i > 0; i--) {
        if (filename.data[i - 1] == '.') {
            extension_dot = i - 1;
            break;
        }
    }
    if ((extension_dot == filename.len)
        || (extension_dot + 1 == filename.len)) {
        // No '.' at all, or nothing follows the final '.': no extension.
        return false;
    }

    // The stem "<component_name>-<version>" is everything before the final '.'.
    GgBuffer stem = gg_buffer_substr(filename, 0, extension_dot);

    // Test candidate '-' separators from right to left. The first split whose
    // suffix is a valid semantic version and whose name is nonempty wins.
    for (size_t i = stem.len; i > 0; i--) {
        if (stem.data[i - 1] != '-') {
            continue;
        }
        GgBuffer name_candidate = gg_buffer_substr(stem, 0, i - 1);
        GgBuffer version_candidate = gg_buffer_substr(stem, i, SIZE_MAX);
        if ((name_candidate.len == 0) || !is_valid_semver(version_candidate)) {
            continue;
        }
        *component_name = name_candidate;
        *version = version_candidate;
        return true;
    }
    return false;
}

#ifdef GG_SDK_TESTING

static struct dirent *(*component_store_readdir)(DIR *) = readdir;

#else

// NOLINTNEXTLINE(readability-identifier-naming)
#define component_store_readdir readdir

#endif

GgError iterate_over_components(
    DIR *dir,
    GgBuffer *component_name_buffer,
    GgBuffer *version,
    struct dirent **entry
) {
    GG_LOGT("Iterating over component recipes in directory");
    // recipe file names follow this format:
    // <component_name>-<version>.<extension>
    while (true) {
        errno = 0;
        // NOLINTNEXTLINE(concurrency-mt-unsafe)
        struct dirent *next_entry = component_store_readdir(dir);
        if (next_entry == NULL) {
            if (errno != 0) {
                int readdir_errno = errno;
                GG_LOGE(
                    "Failed to read recipe directory (errno=%d).", readdir_errno
                );
                return GG_ERR_FAILURE;
            }
            return GG_ERR_NOENTRY;
        }

        GgBuffer entry_buf = gg_buffer_from_null_term(next_entry->d_name);
        GG_LOGT(
            "Found directory entry %.*s", (int) entry_buf.len, entry_buf.data
        );

        GgBuffer recipe_component;
        GgBuffer recipe_version;
        if (!parse_recipe_filename(
                entry_buf, &recipe_component, &recipe_version
            )) {
            // Malformed entries are skipped without touching the outputs.
            GG_LOGD(
                "Recipe file name formatted incorrectly. Continuing to next file."
            );
            continue;
        }
        GG_LOGT(
            "Parsed component: %.*s version: %.*s",
            (int) recipe_component.len,
            recipe_component.data,
            (int) recipe_version.len,
            recipe_version.data
        );

        assert(recipe_component.len < NAME_MAX);
        assert(recipe_version.len < NAME_MAX);
        // Copy out component name and version.
        memcpy(
            component_name_buffer->data,
            recipe_component.data,
            recipe_component.len
        );
        component_name_buffer->len = recipe_component.len;

        memcpy(version->data, recipe_version.data, recipe_version.len);
        version->len = recipe_version.len;
        *entry = next_entry;

        // Found one component. Break out of loop and return.
        return GG_ERR_OK;
    }
}

/// Orders two recipe versions using strverscmp, the same primitive that
/// is_in_range() (via process_version) uses to evaluate version requirements,
/// so selecting the highest satisfying version stays consistent with how the
/// requirement was matched. This ordering may differ from SemVer precedence.
/// Returns <0 if @p a precedes @p b, 0 if equal, and >0 if @p a follows @p b.
/// Both versions must be shorter than NAME_MAX.
static int compare_versions(GgBuffer a, GgBuffer b) {
    assert(a.len < NAME_MAX);
    assert(b.len < NAME_MAX);
    uint8_t a_str[NAME_MAX];
    uint8_t b_str[NAME_MAX];
    memcpy(a_str, a.data, a.len);
    a_str[a.len] = '\0';
    memcpy(b_str, b.data, b.len);
    b_str[b.len] = '\0';
    return strverscmp((char *) a_str, (char *) b_str);
}

/// Tracks the highest satisfying version seen so far. Replaces the current best
/// (stored in @p best_storage, described by @p best) with @p candidate when
/// there is no best yet or when @p candidate orders strictly above the current
/// best. @p best_storage must have room for NAME_MAX bytes. Kept independent of
/// the filesystem so highest-version selection can be tested independent of
/// directory enumeration order.
static void consider_candidate_version(
    GgBuffer candidate, uint8_t *best_storage, GgBuffer *best, bool *have_best
) {
    if (*have_best && (compare_versions(candidate, *best) <= 0)) {
        return;
    }
    assert(candidate.len < NAME_MAX);
    memcpy(best_storage, candidate.data, candidate.len);
    best->data = best_storage;
    best->len = candidate.len;
    *have_best = true;
}

GgError find_available_component(
    GgBuffer component_name, GgBuffer requirement, GgBuffer *version
) {
    GG_LOGT(
        "Searching for component %.*s",
        (int) component_name.len,
        component_name.data
    );
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
    GgBuffer component_name_buffer = { .data = component_name_array, .len = 0 };

    uint8_t version_array[NAME_MAX];
    GgBuffer version_buffer = { .data = version_array, .len = 0 };

    // Track the highest satisfying version in caller-independent storage so the
    // whole directory is scanned before a result is published. The output
    // buffer is only written once, after exhaustion.
    uint8_t best_version_array[NAME_MAX];
    GgBuffer best_version = { .data = best_version_array, .len = 0 };
    bool have_best = false;

    while (true) {
        ret = iterate_over_components(
            dir, &component_name_buffer, &version_buffer, &entry
        );
        if (ret == GG_ERR_NOENTRY) {
            // Scanned every entry in the directory.
            break;
        }
        if (ret != GG_ERR_OK) {
            return ret;
        }
        assert(entry != NULL);

        if (gg_buffer_eq(component_name, component_name_buffer)
            && is_in_range(version_buffer, requirement)) {
            consider_candidate_version(
                version_buffer, best_version_array, &best_version, &have_best
            );
        }
    }

    if (!have_best) {
        // component meeting version requirements not found
        return GG_ERR_NOENTRY;
    }

    assert(best_version.len < NAME_MAX);
    memcpy(version->data, best_version.data, best_version.len);
    version->len = best_version.len;
    return GG_ERR_OK;
}

#ifdef GG_SDK_TESTING

#include "stale_component.h"
#include <gg/test.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unity.h>
#include <stdlib.h>

static GgError component_store_config_error;
static GgObject component_store_config_value;
static char active_temp_recipe_store_root[PATH_MAX];
static bool temp_recipe_store_active;

static void remove_temp_recipe_store(void);

// Defined in stale_component.c under GG_SDK_TESTING. Keep these declarations
// private to the inline-test binary rather than production headers.
void stale_component_reset_test_seams(void);

typedef struct {
    GgBuffer root_path;
    GgByteVec root_path_destination;
    uint8_t root_path_mem[MAX_PATH_LENGTH];
    struct dirent *(*component_store_readdir)(DIR *);
    GgError config_error;
    GgObject config_value;
} ComponentStoreTestState;

static ComponentStoreTestState save_component_store_test_state(void) {
    ComponentStoreTestState state = {
        .root_path = root_path,
        .root_path_destination = root_path_destination,
        .component_store_readdir = component_store_readdir,
        .config_error = component_store_config_error,
        .config_value = component_store_config_value,
    };
    memcpy(state.root_path_mem, root_path_mem, sizeof(root_path_mem));
    return state;
}

static void restore_component_store_test_state(
    const ComponentStoreTestState *state
) {
    ggl_deployment_config_set_reader_for_test(NULL);
    component_store_readdir = state->component_store_readdir;
    root_path = state->root_path;
    root_path_destination = state->root_path_destination;
    memcpy(root_path_mem, state->root_path_mem, sizeof(root_path_mem));
    component_store_config_error = state->config_error;
    component_store_config_value = state->config_value;
}

static void reset_component_store_test_state(void) {
    ggl_deployment_config_set_reader_for_test(NULL);
    component_store_readdir = readdir;
    root_path = GG_STR("/var/lib/greengrass");
    memset(root_path_mem, 0, sizeof(root_path_mem));
    root_path_destination = (GgByteVec) {
        .buf = { .data = root_path_mem, .len = 0 },
        .capacity = sizeof(root_path_mem),
    };
    component_store_config_error = GG_ERR_OK;
    component_store_config_value = (GgObject) { 0 };
}

// This is the single binary-wide Unity teardown. Unity invokes it even after an
// assertion aborts via longjmp. Clean up any active temp store before resetting
// every mutable test seam that can otherwise leak into the next test.
void tearDown(void) {
    remove_temp_recipe_store();
    reset_component_store_test_state();
    stale_component_reset_test_seams();
}

static GgError component_store_fake_config_reader(
    GgBufList key_path, GgArena *alloc, GgObject *result
) {
    (void) key_path;
    (void) alloc;
    if (component_store_config_error != GG_ERR_OK) {
        return component_store_config_error;
    }
    *result = component_store_config_value;
    return GG_ERR_OK;
}

GG_TEST_DEFINE(root_path_failed_reread_preserves_cached_bytes) {
    ComponentStoreTestState saved_state = save_component_store_test_state();
    root_path = GG_STR("/var/lib/greengrass");
    root_path_destination.buf.len = 0;
    memset(root_path_mem, 0, sizeof(root_path_mem));
    component_store_config_error = GG_ERR_OK;
    component_store_config_value = gg_obj_buf(GG_STR("/custom/root"));
    ggl_deployment_config_set_reader_for_test(component_store_fake_config_reader
    );

    GG_TEST_ASSERT_OK(update_root_path());
    TEST_ASSERT_TRUE(gg_buffer_eq(root_path, GG_STR("/custom/root")));
    TEST_ASSERT_EQUAL_UINT8('\0', root_path_mem[root_path.len]);

    component_store_config_value = gg_obj_i64(1);
    GG_TEST_ASSERT_OK(update_root_path());
    TEST_ASSERT_TRUE(gg_buffer_eq(root_path, GG_STR("/custom/root")));
    TEST_ASSERT_EQUAL_MEMORY("/custom/root", root_path_mem, root_path.len);

    component_store_config_error = GG_ERR_NOCONN;
    GG_TEST_ASSERT_OK(update_root_path());
    TEST_ASSERT_TRUE(gg_buffer_eq(root_path, GG_STR("/custom/root")));

    component_store_config_error = GG_ERR_NOMEM;
    TEST_ASSERT_EQUAL_INT(GG_ERR_NOMEM, update_root_path());
    TEST_ASSERT_TRUE(gg_buffer_eq(root_path, GG_STR("/custom/root")));

    restore_component_store_test_state(&saved_state);
}

// Wraps gg_buffer_from_null_term for read-only const test strings.
static GgBuffer test_buf_from_str(const char *str) {
    return gg_buffer_from_null_term((char *) str);
}

// Appends "/" + segment to a path byte-vector under test.
static void test_path_append(GgByteVec *path, GgBuffer segment) {
    GgError ret = gg_byte_vec_append(path, GG_STR("/"));
    gg_byte_vec_chain_append(&ret, path, segment);
    TEST_ASSERT_EQUAL_INT(GG_ERR_OK, ret);
}

// Creates a fresh temporary directory containing a packages/recipes
// subdirectory, writes each name in `names` as an empty recipe file there, and
// wires the fake deployment-config reader so get_recipe_dir_fd resolves the
// recipe directory to it. `root_template` is a writable mkdtemp template that
// receives the created path.
static void create_temp_recipe_store(
    char *root_template, const char *const *names, size_t count
) {
    char *root = mkdtemp(root_template);
    TEST_ASSERT_NOT_NULL(root);

    size_t root_len = strlen(root);
    assert(root_len < sizeof(active_temp_recipe_store_root));
    memcpy(active_temp_recipe_store_root, root, root_len + 1);
    temp_recipe_store_active = true;

    GgBuffer root_buf = gg_buffer_from_null_term(root);

    uint8_t path_mem[PATH_MAX];
    GgByteVec path = GG_BYTE_VEC(path_mem);
    GgError ret = gg_byte_vec_append(&path, root_buf);
    gg_byte_vec_chain_append(&ret, &path, GG_STR("/packages"));
    gg_byte_vec_chain_push(&ret, &path, '\0');
    TEST_ASSERT_EQUAL_INT(GG_ERR_OK, ret);
    TEST_ASSERT_EQUAL_INT(0, mkdir((char *) path.buf.data, 0700));

    path.buf.len = 0;
    ret = gg_byte_vec_append(&path, root_buf);
    gg_byte_vec_chain_append(&ret, &path, GG_STR("/packages/recipes"));
    gg_byte_vec_chain_push(&ret, &path, '\0');
    TEST_ASSERT_EQUAL_INT(GG_ERR_OK, ret);
    TEST_ASSERT_EQUAL_INT(0, mkdir((char *) path.buf.data, 0700));

    for (size_t i = 0; i < count; i++) {
        path.buf.len = 0;
        ret = gg_byte_vec_append(&path, root_buf);
        gg_byte_vec_chain_append(&ret, &path, GG_STR("/packages/recipes"));
        TEST_ASSERT_EQUAL_INT(GG_ERR_OK, ret);
        test_path_append(&path, test_buf_from_str(names[i]));
        ret = gg_byte_vec_push(&path, '\0');
        TEST_ASSERT_EQUAL_INT(GG_ERR_OK, ret);

        int fd
            = open((char *) path.buf.data, O_CREAT | O_WRONLY | O_TRUNC, 0600);
        TEST_ASSERT_TRUE(fd >= 0);
        TEST_ASSERT_EQUAL_INT(0, close(fd));
    }

    component_store_config_error = GG_ERR_OK;
    component_store_config_value = gg_obj_buf(root_buf);
    ggl_deployment_config_set_reader_for_test(component_store_fake_config_reader
    );
}

// Best-effort removal of the active temp recipe store. Recipe entries are
// enumerated from the copied root so cleanup remains valid after a test
// assertion unwinds caller-owned root and name storage.
static void remove_temp_recipe_store(void) {
    if (!temp_recipe_store_active) {
        return;
    }

    GgBuffer root_buf = gg_buffer_from_null_term(active_temp_recipe_store_root);
    uint8_t path_mem[PATH_MAX];
    GgByteVec path = GG_BYTE_VEC(path_mem);

    GgError ret = gg_byte_vec_append(&path, root_buf);
    gg_byte_vec_chain_append(&ret, &path, GG_STR("/packages/recipes"));
    gg_byte_vec_chain_push(&ret, &path, '\0');
    if (ret == GG_ERR_OK) {
        DIR *recipe_dir = opendir((char *) path.buf.data);
        if (recipe_dir != NULL) {
            // Use the real reader so an injected component_store_readdir error
            // cannot prevent teardown cleanup.
            // NOLINTNEXTLINE(concurrency-mt-unsafe)
            struct dirent *entry = readdir(recipe_dir);
            while (entry != NULL) {
                if ((strcmp(entry->d_name, ".") != 0)
                    && (strcmp(entry->d_name, "..") != 0)) {
                    (void) unlinkat(dirfd(recipe_dir), entry->d_name, 0);
                }
                // NOLINTNEXTLINE(concurrency-mt-unsafe)
                entry = readdir(recipe_dir);
            }
            (void) closedir(recipe_dir);
        }
    }

    const char *const subdirs[] = { "/packages/recipes", "/packages", "" };
    for (size_t i = 0; i < (sizeof(subdirs) / sizeof(subdirs[0])); i++) {
        path.buf.len = 0;
        ret = gg_byte_vec_append(&path, root_buf);
        gg_byte_vec_chain_append(&ret, &path, test_buf_from_str(subdirs[i]));
        gg_byte_vec_chain_push(&ret, &path, '\0');
        if (ret == GG_ERR_OK) {
            (void) rmdir((char *) path.buf.data);
        }
    }

    active_temp_recipe_store_root[0] = '\0';
    temp_recipe_store_active = false;
}

// Resolves a component version from a temporary recipe store populated with
// `names`, returning the find_available_component result and the resolved
// version bytes (via `version_out`, which must be NAME_MAX bytes).
static GgError resolve_from_temp_store(
    const char *const *names,
    size_t count,
    GgBuffer component_name,
    GgBuffer requirement,
    GgBuffer *version_out
) {
    ComponentStoreTestState saved_state = save_component_store_test_state();
    char root_template[] = "/tmp/ggl_recipe_store_XXXXXX";
    create_temp_recipe_store(root_template, names, count);
    GgError ret
        = find_available_component(component_name, requirement, version_out);
    remove_temp_recipe_store();
    restore_component_store_test_state(&saved_state);
    return ret;
}

static bool readdir_candidate_returned;

static struct dirent *readdir_error_after_candidate(DIR *dir) {
    if (readdir_candidate_returned) {
        errno = EIO;
        return NULL;
    }

    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    struct dirent *entry = readdir(dir);
    if ((entry != NULL) && (strcmp(entry->d_name, "comp-1.0.0.json") == 0)) {
        readdir_candidate_returned = true;
    }
    return entry;
}

static struct dirent *readdir_error_immediately(DIR *dir) {
    (void) dir;
    errno = EIO;
    return NULL;
}

// --- Characterization tests: behavior preserved across the M7 refactor. ---

GG_TEST_DEFINE(char_valid_ordinary_parsing) {
    const char *const names[] = { "comp-1.0.0.json" };
    uint8_t out[NAME_MAX];
    GgBuffer version = GG_BUF(out);
    GG_TEST_ASSERT_OK(resolve_from_temp_store(
        names, 1, GG_STR("comp"), GG_STR(">=1.0.0"), &version
    ));
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("1.0.0"), version);
}

GG_TEST_DEFINE(char_dotted_component_name) {
    const char *const names[] = { "com.example.thing-1.2.3.json" };
    uint8_t out[NAME_MAX];
    GgBuffer version = GG_BUF(out);
    GG_TEST_ASSERT_OK(resolve_from_temp_store(
        names, 1, GG_STR("com.example.thing"), GG_STR(">=1.0.0"), &version
    ));
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("1.2.3"), version);
}

GG_TEST_DEFINE(char_hyphenated_component_name) {
    const char *const names[] = { "my-cool-comp-2.0.0.json" };
    uint8_t out[NAME_MAX];
    GgBuffer version = GG_BUF(out);
    GG_TEST_ASSERT_OK(resolve_from_temp_store(
        names, 1, GG_STR("my-cool-comp"), GG_STR(">=1.0.0"), &version
    ));
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("2.0.0"), version);
}

GG_TEST_DEFINE(char_last_separator_behavior) {
    // The rightmost '-' that yields a valid version wins, so "a-b" is the
    // component name and "1.5.0" is the version.
    const char *const names[] = { "a-b-1.5.0.json" };
    uint8_t out[NAME_MAX];
    GgBuffer version = GG_BUF(out);
    GG_TEST_ASSERT_OK(resolve_from_temp_store(
        names, 1, GG_STR("a-b"), GG_STR("=1.5.0"), &version
    ));
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("1.5.0"), version);
}

GG_TEST_DEFINE(char_directory_exhaustion_returns_noentry) {
    uint8_t out[NAME_MAX];
    GgBuffer version = GG_BUF(out);
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOENTRY,
        resolve_from_temp_store(
            NULL, 0, GG_STR("comp"), GG_STR(">=0.0.0"), &version
        )
    );
}

GG_TEST_DEFINE(char_name_mismatch_returns_noentry) {
    const char *const names[] = { "comp-1.0.0.json" };
    uint8_t out[NAME_MAX];
    GgBuffer version = GG_BUF(out);
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOENTRY,
        resolve_from_temp_store(
            names, 1, GG_STR("other"), GG_STR(">=1.0.0"), &version
        )
    );
}

GG_TEST_DEFINE(char_unsatisfied_requirement_returns_noentry) {
    const char *const names[] = { "comp-1.0.0.json" };
    uint8_t out[NAME_MAX];
    GgBuffer version = GG_BUF(out);
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOENTRY,
        resolve_from_temp_store(
            names, 1, GG_STR("comp"), GG_STR(">=2.0.0"), &version
        )
    );
}

// --- M7 coverage: parser edge cases and highest-version selection. ---

// F-COMP-01: malformed recipe file names are rejected and the caller's output
// slices are left untouched.
GG_TEST_DEFINE(parse_rejects_malformed_filenames) {
    static const char *const malformed[] = {
        "", // empty
        "noseparator.json", // no '-' separator
        "-1.0.0.json", // empty component name
        "comp-.json", // empty version
        "comp-notsemver.json", // version not a valid semver
        "comp-1.2.json", // version missing patch component
        "comp-1.0.0", // separator and valid semver bytes, but no extension
        "comp-1.0.0.", // empty file extension
    };

    for (size_t i = 0; i < (sizeof(malformed) / sizeof(malformed[0])); i++) {
        GgBuffer name = GG_STR("SENTINEL_NAME");
        GgBuffer version = GG_STR("SENTINEL_VERSION");
        TEST_ASSERT_FALSE(parse_recipe_filename(
            test_buf_from_str(malformed[i]), &name, &version
        ));
        // Outputs must be untouched on rejection.
        GG_TEST_ASSERT_BUF_EQUAL(GG_STR("SENTINEL_NAME"), name);
        GG_TEST_ASSERT_BUF_EQUAL(GG_STR("SENTINEL_VERSION"), version);
    }
}

// F-COMP-01: a semantic-version prerelease suffix stays with the version and is
// not folded into the component name, even when the name itself contains '-'.
GG_TEST_DEFINE(parse_keeps_prerelease_with_version) {
    GgBuffer name = { 0 };
    GgBuffer version = { 0 };

    TEST_ASSERT_TRUE(parse_recipe_filename(
        test_buf_from_str("comp-1.0.0-alpha.json"), &name, &version
    ));
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("comp"), name);
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("1.0.0-alpha"), version);

    TEST_ASSERT_TRUE(parse_recipe_filename(
        test_buf_from_str("my-app-1.2.3-rc.1.json"), &name, &version
    ));
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("my-app"), name);
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("1.2.3-rc.1"), version);
}

GG_TEST_DEFINE(parse_uses_rightmost_valid_semver_split) {
    GgBuffer name = { 0 };
    GgBuffer version = { 0 };

    TEST_ASSERT_TRUE(parse_recipe_filename(
        test_buf_from_str("comp-1.0.0-alpha-2.0.0.json"), &name, &version
    ));
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("comp-1.0.0-alpha"), name);
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("2.0.0"), version);
}

// F-COMP-06: when no satisfying component is found the version output is left
// completely untouched (find_available_component writes the output only once,
// after the whole directory has been scanned).
GG_TEST_DEFINE(find_leaves_version_untouched_on_failure) {
    uint8_t out[NAME_MAX];
    memcpy(out, "9.9.9", 5);
    GgBuffer version = { .data = out, .len = 5 };

    const char *const names[] = { "comp-1.0.0.json" };
    TEST_ASSERT_EQUAL_INT(
        GG_ERR_NOENTRY,
        resolve_from_temp_store(
            names, 1, GG_STR("comp"), GG_STR(">=2.0.0"), &version
        )
    );

    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("9.9.9"), version);
}

GG_TEST_DEFINE(find_readdir_error_does_not_publish_partial_scan) {
    ComponentStoreTestState saved_state = save_component_store_test_state();
    uint8_t out[NAME_MAX];
    memcpy(out, "9.9.9", 5);
    GgBuffer version = { .data = out, .len = 5 };
    const char *const names[] = { "comp-1.0.0.json" };

    readdir_candidate_returned = false;
    component_store_readdir = readdir_error_after_candidate;
    GgError ret = resolve_from_temp_store(
        names, 1, GG_STR("comp"), GG_STR(">=1.0.0"), &version
    );
    restore_component_store_test_state(&saved_state);

    TEST_ASSERT_EQUAL_INT(GG_ERR_FAILURE, ret);
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("9.9.9"), version);
}

GG_TEST_DEFINE(cleanup_stale_versions_propagates_readdir_failure) {
    ComponentStoreTestState saved_state = save_component_store_test_state();
    char root_template[] = "/tmp/ggl_recipe_store_XXXXXX";
    create_temp_recipe_store(root_template, NULL, 0);

    component_store_readdir = readdir_error_immediately;
    GgError ret = cleanup_stale_versions((GgMap) { 0 }, NULL, NULL);
    restore_component_store_test_state(&saved_state);

    remove_temp_recipe_store();
    TEST_ASSERT_EQUAL_INT(GG_ERR_FAILURE, ret);
}

// F-COMP-06: among a directory mixing satisfying, out-of-range, name-mismatch,
// and malformed entries, the highest satisfying version is selected.
GG_TEST_DEFINE(resolves_highest_among_mixed_entries) {
    const char *const names[] = {
        "comp-1.0.0.json", // satisfies
        "comp-1.5.0.json", // satisfies, higher
        "comp-2.0.0.json", // excluded by upper bound
        "comp-0.1.0.json", // below lower bound
        "other-9.9.9.json", // name mismatch
        "not-a-recipe.txt", // malformed: no valid version split
        "comp-bad.json", // malformed: version not a semver
    };
    uint8_t out[NAME_MAX];
    GgBuffer version = GG_BUF(out);
    GG_TEST_ASSERT_OK(resolve_from_temp_store(
        names,
        sizeof(names) / sizeof(names[0]),
        GG_STR("comp"),
        GG_STR(">=1.0.0 <2.0.0"),
        &version
    ));
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("1.5.0"), version);
}

// F-COMP-06: highest-version selection is independent of the order in which
// candidates are considered (directory enumeration order is unspecified).
GG_TEST_DEFINE(selection_is_order_independent) {
    const char *const ascending[] = { "1.1.0", "1.2.0", "1.10.0" };
    const char *const descending[] = { "1.10.0", "1.2.0", "1.1.0" };

    uint8_t asc_mem[NAME_MAX];
    GgBuffer asc_best = { .data = asc_mem, .len = 0 };
    bool asc_have = false;
    for (size_t i = 0; i < (sizeof(ascending) / sizeof(ascending[0])); i++) {
        consider_candidate_version(
            test_buf_from_str(ascending[i]), asc_mem, &asc_best, &asc_have
        );
    }

    uint8_t desc_mem[NAME_MAX];
    GgBuffer desc_best = { .data = desc_mem, .len = 0 };
    bool desc_have = false;
    for (size_t i = 0; i < (sizeof(descending) / sizeof(descending[0])); i++) {
        consider_candidate_version(
            test_buf_from_str(descending[i]), desc_mem, &desc_best, &desc_have
        );
    }

    TEST_ASSERT_TRUE(asc_have);
    TEST_ASSERT_TRUE(desc_have);
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("1.10.0"), asc_best);
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("1.10.0"), desc_best);
}

// F-COMP-06 integration: a temp store with several satisfying versions resolves
// to the highest one end-to-end, exercising numeric (not lexical) ordering
// (1.10.0 > 1.2.0) with the top version excluded by the requirement's upper
// bound.
GG_TEST_DEFINE(resolves_highest_multi_version_store) {
    const char *const names[] = {
        "comp-1.0.0.json",
        "comp-1.10.0.json",
        "comp-1.2.0.json",
        "comp-2.0.0.json",
    };
    uint8_t out[NAME_MAX];
    GgBuffer version = GG_BUF(out);
    GG_TEST_ASSERT_OK(resolve_from_temp_store(
        names,
        sizeof(names) / sizeof(names[0]),
        GG_STR("comp"),
        GG_STR(">=1.0.0 <2.0.0"),
        &version
    ));
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("1.10.0"), version);
}

GG_TEST_DEFINE(temp_store_selection_is_creation_order_independent) {
    const char *const ascending[] = {
        "not-a-recipe",    "comp-bad.json",    "comp-1.1.0.json",
        "comp-1.2.0.json", "comp-1.10.0.json",
    };
    const char *const descending[] = {
        "not-a-recipe",    "comp-bad.json",   "comp-1.10.0.json",
        "comp-1.2.0.json", "comp-1.1.0.json",
    };
    uint8_t ascending_out[NAME_MAX];
    GgBuffer ascending_version = GG_BUF(ascending_out);
    uint8_t descending_out[NAME_MAX];
    GgBuffer descending_version = GG_BUF(descending_out);

    GG_TEST_ASSERT_OK(resolve_from_temp_store(
        ascending,
        sizeof(ascending) / sizeof(ascending[0]),
        GG_STR("comp"),
        GG_STR(">=1.0.0 <2.0.0"),
        &ascending_version
    ));
    GG_TEST_ASSERT_OK(resolve_from_temp_store(
        descending,
        sizeof(descending) / sizeof(descending[0]),
        GG_STR("comp"),
        GG_STR(">=1.0.0 <2.0.0"),
        &descending_version
    ));

    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("1.10.0"), ascending_version);
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("1.10.0"), descending_version);
}

GG_TEST_DEFINE(selection_characterizes_release_and_prerelease_ordering) {
    const char *const names[] = {
        "comp-1.0.0.json",
        "comp-1.0.0-alpha.json",
    };
    uint8_t out[NAME_MAX];
    GgBuffer version = GG_BUF(out);

    GG_TEST_ASSERT_OK(resolve_from_temp_store(
        names,
        sizeof(names) / sizeof(names[0]),
        GG_STR("comp"),
        GG_STR(">=0.0.0"),
        &version
    ));
    GG_TEST_ASSERT_BUF_EQUAL(GG_STR("1.0.0-alpha"), version);
}

#endif
