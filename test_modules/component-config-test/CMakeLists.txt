# aws-greengrass-lite - AWS IoT Greengrass runtime for constrained devices
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# Unit test for apply_component_to_configuration() in component_config.c.
# Compiles component_config.c directly so no live IPC is needed.
ggl_init_module(
  component-config-test LIBS gg-sdk ggl-common ggl-json ggl-policy-validation
                             core-bus core-bus-gg-config)

# The bin/component-config-test.c needs the component configuration and access
# control validation sources and their headers. Add them to the generated test
# binary target.
target_sources(
  component-config-test-bin
  PRIVATE
    ${CMAKE_SOURCE_DIR}/modules/ggdeploymentd/src/component_config.c
    ${CMAKE_SOURCE_DIR}/modules/ggdeploymentd/src/access_control_validation.c)
target_include_directories(
  component-config-test-bin
  PRIVATE ${CMAKE_SOURCE_DIR}/modules/ggdeploymentd/src)
