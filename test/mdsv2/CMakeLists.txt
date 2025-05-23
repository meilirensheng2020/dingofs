# Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(common)
add_subdirectory(filesystem)
add_subdirectory(service)


add_executable(test_mdsv2
  main.cc
)

target_link_libraries(test_mdsv2
  PROTO_OBJS

  $<TARGET_OBJECTS:test_mdsv2_common>
  $<TARGET_OBJECTS:test_mdsv2_filesystem>
  $<TARGET_OBJECTS:test_mdsv2_service>

  mdsv2_common
  mdsv2_filesystem
  mdsv2_storage
  mdsv2_coordinator
  mdsv2_mds
  
  dingofs_utils

  absl::log_internal_message
  absl::cleanup
  absl::btree
  absl::memory
  absl::utility
  absl::strings

  dingo::sdk

  protobuf::libprotobuf
  ${TEST_DEPS_WITHOUT_MAIN}
)

set_target_properties(test_mdsv2 PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH}
)
