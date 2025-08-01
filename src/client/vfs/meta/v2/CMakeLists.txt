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


file(GLOB VFS_META_SRC_V2_LIB
    "*.cpp"
)

add_library(vfs_meta_v2_lib ${VFS_META_SRC_V2_LIB})

target_link_libraries(vfs_meta_v2_lib
    PROTO_OBJS

    mdsv2_mds

    dingofs_common
    dynamic_vlog
    dingofs_utils
    client_options

    stub_common
    gflags::gflags
    glog::glog
    fmt::fmt
    spdlog::spdlog
    brpc::brpc
    absl::memory
    absl::strings
    absl::synchronization
    absl::str_format
    absl::type_traits
    absl::optional
    absl::btree
    ${FUSE3_LIBRARY}
)
