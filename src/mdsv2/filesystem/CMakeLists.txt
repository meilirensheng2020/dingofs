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


file(GLOB DINGOFS_MDSV2_FILESYSTEM_SRCS
    "*.cc"
)

add_library(mdsv2_filesystem
    ${DINGOFS_MDSV2_FILESYSTEM_SRCS}
)

target_link_libraries(mdsv2_filesystem
    mdsv2_common
    mdsv2_service
    mdsv2_background
    mdsv2_quota

    PROTO_OBJS
    protobuf::libprotobuf
    nlohmann_json
)