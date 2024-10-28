/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2024-10-31
 * Author: Jingli Chen (Wine93)
 */

#include <gflags/gflags.h>
#include <glog/logging.h>

namespace curvefs {
namespace metaserver {
namespace common {

namespace {
bool PassBool(const char*, bool) { return true; }
};  // namespace

DEFINE_bool(superpartition_access_logging, true,
            "enable superpartition access logging");

DEFINE_validator(superpartition_access_logging, &PassBool);

}  // namespace common
}  // namespace metaserver
}  // namespace curvefs