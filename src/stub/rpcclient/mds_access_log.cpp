/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#include "stub/rpcclient/mds_access_log.h"

#include <absl/strings/str_format.h>
#include <gflags/gflags.h>

namespace dingofs {
namespace stub {

std::shared_ptr<spdlog::logger> mds_access_logger;

bool InitMdsAccessLog(const std::string& prefix) {
  std::string filename =
      absl::StrFormat("%s/mds_access_%d.log", prefix, getpid());
  mds_access_logger = spdlog::daily_logger_mt("mds_access", filename, 0, 0);
  spdlog::flush_every(std::chrono::seconds(1));
  return true;
}

}  // namespace stub

}  // namespace dingofs