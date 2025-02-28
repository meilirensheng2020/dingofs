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

#include "client/fuse/fuse_log.h"

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

namespace dingofs {
namespace client {
namespace fuse {

// using ::dingofs::base::string::StrFormat;
using MessageHandler = std::function<std::string()>;

static std::shared_ptr<spdlog::logger> logger;

void InitFuseLog(const std::string& prefix) {
  // std::string filename = StrFormat("%s/fuse_%d.log", prefix, getpid());
  std::string filename = prefix + "/fuse_" + std::to_string(getpid()) + ".log";
  logger = spdlog::daily_logger_st("fuse", filename, 0, 0);
  spdlog::flush_every(std::chrono::seconds(1));
}

void FuseLogMessage(const std::string& message) { logger->info(message); }

}  // namespace fuse
}  // namespace client
}  // namespace dingofs
