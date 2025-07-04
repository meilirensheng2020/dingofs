// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mdsv2/common/version.h"

#include "butil/string_printf.h"
#include "fmt/format.h"
#include "mdsv2/common/logging.h"

namespace dingofs {
namespace mdsv2 {

DEFINE_string(git_commit_hash, GIT_VERSION, "current git commit version");
DEFINE_string(git_tag_name, GIT_TAG_NAME, "current dingo git tag version");
DEFINE_string(git_commit_user, GIT_COMMIT_USER, "current dingo git commit user");
DEFINE_string(git_commit_mail, GIT_COMMIT_MAIL, "current dingo git commit mail");
DEFINE_string(git_commit_time, GIT_COMMIT_TIME, "current dingo git commit time");
DEFINE_string(major_version, MAJOR_VERSION, "current dingo major version");
DEFINE_string(minor_version, MINOR_VERSION, "current dingo mino version");
DEFINE_string(dingo_build_type, DINGOFS_BUILD_TYPE, "current dingo build type");
DEFINE_bool(use_tcmalloc, false, "use tcmalloc");
DEFINE_bool(use_profiler, false, "use profiler");
DEFINE_bool(use_sanitizer, false, "use sanitizer");

std::string GetBuildFlag() {
#ifdef LINK_TCMALLOC
  FLAGS_use_tcmalloc = true;
#else
  FLAGS_use_tcmalloc = false;
#endif

#ifdef BRPC_ENABLE_CPU_PROFILER
  FLAGS_use_profiler = true;
#else
  FLAGS_use_profiler = false;
#endif

#ifdef USE_SANITIZE
  FLAGS_use_sanitizer = true;
#else
  FLAGS_use_sanitizer = false;
#endif

  return butil::string_printf(
      "DINGOFS LINK_TCMALLOC:[%s] "
      "BRPC_ENABLE_CPU_PROFILER:[%s] "
      "USE_SANITIZE:[%s]\n",
      FLAGS_use_tcmalloc ? "ON" : "OFF", FLAGS_use_profiler ? "ON" : "OFF", FLAGS_use_sanitizer ? "ON" : "OFF");
}

void DingoShowVersion() {
  printf("DINGOFS VERSION:[%s-%s]\n", FLAGS_major_version.c_str(), FLAGS_minor_version.c_str());
  printf("DINGOFS GIT_TAG_VERSION:[%s]\n", FLAGS_git_tag_name.c_str());
  printf("DINGOFS GIT_COMMIT_HASH:[%s]\n", FLAGS_git_commit_hash.c_str());
  printf("DINGOFS BUILD_TYPE:[%s]\n", FLAGS_dingo_build_type.c_str());
  printf("%s", GetBuildFlag().c_str());
}

void DingoLogVersion() {
  DINGO_LOG(INFO) << "DINGOFS VERSION:[" << FLAGS_major_version << "-" << FLAGS_minor_version << "]";
  DINGO_LOG(INFO) << "DINGOFS GIT_TAG_VERSION:[" << FLAGS_git_tag_name << "]";
  DINGO_LOG(INFO) << "DINGOFS GIT_COMMIT_HASH:[" << FLAGS_git_commit_hash << "]";
  DINGO_LOG(INFO) << "DINGOFS BUILD_TYPE:[" << FLAGS_dingo_build_type << "]";
  DINGO_LOG(INFO) << GetBuildFlag();
  DINGO_LOG(INFO) << "PID: " << getpid();
}

std::vector<std::pair<std::string, std::string>> DingoVersion() {
  std::vector<std::pair<std::string, std::string>> result;
  result.emplace_back("VERSION", fmt::format("{}-{}", FLAGS_major_version, FLAGS_minor_version));
  result.emplace_back("TAG_VERSION", FLAGS_git_tag_name);
  result.emplace_back("COMMIT_HASH", FLAGS_git_commit_hash);
  result.emplace_back("COMMIT_USER", FLAGS_git_commit_user);
  result.emplace_back("COMMIT_MAIL", FLAGS_git_commit_mail);
  result.emplace_back("COMMIT_TIME", FLAGS_git_commit_time);
  result.emplace_back("BUILD_TYPE", FLAGS_dingo_build_type);

  return result;
}

DEFINE_bool(show_version, false, "Print dingofs version flag");

}  // namespace mdsv2

}  // namespace dingofs
