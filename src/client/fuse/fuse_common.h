/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef DINGOFS_SRC_CLIENT_FUSE_COMMON_H_
#define DINGOFS_SRC_CLIENT_FUSE_COMMON_H_

#define FUSE_USE_VERSION 34

#include <fuse3/fuse.h>
#include <fuse3/fuse_lowlevel.h>
#include <sys/stat.h>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <fstream>
#include <string>
#include <unordered_map>

#include "base/string/string.h"
#include "common/define.h"

using ::dingofs::base::string::StrFormat;
using ::dingofs::base::string::TrimSpace;

#ifdef __cplusplus
extern "C" {
#endif

struct MountOption {
  const char* mountPoint;
  const char* fsName;
  const char* fsType;
  char* conf;
  char* mdsAddr;
};

static const struct fuse_opt mount_opts[] = {
    {"fsname=%s", offsetof(struct MountOption, fsName), 0},

    {"fstype=%s", offsetof(struct MountOption, fsType), 0},

    {"conf=%s", offsetof(struct MountOption, conf), 0},

    FUSE_OPT_END};

#ifdef __cplusplus
}  // extern "C"
#endif

inline void PrintOptionHelp(const char* o, const char* msg) {
  printf("    -o %-20s%s\n", o, msg);
}

inline void ExtraOptionsHelp() {
  printf("\nExtra options:\n");
  PrintOptionHelp("fsname", "[required] name of filesystem to be mounted");
  PrintOptionHelp("fstype",
                  "[required] type of filesystem to be mounted (s3/volume)");
  PrintOptionHelp("conf", "[required] path of config file");
  printf("    --mdsAddr              mdsAddr of dingofs cluster\n");
}

inline std::string MatchAnyPattern(
    const std::unordered_map<std::string, char**>& patterns, const char* src) {
  size_t src_len = strlen(src);
  for (const auto& pair : patterns) {
    const auto& pattern = pair.first;
    if (pattern.length() < src_len &&
        strncmp(pattern.c_str(), src, pattern.length()) == 0) {
      return pattern;
    }
  }
  return {};
}

inline int FuseAddOpts(struct fuse_args* args, const char* arg_value) {
  if (fuse_opt_add_arg(args, "-o") == -1) return 1;
  if (fuse_opt_add_arg(args, arg_value) == -1) return 1;
  return 0;
}

inline void ParseOption(int argc, char** argv, int* parsed_argc_p,
                        char** parsed_argv, struct MountOption* opts) {
  // add support for parsing option value with comma(,)
  std::unordered_map<std::string, char**> patterns = {
      {"--mdsaddr=", &opts->mdsAddr}};
  for (int i = 0, j = 0; j < argc; j++) {
    std::string p = MatchAnyPattern(patterns, argv[j]);
    int p_len = p.length();
    int src_len = strlen(argv[j]);
    if (p_len) {
      if (*patterns[p]) {
        free(*patterns[p]);
      }
      *patterns[p] =
          reinterpret_cast<char*>(malloc(sizeof(char) * (src_len - p_len + 1)));
      memcpy(*patterns[p], argv[j] + p_len, src_len - p_len);
      (*patterns[p])[src_len - p_len] = '\0';
      *parsed_argc_p = *parsed_argc_p - 1;
    } else {
      parsed_argv[i] =
          reinterpret_cast<char*>(malloc(sizeof(char) * (src_len + 1)));
      memcpy(parsed_argv[i], argv[j], src_len);
      parsed_argv[i][src_len] = '\0';
      i++;
    }
  }
}

inline void FreeParsedArgv(char** parsed_argv, int alloc_size) {
  for (int i = 0; i < alloc_size; i++) {
    free(parsed_argv[i]);
  }
  free(parsed_argv);
}

// Get file inode number
inline int GetFileInode(const char* file_name) {
  struct stat file_info;

  if (stat(file_name, &file_info) == 0) {
    return file_info.st_ino;
  }
  return -1;
}

// read dingo-fuse runtime information from .stats file
// At present, the purpose is to obtain the PID of old dingo-fuse, and in the
// subsequent smooth upgrade, send signals old dingo-fuse
inline std::unordered_map<std::string, std::string> LoadDingoRunTimeData(
    const std::string& filename) {
  std::unordered_map<std::string, std::string> result;
  std::ifstream file(filename);

  if (!file.is_open()) {
    return result;
  }

  const std::string delimiter = ":";
  std::string line;
  while (std::getline(file, line)) {
    size_t colon_pos = line.find(delimiter);
    if (colon_pos == std::string::npos) {
      continue;
    }
    std::string key = line.substr(0, colon_pos);
    std::string value = line.substr(colon_pos + delimiter.length());
    TrimSpace(key);
    TrimSpace(value);
    if (!key.empty()) {
      result[key] = value;
    }
  }

  return result;
}

// Smooth upgrade requires new dingo-fuse mount at same mountpoint as old
inline bool CanShutdownGracefully(const char* mountpoint) {
  if (GetFileInode(mountpoint) != dingofs::ROOTINODEID) {
    return false;
  }
  std::string file_name = StrFormat("%s/%s", mountpoint, dingofs::STATSNAME);
  auto runtime_data = LoadDingoRunTimeData(file_name);
  return runtime_data.find("pid") != runtime_data.end();
}

#endif  // DINGOFS_SRC_CLIENT_FUSE_COMMON_H_
