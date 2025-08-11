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

#ifndef DINGOFS_CLIENT_COMMON_CONST_H
#define DINGOFS_CLIENT_COMMON_CONST_H

#include <cstdint>

namespace dingofs {
namespace client {

// // ioctl related constants
// constexpr uint32_t FS_IMMUTABLE_FL = 0x00000010; /* Immutable file */
// constexpr uint32_t FS_APPEND_FL =
//     0x00000020; /* writes to file may only append */
// constexpr uint32_t FS_XFLAG_IMMUTABLE =
//     0x00000008; /* extended attribute: immutable*/
// constexpr uint32_t FS_XFLAG_APPEND =
//     0x00000010; /* extended attribute: append only */

constexpr uint8_t kFlagImmutable = (1 << 0);
constexpr uint8_t kFlagAppend = (1 << 1);
constexpr uint8_t kFlagNoDump = (1 << 2);
constexpr uint8_t kFlagNoAtime = (1 << 3);
constexpr uint8_t kFlagSync = (1 << 4);

// constexpr uint64_t FS_IOC_GETFLAGS = 0x80086601;
// constexpr uint64_t FS_IOC_SETFLAGS = 0x40086602;
// constexpr uint64_t FS_IOC32_GETFLAGS = 0x80046601;
// constexpr uint64_t FS_IOC32_SETFLAGS = 0x40046602;
// constexpr uint64_t FS_IOC_FSGETXATTR = 0x801C581F;

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_COMMON_CONST_H_