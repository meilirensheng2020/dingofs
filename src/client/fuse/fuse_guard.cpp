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

#include "client/fuse/fuse_guard.h"

#include <unistd.h>

#include <functional>
#include <iostream>
#include <stack>

namespace dingofs {
namespace client {
namespace fuse {

FuseGuard::~FuseGuard() {
  while (!handlers_.empty()) {
    auto tmp_handler = handlers_.top();
    tmp_handler();
    handlers_.pop();
  }
}

void FuseGuard::Push(CleanupHandler handler) { handlers_.push(handler); }

}  // namespace fuse
}  // namespace client
}  // namespace dingofs
