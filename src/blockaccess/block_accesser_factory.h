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

#ifndef DINGOFS_BLOCK_ACCESS_BLOCK_ACCESSER_FACTORY_H_
#define DINGOFS_BLOCK_ACCESS_BLOCK_ACCESSER_FACTORY_H_

#include <memory>

#include "blockaccess/accesser_common.h"
#include "blockaccess/block_accesser.h"

namespace dingofs {
namespace blockaccess {

class BlockAccesserFactory {
 public:
  virtual ~BlockAccesserFactory() = default;

  virtual BlockAccesserUPtr NewBlockAccesser(const BlockAccessOptions& options);

  virtual BlockAccesserSPtr NewShareBlockAccesser(
      const BlockAccessOptions& options);
};

}  // namespace blockaccess
}  // namespace dingofs

#endif  // DINGOFS_BLOCK_ACCESS_BLOCK_ACCESSER_FACTORY_H_
