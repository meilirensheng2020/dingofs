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
 * Date: Sat Aug  7 22:46:58 CST 2021
 * Author: wuhanqing
 */

#ifndef DINGOFS_SRC_METASERVER_COPYSET_META_OPERATOR_CLOSURE_H_
#define DINGOFS_SRC_METASERVER_COPYSET_META_OPERATOR_CLOSURE_H_

#include <braft/raft.h>

#include "metaserver/copyset/meta_operator.h"

namespace dingofs {
namespace metaserver {
namespace copyset {

class MetaOperatorClosure : public braft::Closure {
 public:
  explicit MetaOperatorClosure(MetaOperator* op) : operator_(op) {}

  void Run() override;

  MetaOperator* GetOperator() const { return operator_; }

 private:
  MetaOperator* operator_;
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_COPYSET_META_OPERATOR_CLOSURE_H_
