//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef SRC_PIKA_HUB_SYNC_COMMAND_H_
#define SRC_PIKA_HUB_SYNC_COMMAND_H_

#include <string>
#include "src/pika_hub_command.h"
#include "src/pika_hub_client_conn.h"

class SetCmd : public Cmd {
 public:
  SetCmd() {}
  virtual void Do() override;
 private:
  virtual void DoInitial(const PikaCmdArgsType &argvs,
      const CmdInfo* const ptr_info) override;
  std::string key_;
  std::string value_;
  int64_t server_id_;
  int32_t exec_time_;
  int32_t number_;
  int64_t offset_;
};

#endif  // SRC_PIKA_HUB_SYNC_COMMAND_H_
