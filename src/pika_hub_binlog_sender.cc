//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>

#include "src/pika_hub_binlog_sender.h"

void* BinlogSender::ThreadMain() {
  rocksutil::Status s;
  uint8_t op;
  std::string key;
  std::string value;
  int32_t server_id;
  int32_t exec_time;
  while (!should_stop()) {
    s = reader_->ReadRecord(&op, &key, &value, &server_id, &exec_time);
    if (s.ok()) {
      Info(info_log_,
          "op: %d, key: %s, value: %s, server_id: %d, exec_time: %d",
          op, key.c_str(), value.c_str(), server_id, exec_time);
    } else {
      Error(info_log_, "ReadRecord, error: %s", s.ToString().c_str());
    }
  }
  return nullptr;
}
