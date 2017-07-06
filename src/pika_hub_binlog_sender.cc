//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>

#include "src/pika_hub_binlog_sender.h"

void* BinlogSender::ThreadMain() {
  std::string scratch;
  rocksutil::Slice record;
  rocksutil::Status s;
  while (!should_stop()) {
    s = reader_->ReadRecord(&record, &scratch);
    if (s.ok()) {
//      Info(info_log_, std::string(record.data(), record.size()).c_str());
      Info(info_log_, std::to_string(record.size()).c_str());
    } else {
      Error(info_log_, s.ToString().c_str());
    }
  }
  return nullptr;
}
