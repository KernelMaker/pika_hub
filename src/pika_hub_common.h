//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_PIKA_HUB_COMMON_H_
#define SRC_PIKA_HUB_COMMON_H_

struct ConnPrivateData {
};

struct PikaStatus {
  bool should_trysync = true;
  bool should_delete = false;
  int32_t rcv_fd = -1;
  int32_t send_fd = -1;
  uint64_t rcv_number = 0;
  uint64_t rcv_offset = 0;
  uint64_t send_number = 1;
  uint64_t send_offset = 0;
  void* sender = nullptr;
};

const char kBinlogPrefix[] = "binlog_";
const int32_t kMaxBinlogFileSize = 100 * 1024 * 1024;
const char kBinlogMagic[] = "__PIKA_X#$SKGI";

#endif  // SRC_PIKA_HUB_COMMON_H_
