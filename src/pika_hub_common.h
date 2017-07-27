//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_PIKA_HUB_COMMON_H_
#define SRC_PIKA_HUB_COMMON_H_

#include <string>
#include <map>

struct PikaStatus {
  bool should_trysync = true;
  bool should_delete = false;
  int32_t server_id = -1;
  int32_t port;
  int32_t rcv_fd_num = 0;
  int32_t send_fd = -1;
  int32_t hb_fd = -1;
  uint64_t rcv_number = 0;
  uint64_t rcv_offset = 0;
  uint64_t send_number = 1;
  uint64_t send_offset = 0;
  void* sender = nullptr;
  void* heartbeat = nullptr;
  std::string ip;
  std::string passwd;
};

typedef std::map<int32_t, PikaStatus> PikaServers;

struct BinlogFields {
  uint8_t op;
  int32_t server_id;
  int32_t exec_time;
  std::string key;
  std::string value;
};

struct CacheEntity {
  CacheEntity(int32_t _server_id,
      int32_t _exec_time)
    : server_id(_server_id),
      exec_time(_exec_time) {}
  int32_t server_id;
  int32_t exec_time;
};

const uint8_t kSetOPCode = 1;
const uint8_t kDeleteOPCode = 2;

const char kBinlogPrefix[] = "binlog_";
const int32_t kMaxBinlogFileSize = 100 * 1024 * 1024;
const char kBinlogMagic[] = "__PIKA_X#$SKGI";

const int32_t kMaxRecoverNums = 100;

#endif  // SRC_PIKA_HUB_COMMON_H_
