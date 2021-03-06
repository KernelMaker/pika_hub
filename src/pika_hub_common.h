//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_PIKA_HUB_COMMON_H_
#define SRC_PIKA_HUB_COMMON_H_

#include <string>
#include <map>
#include <atomic>

enum SyncStatus {
  kShouldConnect = 0,
  kConnected,
  kErrorHappened,
  kShouldDelete
};

struct PikaStatus {
  SyncStatus sync_status = kShouldConnect;
  int32_t server_id = -1;
  int32_t port;
  int32_t rcv_fd_num = 0;
  int32_t send_fd = -1;
  int32_t hb_fd = -1;
  uint64_t rcv_number = 0;
  uint64_t rcv_offset = 0;
  uint64_t send_number = 0;
  uint64_t send_offset = 0;
  void* sender = nullptr;
  void* heartbeat = nullptr;
  std::string ip;
  std::string passwd;
};

typedef std::map<int32_t, PikaStatus> PikaServers;

typedef std::map<int32_t,
        std::map<int32_t, std::atomic<int32_t> > > RecoverOffsetMap;

struct BinlogFields {
  uint8_t op;
  int32_t server_id;
  int32_t exec_time;
  int32_t filenum;
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
const uint8_t kDelOPCode = 2;
const uint8_t kExpireatOPCode = 3;

const char kBinlogPrefix[] = "binlog_";
const int32_t kMaxBinlogFileSize = 100 * 1024 * 1024;
const char kBinlogMagic[] = "__PIKA_X#$SKGI";
const char kLockName[] = "pika_hub_lock#68";
const char kLeaseKey[] = "pika_hub_lease#68";

const int32_t kMaxRecvRollbackNums = 12;
const int32_t kMaxRetryTimes = 10;
const int32_t kPikaPortInterval = 1100;
const int32_t kMaxFloydErrorTimes = 10;
const int32_t kLockDuration = 10;  // 10s
const int32_t kLeaseDuration = 60;  // 60s

#endif  // SRC_PIKA_HUB_COMMON_H_
