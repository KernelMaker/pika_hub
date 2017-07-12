//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>
#include <thread>

#include "src/pika_hub_binlog_sender.h"
#include "src/pika_hub_common.h"
#include "src/pika_hub_binlog_manager.h"
#include "pink/include/pink_cli.h"
#include "pink/include/redis_cli.h"
#include "slash/include/slash_status.h"
#include "rocksutil/cache.h"

void BinlogSender::UpdateSendOffset() {
  {
  rocksutil::MutexLock l(pika_mutex_);
  auto iter = pika_servers_->find(server_id_);
  if (iter != pika_servers_->end()) {
    reader_->GetOffset(&iter->second.send_number, &iter->second.send_offset);
  }
  }
}
void* BinlogSender::ThreadMain() {
  rocksutil::Status read_status;
  uint8_t op;
  std::string key;
  std::string value;
  int32_t server_id;
  int32_t exec_time;
  pink::PinkCli* cli = nullptr;
  pink::RedisCmdArgsType args;
  std::string str_cmd;
  slash::Status s;
  while (!should_stop()) {
    if (cli == nullptr) {
      cli = pink::NewRedisCli();
      cli->set_connect_timeout(1500);
      if ((cli->Connect(ip_, port_+ 1100)).ok()) {
        cli->set_send_timeout(3000);
        cli->set_recv_timeout(3000);
        Info(info_log_, "BinlogSender Connect %d,%s:%d success", server_id_,
            ip_.c_str(), port_);
        {
        rocksutil::MutexLock l(pika_mutex_);
        auto iter = pika_servers_->find(server_id_);
        if (iter != pika_servers_->end()) {
          iter->second.send_fd = cli->fd();
        }
        }
      } else {
        Error(info_log_, "BinlogSender Connect %d,%s:%d failed", server_id_,
            ip_.c_str(), port_);
        cli = nullptr;
      }
      std::this_thread::sleep_for(std::chrono::seconds(2));
      continue;
    }

    if (!args.empty()) {
      pink::SerializeRedisCommand(args, &str_cmd);
      s = cli->Send(&str_cmd);
      if (!s.ok()) {
        Error(info_log_, "BinlogSender Send %d,%s:%d failed", server_id_,
            ip_.c_str(), port_);
        {
        rocksutil::MutexLock l(pika_mutex_);
        auto iter = pika_servers_->find(server_id_);
        if (iter != pika_servers_->end()) {
          iter->second.send_fd = -1;
        }
        }
        cli->Close();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        continue;
      } else {
        UpdateSendOffset();
      }
      args.clear();
      str_cmd.clear();
    }

    read_status = reader_->ReadRecord(&op, &key, &value,
        &server_id, &exec_time);
    if (read_status.ok()) {
//      Info(info_log_,
//          "op: %d, key: %s, value: %s, server_id: %d, exec_time: %d",
//          op, key.c_str(), value.c_str(), server_id, exec_time);

      if (server_id_ == server_id) {
        UpdateSendOffset();
        continue;
      }

      rocksutil::Cache::Handle* handle = manager_->lru_cache()->Lookup(key);
      if (handle) {
        int32_t _exec_time = static_cast<CacheEntity*>(
            manager_->lru_cache()->Value(handle))->exec_time;
        if (exec_time < _exec_time) {
          UpdateSendOffset();
          continue;
        }
      } else {
        Error(info_log_, "BinlogSender check LRU: %s is not in cache",
            key.c_str());
        UpdateSendOffset();
        continue;
      }

      switch (op) {
        case kSetOPCode:
          args.push_back("set");
          break;
      }

      args.push_back(key);

      switch (op) {
        case kSetOPCode:
          args.push_back(value);
          break;
      }

    } else {
      Error(info_log_, "BinlogSender ReadRecord, error: %s",
          read_status.ToString().c_str());
    }
  }
  return nullptr;
}
