//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef SRC_PIKA_HUB_SERVER_H_
#define SRC_PIKA_HUB_SERVER_H_

#include <string>
#include <memory>
#include <chrono>

#include "src/pika_hub_options.h"
#include "src/pika_hub_common.h"
#include "src/pika_hub_client_conn.h"
#include "src/pika_hub_inner_client_conn.h"
#include "src/pika_hub_binlog_manager.h"
#include "src/pika_hub_binlog_sender.h"
#include "src/pika_hub_trysync.h"
#include "floyd/include/floyd.h"
#include "pink/include/server_thread.h"
#include "rocksutil/coding.h"

class PikaHubServer;

class PikaHubServerHandler : public pink::ServerHandle {
 public:
  explicit PikaHubServerHandler(PikaHubServer* pika_hub_server)
    : pika_hub_server_(pika_hub_server) {
    }
  virtual ~PikaHubServerHandler() {}

  using::pink::ServerHandle::AccessHandle;
  virtual bool AccessHandle(std::string& ip) const override;
  using::pink::ServerHandle::CronHandle;
  virtual void CronHandle() const override;
  using::pink::ServerHandle::CreateWorkerSpecificData;
  int CreateWorkerSpecificData(void** data) const override;
  using::pink::ServerHandle::DeleteWorkerSpecificData;
  int DeleteWorkerSpecificData(void* data) const override;

 private:
  PikaHubServer* pika_hub_server_;
};

class PikaHubInnerServerHandler : public pink::ServerHandle {
 public:
  explicit PikaHubInnerServerHandler(PikaHubServer* pika_hub_server)
    : pika_hub_server_(pika_hub_server) {
    }
  virtual ~PikaHubInnerServerHandler() {}

  using::pink::ServerHandle::AccessHandle;
  virtual bool AccessHandle(int fd, std::string& ip) const override;
  using::pink::ServerHandle::CreateWorkerSpecificData;
  int CreateWorkerSpecificData(void** data) const override;
  using::pink::ServerHandle::DeleteWorkerSpecificData;
  int DeleteWorkerSpecificData(void* data) const override;
  using::pink::ServerHandle::FdClosedHandle;
  virtual void FdClosedHandle(int fd,
      const std::string& ip_port) const override;

 private:
  PikaHubServer* pika_hub_server_;
};

class PikaHubServer {
 public:
  explicit PikaHubServer(const Options& options);
  virtual ~PikaHubServer();
  slash::Status Start();

  uint64_t acc_connections() {
    return statistic_data_.acc_connections.load();
  }

  uint64_t last_qps() {
    return statistic_data_.last_qps.load();
  }
  uint64_t query_num() {
    return statistic_data_.query_num.load();
  }

  uint64_t primary_lease_deadline() {
    return primary_lease_deadline_;
  }

  std::string primary() {
    return primary_;
  }

  bool is_primary() {
    return is_primary_;
  }

  BinlogWriter* binlog_writer() {
    return binlog_writer_;
  }

  BinlogManager* binlog_manager() {
    return binlog_manager_;
  }

  std::chrono::system_clock::time_point last_success_save_offset_time() {
    return last_success_save_offset_time_;
  }

  std::shared_ptr<rocksutil::Logger> GetLogger() {
    return options_.info_log;
  }

  void PlusAccConnections() {
    statistic_data_.acc_connections++;
  }

  void PlusQueryNum() {
    statistic_data_.query_num++;
  }

  void ResetLastSecQueryNum() {
    uint64_t cur_time_us = env_->NowMicros();
    statistic_data_.last_qps = (statistic_data_.query_num -
        statistic_data_.last_query_num) *
      1000000 / (cur_time_us - statistic_data_.last_time_us + 1);
    statistic_data_.last_query_num = statistic_data_.query_num.load();
    statistic_data_.last_time_us = cur_time_us;
  }

  void DumpOptions() const {
    options_.Dump(options_.info_log.get());
  }
  bool IsValidInnerClient(int fd, const std::string& ip);
  void ResetRcvFd(int fd, const std::string& ip_port);
  std::string DumpPikaServers();
  void UpdateRcvOffset(int32_t server_id,
      int32_t number, int64_t offset);
  void GetBinlogWriterOffset(uint64_t* number, uint64_t* offset);
  void Exit() {
    should_exit_ = true;
  }
  void DisconnectPika(int32_t server_id, bool reconnect = true);
  bool Transfer(const std::string& server_id,
      const std::string& new_ip,
      int32_t new_port,
      std::string* result);

 private:
  rocksutil::Env* env_;
  const Options options_;

  struct StatisticData {
    explicit StatisticData(rocksutil::Env* env) :
      acc_connections(0),
      last_query_num(0),
      query_num(0),
      last_qps(0),
      last_time_us(env->NowMicros()) {
    }
    std::atomic<uint64_t> acc_connections;
    std::atomic<uint64_t> last_query_num;
    std::atomic<uint64_t> query_num;
    std::atomic<uint64_t> last_qps;
    std::atomic<uint64_t> last_time_us;
  };
  StatisticData statistic_data_;
  std::atomic<bool> should_exit_;
  std::atomic<bool> is_primary_;
  std::chrono::system_clock::time_point last_success_save_offset_time_;
  uint64_t primary_lease_deadline_;
  std::string primary_;

  floyd::Floyd* floyd_;

  PikaHubServerHandler* server_handler_;
  PikaHubClientConnFactory* conn_factory_;
  pink::ServerThread* server_thread_;

  PikaHubInnerServerHandler* inner_server_handler_;
  PikaHubInnerClientConnFactory* inner_conn_factory_;
  pink::ServerThread* inner_server_thread_;

  BinlogManager* binlog_manager_;
  PikaHubTrysync* trysync_thread_;
  BinlogWriter* binlog_writer_;
  bool CheckPikaServers();
  bool RecoverOffset();
  static void EncodeOffset(std::string* value,
      const RecoverOffsetMap::iterator& iter);
  void DecodeOffset(const std::string& value,
      uint64_t* rcv_number, uint64_t* rcv_offset);
  PikaServers pika_servers_;
  slash::Status BecomePrimary();
  void BecomeSecondary();
  static void EncodeLease(std::string* value,
      const std::string& holder, const uint64_t lease_deadline);
  static void DecodeLease(const std::string& value,
      std::string* holder, uint64_t* lease_deadline);
  // protect pika_servers_
  rocksutil::port::Mutex pika_mutex_;

  RecoverOffsetMap recover_offset_;
};

#endif  // SRC_PIKA_HUB_SERVER_H_
