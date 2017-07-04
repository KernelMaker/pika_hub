#ifndef PIKA_HUB_SERVER_H
#define PIKA_HUB_SERVER_H

#include <atomic>

#include "floyd/include/floyd.h"
#include "pink/include/server_thread.h"

#include "rocksutil/mutexlock.h"

#include "pika_hub_options.h"
#include "pika_hub_client_conn.h"
#include "pika_hub_binlog_manager.h"

class PikaHubServer;

class PikaHubServerHandler : public pink::ServerHandle {
 public:
  explicit PikaHubServerHandler(PikaHubServer* pika_hub_server)
    : pika_hub_server_(pika_hub_server) {
    };
  virtual ~PikaHubServerHandler() {};

  virtual void CronHandle() const override; 

 private:
  PikaHubServer* pika_hub_server_;
};

class PikaHubServer {
 public:
  explicit PikaHubServer(const Options& options);
  virtual ~PikaHubServer();
  slash::Status Start();

  uint64_t last_qps() {
    return statistic_data_.last_qps.load();
  }
  uint64_t query_num() {
    return statistic_data_.query_num.load();
  }

//  BinlogWriter* binlog_writer() {
//    return binlog_writer_;
//  }
//
//  BinlogReader* binlog_reader() {
//    return binlog_reader_;
//  }
  BinlogManager* binlog_manager() {
    return binlog_manager_;
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

  void Unlock() {
    server_mutex_.Unlock();
  }

  void DumpOptions() const {
    options_.Dump(options_.info_log.get());
  }

 private:
  rocksutil::Env* env_;
  const Options options_;

  struct StatisticData {
    StatisticData(rocksutil::Env* env):
      last_query_num(0),
      query_num(0),
      last_qps(0),
      last_time_us(env->NowMicros()) {
    };
    std::atomic<uint64_t> last_query_num;
    std::atomic<uint64_t> query_num;
    std::atomic<uint64_t> last_qps;
    std::atomic<uint64_t> last_time_us;
  };
  StatisticData statistic_data_;

  floyd::Floyd* floyd_;

  PikaHubServerHandler* server_handler_;
  PikaHubClientConnFactory* conn_factory_;
  pink::ServerThread* server_thread_;

//  BinlogWriter* binlog_writer_;
//  BinlogReader* binlog_reader_;
  BinlogManager* binlog_manager_;

  rocksutil::port::Mutex server_mutex_;
};

#endif
