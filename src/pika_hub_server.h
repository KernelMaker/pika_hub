#ifndef PIKA_HUB_SERVER_H
#define PIKA_HUB_SERVER_H

#include <atomic>
#include "floyd/include/floyd.h"

#include "pink/include/redis_conn.h"
#include "pink/include/server_thread.h"

#include "rocksutil/mutexlock.h"
#include "rocksutil/env.h"
#include "pika_hub_options.h"

class PikaHubServer;
class PikaHubServerConn;
class PikaHubServerConnFactory;

class PikaHubServerHandler : public pink::ServerHandle {
 public:
  explicit PikaHubServerHandler() {};
  virtual ~PikaHubServerHandler() {};
};

class PikaHubServerConn : public pink::RedisConn {
 public:
  PikaHubServerConn(int fd, const std::string& ip_port,
      pink::ServerThread* server_thread) :
    pink::RedisConn(fd, ip_port, server_thread) {};
  virtual ~PikaHubServerConn() {}

  virtual int DealMessage() override;
};

class PikaHubServerConnFactory : public pink::ConnFactory {
 public:
  explicit PikaHubServerConnFactory() {};

  virtual pink::PinkConn *NewPinkConn(int connfd,
      const std::string& ip_port,
      pink::ServerThread* server_thread,
      void* worker_private_data) const override {
    return new PikaHubServerConn(connfd, ip_port, server_thread);
  }
};

class PikaHubServer {
 public:
  explicit PikaHubServer(const Options& options);
  virtual ~PikaHubServer();
  slash::Status Start();

  uint64_t last_qps() {
    return last_qps_.load();
  }

  void PlusQueryNum() {
    query_num_++;
  }

  void ResetLastSecQueryNum() {
    uint64_t cur_time_us = env_->NowMicros();
    last_qps_ = (query_num_ - last_query_num_) * 1000000 / (cur_time_us - last_time_us_ + 1);
    last_query_num_ = query_num_.load();
    last_time_us_ = cur_time_us;
  }

  void Unlock() {
    server_mutex_.Unlock();
  }

  void DumpOptions() const {
    options_.Dump(options_.info_log.get());
  }


 private:
  floyd::Floyd* floyd_;
  rocksutil::Env* env_;
  const Options options_;

  PikaHubServerHandler* server_handler_;
  PikaHubServerConnFactory* conn_factory_;
  pink::ServerThread* server_thread_;

  rocksutil::port::Mutex server_mutex_;

  std::atomic<uint64_t> last_query_num_;
  std::atomic<uint64_t> query_num_;
  std::atomic<uint64_t> last_qps_;
  std::atomic<uint64_t> last_time_us_;
};

#endif
