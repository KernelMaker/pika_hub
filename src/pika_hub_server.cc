//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>

#include "src/pika_hub_server.h"
#include "src/pika_hub_command.h"
#include "slash/include/slash_string.h"

Options SanitizeOptions(const Options& options) {
  Options result(options);
  if (result.info_log == nullptr) {
    rocksutil::Status s = rocksutil::CreateLogger(result.info_log_path,
        &result.info_log, result.env, result.max_log_file_size,
        result.log_file_time_to_roll, result.info_log_level);
    if (!s.ok()) {
      result.info_log = nullptr;
    }
  }
  return result;
}

floyd::Options BuildFloydOptions(const Options& options) {
  floyd::Options result(options.str_members,
      options.local_ip, options.local_port,
      options.data_path, options.log_path);
  result.members = options.members;
  return result;
}

bool PikaHubServerHandler::AccessHandle(std::string& ip) const {
  pika_hub_server_->PlusAccConnections();
  return true;
}

void PikaHubServerHandler::CronHandle() const {
  pika_hub_server_->ResetLastSecQueryNum();
  char buf[1024*1024];
  memset(buf, 'a', 1024*1024);
  pika_hub_server_->binlog_writer()->Append(std::string(buf));
}

int PikaHubServerHandler::CreateWorkerSpecificData(void** data) const {
  CmdTable* cmds = new CmdTable;
  cmds->reserve(100);
  InitCmdTable(cmds);
  *data = reinterpret_cast<void*>(cmds);
  return 0;
}

int PikaHubServerHandler::DeleteWorkerSpecificData(void* data) const {
  CmdTable* cmds = reinterpret_cast<CmdTable*>(data);
  DestoryCmdTable(cmds);
  delete cmds;
  return 0;
}

bool PikaHubInnerServerHandler::AccessHandle(std::string& ip) const {
  return pika_hub_server_->IsValidClient(ip);
}

//void PikaHubInnerServerHandler::FdClosedHandle(int fd,
//    const std::string& ip_port) const {
//  pika_hub_server_->ResetRcvNumber(ip_port);
//};

PikaHubServer::PikaHubServer(const Options& options)
  : env_(options.env),
    options_(SanitizeOptions(options)),
    statistic_data_(options.env) {
  floyd::Floyd::Open(BuildFloydOptions(options), &floyd_);
  conn_factory_ = new PikaHubClientConnFactory();
  server_handler_ = new PikaHubServerHandler(this);
  server_thread_ = pink::NewHolyThread(options_.port, conn_factory_, 1000,
                            server_handler_);
  inner_conn_factory_ = new PikaHubInnerClientConnFactory();
  inner_server_handler_ = new PikaHubInnerServerHandler(this);
  inner_server_thread_ = pink::NewDispatchThread(options_.port+1000, 2,
                  inner_conn_factory_, 1000, 1000, inner_server_handler_);
  binlog_manager_ = CreateBinlogManager(options.info_log_path, options.env);
  trysync_thread_ = new PikaHubTrysync(options_.info_log, options.local_ip,
      options.port, &pika_servers_, &pika_mutex_);
  binlog_writer_ = binlog_manager_->AddWriter();
  binlog_sender_ = nullptr;
}

PikaHubServer::~PikaHubServer() {
  server_thread_->StopThread();
  inner_server_thread_->StopThread();
  delete binlog_sender_;
  delete binlog_writer_;
  delete trysync_thread_;
  delete binlog_manager_;

  delete inner_server_thread_;
  delete inner_conn_factory_;
  delete inner_server_handler_;

  delete server_thread_;
  delete conn_factory_;
  delete server_handler_;

  delete floyd_;
}

slash::Status PikaHubServer::Start() {
  if (!CheckPikaServers()) {
    rocksutil::Fatal(options_.info_log, "Invalid pika-servers");
    return slash::Status::Corruption("Invalid pika-server");
  }

  slash::Status result = floyd_->Start();
  if (!result.ok()) {
    rocksutil::Fatal(options_.info_log, "Floyd start failed: %s",
        result.ToString().c_str());
    return result;
  }

  int ret = server_thread_->StartThread();
  if (ret != 0) {
    return slash::Status::Corruption("Start server error");
  }
  ret = inner_server_thread_->StartThread();
  if (ret != 0) {
    return slash::Status::Corruption("Start inner_server error");
  }
  ret = trysync_thread_->StartThread();
  if (ret != 0) {
    return slash::Status::Corruption("Start trysync thread error");
  }

  rocksutil::Info(options_.info_log, "Started");
  sleep(1);
  BinlogReader* reader = binlog_manager_->AddReader(1, 0);
  if (reader != nullptr) {
    binlog_sender_ = new BinlogSender("127.0.0.1", 9221, options_.info_log,
                          reader);
    binlog_sender_->StartThread();
  } else {
    rocksutil::Info(options_.info_log, "Create Reader Failed");
  }
  server_mutex_.Lock();
  server_mutex_.Lock();
  server_mutex_.Unlock();
  return slash::Status::OK();
}

bool PikaHubServer::IsValidClient(const std::string& ip) {
  std::string _ip;
  int port = 0;
  rocksutil::MutexLock l(&pika_mutex_);
  for (auto iter = pika_servers_.begin(); iter != pika_servers_.end(); iter++) {
    slash::ParseIpPortString(iter->first, _ip, port);
//    if (_ip == ip && iter->second.rcv_number == 0) {
    if (_ip == ip) {
      rocksutil::Info(options_.info_log, "Check IP: %s success", ip.c_str());
      iter->second.rcv_number = 1;
      return true;
    }
  }
  rocksutil::Warn(options_.info_log, "Check IP: %s failed", ip.c_str());
  return false;
}

//void PikaHubServer::ResetRcvNumber(const std::string& ip_port) {
//  std::string ip;
//  int port = 0;
//  rocksutil::MutexLock l(&pika_mutex_);
//  for (auto iter = pika_servers_.begin(); iter != pika_servers_.end(); iter++) {
//    slash::ParseIpPortString(ip_port, ip, port);
//    if (iter->first == ip && iter->second.rcv_number == 1) {
//      iter->second.rcv_number = 0;
//    }
//  }
//}

bool PikaHubServer::CheckPikaServers() {
  std::string str = options_.pika_servers;
  std::string ip;
  int port = 0;
  PikaStatus status;
  size_t prev_pos = str.find_first_not_of(',', 0);
  size_t pos = str.find(',', prev_pos);

  while (prev_pos != std::string::npos || pos != std::string::npos) {
    std::string token(str.substr(prev_pos, pos - prev_pos));
    if (slash::ParseIpPortString(token, ip, port)) {
      pika_servers_.insert(std::map<std::string, PikaStatus>::
                        value_type(token, status));
    } else {
      return false;
    }
    prev_pos = str.find_first_not_of(',', pos);
    pos = str.find_first_of(',', prev_pos);
  }
  return true;
}
