//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>
#include <cstring>

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
      options.path);
  return result;
}

bool PikaHubServerHandler::AccessHandle(std::string& ip) const {
  pika_hub_server_->PlusAccConnections();
  return true;
}

void PikaHubServerHandler::CronHandle() const {
  pika_hub_server_->ResetLastSecQueryNum();
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

bool PikaHubInnerServerHandler::AccessHandle(int fd, std::string& ip) const {
  return pika_hub_server_->IsValidInnerClient(fd, ip);
}

int PikaHubInnerServerHandler::CreateWorkerSpecificData(void** data) const {
  CmdTable* cmds = new CmdTable;
  cmds->reserve(100);
  InitCmdTable(cmds);
  *data = reinterpret_cast<void*>(cmds);
  return 0;
}

int PikaHubInnerServerHandler::DeleteWorkerSpecificData(void* data) const {
  CmdTable* cmds = reinterpret_cast<CmdTable*>(data);
  DestoryCmdTable(cmds);
  delete cmds;
  return 0;
}

void PikaHubInnerServerHandler::FdClosedHandle(int fd,
    const std::string& ip_port) const {
  pika_hub_server_->ResetRcvFd(fd, ip_port);
}

PikaHubServer::PikaHubServer(const Options& options)
  : env_(options.env),
    options_(SanitizeOptions(options)),
    statistic_data_(options.env) {
  conn_factory_ = new PikaHubClientConnFactory();
  server_handler_ = new PikaHubServerHandler(this);
  server_thread_ = pink::NewHolyThread(options_.port, conn_factory_, 1000,
                            server_handler_);
  inner_conn_factory_ = new PikaHubInnerClientConnFactory();
  inner_server_handler_ = new PikaHubInnerServerHandler(this);
  inner_server_thread_ = pink::NewDispatchThread(options_.port+1000, 2,
                  inner_conn_factory_, 1000, 1000, inner_server_handler_);
  inner_server_thread_->set_keepalive_timeout(0);
  binlog_manager_ = CreateBinlogManager(options.info_log_path, options.env);
  trysync_thread_ = new PikaHubTrysync(options_.info_log, options.local_ip,
      options.port, &pika_servers_, &pika_mutex_, binlog_manager_);
  binlog_writer_ = binlog_manager_->AddWriter();
}

PikaHubServer::~PikaHubServer() {
  server_thread_->StopThread();
  inner_server_thread_->StopThread();
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

  slash::Status result = floyd::Floyd::Open(
      BuildFloydOptions(options_), &floyd_);
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
  server_mutex_.Lock();
  server_mutex_.Lock();
  server_mutex_.Unlock();
  return slash::Status::OK();
}

bool PikaHubServer::IsValidInnerClient(int fd, const std::string& ip) {
  rocksutil::MutexLock l(&pika_mutex_);
  for (auto iter = pika_servers_.begin(); iter != pika_servers_.end(); iter++) {
    if (iter->second.ip == ip && iter->second.rcv_fd == -1 &&
        !(iter->second.should_trysync)) {
      rocksutil::Info(options_.info_log, "Check IP: %s success", ip.c_str());
      iter->second.rcv_fd = fd;
      return true;
    }
  }
  rocksutil::Warn(options_.info_log, "Check IP: %s failed", ip.c_str());
  return false;
}

void PikaHubServer::ResetRcvFd(int fd, const std::string& ip_port) {
  std::string ip;
  int unuse_port = 0;
  rocksutil::MutexLock l(&pika_mutex_);
  for (auto iter = pika_servers_.begin(); iter != pika_servers_.end(); iter++) {
    slash::ParseIpPortString(ip_port, ip, unuse_port);
    if (iter->second.ip == ip && iter->second.rcv_fd == fd) {
      rocksutil::Info(options_.info_log, "Reset receive fd: %s",
          ip_port.c_str());
      iter->second.rcv_fd = -1;
      iter->second.send_fd = -1;
      delete static_cast<BinlogSender*>(iter->second.sender);
      iter->second.sender = nullptr;
      iter->second.should_trysync = true;
    }
  }
}

std::string PikaHubServer::DumpPikaServers() {
  rocksutil::MutexLock l(&pika_mutex_);
  std::string res;
  for (auto iter = pika_servers_.begin(); iter != pika_servers_.end(); iter++) {
    res += ("server_id:" + std::to_string(iter->first) +
        ", ip:" + iter->second.ip +
        ", port:" + std::to_string(iter->second.port) +
        ", password:" + iter->second.passwd +
        ", receive_fd:" + std::to_string(iter->second.rcv_fd) +
        ", recv_offset:" + std::to_string(iter->second.rcv_number) +
        ":" + std::to_string(iter->second.rcv_offset) +
        ", send_fd:" + std::to_string(iter->second.send_fd) +
        ", send_offset:" + std::to_string(iter->second.send_number) +
        ":" + std::to_string(iter->second.send_offset) +
        "\r\n");
  }
  return res;
}

void PikaHubServer::UpdateRcvOffset(int32_t server_id,
    int32_t number, int64_t offset) {
  rocksutil::MutexLock l(&pika_mutex_);
  auto iter = pika_servers_.find(server_id);
  if (iter != pika_servers_.end()) {
    iter->second.rcv_number = number;
    iter->second.rcv_offset = offset;
  }
}

bool PikaHubServer::CheckPikaServers() {
  std::string str = options_.pika_servers;
  char token[1024];
  char* token_in;
  PikaStatus status;
  int server_id = -1;
  size_t prev_pos = str.find_first_not_of(',', 0);
  size_t pos = str.find(',', prev_pos);

  while (prev_pos != std::string::npos || pos != std::string::npos) {
    memset(token, '\0', 1024);
    strncpy(token, str.data() + prev_pos, pos - prev_pos);

    token_in = std::strtok(token, ":");
    status.ip = token_in != NULL ? token_in : "";

    if (token_in) {
      token_in = std::strtok(NULL, ":");
    }
    status.port = token_in != NULL ? std::atoi(token_in) : -1;

    if (token_in) {
      token_in = std::strtok(NULL, ":");
    }
    server_id = token_in != NULL ? std::atoi(token_in) : -1;

    if (token_in) {
      token_in = std::strtok(NULL, ":");
    }
    status.passwd = token_in != NULL ?
      std::string(token_in, strlen(token_in)) : "";

    pika_servers_.insert(std::map<int32_t, PikaStatus>::
                      value_type(server_id, status));

    prev_pos = str.find_first_not_of(',', pos);
    pos = str.find_first_of(',', prev_pos);
  }
  return true;
}
