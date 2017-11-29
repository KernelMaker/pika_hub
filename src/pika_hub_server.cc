//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>
#include <cstring>
#include <map>
#include <utility>

#include "src/pika_hub_server.h"
#include "src/pika_hub_command.h"
#include "src/pika_hub_heartbeat.h"
#include "slash/include/slash_string.h"

Options SanitizeOptions(const Options& options) {
  Options result(options);
  if (result.info_log == nullptr) {
    rocksutil::Status s = rocksutil::CreateLogger(result.info_log_path,
        &result.info_log, result.env, result.max_log_file_size,
        result.log_file_time_to_roll, result.info_log_level, 0);
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
    statistic_data_(options.env),
    should_exit_(false),
    is_primary_(false),
    primary_lease_deadline_(0),
    trysync_thread_(nullptr) {
  conn_factory_ = new PikaHubClientConnFactory();
  server_handler_ = new PikaHubServerHandler(this);
  server_thread_ = pink::NewHolyThread(options_.port, conn_factory_, 1000,
                            server_handler_);
  inner_conn_factory_ = new PikaHubInnerClientConnFactory();
  inner_server_handler_ = new PikaHubInnerServerHandler(this);
  inner_server_thread_ = pink::NewDispatchThread(options_.port+1000, 20,
                  inner_conn_factory_, 1000, 1000, inner_server_handler_);
  inner_server_thread_->set_keepalive_timeout(0);
  binlog_manager_ = CreateBinlogManager(options.info_log_path, options.env,
                      options_.info_log);
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
  rocksutil::Info(options_.info_log, "pika_hub exit...");
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

  rocksutil::Info(options_.info_log, "PikaHub Started");

  slash::Status floyd_status;
  std::string self = options_.local_ip + ":" +
    std::to_string(options_.port);
  int floyd_error = 0;
  std::string value;

  rocksutil::Info(options_.info_log, "Wait[1] for floyd electing leader...");
  while (!should_exit_ && !floyd_->HasLeader()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  if (!should_exit_) {
    rocksutil::Info(options_.info_log, "floyd leader elected[1]");
  }

  while (!should_exit_) {
    std::this_thread::sleep_for(std::chrono::seconds(3));

    value.clear();
    /*
     *  1. Read the lease info;
     */
    floyd_status = floyd_->Read(kLeaseKey, &value);
    bool try_update_lease = false;
    uint64_t now = env_->NowMicros();
    if (floyd_status.ok()) {
      DecodeLease(value, &primary_, &primary_lease_deadline_);
      if ((primary_ != self && primary_lease_deadline_ < now) ||
         (primary_ == self && primary_lease_deadline_ >= now)) {
        try_update_lease = true;
      }
    } else if (floyd_status.IsNotFound()) {
      try_update_lease = true;
    } else {
      rocksutil::Warn(options_.info_log, "Read[1] error once[%d]: %s",
          floyd_error + 1, floyd_status.ToString().c_str());
      if (++floyd_error > kMaxFloydErrorTimes) {
        rocksutil::Error(options_.info_log,
            "Floyd error too many times, become secondary");
        BecomeSecondary();
      }
      continue;
    }
    floyd_error = 0;
    /*
     *  2. if try_update_lease != true, continue;
     */
    if (!try_update_lease) {
      continue;
    }

    if (!is_primary_) {
      rocksutil::Info(options_.info_log, "lease expired or no primary for now,"
          " detail, primary: %s,"
          " primary_lease_deadline: %lu, now: %lu. Start [take over process].",
          primary_.c_str(), primary_lease_deadline_, now);
    }
    /*
     *  3. update lease info, TryLock first
     */
    floyd_status = floyd_->TryLock(kLockName, self, kLockDuration);
    if (floyd_status.IsCorruption() &&
        floyd_status.ToString() == "Corruption: Lock Error") {
      rocksutil::Info(options_.info_log, "Lock is hold by other pika_hub");
      // floyd operation success
      floyd_error = 0;
      continue;
    } else if (!floyd_status.ok()) {
      rocksutil::Warn(options_.info_log, "Lock error once[%d]: %s",
          floyd_error + 1, floyd_status.ToString().c_str());
      if (++floyd_error > kMaxFloydErrorTimes) {
        rocksutil::Error(options_.info_log,
            "Floyd error too many times, become secondary");
        BecomeSecondary();
        floyd_->UnLock(kLockName, self);
      }
      continue;
    }
    floyd_error = 0;
    if (!is_primary_) {
      rocksutil::Info(options_.info_log,
          "[Take over process 3-1]TryLock success");
    }
    /*
     *  4. Read the lease info again;
     */
    value.clear();
    bool update_lease = false;
    floyd_status = floyd_->Read(kLeaseKey, &value);
    now = env_->NowMicros();
    if (floyd_status.ok()) {
      DecodeLease(value, &primary_, &primary_lease_deadline_);
      if ((primary_ != self && primary_lease_deadline_ < now) ||
         (primary_ == self && primary_lease_deadline_ > now)) {
        EncodeLease(&value, self, now + kLeaseDuration * 1000000);
        update_lease = true;
      }
    } else if (floyd_status.IsNotFound()) {
      EncodeLease(&value, self, now + kLeaseDuration * 1000000);
      update_lease = true;
    } else {
      rocksutil::Warn(options_.info_log, "Read[2] error once[%d]: %s",
          floyd_error + 1, floyd_status.ToString().c_str());
      if (++floyd_error > kMaxFloydErrorTimes) {
        rocksutil::Error(options_.info_log,
            "Floyd error too many times, become secondary");
        BecomeSecondary();
        floyd_->UnLock(kLockName, self);
      }
      continue;
    }
    floyd_error = 0;
    if (!is_primary_) {
      rocksutil::Info(options_.info_log, "[Take over process 3-2]Read success");
    }
    /*
     *  5. update the lease info
     */
    if (update_lease) {
      floyd_status = floyd_->Write(kLeaseKey, value);
      if (!floyd_status.ok()) {
        rocksutil::Warn(options_.info_log, "Write error once[%d]: %s",
            floyd_error + 1, floyd_status.ToString().c_str());
        if (++floyd_error > kMaxFloydErrorTimes) {
          rocksutil::Error(options_.info_log,
              "Floyd error too many times, become secondary");
          BecomeSecondary();
          floyd_->UnLock(kLockName, self);
        }
        continue;
      }

      if (!is_primary_) {
        floyd_->UnLock(kLockName, self);
        BecomePrimary();
        rocksutil::Info(options_.info_log,
            "[Take over process 3-3]Write & UnLock success");
      }
    }
    floyd_error = 0;
    /*
     *  The code below is only executed by primary pika_hub
     *  6. save the recover_offset
     */
    if (is_primary_) {
      bool success = true;
      for (auto iter = recover_offset_.begin(); iter != recover_offset_.end();
          iter++) {
        EncodeOffset(&value, iter);
        floyd_status = floyd_->Write(std::to_string(iter->first),
            value);
        if (!floyd_status.ok()) {
          rocksutil::Warn(options_.info_log,
              "Write %d offset to floyd failed %s",
              floyd_status.ToString().c_str());
          success = false;
        }
      }
      if (success) {
        last_success_save_offset_time_ = std::chrono::system_clock::now();
      }
    }
  }
  delete this;
  return slash::Status::OK();
}

bool PikaHubServer::IsValidInnerClient(int fd, const std::string& ip) {
  if (!is_primary_) {
    rocksutil::Warn(options_.info_log, "Check IP: %s failed[not primary]",
        ip.c_str());
    return false;
  }
  rocksutil::MutexLock l(&pika_mutex_);
  for (auto iter = pika_servers_.begin(); iter != pika_servers_.end();
      iter++) {
    if (iter->second.ip == ip && iter->second.sync_status == kConnected) {
      rocksutil::Info(options_.info_log, "Check IP: %s success", ip.c_str());
      iter->second.rcv_fd_num++;
      return true;
    }
  }
  rocksutil::Warn(options_.info_log, "Check IP: %s failed[invalid ip]",
      ip.c_str());
  return false;
}

void PikaHubServer::ResetRcvFd(int fd, const std::string& ip_port) {
  std::string ip;
  int unuse_port = 0;
  /*
   * find relevant items in pika_servers_ and insert its sender pointer
   * into a temp map [senders], because destroy a sender may need some time,
   * we do it later out of the lock scope
   */
  {
  rocksutil::MutexLock l(&pika_mutex_);
  for (auto iter = pika_servers_.begin(); iter != pika_servers_.end(); iter++) {
    slash::ParseIpPortString(ip_port, ip, unuse_port);
    if (iter->second.ip == ip) {
      rocksutil::Info(options_.info_log, "Reset receive fd: %s",
          ip_port.c_str());
      iter->second.rcv_fd_num--;
    }
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
        ", sync_status:" + std::to_string(iter->second.sync_status) +
        ", receive_fd_num:" + std::to_string(iter->second.rcv_fd_num) +
        ", recv_offset:" + std::to_string(iter->second.rcv_number) +
        ":" + std::to_string(iter->second.rcv_offset) +
        ", send_fd:" + std::to_string(iter->second.send_fd) +
        ", send_offset:" + std::to_string(iter->second.send_number) +
        ":" + std::to_string(iter->second.send_offset) +
        ", heartbeat_fd:" + std::to_string(iter->second.hb_fd) +
        "\r\n");
  }
  res += "## Saved recover offset\r\n";
  char buf[64];
  std::time_t tt = std::chrono::system_clock::to_time_t(
        last_success_save_offset_time_);
  res += "last_success_save_offset_time:" + std::string(ctime_r(&tt, buf));
  for (auto iter = recover_offset_.begin(); iter != recover_offset_.end();
      iter++) {
    res += std::to_string(iter->first) + ":";
    for (auto it = iter->second.begin(); it != iter->second.end(); it++) {
      res += std::to_string(it->first) + "-" + std::to_string(it->second) + " ";
    }
    res += "\r\n";
  }
  return res;
}

void PikaHubServer::UpdateRcvOffset(int32_t server_id,
    int32_t number, int64_t offset) {
  rocksutil::MutexLock l(&pika_mutex_);
  auto iter = pika_servers_.find(server_id);
  if (iter != pika_servers_.end()) {
    if (static_cast<uint64_t>(number) < iter->second.rcv_number) {
      return;
    }
    iter->second.rcv_number = number;
    iter->second.rcv_offset = offset;
  }
}

void PikaHubServer::GetBinlogWriterOffset(uint64_t* number,
    uint64_t* offset) {
  rocksutil::MutexLock l(&pika_mutex_);
  *number = binlog_writer_->number();
  *offset = binlog_writer_->GetOffsetInFile();
}

void PikaHubServer::DisconnectPika(int32_t server_id, bool reconnect) {
  BinlogSender* sender = nullptr;
  // Heartbeat* hb = nullptr;
  /*
   * find relevant items in pika_servers_ and set its sender & hb pointer
   * to a temp sender & hb, because destroy a sender & hb may need some time,
   * we do it later out of the lock scope
   */
  {
  rocksutil::MutexLock l(&pika_mutex_);
  auto iter = pika_servers_.find(server_id);
  if (iter != pika_servers_.end()) {
    sender = static_cast<BinlogSender*>(iter->second.sender);
    // may cause deadlock, so I comment it
    // hb = static_cast<Heartbeat*>(iter->second.heartbeat);
  }
  }

  /*
   *  Destroy binlog senders & hb out of the lock scope;
   */
  delete sender;
  // delete hb;

  /*
   *  modify relevant items status[sender pointer & hb & kShouldConnect]
   */
  {
  rocksutil::MutexLock l(&pika_mutex_);
  auto iter = pika_servers_.find(server_id);
  if (iter != pika_servers_.end()) {
    iter->second.send_fd = -1;
    iter->second.sender = nullptr;
    iter->second.hb_fd = -1;
    iter->second.heartbeat = nullptr;
    if (reconnect) {
      iter->second.sync_status = kShouldConnect;
    }
  }
  }
}

bool PikaHubServer::Transfer(const std::string& server_id,
    const std::string& new_ip,
    const int32_t new_port,
    std::string* result) {
  result->clear();
  int32_t id = std::atoi(server_id.c_str());
  std::string value;
  {
  rocksutil::MutexLock l(&pika_mutex_);
  auto iter = pika_servers_.find(id);
  if (iter == pika_servers_.end()) {
    *result = "server_id " + server_id +
      " is not found in pika_servers";
    return false;
  }
  if (iter->second.sync_status == kConnected) {
    *result = "server_id " + server_id +
      " is online, could not transfer";
    return false;
  }
  iter->second.ip = new_ip;
  iter->second.port = new_port;
  }
  return true;
}

bool PikaHubServer::Copy(const std::string& src_server_id,
    const std::string& new_server_id,
    const std::string& new_ip,
    const int32_t new_port,
    const std::string& passwd,
    std::string* result) {
  result->clear();
  int32_t src_id = std::atoi(src_server_id.c_str());
  int32_t new_id = std::atoi(new_server_id.c_str());

  PikaStatus status;
  status.server_id = new_id;
  status.ip = new_ip;
  status.port = new_port;
  status.passwd = passwd;

  std::string value;
  {
  rocksutil::MutexLock l(&pika_mutex_);
  auto src_iter = pika_servers_.find(src_id);
  if (src_iter == pika_servers_.end()) {
    *result = "src_server_id " + src_server_id +
      " is not found in pika_servers";
    return false;
  }
  auto new_iter = pika_servers_.find(new_id);
  if (new_iter != pika_servers_.end()) {
    *result = "new_server_id " + new_server_id +
      " is already exist in pika_servers";
    return false;
  }
  status.rcv_number = src_iter->second.rcv_number;
  status.rcv_offset = src_iter->second.rcv_offset;
  status.send_number = src_iter->second.send_number;
  status.send_offset = src_iter->second.send_offset;

  auto recover_iter = recover_offset_.find(new_id);
  if (recover_iter != recover_offset_.end()) {
//    *result = "new_server_id " + new_server_id +
//      " is already exist in recover_offset";
//    return false;
    rocksutil::Warn(options_.info_log, "copy: new server id %d is"
        "already exist in recover_offset, overwrite", new_id);
  }
  std::map<int32_t, std::atomic<int32_t> > new_id_map;
  for (recover_iter = recover_offset_.begin();
      recover_iter != recover_offset_.end(); recover_iter++) {
    if (recover_iter->first != src_id) {
      recover_iter->second[new_id].store(recover_iter->second[src_id]);
      new_id_map[recover_iter->first].store(
        recover_offset_[src_id][recover_iter->first]);
    } else {
      recover_iter->second[new_id].store(src_iter->second.rcv_number);
      new_id_map[recover_iter->first].store(
          src_iter->second.rcv_number);
    }
  }

  recover_offset_.insert(RecoverOffsetMap::value_type(new_id,
        std::move(new_id_map)));
  pika_servers_.insert(PikaServers::value_type(new_id, status));
  }
  return true;
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
    if (pos == std::string::npos) {
      strncpy(token, str.data() + prev_pos, str.size() - prev_pos);
    } else {
      strncpy(token, str.data() + prev_pos, pos - prev_pos);
    }

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

    pika_servers_.insert(PikaServers::
                      value_type(server_id, status));

    prev_pos = str.find_first_not_of(',', pos);
    pos = str.find_first_of(',', prev_pos);
  }

  return true;
}

bool PikaHubServer::RecoverOffset() {
  rocksutil::Info(options_.info_log, "Wait[2] for floyd electing leader...");
  while (!should_exit_ && !floyd_->HasLeader()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  if (should_exit_) {
    rocksutil::Info(options_.info_log, "RecoverOffset, exit");
    return false;
  }
  rocksutil::Info(options_.info_log, "floyd leader elected[2]");

  /*
   * We call RecoverOffset before starting, so we could modify
   * pika_servers_ without lock protect
   */
  slash::Status s;
  std::string value;
  rocksutil::Info(options_.info_log, "RecoverOffset result:");
  rocksutil::Info(options_.info_log, "--------------------");
  for (auto iter = pika_servers_.begin(); iter != pika_servers_.end();
      iter++) {
    s = floyd_->Read(std::to_string(iter->first), &value);
    if (s.IsNotFound()) {
      continue;
    }
    if (!s.ok()) {
      rocksutil::Error(options_.info_log,
          "RecoverOffset, read floyd error: %s", s.ToString().c_str());
      return false;
    }
    DecodeOffset(value,
        &(iter->second.rcv_number), &(iter->second.rcv_offset));
  }
  rocksutil::Info(options_.info_log, "--------------------");

  return true;
}

void PikaHubServer::EncodeOffset(std::string* value,
    const RecoverOffsetMap::iterator& iter) {
  value->clear();
  rocksutil::PutFixed32(value, iter->first);
  rocksutil::PutFixed32(value, iter->second.size());
  for (auto it = iter->second.begin(); it != iter->second.end();
      it++) {
    rocksutil::PutFixed32(value, it->first);
    rocksutil::PutFixed32(value, it->second);
  }
}

void PikaHubServer::DecodeOffset(const std::string& value,
    uint64_t* rcv_number, uint64_t* rcv_offset) {
  int32_t pos = 0;
  int32_t src_server_id = rocksutil::DecodeFixed32(value.data() + pos);
  pos += 4;
  int32_t num = rocksutil::DecodeFixed32(value.data() + pos);
  pos += 4;
  std::string info_log_content = std::to_string(src_server_id) + ": ";
  int32_t dest_server_id = -1;
  int32_t number = -1;
  for (int i = 0; i < num; i++) {
    dest_server_id = rocksutil::DecodeFixed32(value.data() + pos);
    pos += 4;
    number = rocksutil::DecodeFixed32(value.data() + pos);
    pos += 4;
    info_log_content += std::to_string(dest_server_id) + "-" +
      std::to_string(number) + " ";
    recover_offset_[src_server_id][dest_server_id] = number;
    if (*rcv_number == 0 ||
        static_cast<uint64_t>(number) < *rcv_number) {
      *rcv_number = number;
    }
  }
  info_log_content += "\n";
  rocksutil::Info(options_.info_log, info_log_content.c_str());
  *rcv_offset = 0;
}

slash::Status PikaHubServer::BecomePrimary() {
  rocksutil::Info(options_.info_log, "BecomePrimary start");
  rocksutil::Info(options_.info_log, "BecomePrimary-1: set primary identify");
  is_primary_ = true;
  last_success_save_offset_time_ = std::chrono::system_clock::now();

  rocksutil::Info(options_.info_log, "BecomePrimary-2: recover offset");
  RecoverOffset();
  if (should_exit_) {
    return slash::Status::OK();
  }

  rocksutil::Info(options_.info_log,
      "BecomePrimary-3: create new binlog_writer");
  binlog_writer_ = binlog_manager_->AddWriter();

  rocksutil::Info(options_.info_log,
      "BecomePrimary-4: start inner_server thread");
  int ret = inner_server_thread_->StartThread();
  if (ret != 0) {
    rocksutil::Error(options_.info_log,
        "BecomePrimary-4: start inner_server thread error");
    return slash::Status::Corruption("Start inner_server error");
  }

  rocksutil::Info(options_.info_log,
      "BecomePrimary-5: create & start trysync thread");
  trysync_thread_ = new PikaHubTrysync(options_.info_log, options_.local_ip,
      options_.port, &pika_servers_, &pika_mutex_, &recover_offset_,
      binlog_manager_);
  ret = trysync_thread_->StartThread();
  if (ret != 0) {
    rocksutil::Error(options_.info_log,
        "BecomePrimary-5: start trysync thread error");
    return slash::Status::Corruption("Start trysync thread error");
  }

  rocksutil::Info(options_.info_log, "BecomePrimary done");
  return slash::Status::OK();
}

void PikaHubServer::BecomeSecondary() {
  if (!is_primary_) {
    // alread been secondary
    return;
  }
  rocksutil::Info(options_.info_log, "BecomeSecondary start");
  rocksutil::Info(options_.info_log,
      "BecomeSecondary-1: reset primary identify");
  is_primary_ = false;
  rocksutil::Info(options_.info_log,
      "BecomeSecondary-2: delete trysync thread");
  delete trysync_thread_;
  trysync_thread_ = nullptr;
  rocksutil::Info(options_.info_log,
      "BecomeSecondary-3: reset pika_servers offset");
  {
  rocksutil::MutexLock l(&pika_mutex_);
  for (auto iter = pika_servers_.begin(); iter != pika_servers_.end();
      iter++) {
    iter->second.rcv_number = 0;
    iter->second.rcv_offset = 0;
    iter->second.send_number = 0;
    iter->second.send_offset = 0;
    iter->second.sync_status = kShouldConnect;
  }
  }
  rocksutil::Info(options_.info_log, "BecomeSecondary-4: reset recover offset");
  for (auto iter = recover_offset_.begin(); iter != recover_offset_.end();
      iter++) {
    for (auto it = iter->second.begin(); it != iter->second.end(); it++) {
      it->second = 0;
    }
  }
  rocksutil::Info(options_.info_log,
      "BecomeSecondary-5: stop inner_server thread");
  inner_server_thread_->StopThread();
  rocksutil::Info(options_.info_log, "BecomeSecondary-6: reset binlog_writer");
  delete binlog_writer_;
  binlog_writer_ = nullptr;
  rocksutil::Info(options_.info_log,
      "BecomeSecondary-7: reset binlog_manager offset & binlog");
  binlog_manager_->ResetOffsetAndBinlog();
  primary_ = "NULL";
  rocksutil::Info(options_.info_log, "BecomeSecondary-8: reset primary");
  rocksutil::Info(options_.info_log, "BecomeSecondary done");
}

void PikaHubServer::EncodeLease(std::string* value,
    const std::string& holder, const uint64_t deadline) {
  value->clear();
  rocksutil::PutFixed64(value, deadline);
  value->append(holder);
}

void PikaHubServer::DecodeLease(const std::string& value,
    std::string* holder, uint64_t* lease_deadline) {
  *lease_deadline = rocksutil::DecodeFixed64(value.data());
  *holder = std::string(value.data() + 8);
}
