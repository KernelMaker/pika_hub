//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <sstream>
#include <string>
#include <set>
#include <ctime>

#include "src/pika_hub_admin.h"
#include "src/pika_hub_server.h"
#include "src/pika_hub_conf.h"
#include "src/pika_hub_version.h"
#include "src/build_version.h"

extern PikaHubServer *g_pika_hub_server;
extern PikaHubConf *g_pika_hub_conf;

void PingCmd::DoInitial(const PikaCmdArgsType &argv,
    const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePing);
    return;
  }
}

void PingCmd::Do() {
  res_.SetRes(CmdRes::kPong);
  return;
}

void InfoCmd::DoInitial(const PikaCmdArgsType &argv,
    const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameInfo);
    return;
  }
}

void InfoCmd::Do() {
  std::string info;

  std::stringstream tmp_stream;

  tmp_stream << "# Server\r\n";
  char version[32];
  snprintf(version, sizeof(version), "%d.%d.%d", PIKA_HUB_MAJOR,
      PIKA_HUB_MINOR, PIKA_HUB_PATCH);
  tmp_stream << "version: " << version << "\r\n";
  tmp_stream << pika_hub_build_git_sha << "\r\n";
  tmp_stream << "pika_hub_build_compile_date: " <<
    pika_hub_build_compile_date << "\r\n";
  tmp_stream << "# Stats\r\n";
  tmp_stream << "total_connections_received:" <<
    g_pika_hub_server->acc_connections() << "\r\n";
  tmp_stream << "instantaneous_ops_per_sec:" <<
    g_pika_hub_server->last_qps() << "\r\n";
  tmp_stream << "total_commands_processed:" <<
    g_pika_hub_server->query_num() << "\r\n";
  tmp_stream << "lru_cache_record_num:" <<
    g_pika_hub_server->binlog_manager()->GetLruMemUsage() << "\r\n";

  if (g_pika_hub_server->is_primary()) {
    tmp_stream << "# Info for [Primary]\r\n";
    tmp_stream << "# Pika-Hubs\r\n";
    std::set<std::string> nodes;
    g_pika_hub_server->GetAllHubServers(&nodes);
    for (auto node : nodes) {
      tmp_stream << node << "\r\n";
    }
    tmp_stream << "# Pika-Servers\r\n";
    uint64_t number = 0;
    uint64_t offset = 0;
    g_pika_hub_server->GetBinlogWriterOffset(&number, &offset);
    tmp_stream << "binlog_writer_offset:" << number <<
      ":" << offset << "\r\n";
    char buf[64];
    std::time_t tt = std::chrono::system_clock::to_time_t(
          g_pika_hub_server->last_success_save_offset_time());
    tmp_stream << "last_success_save_offset_time:" << ctime_r(&tt, buf);
    tmp_stream << g_pika_hub_server->DumpPikaServers();
  } else {
    tmp_stream << "# Info for [Secondary]\r\n";
    tmp_stream << " Primary-Info\r\n";
    uint64_t lease_deadline = g_pika_hub_server->primary_lease_deadline();
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    uint64_t now = static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
    tmp_stream << "primary: " << g_pika_hub_server->primary() << "\r\n";
    tmp_stream << "now: " << now << "\r\n";
    tmp_stream << "primary_lease_deadline: " << lease_deadline << "\r\n";
    int64_t seconds_to_expire = static_cast<int64_t>(lease_deadline / 1000000 -
        now / 1000000);
    tmp_stream << "seconds_to_expire_lease: " << seconds_to_expire << "\r\n";
  }

  info.append(tmp_stream.str());

  res_.AppendStringLen(info.size());
  res_.AppendContent(info);
  return;
}

void TransferCmd::DoInitial(const PikaCmdArgsType &argv,
    const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameTransfer);
    return;
  }
  server_id_ = argv[1];
  new_ip_ = argv[2];
  new_port_ = argv[3];
}

void TransferCmd::Do() {
  std::string result;
  int32_t port = std::atoi(new_port_.data());
  bool ret = g_pika_hub_server->Transfer(server_id_, new_ip_, port, &result);
  if (ret) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, result);
  }
}

void CopyCmd::DoInitial(const PikaCmdArgsType &argv,
    const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameCopy);
    return;
  }
  src_server_id_ = argv[1];
  new_server_id_ = argv[2];
  new_ip_ = argv[3];
  new_port_ = argv[4];
  passwd_ = argv.size() > 5 ? argv[5] : "";
}

void CopyCmd::Do() {
  std::string result;
  int32_t port = std::atoi(new_port_.data());
  bool ret = g_pika_hub_server->Copy(src_server_id_, new_server_id_,
      new_ip_, port, passwd_, &result);
  if (ret) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, result);
  }
}

void AuthCmd::DoInitial(const PikaCmdArgsType &argv,
    const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameAuth);
    return;
  }
  passwd_ = argv[1];
}

void AuthCmd::Do() {
  if (g_pika_hub_conf->requirepass().empty()) {
    res_.SetRes(CmdRes::kErrOther,
        "Client sent AUTH, but no password set");
  } else if (passwd_ != g_pika_hub_conf->requirepass()) {
    res_.SetRes(CmdRes::kInvalidPwd);
  } else {
    res_.SetRes(CmdRes::kOk);
  }
}

void AddCmd::DoInitial(const PikaCmdArgsType &argv,
    const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameAdd);
    return;
  }
  addr_ = argv[1];
}

void AddCmd::Do() {
  if (g_pika_hub_server->is_primary()) {
    bool ret = g_pika_hub_server->AddHubServer(addr_);
    if (ret) {
      res_.SetRes(CmdRes::kOk);
    } else {
      res_.SetRes(CmdRes::kErrOther,
            "Add pika hub failed");
    }
  } else {
    res_.SetRes(CmdRes::kErrOther,
        "This operation is only allowed for the primary node");
  }
  return;
}

void RemoveCmd::DoInitial(const PikaCmdArgsType &argv,
    const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRemove);
    return;
  }
  addr_ = argv[1];
}

void RemoveCmd::Do() {
  if (g_pika_hub_server->is_primary()) {
    bool ret = g_pika_hub_server->RemoveHubServer(addr_);
    if (ret) {
      res_.SetRes(CmdRes::kOk);
    } else {
      res_.SetRes(CmdRes::kErrOther,
            "Remove pika hub failed");
    }
  } else {
    res_.SetRes(CmdRes::kErrOther,
        "This operation is only allowed for the primary node");
  }
  return;
}
