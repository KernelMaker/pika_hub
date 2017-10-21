//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <sstream>
#include <string>
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

  info.append(tmp_stream.str());

  res_.AppendStringLen(info.size());
  res_.AppendContent(info);
  return;
}
