//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>

#include "src/pika_hub_inner_client_conn.h"
#include "src/pika_hub_server.h"
#include "slash/include/slash_string.h"

extern PikaHubServer* g_pika_hub_server;

void PikaHubInnerClientConn::DoCmd(const std::string& opt) {
  // Get command info
  const CmdInfo* const cinfo_ptr = GetCmdInfo(opt);
  Cmd* c_ptr = GetCmdFromTable(opt, *cmds_table_);
  if (!cinfo_ptr || !c_ptr) {
    return;
  }
  // Initial
  c_ptr->Initial(argv_, cinfo_ptr);
  if (!c_ptr->res().ok()) {
    return;
  }

  c_ptr->Do();
}

int PikaHubInnerClientConn::DealMessage() {
  g_pika_hub_server->PlusQueryNum();

  std::string opt = argv_[0];
  slash::StringToLower(opt);
  DoCmd(opt);

  return 0;
}
