//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>

#include "src/pika_hub_client_conn.h"
#include "src/pika_hub_server.h"
#include "slash/include/slash_string.h"

extern PikaHubServer* g_pika_hub_server;

std::string PikaHubClientConn::DoCmd(const std::string& opt) {
  // Get command info
  const CmdInfo* const cinfo_ptr = GetCmdInfo(opt);
  Cmd* c_ptr = GetCmdFromTable(opt, *cmds_table_);
  if (!cinfo_ptr || !c_ptr) {
      return "-Err unknown or unsupported command \'" + opt + "\'\r\n";
  }
  // Initial
  c_ptr->Initial(argv_, cinfo_ptr);
  if (!c_ptr->res().ok()) {
    return c_ptr->res().message();
  }

  c_ptr->Do();
  return c_ptr->res().message();
}

int PikaHubClientConn::DealMessage() {
  g_pika_hub_server->PlusQueryNum();

  std::string opt = argv_[0];
  slash::StringToLower(opt);
  std::string res = DoCmd(opt);

  if ((wbuf_size_ - wbuf_len_ < res.size())) {
    if (!ExpandWbufTo(wbuf_len_ + res.size())) {
      memcpy(wbuf_, "-ERR expand writer buffer failed\r\n", 34);
      wbuf_len_ = 34;
      set_is_reply(true);
      return 0;
    }
  }
  memcpy(wbuf_ + wbuf_len_, res.data(), res.size());
  wbuf_len_ += res.size();
  set_is_reply(true);
  return 0;
}
