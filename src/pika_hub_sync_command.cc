//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <sstream>
#include <string>

#include "src/pika_hub_sync_command.h"
#include "src/pika_hub_server.h"
#include "src/pika_hub_common.h"
#include "slash/include/slash_string.h"
#include "rocksutil/coding.h"


extern PikaHubServer *g_pika_hub_server;

void SetCmd::DoInitial(const PikaCmdArgsType &argv,
    const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSet);
    return;
  }
  if (argv[3] != kBinlogMagic) {
    res_.SetRes(CmdRes::kInvalidMagic, kCmdNameSet);
    return;
  }
  key_ = argv[1];
  value_ = argv[2];
  slash::string2l(argv[4].data(), argv[4].size(), &server_id_);
  exec_time_ = rocksutil::DecodeFixed32(argv[5].data());
  number_ = rocksutil::DecodeFixed32(argv[5].data() + 4);
  offset_ = rocksutil::DecodeFixed64(argv[5].data() + 8);
}

void SetCmd::Do() {
  std::cout << "set " << key_ << " " << value_ << " " <<
    server_id_ << " " << exec_time_ << " " << number_ << " " <<
    offset_ << std::endl;
  return;
}
