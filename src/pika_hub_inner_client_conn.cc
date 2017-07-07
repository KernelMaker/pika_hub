//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>

#include "src/pika_hub_inner_client_conn.h"
#include "src/pika_hub_server.h"
#include "slash/include/slash_string.h"

extern PikaHubServer* g_pika_hub_server;

int PikaHubInnerClientConn::DealMessage() {
  g_pika_hub_server->PlusQueryNum();

  std::cout << argv_[0] << " " << argv_.size() << std::endl;
  return 0;
}
