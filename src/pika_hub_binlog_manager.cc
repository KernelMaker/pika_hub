//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/pika_hub_binlog_manager.h"
#include "src/pika_hub_common.h"
#include <string>
#include <vector>
#include <cstdint>

BinlogWriter* BinlogManager::AddWriter() {
  return CreateBinlogWriter(log_path_, number_,
      env_, this);
}

BinlogReader* BinlogManager::AddReader(uint64_t number,
    uint64_t offset) {
  return CreateBinlogReader(log_path_, env_,
      number, offset, this);
}

void BinlogManager::UpdateWriterOffset(uint64_t number,
    uint64_t offset) {
  number_ = number;
  offset_ = offset;
}

void BinlogManager::GetWriterOffset(uint64_t* number,
    uint64_t* offset) {
  *number = number_;
  *offset = offset_;
}

BinlogManager* CreateBinlogManager(const std::string& log_path,
    rocksutil::Env* env, std::shared_ptr<rocksutil::Logger> info_log) {
  std::vector<std::string> result;
  rocksutil::Status s = env->GetChildren(log_path, &result);

  if (!s.ok()) {
    return nullptr;
  }

  std::string prefix;
  for (auto& file : result) {
    prefix = file.substr(0, strlen(kBinlogPrefix));
    if (prefix == kBinlogPrefix) {
      s = env->DeleteFile(log_path + "/" + file);
    }
  }

  return new BinlogManager(log_path, env, info_log);
}
