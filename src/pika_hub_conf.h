//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_PIKA_HUB_CONF_H_
#define SRC_PIKA_HUB_CONF_H_

#include <string>

#include "slash/include/base_conf.h"
#include "rocksutil/mutexlock.h"

class PikaHubConf : public slash::BaseConf {
 public:
  explicit PikaHubConf(const std::string& conf_path);

  const std::string& floyd_servers() {
    rocksutil::ReadLock l(&rw_mutex_);
    return floyd_servers_;
  }
  const std::string& floyd_local_ip() {
    rocksutil::ReadLock l(&rw_mutex_);
    return floyd_local_ip_;
  }
  int floyd_local_port() {
    rocksutil::ReadLock l(&rw_mutex_);
    return floyd_local_port_;
  }
  const std::string& floyd_path() {
    rocksutil::ReadLock l(&rw_mutex_);
    return floyd_path_;
  }
  int sdk_port() {
    rocksutil::ReadLock l(&rw_mutex_);
    return sdk_port_;
  }
  const std::string& conf_path() {
    rocksutil::ReadLock l(&rw_mutex_);
    return conf_path_;
  }
  const std::string& log_path() {
    rocksutil::ReadLock l(&rw_mutex_);
    return log_path_;
  }
  int max_log_file_size() {
    rocksutil::ReadLock l(&rw_mutex_);
    return max_log_file_size_;
  }
  int log_file_time_to_roll() {
    rocksutil::ReadLock l(&rw_mutex_);
    return log_file_time_to_roll_;
  }
  int info_log_level() {
    rocksutil::ReadLock l(&rw_mutex_);
    return info_log_level_;
  }
  const std::string& pika_servers() {
    rocksutil::ReadLock l(&rw_mutex_);
    return pika_servers_;
  }

  int Load();

 private:
  std::string floyd_servers_;
  std::string floyd_local_ip_;
  int floyd_local_port_;
  std::string floyd_path_;
  int sdk_port_;
  std::string conf_path_;
  std::string log_path_;
  int max_log_file_size_;
  int log_file_time_to_roll_;
  int info_log_level_;
  std::string pika_servers_;

  rocksutil::port::RWMutex rw_mutex_;
};

#endif  // SRC_PIKA_HUB_CONF_H_
