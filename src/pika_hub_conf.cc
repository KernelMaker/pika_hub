//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/pika_hub_conf.h"
#include <string>
#include <algorithm>

PikaHubConf::PikaHubConf(const std::string& conf_path)
  : slash::BaseConf(conf_path), conf_path_(conf_path) {
}

int PikaHubConf::Load() {
  int ret = LoadConf();
  if (ret != 0) {
    return ret;
  }
  GetConfStr("floyd-servers", &floyd_servers_);
  GetConfStr("floyd-local-ip", &floyd_local_ip_);
  GetConfInt("floyd-local-port", &floyd_local_port_);
  GetConfStr("floyd-path", &floyd_path_);
  GetConfInt("sdk-port", &sdk_port_);
  GetConfStr("conf-path", &conf_path_);
  GetConfStr("log-path", &log_path_);
  GetConfInt("max-log-file-size", &max_log_file_size_);
  GetConfInt("log-file-time-to-roll", &log_file_time_to_roll_);
  GetConfInt("info-log-level", &info_log_level_);
  GetConfStr("pika-servers", &pika_servers_);

  std::string str;
  GetConfStr("daemonize", &str);
  std::transform(str.begin(), str.end(),
      str.begin(), ::tolower);
  daemonize_ = str == "yes" ? true : false;

  str.clear();
  GetConfStr("binlog-offset-absolute-consistency", &str);
  std::transform(str.begin(), str.end(),
      str.begin(), ::tolower);
  binlog_offset_absolute_consistency_ = str == "yes" ? true : false;

  GetConfStr("pidfile", &pidfile_);
  return 0;
}
