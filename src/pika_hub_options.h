#ifndef PIKA_HUB_OPTIONS_H_
#define PIKA_HUB_OPTIONS_H_

#include "rocksutil/auto_roll_logger.h"

struct PikaHubOptions {
  int port = 9221;

  std::string info_log_path = "./info_log";
  std::shared_ptr<rocksutil::Logger> info_log = nullptr;
  size_t max_log_file_size = 0;
  size_t log_file_time_to_roll = 0;
  rocksutil::InfoLogLevel info_log_level = rocksutil::INFO_LEVEL;

  rocksutil::Env* env = rocksutil::Env::Default();
};

struct Options : public PikaHubOptions, public floyd::Options {
  Options() : PikaHubOptions(), floyd::Options() {};
  std::string str_members;
  void Dump(rocksutil::Logger* log) const {
    Header(log, "-------------------Dump Options------------------------");
    Header(log, "Hub:");
    Header(log, " port = %d", port);
    Header(log, " info_log_path = %s", info_log_path.c_str());
    Header(log, " max_log_file_size = %u", max_log_file_size);
    Header(log, " log_file_time_to_roll = %u", log_file_time_to_roll);
    Header(log, " info_log_level = %d", info_log_level);
    Header(log, "");
    Header(log, "Floyd:");
    Header(log, " members = %s", str_members.c_str());
    Header(log, " local_port = %d", local_port);
    Header(log, " data_path = %s", data_path.c_str());
    Header(log, " log_path = %s", log_path.c_str());
    Header(log, " elect_timeout_ms = %lu", elect_timeout_ms);
    Header(log, " heartbeat_us = %lu", heartbeat_us);
    Header(log, " append_entries_size_once = %lu", append_entries_size_once);
    Header(log, " append_entries_count_once = %lu", append_entries_count_once);
    Header(log, " single_mode = %d", single_mode);
    Header(log, "-------------------Dump Options Done-------------------");
  };
};
#endif
