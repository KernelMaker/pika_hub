#ifndef PIKA_HUB_BINLOG_WRITER_H_
#define PIKA_HUB_BINLOG_WRITER_H_

#include "rocksutil/log_writer.h"
#include "rocksutil/mutexlock.h"
#include "rocksutil/env.h"

class BinlogWriter {
 public:
   BinlogWriter(rocksutil::log::Writer* writer,
      uint64_t number, const std::string& log_path,
      rocksutil::Env* env)
   : writer_(writer), log_path_(log_path),
   number_(number), env_(env) {
   };

   rocksutil::Status Append(const std::string& str);
   
 private:
   void RollFile();
   rocksutil::log::Writer* writer_;
   std::string log_path_;
   uint64_t number_;
   rocksutil::Env* env_;
   rocksutil::port::Mutex mutex_;
};

extern BinlogWriter* CreateBinlogWriter(const std::string& log_path,
    rocksutil::Env* env);
  
#endif
