#ifndef PIKA_HUB_BINLOG_MANAGER_H_
#define PIKA_HUB_BINLOG_MANAGER_H_

#include "pika_hub_binlog_writer.h"
#include "pika_hub_binlog_reader.h"
#include <map>

class BinlogManager {
 public:
   BinlogManager(const std::string& log_path,
      rocksutil::Env* env, BinlogWriter* writer,
      uint64_t number)
   : log_path_(log_path), env_(env),
     number_(number), offset_(0),
     writer_(writer) {
   };

   ~BinlogManager() {
     delete writer_;
     for(auto& reader : readers_) {
       delete reader.second;
     }
   }

   rocksutil::Status Append(const std::string& str);

   void UpdateOffset();
   
 private:
   std::string log_path_;
   rocksutil::Env* env_;
   uint64_t number_;
   uint64_t offset_;
   BinlogWriter* writer_;
   std::map<std::string, BinlogReader*> readers_;
   rocksutil::port::Mutex mutex_;
};

extern BinlogManager* CreateBinlogManager(const std::string& log_path,
    rocksutil::Env* env);
  
#endif
