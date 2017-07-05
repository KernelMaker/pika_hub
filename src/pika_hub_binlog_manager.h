#ifndef PIKA_HUB_BINLOG_MANAGER_H_
#define PIKA_HUB_BINLOG_MANAGER_H_

#include "pika_hub_binlog_writer.h"
#include "pika_hub_binlog_reader.h"
#include "rport/port.h"
#include <map>

class BinlogManager {
 public:
   BinlogManager(const std::string& log_path,
      rocksutil::Env* env,
      uint64_t number)
   : log_path_(log_path), env_(env),
     number_(number), offset_(0),
     cv_(&mutex_) {
   };

   BinlogWriter* AddWriter();
   BinlogReader* AddReader(uint64_t number, uint64_t offset);

   rocksutil::port::Mutex* mutex() {
     return &mutex_;
   }

   rocksutil::port::CondVar* cv() {
     return &cv_;
   }

   void UpdateWriterOffset(uint64_t number, uint64_t offset);
   void GetWriterOffset(uint64_t* number, uint64_t* offset);
   
 private:
   std::string log_path_;
   rocksutil::Env* env_;
   uint64_t number_;
   uint64_t offset_;
   rocksutil::port::Mutex mutex_;
   rocksutil::port::CondVar cv_;
};

extern BinlogManager* CreateBinlogManager(const std::string& log_path,
    rocksutil::Env* env);
  
#endif
