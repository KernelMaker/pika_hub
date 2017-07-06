//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_PIKA_HUB_COMMON_H_
#define SRC_PIKA_HUB_COMMON_H_

struct ConnPrivateData {
};

const char kBinlogPrefix[] = "binlog_";
const int32_t kMaxBinlogFileSize = 100 * 1024 * 1024;

#endif  // SRC_PIKA_HUB_COMMON_H_
