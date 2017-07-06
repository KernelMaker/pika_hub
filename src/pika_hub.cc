//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <getopt.h>
#include <signal.h>

#include "src/pika_hub_server.h"


void Usage();
const struct option long_options[] = {
  {"servers", required_argument, NULL, 's'},
  {"local_ip", required_argument, NULL, 'i'},
  {"local_port", required_argument, NULL, 'p'},
  {"sdk_port", required_argument, NULL, 'P'},
  {"data_path", required_argument, NULL, 'd'},
  {"log_path", required_argument, NULL, 'l'},
  {NULL, 0, NULL, 0}, };

const char* short_options = "s:i:p:d:l:";

PikaHubServer* g_pika_hub_server;
std::shared_ptr<rocksutil::Logger> g_log;

void IntSigHandle(int sig) {
  printf("Catch Signal %d, cleanup...\n", sig);
  g_pika_hub_server->Unlock();
}

void SignalSetup() {
  signal(SIGPIPE, SIG_IGN);
  if (signal(SIGINT, &IntSigHandle) == SIG_ERR) {
    printf("Catch SignalInt error\n");
  }
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

int main(int argc, char** argv) {
  if (argc < 12) {
    printf("Usage:\n"
      " ./main --servers ip1:port1,ip2:port2 --local_ip ip --local_port port\n"
      "   --sdk_port sdk_port --data_path data_path --log_path log_path\n");
    exit(0);
  }

  Options options;

  int ch, longindex;
  int server_port = 9221;

  while ((ch = getopt_long(argc, argv, short_options, long_options,
                           &longindex)) >= 0) {
    switch (ch) {
      case 's':
        options.str_members = std::string(optarg);
        break;
      case 'i':
        options.local_ip = optarg;
        break;
      case 'p':
        options.local_port = atoi(optarg);
        break;
      case 'P':
        server_port = atoi(optarg);
        break;
      case 'd':
        options.data_path = optarg;
        break;
      case 'l':
        options.log_path = optarg;
        break;
      default:
        break;
    }
  }

  options.port = server_port;
  options.info_log_path = "./info_log";

  SignalSetup();

  g_pika_hub_server = new PikaHubServer(options);
  g_pika_hub_server->DumpOptions();
  slash::Status s = g_pika_hub_server->Start();
  if (!s.ok()) {
    printf("Start Server error\n");
    return -1;
  }

  printf("Will Stop Server...\n");
  delete g_pika_hub_server;

  return 0;
}
