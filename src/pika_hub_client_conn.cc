#include "pika_hub_client_conn.h"
#include "pika_hub_server.h"
#include "slash/include/slash_string.h"

extern PikaHubServer* g_pika_hub_server;

int PikaHubClientConn::DealMessage() {
  g_pika_hub_server->PlusQueryNum();
  uint64_t last_qps = g_pika_hub_server->last_qps();
  uint64_t query_num = g_pika_hub_server->query_num();
  
  char len_buf[32];
  char buf[32];
  int len = slash::ll2string(buf, sizeof(buf), last_qps);
  slash::ll2string(len_buf, sizeof(len_buf), len);

  std::string res = "*2\r\n$";
  res.append(len_buf);
  res.append("\r\n");
  res.append(buf);
  res.append("\r\n$");

  len = slash::ll2string(buf, sizeof(buf), query_num);
  slash::ll2string(len_buf, sizeof(len_buf), len);

  res.append(len_buf);
  res.append("\r\n");
  res.append(buf);
  res.append("\r\n");

  memcpy(wbuf_ + wbuf_len_, res.data(), res.size());
  wbuf_len_ += res.size();
  set_is_reply(true);

  return 0;
}
