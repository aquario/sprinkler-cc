#include <gflags/gflags.h>
#include <glog/logging.h>

#include "transport_layer.h"

DEFINE_int32(id, 0, "ID of the Sprinkler node.");
DEFINE_int32(listen_port, 0, "Port number for incoming connections.");
DEFINE_int32(peer_port, 0, "Port number for peer node.");

TransportLayer *tl;
bool sent, received;

void outgoing(SprinklerSocket *ss) {
  LOG(INFO) << "outgoing";
  tl->async_send_message(ss, "hello", 5, true, NULL, NULL);
  sent = true;
}

void deliver(SprinklerSocket *ss,
    const char *data, int size, void (*release)(void *), void *arg) {
  CHECK_STREQ(data, "hello");
  LOG(INFO) << "deliver: got " << size << " bytes: " << data;
  received = true;
  (*release)(arg);
}

void TestWelcomeMessage() {
  CHECK_GT(FLAGS_id, 0);
  CHECK_NE(FLAGS_listen_port, FLAGS_peer_port);
  CHECK_GT(FLAGS_listen_port, 9000);
  CHECK_LT(FLAGS_listen_port, 65536);
  CHECK_GT(FLAGS_peer_port, 9000);
  CHECK_LT(FLAGS_peer_port, 65536);

  sent = received = false;

  tl = new TransportLayer(FLAGS_id, outgoing, deliver);
  tl->tl_listen(FLAGS_listen_port);
  tl->register_peer("localhost", FLAGS_peer_port);

  while (!sent || !received) {
    tl->wait(1000);
  }

  dfree(tl);
}

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  TestWelcomeMessage();
  return 0;
}
