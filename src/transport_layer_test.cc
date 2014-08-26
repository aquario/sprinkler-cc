#include <gflags/gflags.h>
#include <glog/logging.h>

#include "transport_layer.h"

DEFINE_int(id, 0, "ID of the Sprinkler node.");
DEFINE_bool(is_sender, true, "Specify the role of this test agent, either"
                             " a sender or a receiver.");

void outgoing(struct l1_global *l1, struct l1_socket *ls) {
  LOG(INFO) << "outgoing";
  l1_send(l1, ls, "hello", 5, 0, 0);
}

void deliver(struct l1_global *l1, struct l1_socket *ls,
    const char *data, int size, void (*release)(void *), void *arg) {
  LOG(INFO) << "deliver: got " << size << " bytes: " << data;
  (*release)(arg);
}

int main() {
  TransportLayer tl(id, outgoing, deliver);
  
  return 0;
}
