#include "sprinkler_node.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

void SprinklerNode::outgoing(SprinklerSocket *ss) {

}

void SprinklerNode::deliver(SprinklerSocket *ss, const char *data, int size,
    std::function<void(void *)> release, void *env) {
  CHECK_NOTNULL(data);
  CHECK_GT(size, 1);

  MessageTypes msg_type = static_cast<MessageTypes>(data[0]);
  switch (msg_type) {
    case M_WEL:
      break;
    case M_ADV:
      break;
    case M_SUB:
      break;
    case M_UNSUB:
      break;
    case M_DATA:
      break;
    default:
      LOG(ERROR) << "Unrecognized message type: " << msg_type;
  }

  release(env);
}

int main() {
  SprinklerNode s(0, 0, 0);
}
