#include "sprinkler_node.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

void SprinklerNode::outgoing(SprinklerSocket *ss) {
}

void SprinklerNode::deliver(SprinklerSocket *ss,
    const char *, int, void (*)(void *), void *) {
}

int main() {
  SprinklerNode s(0, 0);
}
