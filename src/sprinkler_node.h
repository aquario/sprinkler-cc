#ifndef SPRINKLER_NODE_H_
#define SPRINKLER_NODE_H_

#include <stdint.h>

#include <functional>
#include <vector>

#include "transport_layer.h"

// Subscription information for a stream.
// Stream id is implicit here, since each of this struct will be referenced
// by indexing in an array.
struct SubInfo {
  // ID of the source of current subscription.
  int src;
  // The next seq# to receive.
  int64_t next_seq;
  // Max seq# ever heard on this stream.
  int64_t max_adv;

  // Constructor.
  // src = -1 means no subscription yet; sequence number starts from 1.
  SubInfo() : src(-1), next_seq(1), max_adv(0) {}
};

class SprinklerNode {
 public:
  SprinklerNode(int id, int role, int nstreams)
    : id_(id), role_(role),
      nstreams_(nstreams),
      tl_(id,
          std::bind(&SprinklerNode::outgoing, this, std::placeholders::_1),
          std::bind(&SprinklerNode::deliver, this,
              std::placeholders::_1, std::placeholders::_2,
              std::placeholders::_3, std::placeholders::_4,
              std::placeholders::_5)) {
        sub_info_ = std::vector<SubInfo>(nstreams);
      }

 private:
  // Upcall on establishing a connection.
  void outgoing(SprinklerSocket *ss);
  // Upcall on receiving a message.
  void deliver(SprinklerSocket *ss,
      const char *, int, std::function<void(void *)>, void *);

  // Message types.
  enum MessageTypes {
    M_WEL = 0,  // Welcome message upon connected.
    M_ADV,      // Advertisements.
    M_SUB,      // Subscribe.
    M_UNSUB,    // Unsubscribe.
    M_DATA,     // Events data.
  };

  // A list of possible roles
  static const int kClient = 0;
  static const int kProxy = 1;
  static const int kHead = 2;
  static const int kTail = 4;
  // The role of this node.
  int role_;

  // Node ID; unique across a deployment.
  int id_;
  // Transport layer.
  TransportLayer tl_;
  // #streams
  int nstreams_;
  // Subscriptions
  std::vector<SubInfo> sub_info_;
};

#endif  // SPRINKLER_NODE_H_
