#ifndef SPRINKLER_NODE_H_
#define SPRINKLER_NODE_H_

#include <stdint.h>

#include <functional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "transport_layer.h"

// Proxy/region configuration info.
struct Proxy {
  // Proxy id
  int id;
  // Head of the proxy chain, which is where every request to this region
  // should be directed to.
  std::string host;   // Hostname.
  int port;           // Port number.

  Proxy(int id) : id(id), host(""), port(0) {}
};

// Subscription information for a stream.
// Stream id is implicit here, since each of this struct will be referenced
// by indexing in an array.
struct SubInfo {
  // ID of the source of current subscription.
  int src;
  // The next seq# to receive.  If this is a local stream, this number is the
  // least seq# that is not been used yet.
  int64_t next_seq;
  // Max seq# ever heard on this stream.  If this is a local stream, this number
  // is the most recent seq# added to the stream.
  int64_t max_adv;

  // Constructor.
  // src = -1 means no subscription yet; sequence number starts from 1.
  SubInfo() : src(-1), next_seq(1), max_adv(0) {}
};

class SprinklerNode {
 public:
  SprinklerNode(int id, int role, int nproxies, int nstreams)
    : id_(id), role_(role),
      nproxies_(nproxies), nstreams_(nstreams),
      tl_(id,
          std::bind(&SprinklerNode::outgoing, this,
              std::placeholders::_1, std::placeholders::_2),
          std::bind(&SprinklerNode::deliver, this,
              std::placeholders::_1, std::placeholders::_2,
              std::placeholders::_3, std::placeholders::_4)) {
        sub_info_ = std::vector<SubInfo>(nstreams);
      }

 private:
  // Upcall on establishing a connection.
  void outgoing(const std::string &host, int port);
  // Upcall on receiving a message.
  void deliver(const uint8_t *, int, std::function<void(void *)>, void *);
  
  // Conversion between uint64_t and byte array.
  static void itos(uint8_t *dst, uint64_t val, int len);
  static uint64_t stoi(const uint8_t *src, int len);

  // Broadcast advertisement messages to all peer proxies.
  void send_adv_message();
  // Construct an advertisement message.  Assumes that dst points to an
  // allocated space.
  void encode_adv(uint8_t *dst);
  // Parse an advertisement message and update subscriptions if necessary.
  void decode_adv(const uint8_t *dst);
  // Check if it's necessary to change subscription source.
  bool should_update(int64_t old_max, int64_t new_max, int64_t timestamp);

  // Send message to subscribe for a stream.
  void send_sub_message(int src, int8_t sid, int64_t next_seq);
  // Send message to unsubscribe for a stream.
  void send_unsub_message(int src, int8_t sid);

  // Handle (un)subscription messages sent from a peer proxy.
  // Message format:
  //  |k(Un)SubMsg(1)|pid(1)|sid(1)|next_seq(8, sub-only)|
  void handle_subscription(const uint8_t *data);
  void handle_unsubscription(const uint8_t *data);

  // Add events published from local clients.
  // Message format:
  //  |kPubMsg(1)|cid(1)|sid(1)|nevents(8)|msg0(*)|msg1(*)|...|msgn(*)|
  void handle_publish(const uint8_t *data);

  // Release a chunk of memory.
  static void release_chunk(void *);

  // Message types.
  enum MessageTypes {
    kWelMsg = 0,  // Welcome message upon connected.
    kAdvMsg,      // Advertisements.
    kSubMsg,      // Subscribe.
    kUnsubMsg,    // Unsubscribe.
    kPubMsg,      // Publish (from client).
    kDataMsg,     // Events data.
  };

  // A list of possible roles
  static const int kClient = 0;
  static const int kProxy = 1;
  static const int kHead = 2;
  static const int kTail = 4;
  // Threshold on changing subscription.
  static constexpr double kSubThd = 0.001;

  // Node ID; unique across a deployment.
  int id_;
  // The role of this node.
  int role_;

  // Transport layer.
  TransportLayer tl_;

  // #proxies.
  int nproxies_;
  // Endpoints of proxies.  For non-tail node, this only contains the successor
  // node in the chain; for tail of a chain, this is a list of head nodes
  // in other regions, indexed by their proxy IDs.
  std::vector<Proxy> proxies_;

  // #streams.
  int nstreams_;
  // Subscriptions TO peers.
  std::vector<SubInfo> sub_info_;
  // Demands FROM peers.
  // map<sid, subscribers>; subscriber: pair<pid, next_seq>.
  std::unordered_map<int, std::unordered_map<int, int64_t> > demands_;
  // Streams that this proxy owns; usually a constant set.
  std::unordered_set<int> local_streams_;

};

#endif  // SPRINKLER_NODE_H_
