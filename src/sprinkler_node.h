#ifndef SPRINKLER_NODE_H_
#define SPRINKLER_NODE_H_

#include <stdint.h>

#include <functional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "dmalloc.h"
#include "sprinkler_common.h"
#include "transport_layer.h"
#include "multi_tier_storage.h"

// Proxy/region configuration info.
struct Proxy {
  // Proxy id
  int id;
  // Head of the proxy chain, which is where every request to this region
  // should be directed to.
  std::string host;   // Hostname.
  int port;           // Port number.

  Proxy() : id(-1), host(""), port(0) {}
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
  // The time this max_adv is heard.  The older the timestamp, the less
  // credible this advertisement is.
  int64_t timestamp;

  // Constructor.
  // src = -1 means no subscription yet; sequence number starts from 1.
  SubInfo() : src(-1), next_seq(1), max_adv(0) {}
};

class SprinklerNode {
 public:
  // Constructor for proxy.
  SprinklerNode(int id, int port, int role,
      int nproxies, const std::vector<Proxy> &proxies,
      int nstreams, const std::unordered_set<int> &sids,
      int64_t mem_buf_size, int64_t disk_chunk_size,
      int gc_thread_count, int64_t min_events_to_gc,
      int64_t max_gc_table_size, int64_t max_gc_chunk_size)
    : id_(id), role_(role),
      nproxies_(nproxies), proxies_(proxies),
      tl_(id, port,
          std::bind(&SprinklerNode::outgoing, this,
              std::placeholders::_1, std::placeholders::_2),
          std::bind(&SprinklerNode::deliver, this,
              std::placeholders::_1, std::placeholders::_2,
              std::placeholders::_3, std::placeholders::_4)),
      nstreams_(nstreams), sub_info_(nstreams), local_streams_(sids),
      storage_(nstreams, mem_buf_size, disk_chunk_size,
          gc_thread_count, min_events_to_gc,
          max_gc_table_size, max_gc_chunk_size),
      time_to_adv_(kAdvPeriod), time_to_pub_(kPubPeriod) {}

  // Constructor for client.
  SprinklerNode(int id, int port, int role, int sid,
      const std::vector<Proxy> proxies, bool has_workload)
    : id_(id), role_(role), nproxies_(1), proxies_(proxies),
      has_workload_(has_workload),
      tl_(id, port,
          std::bind(&SprinklerNode::outgoing, this,
              std::placeholders::_1, std::placeholders::_2),
          std::bind(&SprinklerNode::deliver, this,
              std::placeholders::_1, std::placeholders::_2,
              std::placeholders::_3, std::placeholders::_4)),
      storage_(0, 0, 0, 0, 0, 0, 0), client_sid_(sid) {}

  // Main loop of Sprinkler proxy.  Duration is the lifetime of this proxy,
  // in seconds.
  void start_proxy(int64_t duration);

  // Main loop of Sprinkler client.
  // Args:
  //  duration: the lifetime of this client in seconds;
  //  interval: time between publishing two batches of events in microseconds;
  //  batch_size: number of events per batch.
  void start_client(int64_t duration, int interval, int batch_size);

  // A list of possible roles
  static const int kClient = 0;
  static const int kProxy = 1;
  static const int kHead = 2;
  static const int kTail = 4;

  // Log level for this class.
  static const int kLogLevel = 6;

 private:
  // Upcall on establishing a connection.
  void outgoing(const std::string &host, int port);
  // Upcall on receiving a message.
  void deliver(const uint8_t *, int, std::function<void(void *)>, void *);
  
  // Construct advertisement message and broadcast it to all peer proxies.
  void send_adv_message();
  // Parse an advertisement message and update subscriptions if necessary.
  void handle_adv_message(const uint8_t *dst);
  // Check if it's necessary to change subscription source.
  // Returns true is (new_max - old_max) > kSubThd / timestamp_diff.
  //
  // Note that effectively, the difference in sequence numbers is magnified by
  // timestamp_diff, so that older advertisements matter less.
  bool should_update(const SubInfo &sub_info, int64_t new_max,
      int64_t timestamp);

  // Send message to subscribe for a stream.
  void send_sub_message(int src, int8_t sid, int64_t next_seq);
  // Send message to unsubscribe for a stream.
  void send_unsub_message(int src, int8_t sid);

  // Handle (un)subscription messages sent from a peer proxy.
  // Message format:
  //  |k(Un)SubMsg(1)|pid(1)|sid(1)|next_seq(8, sub-only)|
  void handle_subscription(const uint8_t *data);
  void handle_unsubscription(const uint8_t *data);

  // Proxy publishes events to other proxy subscribers.
  // Message format:
  //  |kPxPubMsg(1)|id(1)|sid(1)|nevents(8)|msg0(*)|msg1(*)|...|msgn(*)|
  void proxy_publish();

  // Add events published from peer proxies.
  void handle_proxy_publish(const uint8_t *data);

  // Prepare a publishing message to be sent to the local proxy.
  // Returns the length of the message constructed.
  //
  // Message format:
  //  |kCliPubMsg(1)|cid(2)|sid(1)|nevents(8)|msg0(*)|msg1(*)|...|msgn(*)|
  int prepare_client_publish(uint8_t *msg, int batch_size);

  // Client publishes events to its local proxy.
  // Returns true if the message is sent successfully.
  bool client_publish(uint8_t *msg, int len);

  // Add events published from local clients.
  void handle_client_publish(const uint8_t *data);

  // If this proxy is the tail in a chain, releases the message buffer,
  // otherwise, forward the message to the next node in the chain.
  void forward_or_release(const uint8_t *data, int size,
      bool is_ctrl, std::function<void(void *)> release, void *env);

  // Release a chunk of memory.
  static void release_chunk(void *);

  // Message types.
  enum MessageTypes {
    kWelMsg = 0,  // Welcome message upon connected.
    kAdvMsg,      // Advertisements.
    kSubMsg,      // Subscribe.
    kUnsubMsg,    // Unsubscribe.
    kPxPubMsg,    // Publish formatted events from proxy.
    kCliPubMsg,   // Publish unformatted events from client.
  };

  // Time intervals for periodical events in microseconds.
  static const int64_t kAdvPeriod = 2 * 1000000;    // 2 seconds.
  static const int64_t kPubPeriod = 2 * 1000;      // 2 milliseconds.
  // Maximum time for an advertisement message to be effective.
  // 6 seconds, i.e. 3 advertisement intervals.
  static const int64_t kMaxAdvLease = 6 * 1000000;
  // Threshold on changing subscription.
  static constexpr double kSubThd = 1000;
  // Max header length for any Sprinkler message.
  static const int kMaxHeaderSize = 32;
  // Max #events per message.
  static const int kMaxEventsPerMsg =
      (TransportLayer::kMaxChunkSize - kMaxHeaderSize) / kEventLen;
  // Max #unformatted events per message.
  static const int kMaxRawEventsPerMsg =
      (TransportLayer::kMaxChunkSize - kMaxHeaderSize) / kRawEventLen;

  // Node ID; unique across a deployment.
  int id_;
  // The role of this node.
  int role_;

  // #proxies.
  int nproxies_;
  // Endpoints of proxies.  For non-tail node, this only contains the successor
  // node in the chain; for tail of a chain, this is a list of head nodes
  // in other regions, indexed by their proxy IDs.
  std::vector<Proxy> proxies_;

  // Transport layer.
  TransportLayer tl_;

  // #streams.
  int nstreams_;
  // Subscriptions TO peers.
  std::vector<SubInfo> sub_info_;
  // Demands FROM peers.
  // map<sid, subscribers>; subscriber: pair<pid, next_seq>.
  std::unordered_map<int, std::unordered_map<int, int64_t> > demands_;
  // Streams that this proxy owns; usually a constant set.
  std::unordered_set<int> local_streams_;

  // On-disk storage component.
  MultiTierStorage storage_;

  // Timers for periodical events.
  int64_t time_to_adv_;   // Time to broadcast advertisement messages.
  int64_t time_to_pub_;   // Time to publish events.

  // For client: the stream it publishes to.
  int client_sid_;

  // If the client is using a pre-computed workload file.
  bool has_workload_;
};

#endif  // SPRINKLER_NODE_H_
