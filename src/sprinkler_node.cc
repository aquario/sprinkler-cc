#include "sprinkler_node.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "sprinkler_common.h"

void SprinklerNode::run(int64_t duration) {
  // Convert duration to microseconds.
  duration *= 1000000;

  tl_.tl_listen(proxies_[id_].port);

  for (;;) {
    int64_t now  = tl_.uptime();

    // Terminate if timeout is reached.
    if (duration > 0 && now > duration) {
      LOG(INFO) << "Max duration reached.  Proxy is terminating.";
      return;
    }
    
    // The tail of a proxy chain sends adv & pub messages periodically.
    if (role_ & kTail) {
      if (now > time_to_adv_) {
        send_adv_message();
        time_to_adv_ += kAdvPeriod;
      }

      if (now > time_to_pub_) {
        proxy_publish();
        time_to_pub_ += kPubPeriod;
      }
    }
  }
}

void SprinklerNode::outgoing(const std::string &host, int port) {
  // Do nothing if this is a proxy.
  // TODO(haoyan): register host:port for shuffling if this is a client.
}

void SprinklerNode::deliver(const uint8_t *data, int size,
    std::function<void(void *)> release, void *env) {
  CHECK_NOTNULL(data);
  CHECK_GT(size, 1);

  MessageTypes msg_type = static_cast<MessageTypes>(data[0]);
  switch (msg_type) {
    case kWelMsg:
      break;
    case kAdvMsg:
      CHECK(role_);   // Has something, i.e., not a client.
      decode_adv(data);
      if (role_ & kTail) {
        release(env);
      } else {
        // Forward the message to the next node in the chain.
        tl_.async_send_message(proxies_[0].host, proxies_[0].port,
            data, size, true, release, env);
      }
      break;
    case kSubMsg:
      CHECK(role_);   // Has something, i.e., not a client.
      handle_subscription(data);
      break;
    case kUnsubMsg:
      CHECK(role_);   // Has something, i.e., not a client.
      break;
    case kPxPubMsg:
      CHECK(role_);   // Has something, i.e., not a client.
      handle_proxy_publish(data);
      break;
    case kCliPubMsg:
      CHECK(role_);   // Has something, i.e., not a client.
      handle_client_publish(data);
      break;
    default:
      LOG(ERROR) << "Unrecognized message type: " << msg_type;
  }
}

void SprinklerNode::send_adv_message() {
  for (int i = 0; i < nproxies_; ++i) {
    int msg_len = 2 + 8 * nstreams_;
    uint8_t *msg = static_cast<uint8_t *>(dcalloc(msg_len, 1));

    *msg = kAdvMsg;
    *(msg + 1) = id_;
    for (int j = 0; j < nstreams_; ++j) {
      itos(msg + 2 + 8 * j, sub_info_[j].next_seq - 1, 8);
    }

    tl_.async_send_message(proxies_[i].host, proxies_[i].port,
        msg, msg_len, true, release_chunk, msg);
  }
}

void SprinklerNode::encode_adv(uint8_t *dst) {
  *dst = static_cast<uint8_t>(kAdvMsg);
  *(dst + 1) = static_cast<uint8_t>(id_);
  for (int i = 0; i < nstreams_; ++i) {
    itos(dst + 2 + (i * 8), static_cast<uint64_t>(sub_info_[i].max_adv), 8);
  }
}

void SprinklerNode::decode_adv(const uint8_t *dst) {
  CHECK_EQ(*dst, kAdvMsg);

  int new_src = static_cast<int>(*(dst + 1));
  for (int i = 0; i < nstreams_; ++i) {
    int64_t new_max_adv = static_cast<int64_t>(stoi(dst + 2 + (i * 8), 8));
    // TODO(haoyan): update with a real timestamp.
    if (should_update(sub_info_[i].max_adv, new_max_adv, 0)) {
      // Update the subscription to new source.
      if (role_ & kTail) {
        send_unsub_message(sub_info_[i].src, i);
        send_sub_message(new_src, i, sub_info_[i].next_seq);
      }
      sub_info_[i].max_adv = new_max_adv;
      sub_info_[i].src = new_src;
    }
  }
}

inline bool SprinklerNode::should_update(int64_t old_max, int64_t new_max,
    int64_t timestamp) {
  int64_t time_diff = tl_.uptime() - timestamp;
  return (new_max - old_max) / static_cast<double>(time_diff) > kSubThd;
}

void SprinklerNode::send_sub_message(int src, int8_t sid, int64_t next_seq) {
  uint8_t *msg = static_cast<uint8_t *>(dcalloc(11, 1)); 
  *msg = static_cast<uint8_t>(kSubMsg);
  *(msg + 1) = static_cast<uint8_t>(id_);
  *(msg + 2) = static_cast<uint8_t>(sid);
  itos(msg + 3, static_cast<uint64_t>(next_seq), 8);

  tl_.async_send_message(proxies_[src].host, proxies_[src].port,
      msg, 11, true, release_chunk, msg);
}

void SprinklerNode::send_unsub_message(int src, int8_t sid) {
  uint8_t *msg = static_cast<uint8_t *>(dcalloc(3, 1)); 

  *msg = static_cast<uint8_t>(kSubMsg);
  *(msg + 1) = static_cast<uint8_t>(id_);
  *(msg + 2) = static_cast<uint8_t>(sid);

  tl_.async_send_message(proxies_[src].host, proxies_[src].port,
      msg, 3, true, release_chunk, msg);
}

void SprinklerNode::handle_subscription(const uint8_t *data) {
  int pid = static_cast<int>(*(data + 1));
  int8_t sid = static_cast<int8_t>(*(data + 2));
  CHECK_GE(sid, 0);
  CHECK_LT(sid, nstreams_);
  int64_t next_seq = static_cast<int64_t>(stoi(data + 3, 8));

  if (demands_[sid].count(pid)) {
  // If the subscription already exists, update with next_seq if it is larger.
    // Note that whether it makes sense to lower next_seq depends on failure
    // model we assume.
    if (next_seq > demands_[sid][pid]) {
      demands_[sid][pid] = next_seq;
    }
  } else {
    demands_[sid][pid] = next_seq;
  }
}

void SprinklerNode::handle_unsubscription(const uint8_t *data) {
  int pid = static_cast<int>(*(data + 1));
  int8_t sid = static_cast<int8_t>(*(data + 2));
  CHECK_GE(sid, 0);
  CHECK_LT(sid, nstreams_);

  demands_[sid].erase(pid);
}

void SprinklerNode::proxy_publish() {
  // For each stream with subscribers ...
  for (auto &demand : demands_) {
    int sid = demand.first;
    // For each subscription ...
    for (auto &request : demand.second) {
      int pid = request.first;
      int64_t next_seq = request.second;

      uint8_t *msg =
        static_cast<uint8_t *>(dcalloc(TransportLayer::kMaxChunkSize, 1));
      *msg = kPxPubMsg;
      *(msg + 1) = static_cast<uint8_t>(id_); 
      *(msg + 2) = static_cast<uint8_t>(sid);

      int64_t nevents = storage_.get_events(sid, next_seq, kMaxEventsPerMsg,
          msg + 11);
      if (nevents < 0) {
        LOG(WARNING) << "Failed to get events: error code " << nevents;
      } else {
        int64_t size = 11 + nevents * kEventLen;
        tl_.async_send_message(proxies_[pid].host, proxies_[pid].port,
            msg, size, true, release_chunk, msg);
      }
    }
  }
}

void SprinklerNode::handle_proxy_publish(const uint8_t *data) {
  int pid = static_cast<int>(*(data + 1));
  int sid = static_cast<int>(*(data + 2));
  CHECK_LT(sid, nstreams_);
  int64_t nevents = static_cast<int64_t>(stoi(data + 3, 8));

  storage_.put_events(sid, nevents, data + 11);
}

void SprinklerNode::handle_client_publish(const uint8_t *data) {
  int cid = static_cast<int>(*(data + 1));
  int sid = static_cast<int>(*(data + 2));
  CHECK(local_streams_.count(sid));   // Only accept if the sid is local.
  int64_t nevents = static_cast<int64_t>(stoi(data + 3, 8));

  storage_.put_raw_events(sid, nevents, data + 11);
}

void SprinklerNode::release_chunk(void *chunk) {
  dfree(chunk);
}
