#include "sprinkler_node.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "sprinkler_common.h"
#include "sprinkler_workload.h"

void SprinklerNode::start_proxy(int64_t duration) {
  // This node must be a proxy. 
  CHECK(role_); 
  // Convert duration to microseconds.
  duration *= 1000000;

  // Start listening to incoming sockets.
  tl_.tl_listen();
  // Register peer proxies.
  for (int i = 0; i < nproxies_; ++i) {
    if (proxies_[i].id == id_) {
      continue;
    }
    LOG(INFO) << "Registering proxy " << proxies_[i].id;
    tl_.register_peer(proxies_[i].host, proxies_[i].port);
  }

  for (;;) {
    int64_t now  = tl_.uptime();

    // Terminate if timeout is reached.
    if (duration > 0 && now > duration) {
      LOG(INFO) << "Max duration reached.  Proxy is terminating.";
      return;
    }

    tl_.wait(1);
    
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

void SprinklerNode::start_client(
    int64_t duration, int interval, int batch_size) {
  // This node must be a client. 
  CHECK_EQ(role_, 0); 
  // Batch size must not exceed the maximum allowed.
  CHECK_LE(batch_size, kMaxRawEventsPerMsg);
  // Convert duration to microseconds.
  duration *= 1000000;

  tl_.tl_listen();
  LOG(INFO) << "Registering proxy (" << proxies_[0].host << ", "
      << proxies_[0].port << ")";
  tl_.register_peer(proxies_[0].host, proxies_[0].port);

  int64_t time_to_pub = interval;
  for (;;) {
    int64_t now  = tl_.uptime();

    // Terminate if timeout is reached.
    if (duration > 0 && now > duration) {
      LOG(INFO) << "Max duration reached.  Client is terminating.";
      return;
    }

    tl_.wait(1);
    
    // Publish events to proxy.
    if (interval == 0 || now > time_to_pub) {
      client_publish(batch_size);
      time_to_pub += interval;
    }
  }
}

void SprinklerNode::forward_or_release(const uint8_t *data, int size,
    bool is_ctrl, std::function<void(void *)> release, void *env) {
  if (role_ & kTail) {
    release(env);
  } else {
    // Forward the message to the next node in the chain.
    if (tl_.async_send_message(proxies_[0].host, proxies_[0].port,
        data, size, is_ctrl, release, env)) {
      release(env);
      LOG(ERROR) << "Cannot talk to the successor node. Is it failing?";
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
      handle_adv_message(data);
      forward_or_release(data, size, true, release, env);
      break;
    case kSubMsg:
      CHECK(role_);   // Has something, i.e., not a client.
      handle_subscription(data);
      forward_or_release(data, size, true, release, env);
      break;
    case kUnsubMsg:
      CHECK(role_);   // Has something, i.e., not a client.
      handle_unsubscription(data);
      forward_or_release(data, size, true, release, env);
      break;
    case kPxPubMsg:
      CHECK(role_);   // Has something, i.e., not a client.
      handle_proxy_publish(data);
      forward_or_release(data, size, false, release, env);
      break;
    case kCliPubMsg:
      CHECK(role_);   // Has something, i.e., not a client.
      handle_client_publish(data);
      forward_or_release(data, size, false, release, env);
      break;
    default:
      LOG(ERROR) << "Unrecognized message type: " << msg_type;
      release(env);
  }
}

void SprinklerNode::send_adv_message() {
  for (int i = 0; i < nproxies_; ++i) {
    if (proxies_[i].id == id_) {
      continue;
    }

    int msg_len = 2 + 8 * nstreams_;
    uint8_t *msg = static_cast<uint8_t *>(dcalloc(msg_len, 1));

    *msg = kAdvMsg;
    *(msg + 1) = id_;
    for (int j = 0; j < nstreams_; ++j) {
      itos(msg + 2 + 8 * j, sub_info_[j].next_seq - 1, 8);
    }

    if (tl_.async_send_message(proxies_[i].host, proxies_[i].port,
          msg, msg_len, true, release_chunk, msg)) {
      release_chunk(msg);
      LOG(ERROR) << "send_adv_message: cannot talk to proxy " << proxies_[i].id
          << ". Is it failing?";
    }
  }
}

void SprinklerNode::handle_adv_message(const uint8_t *dst) {
  CHECK_EQ(*dst, kAdvMsg);

  int64_t timestamp = tl_.uptime();
  int new_src = static_cast<int>(*(dst + 1));
  for (int i = 0; i < nstreams_; ++i) {
    // A proxy never need to subscribe to others for a local stream.
    if (local_streams_.count(i) == 1) {
      continue;
    }
    int64_t new_max_adv = static_cast<int64_t>(stoi(dst + 2 + (i * 8), 8));
    // Check if a change of subscription is necessary.
    if (new_src != sub_info_[i].src && new_max_adv > 0 &&
        should_update(sub_info_[i], new_max_adv, timestamp)) {
      // Update the subscription to new source.
      if (role_ & kTail) {
        if (sub_info_[i].src != -1) {
          send_unsub_message(sub_info_[i].src, i);
        }
        send_sub_message(new_src, i, sub_info_[i].next_seq);
      }
      sub_info_[i].src = new_src;
      sub_info_[i].max_adv = new_max_adv;
      sub_info_[i].timestamp = timestamp;
    }
  }
}

inline bool SprinklerNode::should_update(
    const SubInfo &sub_info, int64_t new_max, int64_t timestamp) {
  // If there's no previous subscription, establish one.
  if (sub_info.src == -1) {
    return true;
  }

  int64_t time_diff = (timestamp - sub_info.timestamp) / 1000000;
  if (time_diff == 0) {
    return false;
  }

  return new_max - sub_info.max_adv >
      kSubThd / static_cast<double>(time_diff);
}

void SprinklerNode::send_sub_message(int src, int8_t sid, int64_t next_seq) {
  LOG(INFO) << "SUB " << src << " " << sid << " " << next_seq;
  uint8_t *msg = static_cast<uint8_t *>(dcalloc(11, 1)); 
  *msg = static_cast<uint8_t>(kSubMsg);
  *(msg + 1) = static_cast<uint8_t>(id_);
  *(msg + 2) = static_cast<uint8_t>(sid);
  itos(msg + 3, static_cast<uint64_t>(next_seq), 8);

  if (tl_.async_send_message(proxies_[src].host, proxies_[src].port,
        msg, 11, true, release_chunk, msg)) {
    release_chunk(msg);
    LOG(ERROR) << "send_sub_message: cannot talk to proxy " << proxies_[src].id
        << ". Is it failing?";
  }
}

void SprinklerNode::send_unsub_message(int src, int8_t sid) {
  LOG(INFO) << "UNSUB " << src << " " << sid;
  uint8_t *msg = static_cast<uint8_t *>(dcalloc(3, 1)); 
  *msg = static_cast<uint8_t>(kSubMsg);
  *(msg + 1) = static_cast<uint8_t>(id_);
  *(msg + 2) = static_cast<uint8_t>(sid);

  if (tl_.async_send_message(proxies_[src].host, proxies_[src].port,
        msg, 3, true, release_chunk, msg)) {
    release_chunk(msg);
    LOG(ERROR) << "send_unsub_message: cannot talk to proxy "
        << proxies_[src].id << ". Is it failing?";
  }
}

void SprinklerNode::handle_subscription(const uint8_t *data) {
  int pid = static_cast<int>(*(data + 1));
  int8_t sid = static_cast<int8_t>(*(data + 2));
  CHECK_GE(sid, 0);
  CHECK_LT(sid, nstreams_);
  int64_t next_seq = static_cast<int64_t>(stoi(data + 3, 8));

  LOG(INFO) << "handle_subscription from proxy " << pid << " on stream "
      << sid << " starting seq# " << next_seq;

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

  LOG(INFO) << "handle_unsubscription from proxy " << pid
      << " on stream " << sid;

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
      CHECK_LE(next_seq, sub_info_[sid].next_seq);

      // Already reached as far as what we have, nothing to send.
      if (next_seq == sub_info_[sid].next_seq) {
        continue;
      }

      // If the connection to destination proxy is not available or too busy,
      // do not send this time.
      if (!tl_.available_for_send(proxies_[pid].host, proxies_[pid].port)) {
        continue;
      }

      VLOG(kLogLevel) << "Fetch event (" << sid << ", " << next_seq
          << ") for proxy " << pid;

      uint8_t *msg =
        static_cast<uint8_t *>(dcalloc(TransportLayer::kMaxChunkSize, 1));
      *msg = kPxPubMsg;
      *(msg + 1) = static_cast<uint8_t>(id_); 
      *(msg + 2) = static_cast<uint8_t>(sid);

      int64_t nevents =
          storage_.get_events(sid, next_seq, kMaxEventsPerMsg, msg + 11);
      if (nevents < 0) {
        LOG(WARNING) << "Failed to get events: error code " << nevents;
      } else {
        // Encode #events.
        itos(msg + 3, nevents, 8);
        int64_t size = 11 + nevents * kEventLen;
        // If send is successful, update next_seq accordingly.
        if (!tl_.async_send_message(proxies_[pid].host, proxies_[pid].port,
              msg, size, false, release_chunk, msg)) {
          // Update next_seq.
          next_seq = get_end_seq(msg + 11 + (nevents - 1) * kEventLen);
          request.second = next_seq;
        }
      }
    }
  }
}

void SprinklerNode::handle_proxy_publish(const uint8_t *data) {
  int pid = static_cast<int>(*(data + 1));
  int sid = static_cast<int>(*(data + 2));
  CHECK_LT(sid, nstreams_);
  int64_t nevents = static_cast<int64_t>(stoi(data + 3, 8));
  if (nevents == 0) {
    LOG(ERROR) << "handle_proxy_publish: nevents == 0.";
  } else if (get_begin_seq(data + 11) > sub_info_[sid].next_seq) {
    // These events are out of order.
    // Resend a subscription message with lower next_seq.
    LOG(WARNING) << "handle_proxy_publish: got out-of-order events."
        << "  Re-subscribe with current next_seq.";
    send_sub_message(pid, sid, sub_info_[pid].next_seq);
  } else if (get_end_seq(data + 11 + (nevents - 1) * kEventLen)
      <= sub_info_[pid].next_seq) {
    // We have already received all of these.
    LOG(WARNING) << "handle_proxy_publish: all these events are too old.";
  } else {
    VLOG(kLogLevel) << "handle_proxy_publish from proxy " << pid
        << " on stream " << sid << " with " << nevents << " events";

    int64_t next_seq =
        storage_.put_events(sid, nevents, const_cast<uint8_t *>(data + 11));
    sub_info_[sid].next_seq = next_seq;
  }
}

void SprinklerNode::client_publish(int batch_size) {
  VLOG(kLogLevel) << "client_publish: client_id = " << id_
      << " on stream " << client_sid_ << " with " << batch_size << " events";
  // If the connection to destination proxy is not available or too busy,
  // do not send this time.
  if (!tl_.available_for_send(proxies_[0].host, proxies_[0].port)) {
    return;
  }

  int64_t len = 12 + batch_size * kRawEventLen;
  uint8_t *msg =
      static_cast<uint8_t *>(dcalloc(TransportLayer::kMaxChunkSize, 1));
  *msg = kCliPubMsg;
  itos(msg + 1, id_, 2);
  *(msg + 3) = client_sid_;
  itos(msg + 4, batch_size, 8);
  for (int i = 0; i < batch_size; ++i) {
    itos(msg + 12 + i * kRawEventLen, SprinklerWorkload::get_next_key(), 8);
  }

  if (tl_.async_send_message(proxies_[0].host, proxies_[0].port, msg, len,
        false, release_chunk, msg)) {
    release_chunk(msg);
    LOG(ERROR) << "client_publish: cannot talk to proxy " << proxies_[0].id
        << ". Is it failing?";
  }
}

void SprinklerNode::handle_client_publish(const uint8_t *data) {
  int cid = static_cast<int>(stoi(data + 1, 2));
  int sid = static_cast<int>(*(data + 3));
  CHECK(local_streams_.count(sid));   // Only accept if the sid is local.
  int64_t nevents = static_cast<int64_t>(stoi(data + 4, 8));

  VLOG(kLogLevel) << "handle_client_publish from client " << cid
      << " on stream " << sid << " with " << nevents << " events";

  // Store events and update next_seq for future advertisement messages.
  sub_info_[sid].next_seq = storage_.put_raw_events(sid, nevents, data + 12);
}

void SprinklerNode::release_chunk(void *chunk) {
  dfree(chunk);
}
