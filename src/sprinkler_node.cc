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
    if ((role_ & kTail) && proxies_[i].id == id_) {
      continue;
    }
    LOG(INFO) << "Registering proxy " << proxies_[i].id;
    tl_.register_peer(proxies_[i].host, proxies_[i].port);
  }

  if (client_port_ != 0) {
    tl_.register_peer(client_host_, client_port_);
  }

  time_to_adv_ = 1000;
  time_to_pub_ = 1000;
  for (;;) {
    int64_t now  = tl_.uptime();

    // Terminate if timeout is reached.
    if (duration > 0 && now > duration) {
      storage_.grab_all_locks();
      LOG(INFO) << "Max duration reached.  Proxy is terminating ...";
      storage_.report_state();
      SprinklerWorkload::close_workload();
      return;
    }

    tl_.wait(0);
    
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
  CHECK_LE(batch_size, kMaxEventsPerMsg);
  // Convert duration to microseconds.
  duration *= 1000000;

  tl_.tl_listen();
  LOG(INFO) << "Registering proxy (" << proxies_[0].host << ", "
      << proxies_[0].port << ")";
  tl_.register_peer(proxies_[0].host, proxies_[0].port);

  int64_t time_to_pub = interval;
  uint8_t *msg = NULL;
  int len = 0;
  int64_t total_msgs = 0;

  for (;;) {
    int64_t now  = tl_.uptime();

    // Terminate if timeout is reached.
    if (duration > 0 && now > duration) {
      LOG(INFO) << "Max duration reached.  Client is terminating.";
      return;
    }

    tl_.wait(0);
    
    // Publish events to proxy.
    if (interval == 0 || now > time_to_pub) {
      if (msg == NULL) {
        msg = static_cast<uint8_t *>(dcalloc(TransportLayer::kMaxChunkSize, 1));
        len = prepare_client_publish(msg, batch_size);
      } else {
        VLOG(kLogLevel) << "Retry last message.";
      }

      // If succeeded, set msg to NULL so that new events will be fetched
      // next time.  Do not reset msg if previous publish failed, so that next
      // time we could re-send these events.
      if (client_publish(msg, len)) {
        total_msgs += batch_size;
        LOG(INFO) << "PUB " << total_msgs;
        msg = NULL;
      }
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
    int rc = tl_.async_send_message(proxies_[0].host, proxies_[0].port,
        data, size, is_ctrl, release, env);
    if (rc) {
      release(env);
      LOG(ERROR) << "Cannot talk to the successor node. rc = " << rc;
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
      LOG(ERROR) << "kWelMsg is not implemented ...";
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
    case kAckMsg:
      if (!role_ || (ack_enabled_ && (role_ & kTail))) {
        handle_ack_message(data);
      }
      if (role_) {
        forward_or_release(data, size, true, release, env);
      } else {
        release(env);
      }
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

    int rc = tl_.async_send_message(proxies_[i].host, proxies_[i].port,
        msg, msg_len, true, release_chunk, msg);
    if (rc) {
      release_chunk(msg);
      LOG(ERROR) << "send_adv_message: cannot talk to proxy " << proxies_[i].id
          << ". rc = " << rc;
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
    if (new_src != sub_info_[i].src) {
      if (new_max_adv > 0 &&
          should_update(sub_info_[i], new_max_adv, timestamp)) {
        // Update the subscription to new source.
        if (role_ & kTail) {
          if (sub_info_[i].src != -1) {
            if (!send_unsub_message(sub_info_[i].src, i)) {
              continue;
            }
          }
          if (!send_sub_message(new_src, i, sub_info_[i].next_seq)) {
            sub_info_[i].src = -1;
            continue;
          }
          sub_info_[i].src = new_src;
          sub_info_[i].max_adv = new_max_adv;
          sub_info_[i].timestamp = timestamp;
        }
      }
    } else {
      // The max seq# broadcasted from the same source should be non-descending.
      CHECK_GE(new_max_adv, sub_info_[i].max_adv);
      // Update current subscription.
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

  // No sub if all what the advertiser have is no more than what we've received.
  if (sub_info.next_seq > new_max) {
    return false;
  }

  // If we haven't heard from the current subscription for too long,
  // switch because we want to at least get something.
  if (timestamp - sub_info.timestamp > kMaxAdvLease) {
    return true;
  }

  int64_t time_diff = (timestamp - sub_info.timestamp) / 1000000;
  if (time_diff == 0) {
    return false;
  }

  return new_max - sub_info.max_adv >
      kSubThd / static_cast<double>(time_diff);
}

bool SprinklerNode::send_sub_message(int src, int8_t sid, int64_t next_seq) {
  LOG(INFO) << "SUB " << src << " " << static_cast<int>(sid) << " " << next_seq;
  uint8_t *msg = static_cast<uint8_t *>(dcalloc(11, 1)); 
  *msg = static_cast<uint8_t>(kSubMsg);
  *(msg + 1) = static_cast<uint8_t>(id_);
  *(msg + 2) = static_cast<uint8_t>(sid);
  itos(msg + 3, static_cast<uint64_t>(next_seq), 8);

  int rc = tl_.async_send_message(proxies_[src].host, proxies_[src].port,
      msg, 11, true, release_chunk, msg);
  if (rc) {
    release_chunk(msg);
    LOG(ERROR) << "send_sub_message: cannot talk to proxy " << proxies_[src].id
        << ". rc = " << rc;
    return false;
  }
  return true;
}

bool SprinklerNode::send_unsub_message(int src, int8_t sid) {
  LOG(INFO) << "UNSUB " << src << " " << static_cast<int>(sid);
  uint8_t *msg = static_cast<uint8_t *>(dcalloc(3, 1)); 
  *msg = static_cast<uint8_t>(kSubMsg);
  *(msg + 1) = static_cast<uint8_t>(id_);
  *(msg + 2) = static_cast<uint8_t>(sid);

  int rc = tl_.async_send_message(proxies_[src].host, proxies_[src].port,
      msg, 3, true, release_chunk, msg);
  if (rc) {
    release_chunk(msg);
    LOG(ERROR) << "send_unsub_message: cannot talk to proxy "
        << proxies_[src].id << ". rc = " << rc;
    return false;
  }
  return true;
}

void SprinklerNode::handle_subscription(const uint8_t *data) {
  int pid = static_cast<int>(*(data + 1));
  int8_t sid = static_cast<int8_t>(*(data + 2));
  CHECK_GE(sid, 0);
  CHECK_LT(sid, nstreams_);
  int64_t next_seq = static_cast<int64_t>(stoi(data + 3, 8));

  LOG(INFO) << "handle_subscription from proxy " << pid << " on stream "
      << static_cast<int>(sid) << " starting seq# " << next_seq;

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

      // TODO(haoyan): make this adaptive: if we are catching up on disk,
      // make it fast by having end of loop value larger; but if we are
      // in memory, keep the contention small by fixing this to 1.
      for (int i = 0; i < 1; ++i) {
        // Already reached as far as what we have, nothing to send.
        if (next_seq == sub_info_[sid].next_seq) {
//        LOG(INFO) << "nothing to send.";
          continue;
        }

        // If the connection to destination proxy is not available or too busy,
        // do not send this time.
        if (!tl_.available_for_send(proxies_[pid].host, proxies_[pid].port)) {
          break;
        }

        VLOG(kLogLevel) << "Fetch event (" << sid << ", " << next_seq
          << ") for proxy " << pid;

        if (pub_msg_buffer_ == NULL) {
          pub_msg_buffer_ =
              static_cast<uint8_t *>(dcalloc(TransportLayer::kMaxChunkSize, 1));
        }
        *pub_msg_buffer_ = kPxPubMsg;
        *(pub_msg_buffer_ + 1) = static_cast<uint8_t>(id_); 
        *(pub_msg_buffer_ + 2) = static_cast<uint8_t>(sid);

        int64_t now = local_streams_.count(sid) == 1 ? tl_.uptime() / 1000 : -1;
        int64_t nevents = storage_.get_events(pid, sid, now,
              next_seq, kMaxEventsPerMsg, pub_msg_buffer_ + 11);
        if (nevents < 0) {
          LOG(WARNING) << "Failed to get events: error code " << nevents;
          break;
        } else {
          VLOG(kLogLevel) << "proxy_publish to proxy " << pid << " stream " << sid
            << " seq: [" << get_begin_seq(pub_msg_buffer_ + 11) << ", "
            << get_end_seq(pub_msg_buffer_ + 11 + (nevents - 1) * kEventLen)
            << ") next_seq: " << next_seq
            << " nevents: " << nevents;
          // Encode #events.
          itos(pub_msg_buffer_ + 3, nevents, 8);
          int64_t size = 11 + nevents * kEventLen;
          next_seq =
              get_end_seq(pub_msg_buffer_ + 11 + (nevents - 1) * kEventLen);
          // If send is successful, update next_seq accordingly.
          int rc = tl_.async_send_message(proxies_[pid].host, proxies_[pid].port,
              pub_msg_buffer_, size, false, release_chunk, pub_msg_buffer_);
          if (!rc) {
            // Update next_seq.
            LOG_EVERY_N(INFO, 100) << "PUB " << pid << " " << sid << " "
              << request.second << " " << next_seq << " " << nevents;
            request.second = next_seq;
            pub_msg_buffer_ = NULL;
          } else {
            LOG(ERROR) << "pub failed with rc " << rc;
            break;
          }
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
  VLOG(kLogLevel) << "handle_proxy_publish: proxy " << pid << " stream " << sid
      << " seq: [" << get_begin_seq(data + 11)
      << ", " << get_end_seq(data + 11 + (nevents - 1) * kEventLen)
      << ") next_seq: " << sub_info_[sid].next_seq
      << " nevents: " << nevents;
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
    int64_t next_seq =
        storage_.put_events(sid, nevents, const_cast<uint8_t *>(data + 11));
    sub_info_[sid].next_seq = next_seq;

    if (ack_enabled_ && (role_ & kTail)) {
      send_ack_message(pid, sid, next_seq - 1);
    }
  }
}

int SprinklerNode::prepare_client_publish(uint8_t *msg, int batch_size) {
  VLOG(kLogLevel) << "prepare_client_publish: client_id = " << id_
      << " on stream " << client_sid_ << " with " << batch_size << " events";
  int len = 12 + batch_size * kEventLen;
  *msg = kCliPubMsg;
  itos(msg + 1, id_, 2);
  *(msg + 3) = client_sid_;
  itos(msg + 4, batch_size, 8);
  for (int i = 0; i < batch_size; ++i) {
    int64_t key = has_workload_
        ? SprinklerWorkload::get_next_wl_key()
        : SprinklerWorkload::get_next_key();
    itos(msg + 12 + i * kEventLen + 9, key, 8);
  }

  return len;
}

bool SprinklerNode::client_publish(uint8_t *msg, int len) {
  // If the connection to destination proxy is not available or too busy,
  // do not send this time.
  if (!tl_.available_for_send(proxies_[0].host, proxies_[0].port)) {
    return false;
  }

  int rc = tl_.async_send_message(proxies_[0].host, proxies_[0].port, msg, len,
      false, release_chunk, msg);
  if (rc) {
    LOG(ERROR) << "client_publish: cannot talk to proxy " << proxies_[0].id
        << ". rc = " << rc;
    return false;
  }

  return true;
}

void SprinklerNode::handle_client_publish(const uint8_t *data) {
  int cid = static_cast<int>(stoi(data + 1, 2));
  int sid = static_cast<int>(*(data + 3));
  CHECK(local_streams_.count(sid));   // Only accept if the sid is local.
  int64_t nevents = static_cast<int64_t>(stoi(data + 4, 8));

  VLOG(kLogLevel) << "handle_client_publish from client " << cid
      << " on stream " << sid << " with " << nevents << " events";

  // Store events and update next_seq for future advertisement messages.
  sub_info_[sid].next_seq =
      storage_.put_raw_events(sid, tl_.uptime() / 1000, nevents, data + 12);

  if (ack_enabled_ && (role_ & kTail)) {
    send_ack_message(-1, sid, sub_info_[sid].next_seq - 1);
  }
}

void SprinklerNode::send_ack_message(int pid, int sid, int64_t seq) {
  uint8_t *msg = static_cast<uint8_t *>(dcalloc(12, 1));
  std::string dst_host;
  int dst_port = 0;
  int len = 0;

  msg[0] = kAckMsg;
  switch (pid) {
    case -2:  // Remote ack to client.
      msg[1] = 1;
      // Fall through.
    case -1:  // Local ack to client.
      msg[2] = sid;
      itos(msg + 3, seq, 8);
      dst_host = client_host_;
      dst_port = client_port_;
      len = 11;
      break;
    default:  // Ack to proxy.
      msg[1] = sid;
      itos(msg + 2, seq, 8);
      dst_host = proxies_[pid].host;
      dst_port = proxies_[pid].port;
      len = 10;
  }

  int rc = tl_.async_send_message(dst_host, dst_port, msg, len,
      true, release_chunk, msg);
  if (rc) {
    LOG(ERROR) << "send_ack_message: cannot talk to peer " << dst_host  << ":"
        << dst_port << ". rc = " << rc;
    release_chunk(msg);
  }
}

void SprinklerNode::handle_ack_message(const uint8_t *data) {
  if (role_) {
    // A proxy has received an ack message from a remote proxy.
    // Notify the client that the message is safe at a remote datacenter.
    int64_t seq = stoi(data + 2, 8);
    send_ack_message(-2, data[1], seq);
  } else {
    // This is the client, just log the message.
    if (data[1] == 0) {
      LOG(INFO) << "ACK_LOCAL " << static_cast<int>(data[2]) << " " << stoi(data + 3, 8);
    } else {
      LOG(INFO) << "ACK_REMOTE " << static_cast<int>(data[2]) << " " << stoi(data + 3, 8);
    }
  }
}

void SprinklerNode::release_chunk(void *chunk) {
  dfree(chunk);
}

