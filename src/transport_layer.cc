#include "transport_layer.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#include <functional>
#include <string>

#include <glog/logging.h>

#include "dmalloc.h"
#include "sprinkler_common.h"

#ifndef IOV_MAX
#define IOV_MAX 1024
#endif

TransportLayer::TransportLayer(int id, int port,
      std::function<void(const std::string &, int)> outgoing,
      std::function<void(const uint8_t *, int,
          std::function<void(void *)>, void *)> deliver) {
  id_ = id;
  port_ = port;
  outgoing_ = outgoing;
  deliver_ = deliver;
  gettimeofday(&starttime_, 0);
  LOG(INFO) << "START";
  // TODO(haoyan): make this a constant ...
  time_to_attempt_connect_ = uptime();

  sockets_ = std::list<SprinklerSocket>();
  nsockets_ = 0;

  addr_list_ = std::unordered_map<std::string, SocketAddr>();

  VLOG(kLogLevel) << "Created Sprinkler node with id " << id_;
}

TransportLayer::~TransportLayer() {
  for (SocketIter sit = sockets_.begin(); sit != sockets_.end(); ++sit) {
    close(sit->skt);
  }
}

// Return the number of microseconds since we started.
int64_t TransportLayer::uptime() {
  struct timeval now;
  int64_t result;

  gettimeofday(&now, 0);
  result = (now.tv_sec - starttime_.tv_sec) * 1000000;
  result += now.tv_usec;
  result -= starttime_.tv_usec;
  return result;
}

SocketIter TransportLayer::add_socket(int skt,
    std::function<int(SocketIter)> input,
    std::function<int(SocketIter)> output,
    std::function<void(const uint8_t *, int,
        std::function<void(void *)>, void *)> deliver,
    const std::string &descr, std::string host, int port) {
  VLOG(kLogLevel) << "add_socket";
  SprinklerSocket ss(skt, input, output, deliver, descr, host, port);
  ss.init();

  sockets_.push_back(ss);
  ++nsockets_;

  auto iter = sockets_.end();
  return --iter;
}

void TransportLayer::async_socket_send(SocketIter ss,
    uint8_t *hdr, const uint8_t *data, int size, bool is_ctrl,
    std::function<void(void *)> cleanup, void *env) {
  VLOG(kLogLevel) << "async_socket_send: send " << size << " bytes.";
  CHECK(ss != sockets_.end());

  Chunk chunk(hdr, data, size, cleanup, env);

  if (is_ctrl) {
    ss->ctrl_cqueue.push_back(chunk);
    ss->ctrl_remainder += size;
  } else {
    ss->data_cqueue.push_back(chunk);
    ss->data_remainder += size;
  }

  VLOG(kLogLevel) << "Done async_socket_send";
}

int TransportLayer::do_sendmsg(int skt, struct msghdr *mh) {
  return sendmsg(skt, mh, 0);
}

int TransportLayer::prepare_iovec(const std::list<Chunk> &cqueue,
    int offset, struct iovec *iov, int start) {
  int iovlen = start;

  std::list<Chunk>::const_iterator cit;
  int total = 0;
  if (!cqueue.empty()) {
    for (cit = cqueue.begin();
        cit != cqueue.end() && iovlen < IOV_MAX;
        ++cit) {
      if (offset >= cit->size) {
        offset -= cit->size;
      } else {
        if (offset < 4) {
          iov[iovlen].iov_base =
              static_cast<void *>(const_cast<uint8_t *>(cit->hdr + offset));
          iov[iovlen].iov_len = 4 - offset;
          iovlen++;
          total += 4 - offset;
          offset = 0;
        } else {
          offset -= 4;
        }

        if (iovlen < IOV_MAX) {
          iov[iovlen].iov_base =
              static_cast<void *>(const_cast<uint8_t *>(cit->data + offset));
          iov[iovlen].iov_len = cit->size - 4 - offset;
          iovlen++;
          total += cit->size - 4 - offset;
          offset = 0;
        }
      }
    }
  }

  VLOG(kLogLevel) << "prepare_iovec: added " << total << " bytes.";

  return iovlen;
}

int TransportLayer::send_ready(SocketIter ss) {
  VLOG(kLogLevel) << "send_ready: ctrl_offset = " << ss->ctrl_offset
            << "; ctrl_remainder = " << ss->ctrl_remainder
            << "; data_offset = " << ss->data_offset
            << "; data_remainder = " << ss->data_remainder;

  // If it's the first time, notify the protocol layer that there is an
  // outgoing connection.
  if (ss->first) {
    outgoing_(ss->host, ss->port);
  }

  // If it's the first time, stop here.
  if (ss->first) {
    ss->first = false;
    VLOG(kLogLevel) << "set first to false";
    return 1;
  }

  // Add messages into iovec.  Control messages have higher priority over
  // data messages.
  struct iovec *iov = (struct iovec *) dcalloc(sizeof(*iov), IOV_MAX);
  int iovlen = 0;
  iovlen = prepare_iovec(ss->ctrl_cqueue, ss->ctrl_offset, iov, 0);
  if (iovlen < IOV_MAX) {
    iovlen = prepare_iovec(ss->data_cqueue, ss->data_offset, iov, iovlen);
  }

  // If there's anything in the iovec, send it now.
  if (iovlen > 0) {
    struct msghdr mh;
    memset(&mh, 0, sizeof(mh));
    mh.msg_iov = iov;
    mh.msg_iovlen = iovlen;
    int n = do_sendmsg(ss->skt, &mh);
    if (n <= 0) {
      // TODO(haoyan): should I do anything about this?  Presumably the
      // receive side of the socket is closed and everything
      // is cleaned up automatically.
      return 0;
    } else {
      VLOG(kLogLevel) << "send_ready: sent " << n << " bytes to socket "
          << ss->skt << "; iovlen = " << iovlen;

      if (n <= ss->ctrl_remainder) {
        ss->ctrl_offset += n;
        ss->ctrl_remainder -= n;
      } else {
        ss->data_offset += (n - ss->ctrl_remainder);
        ss->data_remainder -= (n - ss->ctrl_remainder);

        ss->ctrl_offset += ss->ctrl_remainder;
        ss->ctrl_remainder = 0;
      }
    }
    dfree(iov);
  } else {
    LOG(WARNING) << "send_ready: nothing to send??";
  }

  // Release everything that can be released in the queue.
  std::list<Chunk>::iterator cit;
  while ((cit = ss->ctrl_cqueue.begin()) != ss->ctrl_cqueue.end()
      && ss->ctrl_offset >= cit->size) {
    ss->ctrl_offset -= cit->size;
    Chunk &chunk = ss->ctrl_cqueue.front();
    release_chunk(chunk.hdr);
    chunk.cleanup(chunk.env);
    ss->ctrl_cqueue.pop_front();
  }

  while ((cit = ss->data_cqueue.begin()) != ss->data_cqueue.end()
      && ss->data_offset >= cit->size) {
    ss->data_offset -= cit->size;
    Chunk &chunk = ss->data_cqueue.front();
    release_chunk(chunk.hdr);
    chunk.cleanup(chunk.env);
    ss->data_cqueue.pop_front();
  }

  // If we are half way through sending a data chunk, promote it to the front
  // of the ctrl queue so that it can be sent in its entirety.
  if (ss->data_offset > 0) {
    CHECK_EQ(ss->ctrl_cqueue.size(), 0);
    Chunk &chunk = ss->data_cqueue.front();

    ss->ctrl_offset = ss->data_offset;
    ss->ctrl_remainder = chunk.size - ss->data_offset;
    ss->data_offset = 0;
    ss->data_remainder -= ss->ctrl_remainder;

    ss->ctrl_cqueue.push_back(chunk);
    ss->data_cqueue.pop_front();
  }

  // If the buffer is now empty, try to fill it up.
//  if (ss->remainder == 0 && ss->send_rdy != 0) {
//    (*ss->send_rdy)(ss);
//  }
  return 1;
}

int TransportLayer::do_recv(int skt, uint8_t *data, int size) {
  return recv(skt, data, size, 0);
}

void TransportLayer::release_chunk(void *chunk) {
  dfree(chunk);
}

std::string TransportLayer::get_endpoint(std::string host, int port) {
  return std::to_string(port) + "|" + host;
}

int TransportLayer::recv_ready(SocketIter ss) {
//  VLOG(kLogLevel) << "recv_ready";
  if (ss->recv_buffer == NULL) {
    ss->recv_buffer = static_cast<uint8_t *>(dcalloc(kMaxChunkSize, 1));
    ss->received = 0;
  }

  // See how many bytes we're trying to receive.
  int size = kMaxChunkSize;
  if (ss->received >= 3) {
    size = (ss->recv_buffer[0] & 0xFF) +
           ((ss->recv_buffer[1] << 8) & 0xFF00) +
           ((ss->recv_buffer[2] << 16) & 0xFF0000);
  }

  // Try to fill up the chunk.
  int n = do_recv(ss->skt, ss->recv_buffer + ss->received, size - ss->received);
  if (n == 0) {
    VLOG(kLogLevel) << "recv_ready: EOF " << ss->skt << " " << ss->received
        << " " << ss->descr;
    return 0;
  }
  if (n < 0) {
    extern int errno;
    if (errno == EAGAIN) {
      return 1;
    }
    LOG(ERROR) << strerror(errno) << " recv_ready on socket " << ss->skt;
    return 0;
  }

  VLOG(kLogLevel) << "recv_ready: received " << n
      << " out of " << size << " bytes;" << " previously received "
      << ss->received << " bytes";
  ss->received += n;

  // If we do not yet have a complete header, wait for more.
  if (ss->received < 4) {
    return 1;
  }

  // Calculate the size.
  size = (ss->recv_buffer[0] & 0xFF) +
         ((ss->recv_buffer[1] << 8) & 0xFF00) +
         ((ss->recv_buffer[2] << 16) & 0xFF0000);

  // If we don't have enough yet, return.
  if (size > ss->received) {
    return 1;
  }

  // If we received exactly one chunk, deliver it without copying.
  if (size == ss->received) {
    ss->recv_buffer = static_cast<uint8_t *>(drealloc(ss->recv_buffer, size));
    ss->deliver(ss->recv_buffer + 4, size - 4,
        release_chunk, ss->recv_buffer);
    ss->recv_buffer = NULL;
    return 1;
  }

  // Split up the chunk.
  int offset = 0, remainder = 0;
  do {
    // Deliver a part of the chunk.
    uint8_t *copy = static_cast<uint8_t *>(dmalloc(size - 4));
    if (copy == NULL) {
      LOG(FATAL) << "malloc returns null when requesting "
          << size - 4 << " bytes.";
    }
    memmove(copy, ss->recv_buffer + offset + 4, size - 4);
    ss->deliver(copy, size - 4, release_chunk, copy);
    offset += size;

    // See how much is left.
    remainder = ss->received - offset;
    if (remainder < 4) {
      break;
    }
    size = (ss->recv_buffer[offset] & 0xFF) +
           ((ss->recv_buffer[offset + 1] << 8) & 0xFF00) +
           ((ss->recv_buffer[offset + 2] << 16) & 0xFF0000);
  } while (size <= remainder);

  // Copy the rest to the beginning of the chunk.
  memmove(ss->recv_buffer, ss->recv_buffer + offset, remainder);
  ss->received = remainder;

  return 1;
}

int TransportLayer::got_client(SocketIter ss) {
  int clt;
  struct sockaddr_in sin;
  socklen_t len = sizeof(sin);

  VLOG(kLogLevel) << "got client";

  if ((clt = accept(ss->skt, (struct sockaddr *) &sin, &len)) < 0) {
    LOG(ERROR) << strerror(errno) << " got_client: accept";
    return 0;
  }

  // int on = 1;
  // setsockopt(clt, IPPROTO_TCP, TCP_NODELAY,
  //            (void *) &on, sizeof(on));

  int buflen = 1416 * 1024;
  setsockopt(clt, SOL_SOCKET, SO_SNDBUF, &buflen, sizeof(buflen));
  setsockopt(clt, SOL_SOCKET, SO_RCVBUF, &buflen, sizeof(buflen));

  // Put the socket in non-blocking mode.
  int flags;
  if ((flags = fcntl(clt, F_GETFL, 0)) == -1) {
    flags = 0;
  }
  if (fcntl(clt, F_SETFL, flags | O_NONBLOCK) == -1) {
    LOG(ERROR) << strerror(errno) << " got_client: fcntl";
  }

  add_socket(clt,
      std::bind(&TransportLayer::recv_ready, this, std::placeholders::_1),
      std::bind(&TransportLayer::send_ready, this, std::placeholders::_1),
      deliver_, kSocIn, "", 0);
  return 1;
}

void TransportLayer::tl_listen() {
  // Create and bind a socket.
  int skt = socket(PF_INET, SOCK_STREAM, 0);
  if (skt < 0) {
    LOG(ERROR) << strerror(errno) << " listen: inet socket";
    return;
  }

  int on = 1;
  setsockopt(skt, SOL_SOCKET, SO_REUSEADDR,
      static_cast<void *>(&on), sizeof(on));

  int buflen = 1416 * 1024;
  setsockopt(skt, SOL_SOCKET, SO_SNDBUF, &buflen, sizeof(buflen));
  setsockopt(skt, SOL_SOCKET, SO_RCVBUF, &buflen, sizeof(buflen));

  struct sockaddr_in sin;
  memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = INADDR_ANY;
  sin.sin_port = htons(port_);
  if (bind(skt, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
    LOG(ERROR) << strerror(errno) << " listen: inet bind";
    close(skt);
    return;
  }
  if (listen(skt, 100) < 0) {
    LOG(ERROR) << strerror(errno) << " listen: inet listen";
    close(skt);
    return;
  }
  add_socket(skt,
      std::bind(&TransportLayer::got_client, this, std::placeholders::_1),
      NULL, NULL, kSocListen, "", 0);
}

bool TransportLayer::get_inet_address(struct sockaddr_in *sin,
    const char *addr, int port) {
  struct hostent *h;

  memset(sin, 0, sizeof(*sin));
  sin->sin_family = AF_INET;
  sin->sin_port = htons(port);

  // See if it's a DNS name.
  if (*addr < '0' || *addr > '9') {
    if ((h = gethostbyname(addr)) == 0) {
      LOG(ERROR) << "get_inet_address: gethostbyname '" << addr << "' failed";
      return false;
    }
    sin->sin_addr = * (struct in_addr *) h->h_addr_list[0];
  } else {
    sin->sin_addr.s_addr = inet_addr(addr);
  }
  return true;
}

void TransportLayer::register_peer(const std::string &host, int port) {
  struct sockaddr_in sin;

  // Convert the host name and port into an actual TCP/IP address.
  if (!get_inet_address(&sin, host.c_str(), port)) {
    LOG(ERROR) << "register_node: bad host (" << host << ") or port ("
        << port << ")";
    return;
  }

  std::string endpoint = get_endpoint(host, port);
  LOG(INFO) << "Registering endpoint " << endpoint;

  // See if this address is already in the list.
  if (addr_list_.find(endpoint) != addr_list_.end()) {
    LOG(WARNING) << "register_node: " << host << ":" << port
      << " already existed.";
    return;
  }

  // Add the new entry to the list.
  SocketAddr socket_addr;
  socket_addr.sin = sin;
  socket_addr.host = host;
  socket_addr.port = port;
  socket_addr.in = socket_addr.out = sockets_.end();
  addr_list_[endpoint] = socket_addr;
}

void TransportLayer::try_connect(SocketAddr *socket_addr) {
  VLOG(kLogLevel) << "out socket: " << socket_addr->out->skt;
  if (socket_addr->out == sockets_.end()) {
    // Create the socket.
    int skt = socket(AF_INET, SOCK_STREAM, 0);
    if (skt < 0) {
      LOG(ERROR) << strerror(errno) << " try_connect: socket";
      return;
    }

    int buflen = 1416 * 1024;
    setsockopt(skt, SOL_SOCKET, SO_SNDBUF, &buflen, sizeof(buflen));
    setsockopt(skt, SOL_SOCKET, SO_RCVBUF, &buflen, sizeof(buflen));

    // Put the socket in non-blocking mode.
    int flags;
    if ((flags = fcntl(skt, F_GETFL, 0)) == -1) {
      flags = 0;
    }
    if (fcntl(skt, F_SETFL, flags | O_NONBLOCK) == -1) {
      LOG(ERROR) << strerror(errno) << " register_node: fcntl";
    }

    // Start connect.
    if (connect(skt, (struct sockaddr *) &socket_addr->sin,
        sizeof(socket_addr->sin)) < 0) {
      extern int errno;
      if (errno != EINPROGRESS) {
        LOG(ERROR) << strerror(errno) << " register_node: connect";
        close(skt);
        return;
      }
    }

    // Register the socket.
    socket_addr->out = add_socket(skt,
        std::bind(&TransportLayer::recv_ready, this, std::placeholders::_1),
        std::bind(&TransportLayer::send_ready, this, std::placeholders::_1),
        deliver_, kSocOut, socket_addr->host, socket_addr->port);
  }
}

void TransportLayer::try_connect_all() {
  VLOG(kLogLevel) << "try_connect_all: " << addr_list_.size() << " peers.";
  std::unordered_map<std::string, SocketAddr>::iterator sit;
  for (sit = addr_list_.begin(); sit != addr_list_.end(); ++sit) {
    try_connect(&sit->second);
  }
}

bool TransportLayer::available_for_send(const std::string &host, int port) {
  std::string endpoint = get_endpoint(host, port);
  // There should be an entry for this endpoint.
  CHECK_EQ(addr_list_.count(endpoint), 1) << host << ':' << port;
  SocketAddr &sock_addr = addr_list_[endpoint];
  SocketIter ss = sock_addr.out;
  if (ss == sockets_.end()) {
    // If not connected, return with an error.
//    LOG(INFO) << "available_for_send: not connected.";
    return false;
  }
  
  return ss->ctrl_remainder + ss->data_remainder < kMaxDataBacklog;
}

int TransportLayer::async_send_message(const std::string &host, int port,
    const uint8_t *bytes, int len, bool is_ctrl,
    std::function<void(void *)> cleanup, void *env) {
  std::string endpoint = get_endpoint(host, port);
  // There should be an entry for this endpoint.
  CHECK_EQ(addr_list_.count(endpoint), 1) << host << ':' << port;
  SocketAddr &sock_addr = addr_list_[endpoint];
  SocketIter ss = sock_addr.out;
  if (ss == sockets_.end()) {
    // If not connected, return with an error.
    return -1;
  }

  uint8_t *hdr = static_cast<uint8_t *>(dcalloc(4, 1));

  len += 4;
  hdr[0] = len & 0xFF;
  hdr[1] = (len >> 8) & 0xFF;
  hdr[2] = (len >> 16) & 0xFF;
  async_socket_send(ss, hdr, bytes, len, is_ctrl, cleanup, env);

  return 0;
}

int TransportLayer::do_poll(struct pollfd fds[], nfds_t nfds, int timeout) {
  return poll(fds, nfds, 3);
//  return poll(fds, nfds, timeout);
}

void TransportLayer::prepare_poll(struct pollfd *fds) {
  int i;
  short int events;
  std::list<SprinklerSocket>::iterator sit;
  for (sit = sockets_.begin(), i = 0;
       sit != sockets_.end();
       ++sit, i++) {
    if (sit->skt < 0) {
      LOG(FATAL) << "prepare_poll: socket closed???";
    }

    events = 0;

    // Always check for input, unless there is no input function.
    if (sit->input != 0) {
      fds[i].fd = sit->skt;
      events = POLLIN;
    }

    // Check for output if there's something to write or the very first time.
    if (sit->output != 0
        && (sit->ctrl_remainder + sit->data_remainder > 0 || sit->first)) {
      fds[i].fd = sit->skt;
      events |= POLLOUT;
    }
    fds[i].events = events;
  }

  // Sanity check.
  CHECK_EQ(i, nsockets_) << "prepare_poll: nsockets mismatch.";
}

// Invoke poll(), but with the right timeout.  'start' is the time at which
// TransportLayer::wait() was invoked, and 'now' is the current time.  'timeout'
// is the parameter to TransportLayer::wait().
int TransportLayer::tl_poll(int64_t start,
    int64_t now, int timeout, struct pollfd *fds) {
  int64_t max_delay;
  if (timeout == 0) {
    max_delay = 0;
  } else {
    max_delay = time_to_attempt_connect_ - now;
    if (timeout > 0) {
      int64_t max_delay2 = (start + (timeout * 1000)) - now;
      if (max_delay2 < max_delay) {
        max_delay = max_delay2;
      }
    }
  }

  // Invoke poll().
  int n = do_poll(fds, nsockets_, static_cast<int>(max_delay / 1000));
  if (n < 0) {
    if (errno != EINTR) {
      LOG(ERROR) << strerror(errno) << " tl_poll: poll";
    } else {
      n = 0;
    }
  }

  return n;
}

bool TransportLayer::handle_events(struct pollfd *fds, int n) {
  int i;
  bool closed_sockets = false;
  std::list<SprinklerSocket>::iterator sit;

  for (sit = sockets_.begin(), i = 0;
       sit != sockets_.end();
       ++sit, i++) {
    short events;
    if ((events = fds[i].revents) == 0) {
      continue;
    }
    if (events & (POLLIN | POLLHUP)) {
      if (!(sit->input)(sit)) {
        VLOG(kLogLevel) << "handle_events: closing socket";
        close(sit->skt);
        sit->skt = -1;
        closed_sockets = true;
      }
    }
    if (events & POLLOUT) {
      if (!(sit->output)(sit)) {
        VLOG(kLogLevel) << "handle_events: closing cocket";
        close(sit->skt);
        sit->skt = -1;
        closed_sockets = true;
      }
    }
    if (events & POLLERR) {
      LOG(ERROR) << "POLLERR";
    }
    if (events & POLLNVAL) {
      LOG(ERROR) << "POLLNVAL";
    }
  }
  VLOG(kLogLevel) << "return closed_sockets: " << closed_sockets;
  return closed_sockets;
}

void TransportLayer::remove_closed_sockets() {
  std::list<SprinklerSocket>::iterator sit = sockets_.begin();
  while (sit != sockets_.end()) {
    if (sit->skt == -1) {
      VLOG(kLogLevel) << "Remove closed socket with endpiont (" << sit->host
          << ", " << sit->port << ")";
      // Remove the outgoing socket tracker on address list.
      if (sit->port != 0) {
        std::string endpoint = get_endpoint(sit->host, sit->port);
        if (addr_list_.count(endpoint) == 1) {
          addr_list_[endpoint].out = sockets_.end();
        }
      }

      sit = sockets_.erase(sit);
      nsockets_--;
    } else {
      ++sit;
    }
  }
}

int TransportLayer::wait(int timeout) {
  int64_t start = uptime();
  int64_t now = start;

  for (;;) {
    // See if we should attempt some connections.
    if (now >= time_to_attempt_connect_) {
      try_connect_all();
      time_to_attempt_connect_ = now + 100000;  // 0.1 seconds
    }

    // See what sockets need attention.
    struct pollfd *fds;
    fds = (struct pollfd *) dcalloc(nsockets_, sizeof(*fds));
    prepare_poll(fds);

    // This invokes POSIX poll() with the right timeout.
    int n = tl_poll(start, now, timeout, fds);
    if (n < 0) {
      dfree(fds);
      return 0;
    }

    // If there are events, deal with them.
    bool closed_sockets;
    if (n > 0) {
      closed_sockets = handle_events(fds, n);
    } else {
      closed_sockets = false;
    }

    // Clean up.
    dfree(fds);
    if (closed_sockets) {
      remove_closed_sockets();
    }

    // See if we should return.
    if (timeout == 0) {
      break;
    }
    now = uptime();
    if (timeout > 0 && now > start + (timeout * 1000)) {
      break;
    }
  }

  return 1;
}

void SprinklerSocket::init() {
  socklen_t optlen = sizeof(sndbuf_size);
  if (getsockopt(skt, SOL_SOCKET, SO_SNDBUF, &sndbuf_size, &optlen) < 0) {
    LOG(ERROR) << "add_socket: getsockopt SO_SNDBUF";
  } else {
//    VLOG(TransportLayer::kLogLevel) << "add_socket " << descr
    LOG(INFO) << "add_socket " << descr
        << ": sndbuf " << sndbuf_size << "\n";
  }
  optlen = sizeof(rcvbuf_size);
  if (getsockopt(skt, SOL_SOCKET, SO_RCVBUF, &rcvbuf_size, &optlen) < 0) {
    LOG(ERROR) << "add_socket: getsockopt SO_RCVBUF";
  } else {
    VLOG(TransportLayer::kLogLevel) << "add_socket " << descr
      << ": rcvbuf " << rcvbuf_size << "\n";
  }
  if (sndbuf_size == 0) {
    sndbuf_size = rcvbuf_size;
  }
  if (rcvbuf_size == 0) {
    rcvbuf_size = sndbuf_size;
  }
  if (rcvbuf_size == 0 || sndbuf_size == 0) {
    // TODO(haoyan): make this a constant ...
    rcvbuf_size = sndbuf_size = 64 * 1024;
  }
}
