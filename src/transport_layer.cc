#include "transport_layer.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#include <functional>

#include <glog/logging.h>

#include "dmalloc.h"

TransportLayer::TransportLayer(int id,
    void (*outgoing)(TransportLayer *, SprinklerSocket *),
    void (*deliver)(TransportLayer *, SprinklerSocket *,
        const char *, int, void (*)(void *), void *)) {
  id_ = id;
  outgoing_ = outgoing;
  deliver_ = deliver;
  gettimeofday(&starttime_, 0);
  // TODO(haoyan): make this a constant ...
  time_to_attempt_connect_ = uptime() + 2000000;

  sockets_ = std::list<SprinklerSocket>();
  nsockets_ = 0;

  addr_list_ = std::list<SocketAddr>();

  LOG(INFO) << "Created Sprinkler node with id " << id_;
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

SprinklerSocket *TransportLayer::add_socket(int skt,
    std::function<int(SprinklerSocket *)> input,
    std::function<int(SprinklerSocket *)> output,
    void (*deliver)(TransportLayer *, SprinklerSocket *,
        const char *, int, void (*)(void *), void *),
    char *descr) {
  SprinklerSocket ss(skt, input, output, deliver, descr);
  ss.init();

  sockets_.push_back(ss);
  ++nsockets_;

  // TODO(haoyan): this is really ugly ...
  CHECK(&(*sockets_.rbegin()));
  return &(*sockets_.rbegin());
}

void TransportLayer::async_socket_send(
    SprinklerSocket *ss, const char *data, int size,
    void (*cleanup)(void *env), void *env) {
  LOG(INFO) << "async_socket_send: send " << size << " bytes.";
  CHECK(ss);

  LOG(INFO) << "cqueue: " << ss->cqueue.empty();
  Chunk chunk(data, size, cleanup, env);
  ss->cqueue.push_back(chunk);
  LOG(INFO) << "-1";
  LOG(INFO) << "0";
  
  LOG(INFO) << "1";

  ss->remainder += size;
  LOG(INFO) << "2";
}

int TransportLayer::do_sendmsg(int skt, struct msghdr *mh) {
  return sendmsg(skt, mh, 0);
}

int TransportLayer::send_ready(SprinklerSocket *ss) {
  LOG(INFO) << "send_ready: offset = " << ss->offset
            << "; bytes remaining = " << ss->remainder;

  // If it's the first time, notify the protocol layer that there is an
  // outgoing connection.
  if (ss->first) {
    (*outgoing_)(this, ss);
  }

  // If there's room in the send buffer, poll the layer above if it is
  // so capable.
//  if (ss->send_rdy != NULL && ss->remainder < ss->sndbuf_size) {
//    (*ss->send_rdy)(ss);
//  }

  // If it's the first time, stop here.
  if (ss->first) {
    ss->first = false;
    LOG(INFO) << "set first to false";
    return 1;
  }

#ifndef IOV_MAX
#define IOV_MAX 1024
#endif

  // Copy what is queued into an iovec, skipping over what has already
  // been sent.
  //
  // TODO.  Could take into account the size of the socket send buffer.
  struct iovec *iov = 0;
  int iovlen = 0;

  std::list<Chunk>::iterator cit;
  int total = 0;
  if (!ss->cqueue.empty()) {
    int offset = ss->offset;
    for (cit = ss->cqueue.begin();
         cit != ss->cqueue.end() && iovlen < IOV_MAX;
         ++cit) {
      if (offset >= cit->size) {
        offset -= cit->size;
      } else {
        if (iovlen == 0) {
          iov = (struct iovec *) dmalloc(sizeof(*iov));
        } else {
          iov = (struct iovec *) drealloc(iov, (iovlen + 1) * sizeof(*iov));
        }
        iov[iovlen].iov_base = (void *) (cit->data + offset);
        iov[iovlen].iov_len = cit->size - offset;
        iovlen++;
        total += cit->size - offset;
        offset = 0;
      }
    }
  }

  // If there's anything in the iovec, send it now.
  if (iovlen > 0) {
    struct msghdr mh;
    memset(&mh, 0, sizeof(mh));
    mh.msg_iov = iov;
    mh.msg_iovlen = iovlen;
    int n = do_sendmsg(ss->skt, &mh);
    if (n <= 0) {
      // TODO: should I do anything about this?  Presumably the
      // receive side of the socket is closed and everything
      // is cleaned up automatically.
      LOG(FATAL) << "send_ready: sendmsg";
    } else {
      LOG(INFO) << "send_ready: sent " << n << " out of "
                << total << " bytes to socket " << ss->skt
                << "; iovlen = " << iovlen;

      ss->offset += n;
      ss->remainder -= n;
    }
    dfree(iov);
  } else {
    LOG(WARNING) << "send_ready: nothing to send??";
  }

  // Release everything that can be released in the queue.
  while ((cit = ss->cqueue.begin()) != ss->cqueue.end()
      && ss->offset >= cit->size) {
    ss->offset -= cit->size;
    ss->cqueue.pop_front();
  }

  // If the buffer is now empty, try to fill it up.
//  if (ss->remainder == 0 && ss->send_rdy != 0) {
//    (*ss->send_rdy)(ss);
//  }
  return 1;
}

int TransportLayer::do_recv(int skt, char *data, int size) {
  return recv(skt, data, size, 0);
}

void TransportLayer::release_chunk(void *chunk) {
  dfree(chunk);
}

int TransportLayer::recv_ready(SprinklerSocket *ss) {
  LOG(INFO) << "recv_ready";
  if (ss->recv_buffer == NULL) {
    ss->recv_buffer = (char *) dcalloc(MAX_CHUNK_SIZE, 1);
    ss->received = 0;
  }

  // See how many bytes we're trying to receive.
  int size = MAX_CHUNK_SIZE;
  if (ss->received >= 3) {
    size = (ss->recv_buffer[0] & 0xFF) +
           ((ss->recv_buffer[1] << 8) & 0xFF00) +
           ((ss->recv_buffer[2] << 16) & 0xFF0000);
  }

  // Try to fill up the chunk.
  int n = do_recv(ss->skt, ss->recv_buffer + ss->received, size - ss->received);
  if (n == 0) {
    LOG(INFO) << "recv_ready: EOF " << ss->skt << " " << ss->received
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

  LOG(INFO) << "recv_ready: received " << n << " out of " << size << " bytes";
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
    ss->recv_buffer = (char *) drealloc(ss->recv_buffer, size);
    (*ss->deliver)
        (this, ss, ss->recv_buffer + 4, size - 4, release_chunk, ss->recv_buffer);
    ss->recv_buffer = 0;
    return 1;
  }

  // Split up the chunk.
  int offset = 0, remainder = 0;
  do {
    // Deliver a part of the chunk.
    char *copy = (char *) dmalloc(size - 4);
    memcpy(copy, ss->recv_buffer + offset + 4, size - 4);
    (*ss->deliver)(this, ss, copy, size - 4, release_chunk, copy);
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
  memcpy(ss->recv_buffer, ss->recv_buffer + offset, remainder);

  return 1;
}

int TransportLayer::got_client(SprinklerSocket *ss) {
  int clt;
  struct sockaddr_in sin;
  socklen_t len = sizeof(sin);

  LOG(INFO) << "got client";

  if ((clt = accept(ss->skt, (struct sockaddr *) &sin, &len)) < 0) {
    LOG(ERROR) << strerror(errno) << " got_client: accept";
    return 0;
  }

  // int on = 1;
  // setsockopt(clt, IPPROTO_TCP, TCP_NODELAY,
  //            (void *) &on, sizeof(on));

  int buflen = 128 * 1024;
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
      deliver_, "got_client");
  return 1;
}

void TransportLayer::tl_listen(int port) {
  // Create and bind a socket.
  int skt = socket(PF_INET, SOCK_STREAM, 0);
  if (skt < 0) {
    LOG(ERROR) << strerror(errno) << " listen: inet socket";
    return;
  }

  int on = 1;
  setsockopt(skt, SOL_SOCKET, SO_REUSEADDR,
      (void *) &on, sizeof(on));

  int buflen = 128 * 1024;
  setsockopt(skt, SOL_SOCKET, SO_SNDBUF, &buflen, sizeof(buflen));
  setsockopt(skt, SOL_SOCKET, SO_RCVBUF, &buflen, sizeof(buflen));

  struct sockaddr_in sin;
  memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = INADDR_ANY;
  sin.sin_port = htons(port);
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
      NULL, NULL, "listen TCP");
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

void TransportLayer::register_peer(const char *host, int port) {
  struct sockaddr_in sin;

  // Convert the host name and port into an actual TCP/IP address.
  if (!get_inet_address(&sin, host, port)) {
    LOG(ERROR) << "register_node: bad host (" << host << ") or port ("
        << port << ")";
    return;
  }

  // See if this address is already in the list.
  for (std::list<SocketAddr>::iterator sit = addr_list_.begin();
       sit != addr_list_.end();
       ++sit) {
    if (memcmp(&sin, &sit->sin, sizeof(sin)) == 0) {
      LOG(WARNING) << "register_node: " << host << ":" << port
          << " already existed.";
      return;
    }
  }

  // Add the new entry to the list.
  addr_list_.push_back(SocketAddr());
  SocketAddr &soc_addr = *addr_list_.rbegin();

  soc_addr.sin = sin;
  soc_addr.in = soc_addr.out = NULL;
}

void TransportLayer::try_connect(SocketAddr &socket_addr) {
  LOG(INFO) << "out addr: " << (uint64_t) socket_addr.out;
  if (socket_addr.out == NULL) {
    // Create the socket.
    int skt = socket(AF_INET, SOCK_STREAM, 0);
    if (skt < 0) {
      LOG(ERROR) << strerror(errno) << " try_connect: socket";
      return;
    }

    int buflen = 128 * 1024;
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
    if (connect(skt, (struct sockaddr *) &socket_addr.sin,
        sizeof(socket_addr.sin)) < 0) {
      extern int errno;
      if (errno != EINPROGRESS) {
        LOG(ERROR) << strerror(errno) << " register_node: connect";
        close(skt);
        return;
      }
    }

    // Register the socket.
    socket_addr.out = add_socket(skt,
        std::bind(&TransportLayer::recv_ready, this, std::placeholders::_1),
        std::bind(&TransportLayer::send_ready, this, std::placeholders::_1),
        deliver_, "sprinkler_peer");
  }
}

void TransportLayer::try_connect_all() {
  LOG(INFO) << "try_connect_all";
  for (std::list<SocketAddr>::iterator sit = addr_list_.begin();
       sit != addr_list_.end();
       ++sit) {
    try_connect(*sit);
  }
}

void TransportLayer::async_send_message(SprinklerSocket *ss,
    const char *bytes, int len,
    void (*cleanup)(void *env), void *env) {
  char *hdr = (char *) dcalloc(4, 1);

  len += 4;
  hdr[0] = len & 0xFF;
  hdr[1] = (len >> 8) & 0xFF;
  hdr[2] = (len >> 16) & 0xFF;
  async_socket_send(ss, hdr, 4, release_chunk, hdr);
  async_socket_send(ss, bytes, len - 4, cleanup, env);
}

int TransportLayer::do_poll(struct pollfd fds[], nfds_t nfds, int timeout) {
  return poll(fds, nfds, timeout);
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
    if (sit->output != 0 && (sit->remainder > 0 || sit->first)) {
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
  SprinklerSocket *ss;
  std::list<SprinklerSocket>::iterator sit;

  for (sit = sockets_.begin(), i = 0;
       sit != sockets_.end();
       ++sit, i++) {
    short events;
    if ((events = fds[i].revents) == 0) {
      continue;
    }
    if (events & (POLLIN | POLLHUP)) {
      if (!(sit->input)(&*sit)) {
        LOG(INFO) << "handle_events: closing socket";
        close(sit->skt);
        sit->skt = -1;
        closed_sockets = true;
      }
    }
    if (events & POLLOUT) {
      (sit->output)(&*sit);
    }
    if (events & POLLERR) {
      LOG(ERROR) << "POLLERR";
    }
    if (events & POLLNVAL) {
      LOG(ERROR) << "POLLNVAL";
    }
  }
  LOG(ERROR) << "return closed_sockets: " << closed_sockets;
  return closed_sockets;
}

void TransportLayer::remove_closed_sockets() {
  std::list<SprinklerSocket>::iterator sit = sockets_.begin();
  while (sit != sockets_.end()) {
    if (sit->skt == -1) {
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
      time_to_attempt_connect_ = now + 2000000;  // 2 seconds
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
    LOG(INFO) << "add_socket " << descr
              << ": sndbuf " << sndbuf_size << "\n";
  }
  optlen = sizeof(rcvbuf_size);
  if (getsockopt(skt, SOL_SOCKET, SO_RCVBUF, &rcvbuf_size, &optlen) < 0) {
    LOG(ERROR) << "add_socket: getsockopt SO_RCVBUF";
  } else {
    LOG(INFO) << "add_socket " << descr
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
