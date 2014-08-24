#include "transport_layer.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#include "dmalloc.h"

// The optimizer inlines static functions, making profiling harder.
#ifdef PROFILING
#define private   // nothing
#else
#define private static
#endif

// Return the number of microseconds since we started.
private int64_t l1_clock(struct cmgr *l1) {
  struct timeval now;
  int64_t result;

  gettimeofday(&now, 0);
  result = (now.tv_sec - l1->starttime.tv_sec) * 1000000;
  result += now.tv_usec;
  result -= l1->starttime.tv_usec;
  return result;
}

// Return "HH:MM:SS.XXX", where XXX is in milliseconds.
char *l1_now() {
  static char buf[128];
  struct timeval tv;
  gettimeofday(&tv, 0);
  struct tm *tm = localtime(&tv.tv_sec);
  sprintf(buf, "%02d:%02d:%02d.%03d",
      tm->tm_hour, tm->tm_min, tm->tm_sec, (int) (tv.tv_usec / 1000));
  return buf;
}

// Add a new socket to the list of sockets that we know about.
private struct tl_socket *tl_socket_add(struct cmgr *l1, int skt,
    int (*input)(struct cmgr *, struct tl_socket *),
    int (*output)(struct cmgr *, struct tl_socket *),
    void (*deliver)(struct cmgr *, struct tl_socket *,
        const char *, unsigned int, void (*)(void *), void *),
    char *descr) {
  struct tl_socket *ls = (struct tl_socket *) dcalloc(1, sizeof(*ls));

  ls->skt = skt;
  ls->input = input;
  ls->output = output;
  ls->deliver = deliver;
  ls->descr = descr;
  ls->cqlast = &ls->cqueue;
  ls->first = 1;
  ls->next = l1->sockets;
  l1->sockets = ls;
  l1->nsockets++;

  // Get the socket buffer sizes.
  socklen_t optlen = sizeof(ls->sndbuf_size);
  if (getsockopt(skt, SOL_SOCKET, SO_SNDBUF, &ls->sndbuf_size, &optlen) < 0) {
    perror("tl_socket_add: getsockopt SO_SNDBUF");
  } else {
    printf("tl_socket_add %s: sndbuf %d\n", descr, ls->sndbuf_size);
  }
  optlen = sizeof(ls->rcvbuf_size);
  if (getsockopt(skt, SOL_SOCKET, SO_RCVBUF, &ls->rcvbuf_size, &optlen) < 0) {
    perror("tl_socket_add: getsockopt SO_RCVBUF");
  } else {
    printf("tl_socket_add %s: rcvbuf %d\n", descr, ls->rcvbuf_size);
  }
  if (ls->sndbuf_size == 0) {
    ls->sndbuf_size = ls->rcvbuf_size;
  }
  if (ls->rcvbuf_size == 0) {
    ls->rcvbuf_size = ls->sndbuf_size;
  }
  if (ls->rcvbuf_size == 0 || ls->sndbuf_size == 0) {
    ls->rcvbuf_size = ls->sndbuf_size = 64 * 1024;
  }

  return ls;
}

// The first call into transport_layer.  Specifies the id of the proxy.
struct cmgr *l1_init(int id,
    void (*outgoing)(struct cmgr *, struct tl_socket *),
    void (*deliver)(struct cmgr *, struct tl_socket *,
        const char *, unsigned int, void (*release)(void *), void *)) {
  struct cmgr *l1 = (struct cmgr *) dcalloc(1, sizeof(*l1));

  l1->id = id;
  l1->outgoing = outgoing;
  l1->deliver = deliver;
  gettimeofday(&l1->starttime, 0);
  l1->time_to_attempt_connect = l1_clock(l1) + 2000000;

#ifdef MERGE_TOOL
  fprintf(stderr, "#N:%s\n", id);
#endif
  return l1;
}

// Send the given chunk of data to the given socket.  (*release)(arg) is
// invoked (unless 0) when the data has been sent and acknowledged.  It
// is invoked from l1_wait(), like all other upcalls.  Currently we
// simply buffer the message, and send it when l1_wait() is invoked.
void tl_socket_send(struct cmgr *l1, struct tl_socket *ls,
    const char *data, unsigned int size, void (*release)(void *), void *arg) {
#ifdef USE_DEBUG
  printf("tl_socket_send: %d\n", size);
#endif

  struct tl_chunk_queue *cq = (struct tl_chunk_queue *) dcalloc(1, sizeof(*cq));
  cq->data = data;
  cq->size = size;
  cq->release = release;
  cq->arg = arg;

  *ls->cqlast = cq;
  ls->cqlast = &cq->next;
  ls->remainder += size;
}

int do_sendmsg(int skt, struct msghdr *mh) {
  return sendmsg(skt, mh, 0);
}

// The socket is ready for writing.  Try to send everything that is queued.
private int l1_send_ready(struct cmgr *l1, struct tl_socket *ls) {
#ifdef USE_DEBUG
  printf("l1_send_ready %d %d %s\n", ls->offset, ls->remainder, l1_now());
#endif

  // If the first time, notify layer2 that there is an outgoing connection.
  if (ls->first) {
    (*l1->outgoing)(l1, ls);
  }

  // If there's room in the send buffer, poll the layer above if it is
  // so capable.
  if (ls->send_rdy != 0 && ls->remainder < ls->sndbuf_size) {
    (*ls->send_rdy)(l1, ls);
  }

  // If it's the first time, stop here.
  if (ls->first) {
    ls->first = 0;
    return 1;
  }

#ifndef IOV_MAX
#define IOV_MAX 1024
#endif

  // Copy what is queued into an iovec, skipping over what has already
  // been sent.
  //
  // TODO.  Could take into account the size of the socket send buffer.
  struct tl_chunk_queue *cq;
  unsigned int offset = ls->offset;
  struct iovec *iov = 0;
  int iovlen = 0;
  unsigned int total = 0;
  for (cq = ls->cqueue; cq != 0 && iovlen < IOV_MAX; cq = cq->next) {
    if (offset >= cq->size) {
      offset -= cq->size;
    } else {
      if (iovlen == 0) {
        iov = (struct iovec *) dmalloc(sizeof(*iov));
      } else {
        iov = (struct iovec *) drealloc(iov, (iovlen + 1) * sizeof(*iov));
      }
      iov[iovlen].iov_base = cq->data + offset;
      iov[iovlen].iov_len = cq->size - offset;
      iovlen++;
      total += cq->size - offset;
      offset = 0;
    }
  }

  // If there's anything in the iovec, send it now.
  if (iovlen > 0) {
    struct msghdr mh;
    memset(&mh, 0, sizeof(mh));
    mh.msg_iov = iov;
    mh.msg_iovlen = iovlen;
    int n = do_sendmsg(ls->skt, &mh);
#ifdef USE_DEBUG
    fprintf(stderr,
            "l1_send_ready: sent %d bytes out of %d to socket %d (%s) %d\n",
            n, total, ls->skt, l1_now(), iovlen);
#endif
    if (n <= 0) {
      // TODO: should I do anything about this?  Presumably the
      // receive side of the socket is closed and everything
      // is cleaned up automatically.
      perror("l1_send_ready: sendmsg");
      exit(1);
    } else {
      ls->offset += n;
      ls->remainder -= n;
    }
    dfree(iov);
  } else {
    fprintf(stderr, "l1_send_ready: nothing to send %s??\n", l1_now());
  }

  // Release everything that can be released in the queue.
  while ((cq = ls->cqueue) != 0 && ls->offset >= cq->size) {
    if (cq->release != 0) {
      (*cq->release)(cq->arg);
    }
    ls->offset -= cq->size;
    ls->cqueue = cq->next;
    dfree(cq);
  }
  if (ls->cqueue == 0) {
    ls->cqlast = &ls->cqueue;
  }

  // If the buffer is now empty, try to fill it up.
  if (ls->remainder == 0 && ls->send_rdy != 0) {
    (*ls->send_rdy)(l1, ls);
  }

  return 1;
}

private void l1_release_chunk(void *chunk) {
  dfree(chunk);
}

int do_recv(int skt, char *data, int size) {
  return recv(skt, data, size, 0);
}

// Input is available on some socket.  Peers send packets that are
// up to size MAX_CHUNK_SIZE and start with a 4 byte header, the first
// three of which contain the actual packet size (including the header
// itself).
private int l1_recv_ready(struct cmgr *l1, struct tl_socket *ls) {
  if (ls->chunk == 0) {
    ls->chunk = (char *) dmalloc(MAX_CHUNK_SIZE);
    ls->received = 0;
  }

  // See how many bytes we're trying to receive.
  int size = MAX_CHUNK_SIZE;
  if (ls->received >= 3) {
    size = (ls->chunk[0] & 0xFF) +
           ((ls->chunk[1] << 8) & 0xFF00) +
           ((ls->chunk[2] << 16) & 0xFF0000);
  }

  // Try to fill up the chunk.
  int n = do_recv(ls->skt, ls->chunk + ls->received, size - ls->received);
  if (n == 0) {
    fprintf(stderr, "l1_recv_ready: EOF %d %d %d %s\n",
        ls->skt, ls->received, ls->type, ls->descr);
    return 0;
  }
  if (n < 0) {
    extern int errno;
    if (errno == EAGAIN) {
      return 1;
    }
    perror("l1_recv_ready: recv");
    return 0;
  }

#ifdef USE_DEBUG
  fprintf(stderr, "l1_recv_ready: received %d bytes out of %d (%s)\n",
      n, size, l1_now());
#endif
  ls->received += n;

  // If we do not yet have a complete header, wait for more.
  if (ls->received < 4) {
    return 1;
  }

  // Calculate the size.
  size = (ls->chunk[0] & 0xFF) +
         ((ls->chunk[1] << 8) & 0xFF00) +
         ((ls->chunk[2] << 16) & 0xFF0000);

  // If we don't have enough yet, return.
  if (size > ls->received) {
    return 1;
  }

  // If we received exactly one chunk, deliver it without copying.
  if (size == ls->received) {
    ls->chunk = (char *) drealloc(ls->chunk, size);
    (*ls->deliver)
        (l1, ls, ls->chunk + 4, size - 4, l1_release_chunk, ls->chunk);
    ls->chunk = 0;
    return 1;
  }

  // Split up the chunk.
  int offset = 0, remainder = 0;
  do {
    // Deliver a part of the chunk.
    char *copy = dmalloc(size - 4);
    memcpy(copy, ls->chunk + offset + 4, size - 4);
    (*ls->deliver)(l1, ls, copy, size - 4, l1_release_chunk, copy);
    offset += size;

    // See how much is left.
    remainder = ls->received - offset;
    if (remainder < 4) {
      break;
    }
    size = (ls->chunk[offset] & 0xFF) +
           ((ls->chunk[offset + 1] << 8) & 0xFF00) +
           ((ls->chunk[offset + 2] << 16) & 0xFF0000);
  } while (size <= remainder);

  // Copy the rest to the beginning of the chunk.
  memcpy(ls->chunk, ls->chunk + offset, remainder);

  return 1;
}

// Invoked when there is a client waiting on the server socket.
private int l1_gotclient(struct cmgr *l1, struct tl_socket *ls) {
  int clt;
  struct sockaddr_in sin;
  socklen_t len = sizeof(sin);

  fprintf(stderr, "l1_gotclient\n");

  if ((clt = accept(ls->skt, (struct sockaddr *) &sin, &len)) < 0) {
    perror("l1_gotclient: accept");
    return 0;
  }

  int on = 1;
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
    perror("l1_gotclient: fcntl");
  }

  tl_socket_add(l1, clt, l1_recv_ready,
      l1_send_ready, l1->deliver, "l1_gotclient");
  return 1;
}

// A TCP port to wait for connections.
void l1_listen(struct cmgr *l1, unsigned int port) {
  // Create and bind a socket.
  int skt = socket(PF_INET, SOCK_STREAM, 0);
  if (skt < 0) {
    perror("l1_listen: inet socket");
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
    perror("l1_listen: inet bind");
    close(skt);
    return;
  }
  if (listen(skt, 100) < 0) {
    perror("l1_listen: inet listen");
    close(skt);
    return;
  }
  tl_socket_add(l1, skt, l1_gotclient, 0, 0, "l1_listen TCP");
}

private int l1_get_addr(struct sockaddr_in *sin,
    const char *addr, unsigned int port) {
  struct hostent *h;

  memset(sin, 0, sizeof(*sin));
  sin->sin_family = AF_INET;
  sin->sin_port = htons(port);

  // See if it's a DNS name.
  if (*addr < '0' || *addr > '9') {
    if ((h = gethostbyname(addr)) == 0) {
      fprintf(stderr, "l1_get_addr: gethostbyname '%s' failed\n", addr);
      return 0;
    }
    sin->sin_addr = * (struct in_addr *) h->h_addr_list[0];
  } else {
    sin->sin_addr.s_addr = inet_addr(addr);
  }
  return 1;
}

void l1_connect(struct cmgr *l1, const char *host, unsigned int port) {
  struct sockaddr_in sin;
  struct tl_addrlist *al;

  // Convert the host name and port into an actual TCP/IP address.
  if (!l1_get_addr(&sin, host, port)) {
    fprintf(stderr, "l1_connect: bad host (%s) or port (%d)\n", host, port);
    return;
  }

  // See if this address is already in the list.
  for (al = l1->addrs; al != 0; al = al->next) {
    if (memcmp(&sin, &al->sin, sizeof(sin)) == 0) {
      fprintf(stderr, "l1_connect: warning: duplicate %s:%d\n", host, port);
      return;
    }
  }

  // Add the new entry to the list.
  al = (struct tl_addrlist *) dcalloc(1, sizeof(*al));
  al->sin = sin;
  al->next = l1->addrs;
  l1->addrs = al;
}

// Connect to a remote proxy.  The proxy will identify itself so no
// need to specify which proxy it is.
void l1_try_connect(struct cmgr *l1) {
  struct tl_addrlist *al;

  fprintf(stderr, "l1_try_connect\n");
  for (al = l1->addrs; al != 0; al = al->next) {
    if (al->out == 0) {
      // Create the socket.
      int skt = socket(AF_INET, SOCK_STREAM, 0);
      if (skt < 0) {
        perror("l1_connect: socket");
        return;
      }

      int on = 1;
      // setsockopt(skt, IPPROTO_TCP, TCP_NODELAY,
      //            (void *) &on, sizeof(on));

      int buflen = 128 * 1024;
      setsockopt(skt, SOL_SOCKET, SO_SNDBUF, &buflen, sizeof(buflen));
      setsockopt(skt, SOL_SOCKET, SO_RCVBUF, &buflen, sizeof(buflen));

      // Put the socket in non-blocking mode.
      int flags;
      if ((flags = fcntl(skt, F_GETFL, 0)) == -1) {
        flags = 0;
      }
      if (fcntl(skt, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("l1_connect: fcntl");
      }

      // Start connect.
      if (connect(skt, (struct sockaddr *) &al->sin, sizeof(al->sin)) < 0) {
        extern int errno;
        if (errno != EINPROGRESS) {
          perror("l1_connect: connect");
          close(skt);
          return;
        }
      }

      // Register the socket.
      al->out = tl_socket_add(l1, skt, l1_recv_ready,
          l1_send_ready, l1->deliver, "l1_connect");

      break;
    }
  }
}

// Send data to the given connection, and invoke upcall when done.
void l1_send(struct cmgr *l1, struct tl_socket *ls,
    const char *bytes, unsigned int len,
    void (*upcall)(void *env), void *env) {
  char *hdr = dcalloc(4, 1);

  len += 4;
  hdr[0] = len & 0xFF;
  hdr[1] = (len >> 8) & 0xFF;
  hdr[2] = (len >> 16) & 0xFF;
  tl_socket_send(l1, ls, hdr, 4, l1_release_chunk, hdr);
  tl_socket_send(l1, ls, bytes, len - 4, upcall, env);
}

int do_poll(struct pollfd fds[], nfds_t nfds, int timeout) {
  return poll(fds, nfds, timeout);
}

// Go through the registered sockets and see which need attention.
void l1_prepare_poll(struct cmgr *l1, struct pollfd *fds) {
  unsigned int i;
  short int events;
  struct tl_socket *ls;
  for (ls = l1->sockets, i = 0; ls != 0; ls = ls->next, i++) {
#ifdef USE_DEBUG
    if (ls->skt < 0) {
      fprintf(stderr, "l1_prepare_poll: socket closed???\n");
      exit(1);
    }
#endif
    events = 0;

    // Always check for input, unless there is no input function.
    if (ls->input != 0) {
      fds[i].fd = ls->skt;
      events = POLLIN;
    }

    // Check for output if there's something to write or the very first time.
    if (ls->output != 0 && (ls->remainder > 0 || ls->first)) {
      fds[i].fd = ls->skt;
      events |= POLLOUT;
    }
    fds[i].events = events;
  }

  // Sanity check.
  if (i != l1->nsockets) {
    fprintf(stderr, "l1_prepare_poll: nsockets %d %d\n",
        i, l1->nsockets);
    exit(1);
  }
}

// Invoke poll(), but with the right timeout.  'start' is the time at which
// l1_wait() was invoked, and 'now' is the current time.  'timeout' is the
// parameter to l1_wait().
private int l1_poll(struct cmgr *l1, int64_t start,
    int64_t now, int timeout, struct pollfd *fds) {
  int64_t max_delay;
  if (timeout == 0) {
    max_delay = 0;
  } else {
    max_delay = l1->time_to_attempt_connect - now;
    if (timeout > 0) {
      int64_t max_delay2 =
          (start + (timeout * 1000)) - now;
      if (max_delay2 < max_delay) {
        max_delay = max_delay2;
      }
    }
  }

  // Invoke poll().
  int n = do_poll(fds, l1->nsockets, (int) (max_delay / 1000));
  if (n < 0) {
    if (errno != EINTR) {
      perror("l1_poll: poll");
    } else {
      n = 0;
    }
  }

  return n;
}

// There are events on one or more sockets.
private int l1_handle_events(struct cmgr *l1, struct pollfd *fds, int n) {
  int i, closed_sockets = 0;
  struct tl_socket *ls;

  for (ls = l1->sockets, i = 0; ls != 0; ls = ls->next, i++) {
    short events;
    if ((events = fds[i].revents) == 0) {
      continue;
    }
    if (events & (POLLIN | POLLHUP)) {
      if (!(*ls->input)(l1, ls)) {
        printf("l1_handle_events: closing socket\n");
        close(ls->skt);
        ls->skt = -1;
        closed_sockets = 1;
      }
    }
    if (events & POLLOUT) {
      (*ls->output)(l1, ls);
    }
    if (events & POLLERR) {
      fprintf(stderr, "POLLERR\n");
    }
    if (events & POLLNVAL) {
      fprintf(stderr, "POLLNVAL\n");
    }
  }
  return closed_sockets;
}

// Release a socket structure.
private void tl_socket_release(struct cmgr *l1, struct tl_socket *ls) {
  // Release the outgoing message queue.
  struct tl_chunk_queue *cq;
  while ((cq = ls->cqueue) != 0) {
    if (cq->release != 0) {
      (*cq->release)(cq->arg);
    }
    ls->cqueue = cq->next;
    dfree(cq);
  }

  struct tl_addrlist *al;
  for (al = l1->addrs; al != 0; al = al->next) {
    if (al->out == ls) {
      al->out = 0;
      break;
    }
  }

  // Release the structure itself.
  dfree(ls);

  l1->nsockets--;
}

// Remove sockets that are now closed.
private void remove_closed_sockets(struct cmgr *l1) {
  struct tl_socket **pls, *ls;
  for (pls = &l1->sockets; (ls = *pls) != 0;) {
    if (ls->skt == -1) {
      *pls = ls->next;
      tl_socket_release(l1, ls);
    } else {
      pls = &ls->next;
    }
  }
}

// Wait for things to be ready.  Timeout is in milliseconds.  If negative,
// l1_wait never returns.
int l1_wait(struct cmgr *l1, int timeout) {
  int64_t start = l1_clock(l1);
  int64_t now = start;

  for (;;) {
    // See if we should attempt some connections.
    if (now >= l1->time_to_attempt_connect) {
      l1_try_connect(l1);
      l1->time_to_attempt_connect = now + 2000000;  // 2 seconds
    }

    // See what sockets need attention.
    struct pollfd *fds;
    fds = (struct pollfd *) dcalloc(l1->nsockets, sizeof(*fds));
    l1_prepare_poll(l1, fds);

    // This invokes POSIX poll() with the right timeout.
    int n = l1_poll(l1, start, now, timeout, fds);
    if (n < 0) {
      dfree(fds);
      return 0;
    }

    // If there are events, deal with them.
    int closed_sockets;
    if (n > 0) {
      closed_sockets = l1_handle_events(l1, fds, n);
    } else {
      closed_sockets = 0;
    }

    // Clean up.
    dfree(fds);
    if (closed_sockets) {
      remove_closed_sockets(l1);
    }

    // See if we should return.
    if (timeout == 0) {
      break;
    }
    now = l1_clock(l1);
    if (timeout > 0 && now > start + (timeout * 1000)) {
      break;
    }
  }

  return 1;
}
