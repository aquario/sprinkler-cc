#ifndef TRANSPORT_LAYER_H_
#define TRANSPORT_LAYER_H_

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <strings.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define MAX_CHUNK_SIZE (32 * 1024)

// TODO(haoyan): add EPOLL support if necessary.

// Connection manager that manages all the socket connections.
// One per Sprinkler node.
struct cmgr {
  // Proxy ID; unique across a deployment.
  int id;
  // Linked list of sockets.
  struct tl_socket *sockets;
  // #sockets in the list.
  int nsockets;
  // Addresses of peers.
  struct tl_addrlist *addrs;
  // Time at which the node starts, i.e. l1_init is invoked.
  struct timeval starttime;
  // Interval between connection attempts.
  int64_t time_to_attempt_connect;

  // Upcalls.
  void (*outgoing)(struct cmgr *, struct tl_socket *);
  void (*deliver)(struct cmgr *, struct tl_socket *,
      const char *, int, void (*release)(void *), void *);
};

// One of these is allocated for each remote proxy interface.
struct tl_addrlist {
  struct tl_addrlist *next;
  struct sockaddr_in sin;
  struct tl_socket *out;    // outgoing connection
  struct tl_socket *in;     // incoming connection
};

// Outgoing message queue on socket.  release(arg) is invoked (from l1_wait())
// when the data has been sent.
struct tl_chunk_queue {
  struct tl_chunk_queue *next;
  const char *data;
  int size;
  void (*release)(void *arg);
  void *arg;
};

// One of these is allocated for each socket.
struct tl_socket {
  struct tl_socket *next;
  int skt;
  int (*input)(struct cmgr *, struct tl_socket *);
  int (*output)(struct cmgr *, struct tl_socket *);
  int first;      // controls call to l1->outgoing
  char *descr;    // for debugging
  int sndbuf_size, rcvbuf_size;   // socket send and receive buffer sizes

  // This upcall is invoked when the socket is writable.
  void (*send_rdy)(struct cmgr *, struct tl_socket *);

  // Queue of chunks of data to send.
  struct tl_chunk_queue *cqueue;    // outgoing chunk queue
  struct tl_chunk_queue **cqlast;   // indirectly points to end
  int offset;                       // #bytes that are sent already
  unsigned int remainder;           // #bytes waiting to be sent

  // Chunk to be sent.
  char *chunk;
  unsigned int received;    // #bytes currently in the chunk

  void (*deliver)(struct cmgr *, struct tl_socket *, const char *,
      unsigned int, void (*)(void *), void *);
  enum { PS_UNKNOWN, PS_LOCAL, PS_PEER } type;
  union {
    struct l3_local *local;
    struct l2_peer *peer;
  } u;
};

struct cmgr *l1_init(int id,
    void (*outgoing)(struct cmgr *, struct tl_socket *),
    void (*deliver)(struct cmgr *, struct tl_socket *,
      const char *, unsigned int, void (*release)(void *), void *));

void l1_send(struct cmgr *l1, struct tl_socket *ls,
    const char *bytes, unsigned int len,
    void (*upcall)(void *env), void *env);

#endif  // TRANSPORT_LAYER_H_
