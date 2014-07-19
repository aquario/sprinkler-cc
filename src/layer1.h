#ifndef LAYER1_H_
#define LAYER1_H_

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netdb.h>
#include <poll.h>
#include <strings.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define MAX_CHUNK_SIZE		(32 * 1024)

#ifdef USE_EPOLL
#include <sys/epoll.h>
#define MAX_EPOLL_EVENTS	10
#endif // USE_EPOLL

struct l1_global {
  // id of proxy.
  int id;
  // linked list of sockets.
  struct l1_socket *sockets;
  // #sockets in the list.
  unsigned int nsockets;
  // addresses of peers.
  struct l1_addrlist *addrs;
  // epoll fd (-1 means no epoll).
  int epoll;
  // time at which l1_init was invoked.
  struct timeval starttime;
  // periodically attempting connects.
  unsigned long long time_to_attempt_connect;

  // Upcalls.
  void (*outgoing)(struct l1_global *, struct l1_socket *);
  void (*deliver)(struct l1_global *, struct l1_socket *,
      const char *, unsigned int, void (*release)(void *), void *);
};

// One of these is allocated for each remote proxy interface.
struct l1_addrlist {
  struct l1_addrlist *next;
  struct sockaddr_in sin;
  struct l1_socket *out;			// outgoing connection
  struct l1_socket *in;			// incoming connection
};

// Outgoing message queue on socket.  release(arg) is invoked (from l1_wait())
// when the data has been sent.
struct l1_chunk_queue {
  struct l1_chunk_queue *next;
  const char *data;
  unsigned int size;
  void (*release)(void *arg);
  void *arg;
};

// One of these is allocated for each socket.
struct l1_socket {
  struct l1_socket *next;
  int skt;
  int (*input)(struct l1_global *, struct l1_socket *);
  int (*output)(struct l1_global *, struct l1_socket *);
  int first;							// controls call to l1->outgoing
  char *descr;						// for debugging
  int sndbuf_size, rcvbuf_size;		// socket send and receive buffer sizes

  // This upcall is invoked when the socket is writable.
  void (*send_rdy)(struct l1_global *, struct l1_socket *);

  // Queue of chunks of data to send.
  struct l1_chunk_queue *cqueue;		// outgoing chunk queue
  struct l1_chunk_queue **cqlast;		// indirectly points to end
  unsigned int offset;				// #bytes that are sent already
  unsigned int remainder;				// #bytes waiting to be sent

  // Chunk to be sent.
  char *chunk;
  unsigned int received;				// #bytes currently in the chunk

  void (*deliver)(struct l1_global *, struct l1_socket *, const char *,
      unsigned int, void (*)(void *), void *);
  enum { PS_UNKNOWN, PS_LOCAL, PS_PEER } type;
  union {
    struct l3_local *local;
    struct l2_peer *peer;
  } u;
};

struct l1_conn {
  struct l1_global *global;		// pointer to global data
};

// There's a record for each peer proxy.
struct l1_record {
  struct l1_record *next;
  char *proxyid;					// proxy identifier
  int port;
  struct l1_addrlist *addrs;	// list of TCP/IP addresses

  // Layer3 stats.
  unsigned long long data_sent;			// # bytes sent to this proxy
  unsigned long long data_recvd;			// # bytes received from this proxy
};

struct l1_global *l1_init(int id,
    void (*outgoing)(struct l1_global *, struct l1_socket *),
    void (*deliver)(struct l1_global *, struct l1_socket *,
      const char *, unsigned int, void (*release)(void *), void *)
    );

void l1_send(struct l1_global *l1, struct l1_socket *ls,
    const char *bytes, unsigned int len,
    void (*upcall)(void *env), void *env);

#endif  // LAYER1_H_
