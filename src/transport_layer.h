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

#include <list>
#include <string>

#include "dmalloc.h"

#define MAX_CHUNK_SIZE (32 * 1024)

// TODO(haoyan): add EPOLL support if necessary.

struct Chunk;
struct SocketAddr;
struct SprinklerSocket;

// Connection manager that manages all the socket connections.
// One per Sprinkler node.
class TransportLayer {
 public:
  // Constructor that specifies the id of this Sprinkler node, and upcalls
  // to the protocol layer.
  TransportLayer(int id,
      void (*outgoing)(TransportLayer*, SprinklerSocket *),
      void (*deliver)(TransportLayer*, SprinklerSocket *,
          const char *, int, void (*release)(void *), void *));

  // Add a new socket to the list of sockets that we know about.
  // Returns the address of that SprinklerSocket object so that upper
  // layers could reference.
  SprinklerSocket *add_socket(int skt,
      int (*input)(TransportLayer *, SprinklerSocket *),
      int (*output)(TransportLayer *, SprinklerSocket *),
      void (*deliver)(TransportLayer *, SprinklerSocket *,
        const char *, int, void (*)(void *), void *),
      char *descr);

  // Send the given chunk of data to the given socket.  It is invoked from
  // l1_wait(), like all other upcalls.  Currently we  simply buffer the
  // message, and send it when l1_wait() is invoked.  Data buffer is freed
  // after the data is sent and the chunk's deconstructor is called.
  void async_socket_send(SprinklerSocket *ss, const char *data, int size);

  // The socket is ready for writing.  Try to send everything that is queued.
  void send_ready(SprinklerSocket *ss);

  // Input is available on some socket.  Peers send packets that are
  // up to size MAX_CHUNK_SIZE and start with a 4 byte header, the first
  // three of which contain the actual packet size (including the header
  // itself).
  int recv_ready(SprinklerSocket *ss);

  // Invoked when there is a client waiting on the server socket.
  int got_client(SprinklerSocket *ls);

  // Listen on a TCP port to wait for connections.
  void listen(int port);

  // Get inet address from (host, port).
  bool get_inet_address(struct sockaddr_in *sin, const char *addr, int port);

  // Register a remote Sprinkler node available to connect.
  void register_node(const char *host, int port);

  // Connect to a remote Sprinkler node.  The node will identify itself so no
  // need to specify which node it is.
  void try_connect(SocketAddr &socket_addr);

  // Try to make connection to all available peers.
  void try_connect_all();

  // Send data to the given connection.
  void async_send_message(SprinklerSocket *ss, const char *bytes, int len);

  // Go through the registered sockets and see which need attention.
  void prepare_poll(struct pollfd *fds);

  // Invoke poll(), but with the right timeout.  'start' is the time at which
  // l1_wait() was invoked, and 'now' is the current time.  'timeout' is the
  // parameter to l1_wait().
  int tl_poll(int64_t start, int64_t now, int timeout, struct pollfd *fds);

  // There are events on one or more sockets.
  // Return true if there is closed socket(s), false otherwise.
  bool handle_events(struct pollfd *fds, int n);

  // Remove sockets that are now closed.
  void remove_closed_sockets();

  // Wait for things to be ready.  Timeout is in milliseconds.  If negative,
  // l1_wait never returns.
  int wait(int timeout);

 private:
  // Invoke send/recv/poll syscalls.
  int do_sendmsg(int skt, struct msghdr *mh);
  int do_recv(int skt, char *data, int size);
  int do_poll(struct pollfd fds[], nfds_t nfds, int timeout);

  // Free chunks sent to upper layer.
  void release_chunk(void *chunk);

  // Proxy ID; unique across a deployment.
  int id_;
  // Linked list of sockets.
  std::list<SprinklerSocket> sockets_;
  // #sockets in the list.
  int nsockets_;
  // Addresses of peers.
  std::list<SocketAddr> addr_list_;
  // Time at which the node starts, i.e. l1_init is invoked.
  timeval starttime_;
  // Interval between connection attempts.
  int64_t time_to_attempt_connect_;

  // Upcalls.
  void (*outgoing_)(TransportLayer *, SprinklerSocket *);
  void (*deliver_)(TransportLayer *, SprinklerSocket *,
      const char *, int, void (*release)(void *), void *);
};

// One of these is allocated for each socket.
struct SprinklerSocket {
  int skt;
  int (*input)(TransportLayer *, SprinklerSocket *);
  int (*output)(TransportLayer *, SprinklerSocket *);
  bool first;      // controls call to l1->outgoing
  std::string descr;    // for debugging
  int sndbuf_size, rcvbuf_size;   // socket send and receive buffer sizes

  // This upcall is invoked when the socket is writable.
  void (*send_rdy)(TransportLayer *, SprinklerSocket *);

  // Queue of chunks of data to send.
  std::list<Chunk> cqueue;    // outgoing chunk queue
  int offset;                 // #bytes that are sent already
  int remainder;              // #bytes waiting to be sent

  // Data received from network to be delivered.
  char *recv_buffer;
  int received;    // #bytes currently in the chunk

  void (*deliver)(TransportLayer *, SprinklerSocket *, const char *,
      int, void (*)(void *), void *);

  // Constructor.
  SprinklerSocket(int skt,
      int (*input)(TransportLayer *, SprinklerSocket *),
      int (*output)(TransportLayer *, SprinklerSocket *),
      void (*deliver)(TransportLayer *, SprinklerSocket *,
          const char *, int, void (*)(void *), void *),
      char *descr)
      : skt(skt),
        input(input), output(output), deliver(deliver),
        descr(descr) {
    first = true;
  }

  // Initialize socket buffer sizes.
  void init();
};

// One of these is allocated for each remote proxy interface.
struct SocketAddr {
  sockaddr_in sin;
  SprinklerSocket *out;    // outgoing connection
  SprinklerSocket *in;     // incoming connection
};

// Data chunk to be sent out from a socket.
struct Chunk {
  const char *data;
  int size;

  ~Chunk() {
    dfree(const_cast<char *>(data));
  }
};

#endif  // TRANSPORT_LAYER_H_
