#ifndef TRANSPORT_LAYER_H_
#define TRANSPORT_LAYER_H_

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <functional>
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
      std::function<void(SprinklerSocket *)> outgoing,
      std::function<void(SprinklerSocket *,
          const char *, int, std::function<void(void *)>, void *)> deliver);

  // Return the number of microseconds since we started.
  int64_t uptime();

  // Listen on a TCP port to wait for connections.
  void tl_listen(int port);

  // Register a peer Sprinkler node available to connect.
  void register_peer(const char *host, int port);

  // Send data to the given connection.
  void async_send_message(SprinklerSocket *ss,
      const char *bytes, int len, bool is_ctrl,
      void (*cleanup)(void *env), void *env);

  // Wait for things to be ready.  Timeout is in milliseconds.  If negative,
  // l1_wait never returns.
  int wait(int timeout);

 private:
  // Invoke send/recv/poll syscalls.
  int do_sendmsg(int skt, struct msghdr *mh);
  int do_recv(int skt, char *data, int size);
  int do_poll(struct pollfd fds[], nfds_t nfds, int timeout);

  // Free chunks sent to upper layer.
  static void release_chunk(void *chunk);

  // Add a new socket to the list of sockets that we know about.
  // Returns the address of that SprinklerSocket object so that upper
  // layers could reference.
  SprinklerSocket *add_socket(int skt,
      std::function<int(SprinklerSocket *)> input,
      std::function<int(SprinklerSocket *)> output,
      std::function<void(SprinklerSocket *,
        const char *, int, std::function<void(void *)>, void *)> deliver,
      const std::string &descr);

  // Send the given chunk of data to the given socket.  It is invoked from
  // TransportLayer::wait(), like all other upcalls.  Currently we simply
  // buffer the message, and send it when TransportLayer::wait() is invoked.
  // Data buffer is freed by calling the cleanup handler after the data is
  // sent and the chunk's deconstructor is called.
  void async_socket_send(SprinklerSocket *ss,
      const char *data, int size, bool is_ctrl,
      void (*cleanup)(void *env), void *env);

  // Copy what is queued into an iovec, skipping over what has already
  // been sent.
  //
  // TODO.  Could take into account the size of the socket send buffer.
  int prepare_iovec(const std::list<Chunk> &cqueue, int offset,
      struct iovec *iov, int start);

  // The socket is ready for writing.  Try to send everything that is queued.
  int send_ready(SprinklerSocket *ss);

  // Input is available on some socket.  Peers send packets that are
  // up to size MAX_CHUNK_SIZE and start with a 4 byte header, the first
  // three of which contain the actual packet size (including the header
  // itself).
  int recv_ready(SprinklerSocket *ss);

  // Invoked when there is a client waiting on the server socket.
  int got_client(SprinklerSocket *ls);

  // Get inet address from (host, port).
  bool get_inet_address(struct sockaddr_in *sin, const char *addr, int port);

  // Connect to a remote Sprinkler node.  The node will identify itself so no
  // need to specify which node it is.
  void try_connect(SocketAddr *socket_addr);

  // Try to make connection to all available peers.
  void try_connect_all();

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

  // Type of socket connections.
  const std::string kSocIn = "incoming connection";
  const std::string kSocOut = "outgoing connection";
  const std::string kSocListen = "listening socket";

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
  std::function<void(SprinklerSocket *)> outgoing_;
  std::function<void(SprinklerSocket *,
      const char *, int, std::function<void(void *)>, void *)> deliver_;
};

// Data chunk to be sent out from a socket.
struct Chunk {
  const char *data;
  int size;
  std::function<void(void *)> cleanup;
  void *env;

  Chunk(const char *data, int size,
      std::function<void(void *)> cleanup, void *env)
    : data(data), size(size), cleanup(cleanup), env(env) {}

  ~Chunk() {}
};

// One of these is allocated for each socket.
struct SprinklerSocket {
  int skt;
  std::function<int(SprinklerSocket *)> input;
  std::function<int(SprinklerSocket *)> output;
  bool first;      // controls call to l1->outgoing
  std::string descr;    // for debugging
  int sndbuf_size, rcvbuf_size;   // socket send and receive buffer sizes

  // Queue of chunks of data to send.
  std::list<Chunk> ctrl_cqueue;    // outgoing chunk queue
  int ctrl_offset;                 // #bytes that are sent already
  int ctrl_remainder;              // #bytes waiting to be sent

  std::list<Chunk> data_cqueue;    // outgoing chunk queue
  int data_offset;                 // #bytes that are sent already
  int data_remainder;              // #bytes waiting to be sent

  // Data received from network to be delivered.
  char *recv_buffer;
  int received;    // #bytes currently in the chunk

  std::function<void(SprinklerSocket *,
      const char *, int, std::function<void(void *)>, void *)> deliver;

  // Constructor.
  SprinklerSocket(int skt,
      std::function<int(SprinklerSocket *)> input,
      std::function<int(SprinklerSocket *)> output,
      std::function<void(SprinklerSocket *,
        const char *, int, std::function<void(void *)>, void *)> deliver,
      const std::string &descr)
    : skt(skt),
      input(input), output(output), deliver(deliver),
      descr(descr) {
    first = true;
    sndbuf_size = rcvbuf_size = 0;
    ctrl_offset = ctrl_remainder = 0;
    data_offset = data_remainder = 0;
    recv_buffer = NULL;
    received = 0;
  }

  // Initialize socket buffer sizes.
  void init();
};

// One of these is allocated for each remote proxy interface.
struct SocketAddr {
  sockaddr_in sin;
  SprinklerSocket *out;    // outgoing connection
  SprinklerSocket *in;     // incoming connection

  SocketAddr() : out(NULL), in(NULL) {
    memset(&sin, 0, sizeof(sin));
  }
};

#endif  // TRANSPORT_LAYER_H_
