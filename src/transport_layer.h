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
#include <unordered_map>

#include "dmalloc.h"

// TODO(haoyan): add EPOLL support if necessary.

struct SprinklerSocket;
typedef std::list<SprinklerSocket>::iterator SocketIter;

// Data chunk to be sent out from a socket.
struct Chunk {
  uint8_t *hdr;
  const uint8_t *data;
  int size;
  std::function<void(void *)> cleanup;
  void *env;

  Chunk(uint8_t *hdr, const uint8_t *data, int size,
      std::function<void(void *)> cleanup, void *env)
    : hdr(hdr), data(data), size(size), cleanup(cleanup), env(env) {}

  ~Chunk() {}
};

// One of these is allocated for each socket.
struct SprinklerSocket {
  int skt;
  std::function<int(SocketIter)> input;
  std::function<int(SocketIter)> output;
  bool first;      // controls call to l1->outgoing
  std::string descr;    // for debugging
  int sndbuf_size, rcvbuf_size;   // socket send and receive buffer sizes

  // Endpoint the socket connects, used for cleanup.
  std::string host;
  int port;

  // Queue of chunks of data to send.
  std::list<Chunk> ctrl_cqueue;    // outgoing chunk queue
  int ctrl_offset;                 // #bytes that are sent already
  int ctrl_remainder;              // #bytes waiting to be sent

  std::list<Chunk> data_cqueue;    // outgoing chunk queue
  int data_offset;                 // #bytes that are sent already
  int data_remainder;              // #bytes waiting to be sent

  // Data received from network to be delivered.
  uint8_t *recv_buffer;
  int received;    // #bytes currently in the chunk

  std::function<void(const uint8_t *, int,
      std::function<void(void *)>, void *)> deliver;

  // Constructor.
  SprinklerSocket(int skt,
      std::function<int(SocketIter)> input,
      std::function<int(SocketIter)> output,
      std::function<void(const uint8_t *, int,
          std::function<void(void *)>, void *)> deliver,
      const std::string &descr,
      std::string host, int port)
    : skt(skt),
      input(input), output(output), deliver(deliver),
      descr(descr), host(host), port(port) {
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
  std::string host;
  int port;
  SocketIter out;    // outgoing connection
  SocketIter in;     // incoming connection
};

// Connection manager that manages all the socket connections.
// One per Sprinkler node.
class TransportLayer {
 public:
  // Constructor that specifies the id of this Sprinkler node, and upcalls
  // to the protocol layer.
  TransportLayer(int id, int port,
      std::function<void(const std::string &, int)> outgoing,
      std::function<void(const uint8_t *, int,
          std::function<void(void *)>, void *)> deliver);

  ~TransportLayer();

  // Return the number of microseconds since we started.
  int64_t uptime();

  // Listen on a TCP port to wait for connections.
  void tl_listen();

  // Register a peer Sprinkler node available to connect.
  void register_peer(const std::string &host, int port);

  // Returns true if there is a socket connection to the given endpoint and
  // the sending buffer is not backlogged too much.
  bool available_for_send(const std::string &host, int port);

  // Send data to the given socket endpoint identified by (host, port).
  // Returns:
  //  0 - if the send is successful;
  //  -1 - if connection to the remote host cannot be established;
  //  -2 - if the outgoing buffer is too congested to take new data.
  int async_send_message(const std::string &host, int port,
      const uint8_t *bytes, int len, bool is_ctrl,
      std::function<void(void *)> cleanup, void *env);

  // Wait for things to be ready.  Timeout is in milliseconds.  If negative,
  // l1_wait never returns.
  int wait(int timeout);

  // Maximum chunk size for a single message
  static const int kMaxChunkSize = 32768;

  // Maximum data backlog size allowed on any outgoing socket buffer, in bytes.
  // Control messages are exempted.
  static const int kMaxDataBacklog = 10485760;  // 10 MB.

  // Log level for this class.
  static const int kLogLevel = 10;

 private:
  // Invoke send/recv/poll syscalls.
  int do_sendmsg(int skt, struct msghdr *mh);
  int do_recv(int skt, uint8_t *data, int size);
  int do_poll(struct pollfd fds[], nfds_t nfds, int timeout);

  // Free chunks sent to upper layer.
  static void release_chunk(void *chunk);

  // Construct a string representation of an endpoint.
  static std::string get_endpoint(std::string host, int port);

  // Add a new socket to the list of sockets that we know about.
  // Returns the address of that SprinklerSocket object so that upper
  // layers could reference.
  SocketIter add_socket(int skt,
      std::function<int(SocketIter)> input,
      std::function<int(SocketIter)> output,
      std::function<void(const uint8_t *, int,
          std::function<void(void *)>, void *)> deliver,
      const std::string &descr, std::string host, int port);

  // Send the given chunk of data to the given socket.  It is invoked from
  // TransportLayer::wait(), like all other upcalls.  Currently we simply
  // buffer the message, and send it when TransportLayer::wait() is invoked.
  // Data buffer is freed by calling the cleanup handler after the data is
  // sent and the chunk's deconstructor is called.
  void async_socket_send(SocketIter ss,
      uint8_t *hdr, const uint8_t *data, int size, bool is_ctrl,
      std::function<void(void *)> cleanup, void *env);

  // Copy what is queued into an iovec, skipping over what has already
  // been sent.
  int prepare_iovec(const std::list<Chunk> &cqueue, int offset,
      struct iovec *iov, int start);

  // The socket is ready for writing.  Try to send everything that is queued.
  int send_ready(SocketIter ss);

  // Input is available on some socket.  Peers send packets that are
  // up to size kMaxChunkSize and start with a 4 byte header, the first
  // three of which contain the actual packet size (including the header
  // itself).
  int recv_ready(SocketIter ss);

  // Invoked when there is a client waiting on the server socket.
  int got_client(SocketIter ss);

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
  // Listening port.
  int port_;
  // Linked list of sockets.
  std::list<SprinklerSocket> sockets_;
  // #sockets in the list.
  int nsockets_;
  // Addresses of peers.
  std::unordered_map<std::string, SocketAddr> addr_list_;
  // Time at which the node starts, i.e. l1_init is invoked.
  timeval starttime_;
  // Interval between connection attempts.
  int64_t time_to_attempt_connect_;

  // Upcalls.
  std::function<void(const std::string &, int)> outgoing_;
  std::function<void(const uint8_t *, int,
      std::function<void(void *)>, void *)> deliver_;
};

#endif  // TRANSPORT_LAYER_H_
