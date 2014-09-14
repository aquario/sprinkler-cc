#include <gflags/gflags.h>
#include <glog/logging.h>

#include "sprinkler_node.h"

// Required parameters.
DEFINE_int32(id, -1,
    "Unique ID to identify a Sprinkler client, must be >= 10000.");
DEFINE_int32(stream_id, -1, "The stream this client publishes to.");
DEFINE_int32(proxy_id, -1, "ID of the local proxy.");
DEFINE_string(proxy_host, "", "Hostname of the local proxy.");
DEFINE_int32(proxy_port, 0, "Hostname of the local proxy.");
DEFINE_int32(listen_port, 0, "Port to listen to incoming connections.");

// Optional parameters.
DEFINE_int64(duration, 0,
    "Lifetime of the client in seconds, 0 means infinite.");
DEFINE_int32(interval, 1000000,
    "Interval between publishing two batches of events, default 1s.");
DEFINE_int32(batch_size, 1, "Number of events per batch, default 1.");

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

//  FLAGS_log_dir = "./logc";
//  google::InitGoogleLogging(argv[0]);

  // Must provide a valid client id.
  CHECK_GE(FLAGS_id, 10000);
  // Must specify a stream to publish.
  CHECK_GE(FLAGS_stream_id, 0);
  // Must provide a proxy id.
  CHECK_GE(FLAGS_proxy_id, 0);
  // Must provide a proxy hostname.
  CHECK_NE(FLAGS_proxy_host, "");
  // Must provide a proxy port.
  CHECK_GE(FLAGS_proxy_port, 10000);
  // Must provide a listening port.
  CHECK_GE(FLAGS_listen_port, 10000);

  Proxy proxy;
  proxy.id = FLAGS_proxy_id;
  proxy.host = FLAGS_proxy_host;
  proxy.port = FLAGS_proxy_port;

  std::vector<Proxy> proxies;
  proxies.push_back(proxy);
  
  SprinklerNode node(FLAGS_id, FLAGS_listen_port, SprinklerNode::kClient,
      FLAGS_stream_id, proxies);
  node.start_client(FLAGS_duration, FLAGS_interval, FLAGS_batch_size);

  return 0;
}
