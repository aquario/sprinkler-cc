#include <gflags/gflags.h>
#include <glog/logging.h>

#include "sprinkler_node.h"
#include "sprinkler_workload.h"

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
DEFINE_int32(workload_cid, -1,
    "Chunk# of pre-generated workload file. -1 means not used.");

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

//  FLAGS_log_dir = "./logc";
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

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

  LOG(INFO) << "Starting client " << FLAGS_id << " ...\n"
      << "Duration: " << FLAGS_duration << " seconds.\n"
      << "Publish to straem " << FLAGS_stream_id << ".\n"
      << "Proxy: (" << FLAGS_proxy_id << ", "
      << FLAGS_proxy_host << ", " << FLAGS_proxy_port << ")\n"
      << "Publish interval: " << FLAGS_interval << " microseconds.\n"
      << "Batch size: " << FLAGS_batch_size << ".\n";

  if (FLAGS_workload_cid > -1) {
    SprinklerWorkload::init_workload(FLAGS_workload_cid);
  }
  
  SprinklerNode node(FLAGS_id, FLAGS_listen_port, SprinklerNode::kClient,
      FLAGS_stream_id, proxies, (FLAGS_workload_cid > -1));
  node.start_client(FLAGS_duration, FLAGS_interval, FLAGS_batch_size);

  if (FLAGS_workload_cid > -1) {
    SprinklerWorkload::close_workload();
  }
  return 0;
}
