#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "sprinkler_node.h"

// Required parameters.
DEFINE_int32(id, -1, "Unique ID to identify a Sprinkler proxy.");
DEFINE_int32(port, 0, "Port that listens to incoming connections.");

// Optional parameters.
DEFINE_bool(on_disk, true, "Is on-disk storage enabled?");
DEFINE_int64(mem_cap, static_cast<int64_t>(1) << 31,
    "Total size of in-memory buffer, 2 GB default.");
DEFINE_int64(disk_chunk_size, 1 << 24,
    "Size of an on-disk data chunk, 16 MB default.");
DEFINE_int64(pub_delay, 0,
    "Minimum time between an event is published to the local proxy and " \
    "the earliest time that proxy will re-publish it, in milliseconds.");
DEFINE_int32(gc_thread_count, 0, "Number of garbage collection threads.");
DEFINE_int64(max_gc_table_size, 1 << 19,
    "Maximum number of events in the hash table used to GC previous events." \
    "524288 events by default.");
DEFINE_int64(max_gc_chunk_size, 1 << 20,
    "Max #bytes to be GCed before a pause in bytes, 1 MB by default.");
DEFINE_int64(min_gc_pass, 65536,
    "Min bytes in a stream to trigger GC, 64 KB by default.");
DEFINE_int64(max_gc_pass, static_cast<int64_t>(1) << 31,
    "Max bytes a GC pass will touch, 2 GB by default.");
DEFINE_int32(gc_disk_thread_count, 0,
    "Number of on-disk garbage collection threads.");
DEFINE_int64(duration, 0,
    "Lifetime of the proxy in seconds, 0 means infinite.");
DEFINE_bool(ack_enabled, false,
    "Whether ack messages are sent upon receiving events.");
DEFINE_string(client_host, "", "Host name of the client to send ACKs.");
DEFINE_int32(client_port, 0, "Port of the client to send ACKs.");
DEFINE_string(expr_name, "default", "Name of current experiment.");

// Format of a proxy configuration file:
//   <role>   (Bitwise OR of: 1 -- any node; 2 -- head; 4 -- tail.
//   <succ-host> <succ-port>    (Optional, non-tail node only.)
//   <nproxies>  (Optional, tail node only.)
//   <host-1> <port-1> <proxy-id-1>  (Optional, tail node only.)
//   ...
//   <host-n> <port-n> <proxy-id-n>  (Optional, tail node only.)
//   <nstreams>
//   <n-local-streams> <sid-1> ... <sid-n>

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

//  FLAGS_log_dir = "./logp";
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  // Must provide a non-negative proxy id.
  CHECK_GT(FLAGS_id, -1);
  // Must provide a valid listening port.
  CHECK_GE(FLAGS_port, 10000);

  std::string cfg_filename =
      FLAGS_expr_name + "-proxy-config-" + std::to_string(FLAGS_id);
  LOG(INFO) << "Reading config file " << cfg_filename;
  std::ifstream cfg_file(cfg_filename);

  int role = 0;
  int nproxies = 0;
  int nstreams = 0;
  int nstreams_local = 0; 
  std::unordered_set<int> local_streams;
  std::vector<Proxy> proxies;
  Proxy proxy;

  // Configuration of proxies.
  cfg_file >> role;
  CHECK(role);  // Must be non-zero, i.e., a proxy.
  if (role & SprinklerNode::kTail) {
    // If this is the tail, read a list of contact point for each proxy.
    cfg_file >> nproxies;
    for (int i = 0; i < nproxies; i++) {
      cfg_file >> proxy.host >> proxy.port >> proxy.id;
      proxies.push_back(proxy);
    }
  } else {
    nproxies = 1;
    proxy.id = FLAGS_id;
    cfg_file >> proxy.host >> proxy.port;
    proxies.push_back(proxy);
  }

  // Configuration of streams.
  cfg_file >> nstreams >> nstreams_local;
  for (int i = 0; i < nstreams_local; ++i) {
    int sid;
    cfg_file >> sid;
    local_streams.insert(sid);
  }

  cfg_file.close();

  // Determine the size of in-memory buffer per stream.
  // For convenience of alignment, it is set to be multiples of event length.
  int64_t mem_buf_size = (FLAGS_mem_cap / nstreams / kEventLen) * kEventLen;

  LOG(INFO) << "Starting proxy " << FLAGS_id << " ...\n"
      << "Experiment: " << FLAGS_expr_name << ".\n"
      << "Duration: " << FLAGS_duration << " seconds.\n"
      << "Role of this proxy: " << role << ".\n"
      << "Listening to port " << FLAGS_port << ".\n"
      << "Number of straems: " << nstreams << ".\n"
      << "Number of local streams: " << local_streams.size() << ".\n"
      << "In-memory buffer for each stream: " << mem_buf_size << " bytes.\n"
      << "Data chunks on disk: " << FLAGS_disk_chunk_size << " bytes.\n"
      << "Number of GC threads: " << FLAGS_gc_thread_count << ".\n"
      << "Min bytes in a stream to trigger GC: " << FLAGS_min_gc_pass << ".\n"
      << "Max bytes a GC pass will touch: " << FLAGS_max_gc_pass << ".\n"
      << "Max GC table size: " << FLAGS_max_gc_table_size << " events.\n"
      << "Max GC chunk size: " << FLAGS_max_gc_chunk_size << " bytes.\n"
      << "Number of on-disk GC threads: "
      << FLAGS_gc_disk_thread_count << ".\n"
      << "Ack enabled: " << FLAGS_ack_enabled << "\n"
      << "Client host: " << FLAGS_client_host << "\n"
      << "Client port: " << FLAGS_client_port << "\n";

  SprinklerNode node(FLAGS_id, FLAGS_port, role,
      FLAGS_ack_enabled, FLAGS_client_host, FLAGS_client_port,
      nproxies, proxies,
      nstreams, local_streams, mem_buf_size, FLAGS_disk_chunk_size,
      FLAGS_pub_delay,
      FLAGS_gc_thread_count, FLAGS_min_gc_pass, FLAGS_max_gc_pass,
      FLAGS_max_gc_table_size, FLAGS_max_gc_chunk_size,
      FLAGS_gc_disk_thread_count);
  node.start_proxy(FLAGS_duration);

  return 0;
}
