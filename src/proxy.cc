#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "sprinkler_node.h"

// Required parameters.
DEFINE_int32(id, -1, "Unique ID to identify a Sprinkler proxy.");

// Optional parameters.
DEFINE_bool(on_disk, true, "Is on-disk storage enabled?");
DEFINE_int64(mem_cap, 1 << 31, "Total size of in-memory buffer, 2 GB default.");
DEFINE_int64(duration, 0,
    "Lifetime of the proxy in seconds, 0 means infinite.");

// Prefix of proxy configuration file.  To get the full name, append the proxy
// id to the end of this prefix.
const std::string kPxCfgPrefix = "proxy_config_";

// Format of a proxy configuration file:
//   <role>
//   <succ-host> <succ-port>    (Optional, non-tail node only.)
//   <nproxies>  (Optional, tail node only.)
//   <host-1> <port-1> <proxy-id-1>  (Optional, tail node only.)
//   ...
//   <host-n> <port-n> <proxy-id-n>  (Optional, tail node only.)
//   <nstreams>
//   <n-local-streams> <sid-1> ... <sid-n>

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // Must provide a non-negative proxy id.
  CHECK_GT(FLAGS_id, -1);

  std::string cfg_filename = kPxCfgPrefix + std::to_string(FLAGS_id);
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
  }

  // Configuration of streams.
  cfg_file >> nstreams >> nstreams_local;
  for (int i = 0; i < nstreams_local; ++i) {
    int sid;
    cfg_file >> sid;
    local_streams.insert(sid);
  }

  cfg_file.close();

  SprinklerNode node(FLAGS_id, role,
      nproxies, proxies, nstreams, local_streams);
  node.run(FLAGS_duration);

  return 0;
}
