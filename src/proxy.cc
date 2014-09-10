#include <gflags/gflags.h>
#include <glog/logging.h>

#include "sprinkler_node.h"

// Required parameters.
DEFINE_int32(id, -1, "Unique ID to identify a Sprinkler proxy.");

// Optional parameters.
// Replication options:
//  global -- Store each stream on disk at one local node and one remote node;
//            must have more than one datacenters;
//  local  -- Store each stream on disk at all local nodes, but not in other
//            datacenters;
//  none   -- Only store data in memory.
DEFINE_string(replication, "global", "Replication options.");

// Format of a proxy configuration file:
//   <role>
//   <succ-host> <succ-port>    (Optional, only present for non-tail node.)
//   <nstreams>
//   <n-local-streams> <sid-1> ... <sid-n>
//   <n-nonlocal-sids> <sid-1> ... <sid-n>

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // Must provide a non-negative proxy id.
  CHECK_GT(FLAGS_id, -1);

  CHECK(FLAGS_replication == "global" || FLAGS_replication == "local" ||
      FLAGS_replication == "none");

  SprinklerNode proxy(FLAGS_id, 0, 0, 0); 

  return 0;
}
