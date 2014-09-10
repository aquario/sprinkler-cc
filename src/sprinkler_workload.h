#ifndef SPRINKLER_WORKLOAD_H_
#define SPRINKLER_WORKLOAD_H_

#include <stdint.h>

class SprinklerWorkload {
 public:
  static int64_t get_next_key();
};

#endif  // SPRINKLER_WORKLOAD_H_
