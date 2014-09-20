#ifndef SPRINKLER_WORKLOAD_H_
#define SPRINKLER_WORKLOAD_H_

#include <stdint.h>
#include <stdio.h>

class SprinklerWorkload {
 public:
  static int64_t get_next_key();
  static int64_t get_next_key(int64_t limit);

  static void init_workload(int cid);
  static void close_workload();
  static int64_t get_next_wl_key();

 private:
  static FILE *fin;
  static uint8_t buffer[8];
};

#endif  // SPRINKLER_WORKLOAD_H_
