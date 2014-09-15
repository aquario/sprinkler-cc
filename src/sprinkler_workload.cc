#include "sprinkler_workload.h"

#include <limits.h>
#include <stdint.h>
#include <stdlib.h>

int64_t SprinklerWorkload::get_next_key() {
  int64_t result;
  if (RAND_MAX == 32767) {
    result = (rand() << 16) + rand();
  }
  result = rand();
  return result % 128;
}
