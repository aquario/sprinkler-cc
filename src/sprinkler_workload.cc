#include "sprinkler_workload.h"

#include <limits.h>
#include <stdint.h>
#include <stdlib.h>

int64_t SprinklerWorkload::get_next_key() {
  if (RAND_MAX == 32767) {
    return (rand() << 16) + rand();
  }
  return rand();
}
