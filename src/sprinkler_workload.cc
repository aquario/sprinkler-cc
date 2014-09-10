#include "sprinkler_workload.h"

#include <stdlib.h>

int64_t SprinklerWorkload::get_next_key() {
  if (RAND_MAX == INT16_MAX) {
    return (rand() << 16) + rand();
  }
  return rand();
}
