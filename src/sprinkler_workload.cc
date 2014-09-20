#include "sprinkler_workload.h"

#include <limits.h>
#include <stdint.h>
#include <stdlib.h>

#include <string>

#include "sprinkler_common.h"

FILE *SprinklerWorkload::fin;
uint8_t SprinklerWorkload::buffer[8];

int64_t SprinklerWorkload::get_next_key() {
  int64_t result;
  if (RAND_MAX == 32767) {
    result = (rand() << 16) + rand();
  }
  result = rand();
  return result;
}

int64_t SprinklerWorkload::get_next_key(int64_t limit) {
  return get_next_key() % limit;
}

void SprinklerWorkload::init_workload(int chunk_id) {
  std::string filename = "workload-" + std::to_string(chunk_id);
  fin = fopen(filename.c_str(), "rb");
}

void SprinklerWorkload::close_workload() {
  fclose(fin);
}

int64_t SprinklerWorkload::get_next_wl_key() {
  fread(buffer, 8, 1, fin);
  return stoi(buffer, 8);
}
