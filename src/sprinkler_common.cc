#include "sprinkler_common.h"

#include <stdint.h>


void itos(uint8_t *dst, uint64_t val, int len) {
  for (int i = 0; i < 8; ++i) {
    *dst++ = static_cast<uint8_t>(val & 0xff);
    val >>= 8;
  }
}

uint64_t stoi(const uint8_t *src, int len) {
  uint64_t result = 0;
  for (int i = 0; i < len; ++i) {
    result |= (static_cast<uint64_t>(*src)) << (i * 8);
  }
  return result;
}

int64_t get_begin_seq(const uint8_t *event) {
  // No matter what kind of event, this is always the right place.
  return static_cast<int64_t>(stoi(event + 1, 8));
}

int64_t get_end_seq(const uint8_t *event) {
  if (*event == 0) {
    return static_cast<int64_t>(stoi(event + 1, 8)) + 1;
  } else {
    return static_cast<int64_t>(stoi(event + 9, 8));
  }
}

bool in_range(const uint8_t *event, int64_t seq) {
  if (*event == 0) {
    return seq == get_begin_seq(event);
  } else {
    return seq >= get_begin_seq(event) && seq < get_end_seq(event);
  }
}
