#include "sprinkler_common.h"

#include <stdint.h>
#include <stdlib.h>

#include <glog/logging.h>

void itos(uint8_t *dst, uint64_t val, int len) {
  for (int i = 0; i < 8; ++i) {
    *dst++ = static_cast<uint8_t>(val & 0xff);
    val >>= 8;
  }
}

uint64_t stoi(const uint8_t *src, int len) {
  uint64_t result = 0;
  for (int i = 0; i < len; ++i) {
    result |= static_cast<uint64_t>(*(src + i)) << (i * 8);
  }
  return result;
}

bool is_data_event(const uint8_t *event) {
  return *event == 0;
}

bool is_tombstone(const uint8_t *event) {
  return *event == 1;
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

int64_t get_object_id(const uint8_t *event) {
  CHECK_EQ(*event, 0);
  return static_cast<int64_t>(stoi(event + 9, 8));
}

int64_t get_timestamp(const uint8_t *event) {
  return static_cast<int64_t>(stoi(event + kEventLen - 8, 8));
}

void to_tombstone(uint8_t *event) {
  CHECK_EQ(*event, 0);
  int64_t end_seq = get_end_seq(event);

  *event = 1;
  itos(event + 9, end_seq, 8);
  memset(event + 17, 0, kEventLen - 17);
}

void debug_show_memory(const uint8_t *ptr, int size, int log_level) {
  std::string logmsg = "";
  for (int i = 0; i < size; ++i) {
    if (i) {
      logmsg += " ";
    }
    logmsg += std::to_string(*(ptr + i));
  }
  VLOG(log_level) << logmsg;
}
