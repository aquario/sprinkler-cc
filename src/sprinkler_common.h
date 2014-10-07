#ifndef SPRINKLER_COMMON_H_
#define SPRINKLER_COMMON_H_

#include <stdint.h>

// Length of a formatted Sprinkler event in bytes.
// Format of a data event:
//   |0(1)|seq#(8)|obj-id(8)|other-payload(15)|
// Format of a GC-by-key event: same as above.
// Format of a Tombstone event:
//   |1(1)|start-seq#(8)|end-seq#(8)|empty(15)|
// Format of an unformatted event from clients:
//   |0(9)|obj-id(8)|other-payload(15)|
const int64_t kEventLen = 32;

// Conversion between uint64_t and byte array.
void itos(uint8_t *dst, uint64_t val, int len);
uint64_t stoi(const uint8_t *src, int len);

// Check for event types.
bool is_data_event(const uint8_t *event);
bool is_tombstone(const uint8_t *event);

// Return the seq# of a data event, or the left boundary of a tombstone event.
int64_t get_begin_seq(const uint8_t *event);
// Return the seq# + 1 of a data event, or the right boundary of a
// tombstone event.
int64_t get_end_seq(const uint8_t *event);
// Check if a seq# fits in the range of an event.
// For data event, return true if the seq# equals its own; for tombstone,
// return true if begin <= seq# < end holds.
bool in_range(const uint8_t *event, int64_t seq);

// Returns the object id in a data event.
int64_t get_object_id(const uint8_t *event);

// Get the timestamp when this events is first stored at the local proxy.
int64_t get_timestamp(const uint8_t *event);

// Convert a data event into a singleton tombstone.
void to_tombstone(uint8_t *event);

// For debugging: print a chunk of memory in integers.
void debug_show_memory(const uint8_t *ptr, int size, int log_level);

#endif  // SPRINKLER_COMMON_H_
