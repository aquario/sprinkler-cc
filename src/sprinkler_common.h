#ifndef SPRINKLER_COMMON_H_
#define SPRINKLER_COMMON_H_

// Length of a formatted Sprinkler event in bytes.
// Format of a data event:
//   |0(1)|seq#(8)|obj-id(8)|other-payload(15)|
// Format of a GC-by-key event: same as above.
// Format of a Tombstone event:
//   |1(1)|start-seq#(8)|end-seq#(8)|empty(15)|
const int64_t kEventLen = 32;
// Length of an unformatted event sent by Sprinkler clients.
// Format of a raw event:
//   |obj-id(8)|other-payload(8)|
const int64_t kRawEventLen = 16;

// Conversion between uint64_t and byte array.
void itos(uint8_t *dst, uint64_t val, int len);
uint64_t stoi(const uint8_t *src, int len);

// Return the seq# of a data event, or the left boundary of a tombstone event.
int64_t get_begin_seq(const uint8_t *event);
// Return the seq# + 1 of a data event, or the right boundary of a
// tombstone event.
int64_t get_end_seq(const uint8_t *event);
// Check if a seq# fits in the range of an event.
// For data event, return true if the seq# equals its own; for tombstone,
// return true if begin <= seq# < end holds.
bool in_range(const uint8_t *event, int64_t seq);

#endif  // SPRINKLER_COMMON_H_
