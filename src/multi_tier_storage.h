#ifndef MULTI_TIER_STORAGE_H_
#define MULTI_TIER_STORAGE_H_

#include <stdint.h>

#include <deque>
#include <string>
#include <unordered_map>
#include <vector>

#include "dmalloc.h"

// Stores events received by a Sprinkler node.  An in-memory buffer stores most
// recent events from every stream registered with the system, and an on-disk
// permanent storage unit keeps all the history for streams that should be kept
// at this node.
class MultiTierStorage {
 public:
  MultiTierStorage(int nstreams, int64_t mem_buf_size, int64_t disk_chunk_size)
    : nstreams_(nstreams), mem_store_(nstreams), next_chunk_no_(nstreams, 0) {
    mem_buf_size_ = mem_buf_size;
    disk_chunk_size_ = disk_chunk_size;
  }

  // Add a block of raw events from a client.
  // "data" here contains only messages, no header is included.
  void put_raw_events(int sid, int64_t nevents, const uint8_t *data);

  // Add a block of formatted events from a peer proxy.
  // "data" here contains only messages, no header is included.
  void put_events(int sid, int64_t nevents, const uint8_t *data);

  // Retrieve events from a stream.
  // Return #events fetched into buffer; or a negative value indicating a type
  // of error.  See error code constants for error types.
  int64_t get_events(int sid, int64_t first_seq, int64_t max_events,
      uint8_t *buffer);

  // Error codes.
  static const int64_t kErrFuture = -1;  // Asked for a future seq#.
  static const int64_t kErrPast = -2;  // Asked for a seq# that is discarded.

 private:
  // In-memory buffer for a stream.
  struct MemBuffer {
    int64_t begin_seq, end_seq;
    int64_t begin_offset, end_offset;
    bool is_empty;
    uint8_t *chunk;

    MemBuffer() {
      begin_seq = end_seq = 1;
      begin_offset = end_offset = 0;
      is_empty = true;
      chunk = static_cast<uint8_t *>(dcalloc(mem_buf_size_, 1));
    }

    ~MemBuffer() {
      dfree(chunk);
    }
  };

  // Returns amount of free space available in a MemBuffer, in bytes.
  int64_t get_free_space(const MemBuffer &membuf);

  // Returns the offset in an array of events such that seq fits into the
  // event at that offset, or -1 in case no event fits.
  int64_t adjust_offset_linear(int64_t seq, int64_t nevents,
      const uint8_t *chunk);

  // Returns the offset in a circular array (data chunk in a MemBuffer struct)
  // such that seq fits into the event at that offset, or -1 in case no event
  // fits.
  int64_t adjust_offset_circular(int64_t seq,
      int64_t begin, int64_t end, const uint8_t *chunk);

  // Flush a chunk of in-memory buffer to disk.
  void flush_to_disk(int sid);

  // Generate the name of next data chunk to be stored on disk.
  std::string get_chunk_name(int sid, chunk_id);

  // #streams.
  int nstreams_;
  // Mapping from stream id to array index for permanent storage.
  std::unordered_map<int, int> stream_index_;

  // Size of in-memory buffer for each stream in bytes.
  static int64_t mem_buf_size_;
  // In-memory buffer for all streams.
  std::vector<MemBuffer> mem_store_;

  // Size of an on-disk data chunk in bytes.
  static int64_t disk_chunk_size_;
  // Next chunk# to assign for each stream stored on disk.
  std::vector<int64_t> next_chunk_no_;
  // Filenames for streams stored on this node.
  std::vector< std::deque<std::string> > chunk_no_;
};

#endif  // MULTI_TIER_STORAGE_H_
