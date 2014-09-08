#ifndef MULTI_TIER_STORAGE_H_
#define MULTI_TIER_STORAGE_H_

#include <stdint.h>

#include <deque>
#include <unordered_map>
#include <vector>

// Stores events received by a Sprinkler node.  An in-memory buffer stores most
// recent events from every stream registered with the system, and an on-disk
// permanent storage unit keeps all the history for streams that should be kept
// at this node.
class MultiTierStorage {
 public:
  MultiTierStorage(int nstreams) : nstreams_(nstreams) {
    mem_store_ = std::vector<MemBuffer>(nstreams);
  }

  // Construct the mapping (stream_id -> array_index).
  // sids -- a list of streams that will be stored permanently on this node.
  void set_stream_index(const std::vector &sids);

  // Add a block of raw events from a client.
  // Returns the least unused seq# after the put.
  int64_t put_raw_events(int sid, int64_t nevents, uint8_t *data);

  // Add a block of formatted events from a peer proxy.
  // Returns the least unused seq# after the put.
  int64_t put_events(int sid, int64_t nevents, uint8_t *data);

  // Retrieve events from a stream.
  // Return #events fetched into buffer.
  int64_t get_events(int sid, int64_t max_events, uint8_t *buffer);

 private:
  // In-memory buffer for a stream.
  struct MemBuffer;

  // Size of in-memory buffer for each stream in bytes.
  static const int64_t kMemBufSize = 128 * (1 << 20);   // 128 MB.

  // Returns amount of free space available in a MemBuffer, in bytes.
  int64_t get_free_space(const MemBuffer &membuf);

  // Returns the offset in a sequence of events such that seq fits into the
  // event at that offset, or -1 in case no event fits.
  int64_t adjust_offset(int64_t seq, int64_t nevents, const uint8_t *chunk);

  // #streams.
  int nstreams_;
  // Mapping from stream id to array index for permanent storage.
  std::unordered_map<int, int> stream_index_;

  // In-memory buffer for all streams.  Use C-style array for efficiency.
  std::vector<MemBuffer> mem_store_;
  // Filenames for streams stored on this node.
  std::vector< std::deque<std::string> > filenames_;
  // TODO(haoyan): enable an additional layer of permanent storage.
};

#endif  // MULTI_TIER_STORAGE_H_
