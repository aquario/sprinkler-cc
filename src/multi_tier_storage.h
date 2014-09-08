#ifndef MULTI_TIER_STORAGE_H_
#define MULTI_TIER_STORAGE_H_

class MultiTierStorage {
 public:
  MultiTierStorage(int nstreams) : nstreams_(nstreams) {
    mem_store_ = static_cast<uint8_t **>(dcalloc(kMemBufSize, nstreams));
  }

  ~MultiTierStorage() {
    free(mem_store_);
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


 private:
  // Size of in-memory buffer for each stream in bytes.
  static const int64_t kMemBufSize = 128 * (1 << 20);   // 128 MB.
  // #streams.
  int nstreams_;
  // Mapping from stream id to array index for permanent storage.
  std::unordered_map<int, int> stream_index_;

  // In-memory buffer for all streams.  Use C-style array for efficiency.
  uint8_t **mem_store_;
  // Filenames for streams stored on this node.
  std::vector< std::deque<std::string> > filenames_;
  // TODO(haoyan): enable an additional layer of permanent storage.
};

#endif  // MULTI_TIER_STORAGE_H_
