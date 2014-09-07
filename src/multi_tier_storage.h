#ifndef MULTI_TIER_STORAGE_H_
#define MULTI_TIER_STORAGE_H_

class MultiTierStorage {
 public:
  MultiTierStorage();

 private:
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
