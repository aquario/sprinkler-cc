#ifndef MULTI_TIER_STORAGE_H_
#define MULTI_TIER_STORAGE_H_

class MultiTierStorage {
 public:
  MultiTierStorage() {}

 private:
  // #streams.
  int nstreams_;
  // Mapping from stream id to array index.
  std::unordered_map<int, int> stream_index_;

  // TODO(haoyan): in-memory buffer.
  // TODO(haoyan): permanent storage layer1.
  // TODO(haoyan): permanent storage layer2.
};

#endif  // MULTI_TIER_STORAGE_H_
