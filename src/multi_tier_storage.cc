#include "multi_layer_storage.h"

void MultiTierStorage::set_stream_index(const std::vector &sids) {
  streams_index_.clear();

  for (int i = 0; i < sids.size(); ++i) {
    streams_index_[sids[i]] = i;
  }
}

int64_t MultiTierStorage::put_raw_events(
    int sid, int64_t nevents, uint8_t *data) {
  // TODO(haoyan).
}

int64_t MultiTierStorage::put_events(int sid, int64_t nevents, uint8_t *data) {
  // TODO(haoyan).
}
