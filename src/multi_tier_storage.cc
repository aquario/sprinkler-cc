#include "multi_layer_storage.h"

#include <string.h>

#include "sprinkler_common.h"

struct MemBuffer {
  int64_t begin_seq, end_seq;
  int64_t begin_offset, end_offset;
  bool is_empty;
  uint8_t *chunk;

  MemBuffer() {
    begin_seq = end_seq = 1;
    begin_offset = end_offset = 0;
    is_empty = true;
    chunk = static_cast<uint8_t *>(dcalloc(kMemBufSize, 1));
  }

  ~MemBuffer() {
    free(chunk);
  }
};

void MultiTierStorage::set_stream_index(const std::vector &sids) {
  streams_index_.clear();

  for (int i = 0; i < sids.size(); ++i) {
    streams_index_[sids[i]] = i;
  }
}

int64_t MultiTierStorage::put_raw_events(
    int sid, int64_t nevents, uint8_t *data) {
  if (nevents == 0) {
    LOG(WARNING) << "put_raw_events invoked with 0 events";
    return;
  }
  CHECK_NOTNULL(data);

  if (stream_index_.count(sid) == 1 &&
      get_free_space(mem_store_[sid]) < nevents * kEventLen) {
    // TODO(haoyan): Not enough space and we want to keep all events, so flush
    // something to disk.
  }

  uint8_t *ptr = mem_store_[sid].chunk;

  // First, reset the memory region.
  int64_t end_offset = mem_store_[sid].end_offset;
  if (end_offset + nevents * kEventLen <= kMemBufSize) {
    memset(ptr + end_offset, 0, nevents * kEventLen);
  } else {
    memset(ptr + end_offset, 0, kMemBufSize - end_offset);
    memset(ptr, 0, nevents * kEventLen - (kMemBufSize - end_offset));
  }

  // Next, format events with seq#'s.
  for (int i = 0; i < nevents; ++i) {
    itos(ptr + end_offset + 1, mem_store_[sid].end_seq++, 8);
    memmove(ptr + end_offset + 9, data + i * kEventLen, kRawEventLen);
    end_offset += kEventLen;
    if (end_offset == kMemBufSize) {
      end_offset = 0;
    }
  }

  // Finally, set the new end_offset and empty flag.
  mem_store_[sid].end_offset = end_offset;
  mem_store_[sid].is_empty = false;
}

int64_t MultiTierStorage::put_events(int sid, int64_t nevents, uint8_t *data) {
  if (nevents == 0) {
    LOG(WARNING) << "put_events invoked with 0 events";
    return;
  }
  CHECK_NOTNULL(data);

  int64_t end_seq = mem_store_[sid].end_seq;
  int64_t end_offset = mem_store_[sid].end_offset;

  // Find offset to the first event that is needed.
  if (!in_range(data, end_seq)) {
    int64_t fit_offset = adjust_offset_linear(end_seq, nevents, data);
    if (fit_offset == -1) {
      LOG(WARNING) << "Got out of range events starting at "
          << get_begin_seq(data);
      return;
    }
    data += fit_offset;
    nevents -= fit_offset / kEventLen;
  }

  if (stream_index_.count(sid) == 1 &&
      get_free_space(mem_store_[sid]) < nevents * kEventLen) {
    // TODO(haoyan): Not enough space and we want to keep all events, so flush
    // something to disk.
  }

  // Copy the data over.
  uint8_t *ptr = mem_store_[sid].chunk;
  if (end_offset + nevents * kEventLen <= kMemBufSize) {
    memmove(ptr + end_offset, data, nevents * kEventLen);
  } else {
    memmove(ptr + end_offset, data, kMemBufSize - end_offset);
    memmove(ptr, data + (kMemBufSize - end_offset),
        nevents * kEventLen - (kMembufSize - end_offset));
  }

  // Set new offset, seq#, and empty flag.
  end_offset += nevents * kEventLen;
  mem_store_[sid].end_offset = (end_offset < kMemBufSize
      ? end_offset
      : end_offseta - kMemBufSize);
  mem_store_[sid].end_pos = get_end_seq(data + (nevents - 1) * kEventLen);
  mem_store_[sid].is_empty = false;
}

int64_t MultiTierStorage::get_events(
    int sid, int64_t first_seq, int64_t max_events, uint8_t *buffer) {
  // TODO(haoyan).
}

int64_t MultiTierStorage::get_free_space(const MemBuffer &membuf) {
  if (membuf.is_empty) {
    return kMemBufSize;
  }

  if (membuf.begin_offset < membuf.end_offset) {
    return kMemBufSize - (membuf.end_offset - membuf.begin_offset);
  } else {
    return membuf.begin_offset - membuf.end_offset;
  }
}

int64_t MultiTierStorage::adjust_offset_linear(
    int64_t seq, int64_t nevents, const uint8_t *chunk) {
  int64_t lo = 0;
  int64_t hi = (nevents - 1) * kEventLen;
  
  while (lo <= hi) {
    int64_t mid = (lo + hi) >> 1;   // div 2.
    if (in_range(chunk + mid, seq)) {
      return mid;
    }
    if (get_begin_seq(chunk + mid) > seq) {
      hi = mid - kEventLen;
    } else if (get_end_seq(chunk + mid) <= seq) {
      lo = mid + kEventLen;
    }
  }
  return -1;
}

int64_t MultiTierStorage::adjust_offset_circular(int64_t seq,
    int64_t begin, int64_t end, const uint8_t *chunk) {

  if (begin < end) {
    int64_t nevents = (end - begin) / kEventLen;
    int64_t result = adjust_offset_linear(seq, nevents, chunk + begin);
    if (result != -1) {
      result += begin;
    }
    return result;
  }

  int64_t lo = begin;
  int64_t hi = end + kMemBufSize;

  while (lo <= hi) {
    int64_t mid = (lo + hi) >> 1;   // div 2.
    int64_t idx = mid;
    if (idx >= kMemBufSize) {
      idx -= kMemBufSize;
    }

    if (in_range(chunk + idx, seq)) {
      return idx;
    }
    if (get_begin_seq(chunk + idx) > seq) {
      hi = mid - kEventLen;
    } else if (get_end_seq(chunk + idx) <= seq) {
      lo = mid + kEventLen;
    }
  }
  return -1;
}
