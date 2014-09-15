#include "multi_tier_storage.h"

#include <pthread.h>
#include <string.h>

#include <algorithm>
#include <string>

#include <glog/logging.h>

#include "sprinkler_common.h"

int64_t MultiTierStorage::mem_buf_size_;
int64_t MultiTierStorage::disk_chunk_size_;

void MultiTierStorage::init_gc() {
  if (gc_thread_count_ > 0) {
    for (int i = 0; i < gc_thread_count_; ++i) {
      gc_hints_[i].ptr = this;
      gc_hints_[i].tid = i;
      int rc = pthread_create(&gc_threads_[i], NULL,
          &MultiTierStorage::start_gc, static_cast<void *>(&gc_hints_[i]));
      if (rc) {
        LOG(ERROR) << "pthread_create failed with rc " << rc << ".";
      }
    }
  }
}

void MultiTierStorage::put_raw_events(
    int sid, int64_t nevents, const uint8_t *data) {
  VLOG(kLogLevel) << "put_raw_events: stream " << sid << "; " << nevents
      << " events.";
  if (nevents == 0) {
    LOG(WARNING) << "put_raw_events invoked with 0 events";
    return;
  }
  CHECK_NOTNULL(data);

  // Not enough space, flush something to disk.
  if (get_free_space(mem_store_[sid]) < nevents * kEventLen) {
    flush_to_disk(sid);
  }

  uint8_t *ptr = mem_store_[sid].chunk;

  // First, reset the memory region.
  int64_t end_offset = mem_store_[sid].end_offset;
  if (end_offset + nevents * kEventLen <= mem_buf_size_) {
    memset(ptr + end_offset, 0, nevents * kEventLen);
  } else {
    memset(ptr + end_offset, 0, mem_buf_size_ - end_offset);
    memset(ptr, 0, nevents * kEventLen - (mem_buf_size_ - end_offset));
  }

  // Next, format events with seq#'s.
  for (int i = 0; i < nevents; ++i) {
    itos(ptr + end_offset + 1, mem_store_[sid].end_seq, 8);
    memmove(ptr + end_offset + 9, data + i * kEventLen, kRawEventLen);
    end_offset += kEventLen;
    if (end_offset == mem_buf_size_) {
      end_offset = 0;
    }
    // TODO(haoyan): Change this to LOG_EVERY_N when load gets heavier.
    LOG(INFO) << "PUT " << sid << " " << mem_store_[sid].end_seq;
    ++mem_store_[sid].end_seq;
  }

  // Finally, set the new end_offset and empty flag.
  mem_store_[sid].end_offset = end_offset;
  mem_store_[sid].is_empty = false;
}

void MultiTierStorage::put_events(
    int sid, int64_t nevents, const uint8_t *data) {
  VLOG(kLogLevel) << "put_events: stream " << sid << "; " << nevents
      << " events.";
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

  // Not enough space, flush something to disk.
  if (get_free_space(mem_store_[sid]) < nevents * kEventLen) {
    flush_to_disk(sid);
  }

  // Copy the data over.
  uint8_t *ptr = mem_store_[sid].chunk;
  if (end_offset + nevents * kEventLen <= mem_buf_size_) {
    memmove(ptr + end_offset, data, nevents * kEventLen);
  } else {
    memmove(ptr + end_offset, data, mem_buf_size_ - end_offset);
    memmove(ptr, data + (mem_buf_size_ - end_offset),
        nevents * kEventLen - (mem_buf_size_ - end_offset));
  }

  // Set new offset, seq#, and empty flag.
  end_offset += nevents * kEventLen;
  mem_store_[sid].end_offset = (end_offset < mem_buf_size_
      ? end_offset
      : end_offset - mem_buf_size_);
  mem_store_[sid].end_seq = get_end_seq(data + (nevents - 1) * kEventLen);
  mem_store_[sid].is_empty = false;

  // TODO(haoyan): Change this to LOG_EVERY_N when load gets heavier.
  LOG(INFO) << "PUT " << sid << " " << (mem_store_[sid].end_seq - 1);
}

int64_t MultiTierStorage::get_events(
    int sid, int64_t first_seq, int64_t max_events, uint8_t *buffer) {
  VLOG(kLogLevel) << "get_events: stream " << sid << "; staring at "
      << first_seq << "; at most " << nevents << " events are needed.";
  // first_seq is too large.
  if (first_seq >= mem_store_[sid].end_seq) {
    return kErrFuture;
  }

  // first_seq is not in memory.
  if (first_seq < mem_store_[sid].begin_seq) {
    // TODO(haoyan): Fetch events from disk.
    LOG(WARNING) << "Data not found in memory.";
    return kErrPast;
  } else {
    // Everything wanted is in-memory.
    // First, determine the memory range needs to copy.
    int64_t begin_offset = adjust_offset_circular(first_seq,
        mem_store_[sid].begin_offset, mem_store_[sid].end_offset,
        mem_store_[sid].chunk);
    int64_t end_offset = mem_store_[sid].end_offset;
    int64_t nevents = (end_offset > begin_offset
        ? (end_offset - begin_offset) / kEventLen
        : (mem_buf_size_ - (begin_offset - end_offset)) / kEventLen);
    if (nevents > max_events) {
      nevents = max_events;
      end_offset = begin_offset + nevents * kEventLen;
      if (end_offset >= mem_buf_size_) {
        end_offset -= mem_buf_size_;
      }
    }

    // Next, copy events into buffer.
    if (end_offset > begin_offset) {
      memmove(buffer, mem_store_[sid].chunk + begin_offset,
          end_offset - begin_offset);
    } else {
      memmove(buffer, mem_store_[sid].chunk + begin_offset,
          mem_buf_size_ - begin_offset);
      memmove(buffer + (mem_buf_size_ - begin_offset),
          mem_store_[sid].chunk, end_offset);
    }

    // nevents == 0 indicates an error behavior and should've been caught above.
    CHECK_NE(nevents, 0);
    return nevents;
  }
}

int64_t MultiTierStorage::distance(int64_t begin, int64_t end) {
  if (end > begin) {
    return end - begin;
  } else {
    return mem_buf_size_ - (begin - end);
  }
}

int64_t MultiTierStorage::get_used_space(const MemBuffer &membuf) {
  if (membuf.is_empty) {
    return 0;
  }
  return distance(membuf.begin_offset, membuf.end_offset);
}

int64_t MultiTierStorage::get_free_space(const MemBuffer &membuf) {
  return mem_buf_size_ - get_used_space(membuf);
}

int64_t MultiTierStorage::next_offset(int64_t offset) {
  offset += kEventLen;
  if (offset == mem_buf_size_) {
    offset = 0;
  }
  return offset;
}

int64_t MultiTierStorage::prev_offset(int64_t offset) {
  return offset == 0 ? mem_buf_size_ - kEventLen : offset - kEventLen;
}

bool MultiTierStorage::is_valid_offset(
    const MemBuffer &membuf, int64_t offset) {
  if (membuf.is_empty) {
    return false;
  }
  if (membuf.end_offset > membuf.begin_offset) {
    return offset >= membuf.begin_offset && offset < membuf.end_offset;
  } else {
    // Assumes that offset is always within [0, mem_buf_size_).
    return offset >= membuf.begin_offset || offset < membuf.end_offset;
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
  int64_t hi = end + mem_buf_size_;

  while (lo <= hi) {
    int64_t mid = (lo + hi) >> 1;   // div 2.
    int64_t idx = mid;
    if (idx >= mem_buf_size_) {
      idx -= mem_buf_size_;
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

void MultiTierStorage::flush_to_disk(int sid) {
  int64_t begin_offset = mem_store_[sid].begin_offset;
  uint8_t *ptr = mem_store_[sid].chunk;
  int unit_size = sizeof(uint8_t);

  std::string filename = get_chunk_name(sid, next_chunk_no_[sid]);
  FILE *fout = fopen(filename.c_str(), "wb");

  if (mem_store_[sid].begin_offset + disk_chunk_size_ <= mem_buf_size_) {
    // No carry-over, a single write is sufficient.
    fwrite(ptr + begin_offset, unit_size, disk_chunk_size_, fout);
  } else {
    // Two writes.
    fwrite(ptr + begin_offset, unit_size, mem_buf_size_ - begin_offset, fout);
    fwrite(ptr, unit_size,
        disk_chunk_size_ - (mem_buf_size_ - begin_offset), fout);
  }

  fclose(fout);
  used_chunk_no_[sid].push_back(next_chunk_no_[sid]);
  ++next_chunk_no_[sid];
}

std::string MultiTierStorage::get_chunk_name(int sid, int64_t chunk_id) {
  return "chunk-" + std::to_string(sid) + "-" + std::to_string(chunk_id);
}

void *MultiTierStorage::start_gc(void *arg) {
  GcHint *hint_t = static_cast<GcHint *>(arg);
  MultiTierStorage *storage_t = hint_t->ptr;
  int tid = hint_t->tid;

  storage_t->run_gc(tid);
}

void MultiTierStorage::run_gc(int thread_id) {
  VLOG(kLogLevel) << "Garbage collection thread #" << thread_id << " started.";

  std::vector<int> my_streams;
  std::unordered_map<int, GcInfo> metadata;

  // First, determine which streams am I responsible for.
  for (int i = thread_id; i < nstreams_; i += gc_thread_count_) {
    my_streams.push_back(i);
    metadata.insert(std::make_pair(i, GcInfo()));
  }

  // Round robin across these streams.
  int stream_idx = -1;
  while (true) {
    if (++stream_idx == my_streams.size()) {
      stream_idx = 0;
    }

    MemBuffer &membuf = mem_store_[my_streams[stream_idx]];
    uint8_t *ptr = membuf.chunk;
    GcInfo &gc_info = metadata[my_streams[stream_idx]];
    
    // Skip GC if there are too few events.
    if (distance(membuf.begin_offset, membuf.end_offset) / kEventLen
        < min_events_to_gc_) {
      continue;
    }

    // Construct GC table.
    // Here we assume that flush_to_disk will never interfere with GC table
    // region, therefore no locking is needed here.  We assume this because
    // flushing is only invoked when the buffer is almost full, at which point
    // the GC table region is far away enough from being flushed.
    int64_t gc_table_size =
        get_used_space(membuf) / kEventLen > max_gc_table_size_ * 2
        ? max_gc_table_size_
        : get_used_space(membuf) / kEventLen / 2;

    gc_info.table.clear();
    int64_t end_offset = membuf.end_offset;
    for (int i = 0; i < gc_table_size; ++i) {
      end_offset = prev_offset(end_offset);
      // This has to be a data event.
      CHECK_EQ(*(ptr + end_offset), 0);
      gc_info.table.insert(get_begin_seq(ptr + end_offset));
    }

    // Perform GC with mutex.
    pthread_mutex_lock(&mutex_[my_streams[stream_idx]]);

    int64_t begin_seq = membuf.begin_seq;
    int64_t begin_offset = membuf.begin_offset;
    while (begin_offset != end_offset) {
      // a) Determine where to take a breathe.
      int64_t pause_offset = end_offset;
      if (distance(begin_offset, end_offset) > max_gc_chunk_size_) {
        pause_offset = begin_offset + max_gc_chunk_size_;
        if (pause_offset >= mem_buf_size_) {
          pause_offset -= mem_buf_size_;
        }
      } else {
        pause_offset = end_offset;
      }

      // b) In buffer [begin_offset, pause_offset), scan for events should be
      // GCed, and turn them into (singleton) tombstones.
      int64_t cursor = begin_offset;
      while (cursor != pause_offset) {
        if (is_data_event(ptr + cursor) &&
            gc_info.table.count(get_begin_seq(ptr + cursor)) == 1) {
          to_tombstone(ptr + cursor);
        }

        cursor = next_offset(cursor);
      }

      // c) Merge tombstones.
      // Variables used in this phase:
      //   processed: all events AFTER this offset are done;
      //   cursor: offset of current event being examined;
      //   current_gc: are we merging GC events now?
      //   lo: if current_gc is true, the lower bound of current GC event.
      int64_t processed = pause_offset;
      cursor = pause_offset;
      bool current_gc = false;
      int64_t lo = 0;
      while (cursor != begin_offset) {
        cursor = prev_offset(processed);
        if (is_data_event(ptr + cursor)) {
          // For data events, move them rightwards if necessary.
          processed = prev_offset(processed);
          if (cursor != processed) {
            memmove(ptr + processed, ptr + cursor, kEventLen);
          }
          current_gc = false;
        } else {
          // For GC events, set a mark if this is the first one in a series,
          // otherwise, update the mark.
          if (!current_gc) {
            // Write the GC event if this is the first in a series.
            current_gc = true;
            processed = prev_offset(processed);
            if (cursor != processed) {
              memmove(ptr + processed, ptr + cursor, kEventLen);
            }
          } else {
            // Ranges of adjacent GC events should be continuous.
            CHECK_EQ(get_end_seq(ptr + cursor), lo);
            // Update the lower bound for current GC event.
            lo = get_begin_seq(ptr + cursor);
            memmove(ptr + processed + 1, ptr + cursor + 1, 8);
          }
        }
      }

      // d) Squeeze the buffer rightwards.
      // If there is valid buffer region before where we started, we need
      // to move that chunk of space rightwards.
      if (begin_offset != membuf.begin_offset) {
        // If the scan ended with a GC event, we should check if the event just
        // before begin_offset is also a GC event, and if so, merge them.
        cursor = prev_offset(begin_offset);
        if (is_tombstone(ptr + cursor)) {
          CHECK_EQ(get_end_seq(ptr + cursor), get_begin_seq(ptr + processed));
          memmove(ptr + processed + 1, ptr + cursor + 1, 8);
          // From this point on, begin_offset is used as the right boundary
          // of copying the previous chunk.
          begin_offset = cursor;
        }

        // TODO(haoyan): Move the data before begin_offset rightwards.
        // Calculate the destination offset to shift the buffer.
        int64_t dist = distance(begin_offset, processed);
        int64_t src_offset = membuf.begin_offset;
        int64_t dst_offset = membuf.begin_offset + dist;
        if (dst_offset >= mem_buf_size_) {
          dst_offset -= mem_buf_size_;
        }

        // Offsets at which the source or destination buffer breaks.
        std::vector<int> cutoffs;
        if (begin_offset < membuf.begin_offset) {
          cutoffs.push_back(mem_buf_size_ - membuf.begin_offset);
        }
        if (processed < dst_offset) {
          cutoffs.push_back(mem_buf_size_ - processed);
        }
        cutoffs.push_back(0);
        cutoffs.push_back(distance(membuf.begin_offset, begin_offset));

        std::sort(cutoffs.begin(), cutoffs.end());
        auto last = std::unique(cutoffs.begin(), cutoffs.end());
        cutoffs.erase(last, cutoffs.end());

        // Copy the memory chunks.  By using cutoffs, it is guaranteed that
        // in each copy, [offset, offset + len) is a valid region, i.e., do not
        // pass through offset mem_buf_size_.
        for (int i = cutoffs.size() - 2; i >= 0; --i) {
          int64_t this_src = src_offset + cutoffs[i];
          if (this_src >= mem_buf_size_) {
            this_src -= mem_buf_size_;
          }
          int64_t this_dst = dst_offset + cutoffs[i];
          if (this_dst >= mem_buf_size_) {
            this_dst -= mem_buf_size_;
          }
          int64_t this_len = cutoffs[i + 1] - cutoffs[i];
          memmove(ptr + this_dst, ptr + this_src, this_len);
        }

        // Update offsets.
        membuf.begin_offset = dst_offset;
        // Advance to the next area.
        begin_offset = pause_offset;
        begin_seq = get_begin_seq(ptr + pause_offset);

        // Release the lock so that other threads get a chance to enter.
        pthread_mutex_unlock(&mutex_[my_streams[stream_idx]]);
        int rc = pthread_yield();
        if (rc) {
          LOG(ERROR) << "pthread_yield() failed on thread " << thread_id
              << " with rc " << rc << ".";
        }

        // Re-aquire the lock before proceeding to another area.
        pthread_mutex_lock(&mutex_[my_streams[stream_idx]]);
        // Check if our new begin position is still valid.  If not, update it to
        // the new begin position.
        if (membuf.begin_seq > begin_seq) {
          begin_offset = membuf.begin_offset;
          begin_seq = membuf.begin_seq;
        }
      }
    }

    pthread_mutex_unlock(&mutex_[my_streams[stream_idx]]);
  }
}
