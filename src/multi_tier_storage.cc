#include "multi_tier_storage.h"

#include <fcntl.h>
#include <pthread.h>
#include <string.h>

#include <algorithm>
#include <string>

#include <glog/logging.h>

#include "sprinkler_common.h"

int64_t MultiTierStorage::mem_buf_size_;
int64_t MultiTierStorage::disk_chunk_size_;

void MultiTierStorage::init_gc() {
  // Make sure on-disk gc and use of erasure code are mutual exclusive.
  CHECK(gc_disk_thread_count_ == 0 || fragments_ == 0);

  if (gc_thread_count_ > 0) {
    LOG(INFO) << "Initialize " << gc_thread_count_
        << " in-memory GC threads ...";
    for (int i = 0; i < gc_thread_count_; ++i) {
      gc_hints_[i].ptr = this;
      gc_hints_[i].tid = i;
      gc_hints_[i].on_disk = false;
      int rc = pthread_create(&gc_threads_[i], NULL,
          &MultiTierStorage::start_gc, static_cast<void *>(&gc_hints_[i]));
      if (rc) {
        LOG(ERROR) << "pthread_create failed with rc " << rc << ".";
      }
    }
  }

  if (gc_disk_thread_count_ > 0) {
    LOG(INFO) << "Initialize " << gc_disk_thread_count_
        << " on-disk GC threads ...";
    for (int i = gc_thread_count_; i < gc_thread_count_ + gc_disk_thread_count_;
        ++i) {
      gc_hints_[i].ptr = this;
      gc_hints_[i].tid = i - gc_thread_count_;
      gc_hints_[i].on_disk = true;
      int rc = pthread_create(&gc_threads_[i], NULL,
          &MultiTierStorage::start_gc, static_cast<void *>(&gc_hints_[i]));
      if (rc) {
        LOG(ERROR) << "pthread_create failed with rc " << rc << ".";
      }
    }
  }

  if (fragments_ != 0) {
    LOG(INFO) << "Initialize erasure code.";
    int i = gc_thread_count_;
    gc_hints_[i].ptr = this;
    gc_hints_[i].tid = -1;
    int rc = pthread_create(&gc_threads_[i], NULL,
        &MultiTierStorage::start_gc, static_cast<void *>(&gc_hints_[i]));
    if (rc) {
      LOG(ERROR) << "pthread_create failed with rc " << rc << ".";
    }

    chunk_sem_ = sem_open("chunk_sem", O_CREAT, 0, 3);
    ec_watermark_ = 0;
  }
}

int64_t MultiTierStorage::put_raw_events(
    int sid, int64_t now, int64_t nevents, const uint8_t *data) {
  MemBuffer &membuf = mem_store_[sid];
//  VLOG(kLogLevel) << "put_raw_events: stream " << sid << "; " << nevents
//      << " events, staring " << membuf.end_seq << ".";
  if (nevents == 0) {
    LOG(WARNING) << "put_raw_events invoked with 0 events";
    return membuf.end_seq;
  }
  CHECK_NOTNULL(data);

  // Not enough space, flush something to disk.
  if (get_free_space(membuf) < nevents * kEventLen) {
    flush_to_disk(sid);
  }

  uint8_t *ptr = membuf.chunk;

  // First, copy over unformatted events.
  int64_t end_offset = membuf.end_offset;
  if (end_offset + nevents * kEventLen <= mem_buf_size_) {
    memmove(ptr + end_offset, data, nevents * kEventLen);
  } else {
    memmove(ptr + end_offset, data, mem_buf_size_ - end_offset);
    memmove(ptr, data + (mem_buf_size_ - end_offset),
        nevents * kEventLen - (mem_buf_size_ - end_offset));
  }

  // Next, format events with seq#'s and timestamps.
  for (int i = 0; i < nevents; ++i) {
    itos(ptr + end_offset + 1, membuf.end_seq, 8);
    itos(ptr + end_offset + kEventLen - 8, now, 8);
    end_offset = next_offset(end_offset);
    ++membuf.end_seq;
  }

  LOG_EVERY_N(INFO, 5) << "PUT " << sid << " " << (membuf.end_seq - 1);

  // Finally, set the new end_offset, empty flag, and update the counter.
  membuf.end_offset = end_offset;
  membuf.is_empty = false;
  membuf.bytes_wrote += nevents * kEventLen;

  return membuf.end_seq;
}

int64_t MultiTierStorage::put_events(
    int sid, int64_t nevents, uint8_t *data) {
  VLOG(kLogLevel) << "put_events: stream " << sid << "; " << nevents
      << " events.";
  MemBuffer &membuf = mem_store_[sid];
  if (nevents == 0) {
    LOG(WARNING) << "put_events invoked with 0 events";
    return membuf.end_seq;
  }
  CHECK_NOTNULL(data);

  int64_t end_seq = membuf.end_seq;
  int64_t end_offset = membuf.end_offset;
  uint8_t *ptr = membuf.chunk;

  // Find offset to the first event that is needed.
  if (!in_range(data, end_seq)) {
    int64_t fit_offset = adjust_offset_linear(end_seq, nevents, data);
    if (fit_offset == -1) {
      LOG(WARNING) << "Got out of range events starting at "
          << get_begin_seq(data);
      return end_seq;
    }
    data += fit_offset;
    nevents -= fit_offset / kEventLen;
  }

  // Discard previous events if the first new event is a tombstone that overlaps
  // with previously received events.
  if (is_tombstone(data)) {
    pthread_mutex_lock(&mem_mutex_[sid]);
    int64_t new_begin = get_begin_seq(data);
    if (membuf.begin_seq >= new_begin) {
      // Invalidate any on-going GC avtivity.
      membuf.gc_begin_offset = -1;
      membuf.gc_table_begin_offset = -1;
      // The first new event covers the all the content currently in the buffer.
      membuf.bytes_saved += get_used_space(membuf);
      // Clear up the entire buffer.
      membuf.begin_offset = end_offset;
      membuf.begin_seq = new_begin;
      membuf.is_empty = true;
    } else if (new_begin == end_seq) {
      // If the last previous event is also a tombstone, merge with *this.
      if (!membuf.is_empty &&
          is_tombstone(ptr + prev_offset(end_offset))) {
        // Invalidate on-going GC activity if this interferes with GC table.
        if (membuf.gc_table_end_offset == end_offset) {
          membuf.gc_begin_offset = -1;
          membuf.gc_table_begin_offset = -1;
        }
        // Move pointers backwards for one event.
        membuf.bytes_saved += kEventLen;
        membuf.end_offset = end_offset = prev_offset(end_offset);
        membuf.end_seq = end_seq = get_begin_seq(ptr + end_offset);
        memmove(data + 1, ptr + end_offset + 1, 8);
      }
    } else {
      // data contains end_seq, and it's not the begin, so there are some events
      // to merge.
      int64_t fit_offset = adjust_offset_circular(new_begin,
          membuf.begin_offset, end_offset, ptr);
      CHECK_GT(fit_offset, -1);
      // Invalidate on-going GC activity if there is interference.
      if (membuf.gc_table_begin_offset != -1 &&
          (in_between(fit_offset, end_offset, membuf.gc_table_begin_offset) ||
           in_between(fit_offset, end_offset, membuf.gc_table_end_offset))) {
        membuf.gc_begin_offset = -1;
        membuf.gc_table_begin_offset = -1;
      }
      // Update begin seq for the first new event.
      if (is_tombstone(ptr + fit_offset)) {
        memmove(data + 1, ptr + fit_offset + 1, 8);
      }

      membuf.bytes_saved += distance(fit_offset, end_offset);

      membuf.end_offset = end_offset = fit_offset;
      membuf.end_seq = end_seq = get_begin_seq(data);
    }
    pthread_mutex_unlock(&mem_mutex_[sid]);
  }

  // Not enough space, flush something to disk.
  if (get_free_space(membuf) < nevents * kEventLen) {
    flush_to_disk(sid);
  }

  // Copy the data over.
  if (end_offset + nevents * kEventLen <= mem_buf_size_) {
    memmove(ptr + end_offset, data, nevents * kEventLen);
  } else {
    memmove(ptr + end_offset, data, mem_buf_size_ - end_offset);
    memmove(ptr, data + (mem_buf_size_ - end_offset),
        nevents * kEventLen - (mem_buf_size_ - end_offset));
  }

  // Set new offset, seq#, and empty flag.
  end_offset += nevents * kEventLen;
  membuf.end_offset = (end_offset < mem_buf_size_
      ? end_offset
      : end_offset - mem_buf_size_);
  membuf.end_seq = get_end_seq(data + (nevents - 1) * kEventLen);
  membuf.is_empty = false;
  // Update the counter.
  membuf.bytes_wrote += nevents * kEventLen;

  LOG_EVERY_N(INFO, 5) << "PUT " << sid << " " << (membuf.end_seq - 1);

  return membuf.end_seq;
}

int64_t MultiTierStorage::get_events(int pid, int sid, int64_t now,
    int64_t first_seq, int64_t max_events, uint8_t *buffer) {
  MemBuffer &membuf = mem_store_[sid];
  VLOG(kLogLevel) << "get_events for proxy " << pid
      << " on stream " << sid << "; starting at "
      << first_seq << "; at most " << max_events << " events are needed;"
      << " begin_offset is " << membuf.begin_offset
      << " begin_seq is " << membuf.begin_seq;
  // first_seq is too large.
  if (first_seq >= membuf.end_seq) {
    LOG(ERROR) << "get_events on stream " << sid << ": asking for " << first_seq
        << ", end_seq is " << membuf.end_seq;
    return kErrFuture;
  }

  PublishBuffer &pubbuf = publish_buffer_[pid][sid];
  if (pubbuf.remainder > 0 &&
      in_range(pubbuf.buffer + pubbuf.offset, first_seq)) {
    // Reuested events matches publish buffer, extract from the buffer directly.
    // No need for locking since we are reading from the PublishBuffer,
    // not the main memory buffer.
    VLOG(kLogLevel) << "Requested seq# " << first_seq << " is in pubbuf.";
    int64_t nevents = pubbuf.remainder / kEventLen;
    if (nevents > max_events) {
      nevents = max_events;
    }
    memmove(buffer, pubbuf.buffer + pubbuf.offset, nevents * kEventLen);
    pubbuf.offset += nevents * kEventLen;
    pubbuf.remainder -= nevents * kEventLen;

    // No need to update timestamp since it was updated when these events were
    // loaded into pubbuf.
    return nevents;
  } else if (first_seq < membuf.begin_seq) {
    // first_seq is not in memory.
    // Load the chunk that contains the seq# into publish buffer.
    VLOG(kLogLevel) << "Requested seq# " << first_seq << " is on disk.";
    int64_t chunk_id = get_chunk_id_by_seq(sid, first_seq);
    // We know that this seq# is on disk for sure ...
    CHECK_NE(chunk_id, -1);
    std::string filename = chunk_summary_[sid][chunk_id]->filename;

    pthread_mutex_lock(chunk_summary_[sid][chunk_id]->mutex_ptr);
    FILE *fin = fopen(filename.c_str(), "r");
    int64_t rc =
        fread(pubbuf.buffer, kEventLen, disk_chunk_size_ / kEventLen, fin);
    fclose(fin);
    pthread_mutex_unlock(chunk_summary_[sid][chunk_id]->mutex_ptr);

    VLOG(kLogLevel) << "Loaded chunk " << chunk_id << " into publish buffer.";

    pubbuf.offset = adjust_offset_linear(
        first_seq, rc, pubbuf.buffer);
    pubbuf.remainder = rc * kEventLen - pubbuf.offset;

    int64_t nevents = pubbuf.remainder / kEventLen;
    if (nevents > max_events) {
      nevents = max_events;
    }
    memmove(buffer, pubbuf.buffer + pubbuf.offset, nevents * kEventLen);
    pubbuf.offset += nevents * kEventLen;
    pubbuf.remainder -= nevents * kEventLen;

    // Update the timestamp.
    if (now != -1) {
      last_pub_timestamp_[pid][sid] =
          get_timestamp(pubbuf.buffer + disk_chunk_size_ - kEventLen);
    }

    return nevents;
  } else {
    // If nothing is left in the publish buffer, we have to fetch events from
    // the main in-memory buffer.
    VLOG(kLogLevel) << "Requested seq# " << first_seq
        << " is in memory, but not in the pubbuf.";

    if (now != -1 && now - last_pub_timestamp_[pid][sid] < pub_delay_) {
      return kErrDelay;
    }

    pthread_mutex_lock(&mem_mutex_[sid]);
    // First, determine the memory range needs to copy.
    int64_t begin_offset = adjust_offset_circular(first_seq,
        membuf.begin_offset, membuf.end_offset,
        membuf.chunk);
    // If it gets -1, it has to be an error ...
    CHECK_GT(begin_offset, -1);
    int64_t end_offset = membuf.end_offset;

    // First, load as much as a chunk of memory into publish buffer
    // for batched sends.
    int64_t len = distance(begin_offset, end_offset);
    if (len >= disk_chunk_size_) {
      len = disk_chunk_size_;
    } 
    VLOG(kLogLevel) << "In-memory buffer: [" << membuf.begin_offset << ", "
        << membuf.end_offset << ")";
    VLOG(kLogLevel) << "pubbuf: " << pubbuf.offset << " " << pubbuf.remainder
        << " " << (uint64_t) pubbuf.buffer;
    VLOG(kLogLevel) << "Memmove: " << begin_offset << " " << len << " "
        << mem_buf_size_;

    if (begin_offset + len <= mem_buf_size_) {
      memmove(pubbuf.buffer, membuf.chunk + begin_offset, len);
    } else {
      memmove(pubbuf.buffer, membuf.chunk + begin_offset,
          mem_buf_size_ - begin_offset);
      memmove(pubbuf.buffer + (mem_buf_size_ - begin_offset), membuf.chunk,
          len - (mem_buf_size_ - begin_offset));
    }

    VLOG(kLogLevel) << "Loaded " << len << " bytes from " << begin_offset
        << " into publish buffer.";

    // Unlock the mutex.  No further code in this method touches the in-memory
    // buffer anymore.
    pthread_mutex_unlock(&mem_mutex_[sid]);

    // Update the timestamp.
    if (now != -1) {
      last_pub_timestamp_[pid][sid] =
          get_timestamp(pubbuf.buffer + len - kEventLen);
    }

    int64_t nevents = len / kEventLen > max_events
        ? max_events
        : len / kEventLen;

    // nevents == 0 indicates an error behavior and should've been caught above.
    CHECK_NE(nevents, 0);

    // Next, copy events into buffer.
    memmove(buffer, pubbuf.buffer, nevents * kEventLen);

    // Finally, update offset/remainder.
    pubbuf.offset = nevents * kEventLen;
    pubbuf.remainder = len - pubbuf.offset;

    return nevents;
  }
}

void MultiTierStorage::report_state() {
  for (int i = 0; i < nstreams_; ++i) {
    report_state(i);
  }
}

void MultiTierStorage::report_state(int sid) {
  LOG(INFO) << "STATS: stream " << sid << " "
      << mem_store_[sid].end_seq - 1 << " "
      << mem_store_[sid].bytes_wrote << " "
      << mem_store_[sid].bytes_saved << " "
      << bytes_saved_disk_[sid] << " ["
      << mem_store_[sid].begin_offset << ", "
      << mem_store_[sid].end_offset << ")";
}

void MultiTierStorage::grab_all_locks() {
  for (int i = 0; i < nstreams_; ++i) {
    pthread_mutex_lock(&mem_mutex_[i]);
  }
}

int64_t MultiTierStorage::distance(int64_t begin, int64_t end) {
  if (end >= begin) {
    return end - begin;
  } else {
    return mem_buf_size_ - (begin - end);
  }
}

int64_t MultiTierStorage::get_used_space(const MemBuffer &membuf) {
  if (membuf.is_empty) {
    return 0;
  }
  int64_t result = distance(membuf.begin_offset, membuf.end_offset);
  if (result == 0) {
    result = mem_buf_size_;
  }
  return result;
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

bool MultiTierStorage::in_between(int64_t left, int64_t right, int64_t offset) {
  if (left == right) {
    return false;
  } else if (left < right) {
    return left <= offset && offset < right;
  } else {
    return left <= offset || offset < right;
  }
}

int64_t MultiTierStorage::adjust_offset_linear(
    int64_t seq, int64_t nevents, const uint8_t *chunk) {
  int64_t lo = 0;
  int64_t hi = nevents - 1;
  
  while (lo <= hi) {
    int64_t mid = ((lo + hi) >> 1) * kEventLen;   // Event offset at midpoint.
    if (in_range(chunk + mid, seq)) {
      return mid;
    }
    if (get_begin_seq(chunk + mid) > seq) {
      hi = mid / kEventLen - 1;
    } else if (get_end_seq(chunk + mid) <= seq) {
      lo = mid / kEventLen + 1;
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

  int64_t lo = begin / kEventLen;
  int64_t hi = (end + mem_buf_size_) / kEventLen;

  while (lo <= hi) {
    int64_t mid = (lo + hi) >> 1;   // Event at midpoint.
    int64_t idx = mid * kEventLen;
    if (idx >= mem_buf_size_) {
      idx -= mem_buf_size_;
    }

    if (in_range(chunk + idx, seq)) {
      return idx;
    }
    if (get_begin_seq(chunk + idx) > seq) {
      hi = mid - 1;
    } else if (get_end_seq(chunk + idx) <= seq) {
      lo = mid + 1;
    }
  }
  return -1;
}

int MultiTierStorage::get_chunk_id_by_seq(int sid, int64_t seq) {
  pthread_mutex_lock(&meta_mutex_[sid]);
  int lo = 0;
  int hi = chunk_summary_[sid].size() - 1;
  int result = -1;

  while (lo <= hi) {
    int mid = (lo + hi) >> 1;
    if (chunk_summary_[sid][mid]->begin_seq <= seq &&
        seq < chunk_summary_[sid][mid]->end_seq) {
      result = mid;
      break;
    }
    if (chunk_summary_[sid][mid]->begin_seq > seq) {
      hi = mid - 1;
    } else if (chunk_summary_[sid][mid]->end_seq <= seq) {
      lo = mid + 1;
    }
  }
  pthread_mutex_unlock(&meta_mutex_[sid]);
  return result;
}

void MultiTierStorage::flush_to_disk(int sid) {
  pthread_mutex_lock(&mem_mutex_[sid]);

  MemBuffer &membuf = mem_store_[sid];

  int64_t begin_offset = membuf.begin_offset;
  int64_t end_offset = begin_offset + disk_chunk_size_;
  if (end_offset >= mem_buf_size_) {
    end_offset -= mem_buf_size_;
  }

  LOG(INFO) << "FLUSH " << sid << " [" << begin_offset
      << ", " << end_offset << ")";

  uint8_t *ptr = membuf.chunk;

  std::string filename = get_chunk_name(sid, next_chunk_no_[sid]);

  // Write the chunk to disk.
  FILE *fout = fopen(filename.c_str(), "w");
  if (begin_offset < end_offset) {
    // No carry-over, a single write is sufficient.
    fwrite(ptr + begin_offset, 1, disk_chunk_size_, fout);
  } else {
    // Two writes.
    fwrite(ptr + begin_offset, 1, mem_buf_size_ - begin_offset, fout);
    fwrite(ptr, 1, disk_chunk_size_ - (mem_buf_size_ - begin_offset), fout);
  }
  fclose(fout);

  // Update chunk metadata.
  ChunkInfo *chunk_info = new ChunkInfo(filename, membuf.begin_seq,
      get_end_seq(ptr + prev_offset(end_offset)));

  pthread_mutex_lock(&meta_mutex_[sid]);
  chunk_summary_[sid].push_back(chunk_info);
  ++next_chunk_no_[sid];
  pthread_mutex_unlock(&meta_mutex_[sid]);

  // Update new begin_{seq, offset}.
  membuf.begin_seq = get_begin_seq(ptr + end_offset);
  membuf.begin_offset = end_offset;

  // Invalidate on-going GC activity if there is interference.
  if (in_between(begin_offset, end_offset, membuf.gc_begin_offset)) {
    membuf.gc_begin_offset = -1;
  }
  if (in_between(begin_offset, end_offset,
        membuf.gc_table_begin_offset)) {
    membuf.gc_table_begin_offset = -1;
  }

  VLOG(kLogLevel) << "New begin_offset: " << membuf.begin_offset
      << " gc_begin_offset " << membuf.gc_begin_offset
      << " gc_table_begin_offset " << membuf.gc_table_begin_offset;
/*
  if (fragments_ > 0) {
    sem_post(chunk_sem_);
  }
*/
  pthread_mutex_unlock(&mem_mutex_[sid]);
}

std::string MultiTierStorage::get_chunk_name(int sid, int64_t chunk_id) {
  return "chunk-" + std::to_string(sid) + "-" + std::to_string(chunk_id);
}

void *MultiTierStorage::start_gc(void *arg) {
  GcHint *hint_t = static_cast<GcHint *>(arg);
  MultiTierStorage *storage_t = hint_t->ptr;
  int tid = hint_t->tid;
  bool on_disk = hint_t->on_disk;

  if (tid == -1) {
    // Erasure coding.
    storage_t->run_erasure_encoder();
  } if (on_disk) {
    storage_t->run_gc_on_disk(tid);
  } else {
    storage_t->run_gc_in_memory(tid);
  }
}

void MultiTierStorage::run_gc_in_memory(int thread_id) {
  LOG(INFO) << "In-memory garbage collection thread #" << thread_id
      << " started.";

  std::vector<int> my_streams;
  std::unordered_map<int, GcInfo> metadata;

  // First, determine which streams am I responsible for.
  for (int i = thread_id; i < nstreams_; i += gc_thread_count_) {
    my_streams.push_back(i);
    metadata.insert(std::make_pair(i, GcInfo()));
  }

  // Round robin across these streams.
  int stream_idx = 0;
  while (true) {
    int sid = my_streams[stream_idx];
    MemBuffer &membuf = mem_store_[sid];
    uint8_t *ptr = membuf.chunk;
    GcInfo &gc_info = metadata[sid];
    
    // Skip GC if there are too few events.
    if (get_used_space(membuf) >= min_gc_pass_) {
      // Perform GC with mutex.
      pthread_mutex_lock(&mem_mutex_[sid]);

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
      int64_t gc_table_end_offset = membuf.end_offset;
      int64_t gc_table_begin_offset =
        gc_table_end_offset - gc_table_size * kEventLen;
      if (gc_table_begin_offset < 0) {
        gc_table_begin_offset += mem_buf_size_;
      }

      // Make sure that GC table always start with a data event.
      while (gc_table_begin_offset != gc_table_end_offset &&
          is_tombstone(ptr + gc_table_begin_offset)) {
        gc_table_begin_offset = next_offset(gc_table_begin_offset);
      }
      if (gc_table_begin_offset == gc_table_end_offset) {
        continue;
      }

      for (int64_t i = gc_table_begin_offset; i != gc_table_end_offset;
          i = next_offset(i)) {
        // Skip this event if it is a tombstone.
        if (is_tombstone(ptr + i)) {
          continue;
        }
        int64_t key = get_object_id(ptr + i);
        gc_info.table.insert(key);
      }
      LOG(INFO) << "GC started on stream " << sid
        << ": (" << membuf.begin_offset << ", " << gc_table_begin_offset
        << ", " << gc_table_end_offset << ") with " << gc_info.table.size()
        << " distinct events in GC table. Seq#: ["
        << get_begin_seq(ptr + gc_table_begin_offset)
        << ", " << get_end_seq(ptr + prev_offset(gc_table_end_offset)) << ").";

      // Set up flags in MemBuffer marking an on-going GC activity.
      membuf.gc_begin_offset = membuf.begin_offset;
      membuf.gc_table_begin_offset = gc_table_begin_offset;
      membuf.gc_table_end_offset = gc_table_end_offset;

      VLOG(kLogLevel) << "membuf before GC: [" << membuf.begin_offset << ", "
        << membuf.end_offset << ")";

      int64_t begin_seq = membuf.begin_seq;
      int64_t begin_offset = membuf.begin_offset;
      int64_t end_offset = gc_table_begin_offset;

      if (distance(begin_offset, end_offset) > max_gc_pass_) {
        begin_offset += distance(begin_offset, end_offset) - max_gc_pass_;
        if (begin_offset >= mem_buf_size_) {
          begin_offset -= mem_buf_size_;
        }
        begin_seq = get_begin_seq(ptr + begin_offset);
        LOG(INFO) << "GC begin_offset is set to " << begin_offset;
      }

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

        VLOG(kLogLevel) << "GC processing stream " << sid
          << ", [" << begin_offset << ", " << pause_offset << ")";

        // b) In buffer [begin_offset, pause_offset), scan for events should be
        // GCed, and turn them into (singleton) tombstones.
        int64_t cursor = begin_offset;
        while (cursor != pause_offset) {
          if (is_data_event(ptr + cursor) &&
              gc_info.table.count(get_object_id(ptr + cursor)) == 1) {
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
          cursor = prev_offset(cursor);
          if (is_data_event(ptr + cursor)) {
            // For data events, move them rightwards if necessary.
            processed = prev_offset(processed);
            if (cursor != processed) {
              memmove(ptr + processed, ptr + cursor, kEventLen);
            }
            current_gc = false;
          } else {
            // For GCed events, set a mark if this is the first one in a series,
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
              memmove(ptr + processed + 1, ptr + cursor + 1, 8);
            }
            lo = get_begin_seq(ptr + cursor);
          }
        }

        membuf.bytes_saved += distance(begin_offset, processed);

        // d) Squeeze the buffer rightwards.
        // If there is valid buffer region before where we started, we need
        // to move that chunk of space rightwards.
        if (begin_offset != membuf.begin_offset) {
          // If the scan ended with a GC event, we should check if the event just
          // before begin_offset is also a GC event, and if so, merge them.
          cursor = prev_offset(begin_offset);
          if (is_tombstone(ptr + processed) && is_tombstone(ptr + cursor)) {
            CHECK_EQ(get_end_seq(ptr + cursor), get_begin_seq(ptr + processed));
            memmove(ptr + processed + 1, ptr + cursor + 1, 8);
            // From this point on, begin_offset is used as the right boundary
            // of copying the previous chunk.
            begin_offset = cursor;
            membuf.bytes_saved += kEventLen;
          }

          // Move the data before begin_offset rightwards.
          // Calculate the destination offset to shift the buffer.
          int64_t dist = distance(begin_offset, processed);
          int64_t src_offset = membuf.begin_offset;
          int64_t dst_offset = membuf.begin_offset + dist;
          if (dst_offset >= mem_buf_size_) {
            dst_offset -= mem_buf_size_;
          }

          // Offsets at which the source or destination buffer breaks.
          std::vector<int> cutoffs;
          if (begin_offset < src_offset) {
            cutoffs.push_back(mem_buf_size_ - src_offset);
          }
          if (processed < dst_offset) {
            cutoffs.push_back(mem_buf_size_ - dst_offset);
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
        } else {
          membuf.begin_offset = processed;
        }

        VLOG(kLogLevel) << "New starting offset: " << membuf.begin_offset;

        // Advance to the next area.
        begin_offset = pause_offset;
        begin_seq = get_begin_seq(ptr + pause_offset);

        // Update the flag.
        membuf.gc_begin_offset = begin_offset;

        // Release the lock so that other threads get a chance to enter.
        VLOG(kLogLevel) << "GC: yield.";
        pthread_mutex_unlock(&mem_mutex_[sid]);

        timespec time_for_sleep;
        time_for_sleep.tv_sec = 0;
        time_for_sleep.tv_nsec = 10000000;  // 0.01 sec.

        int rc = 0;
        if (rc = nanosleep(&time_for_sleep, NULL) < 0) {
          LOG(ERROR) << "nanosleep() failed on thread " << thread_id
            << " with rc " << rc << ".";
        }

        // Re-aquire the lock before proceeding to another area.
        pthread_mutex_lock(&mem_mutex_[sid]);
        VLOG(kLogLevel) << "GC: I'm back.";

        // If the GC table area is corrupted, abort this GC pass.
        if (membuf.gc_table_begin_offset == -1) {
          // Log as WARNING here since this is really unlikely.
          // Take a deeper look at it if encountered.
          LOG(WARNING) << "Abort GC since the GC table area is corrupted.";
          break;
        }

        // If the begin offset of GC is corrupted, update to current begin_offset.
        if (membuf.gc_begin_offset == -1) {
          VLOG(kLogLevel) << "Update gc_begin_offset to " << membuf.begin_offset
            << " since the buffer before this offset has been flushed to disk.";
          begin_offset = membuf.gc_begin_offset = membuf.begin_offset;
          begin_seq = get_begin_seq(ptr + begin_offset);
        }
      }

      LOG(INFO) << "GC finished on stream " << sid
        << ". New begin_offset is " << membuf.begin_offset << ".";

      report_state(sid);

      pthread_mutex_unlock(&mem_mutex_[sid]);
    }

    if (++stream_idx == my_streams.size()) {
      stream_idx = 0;
    }
  }
}

void MultiTierStorage::run_gc_on_disk(int thread_id) {
  LOG(INFO) << "On-disk garbage collection thread #" << thread_id
      << " started.";

  // ID of streams this thread will garbage collect.
  std::vector<int> my_streams;
  // Offset of the last chunk being used for on-disk GC.
  std::vector<int64_t> last_chunk;
  // Chunk buffer.
  uint8_t *chunk = static_cast<uint8_t *>(dcalloc(disk_chunk_size_, 1));
  int64_t chunk_size = 0, new_chunk_size = 0;
  // Hashtable of GC events.
  std::unordered_set<int64_t> table;
  // Is the content of a chunk changed?
  bool has_changed = false;

  // First, determine which streams am I responsible for.
  for (int i = thread_id; i < nstreams_; i += gc_thread_count_) {
    my_streams.push_back(i);
    last_chunk.push_back(0);
  }

  // Round robin across these streams.
  int stream_idx = 0;
  while (true) {
    int sid = my_streams[stream_idx];

    if (last_chunk[sid] >= next_chunk_no_[sid]) {
      timespec time_for_sleep;
      time_for_sleep.tv_sec = 0;
      time_for_sleep.tv_nsec = 100000000;  // 0.1 sec.

      int rc = 0;
      if (rc = nanosleep(&time_for_sleep, NULL) < 0) {
        LOG(ERROR) << "nanosleep() failed on thread " << thread_id
          << " with rc " << rc << ".";
      }

      continue;
    }

    LOG(INFO) << "On-disk GC started for stream " << sid
        << " up to chunk index " << last_chunk[sid];

    ChunkInfo *cur_chunk = chunk_summary_[sid][last_chunk[sid]];

    // First, garbage collection within the last chunk and construct the
    // GC table.
    std::string filename = cur_chunk->filename;
    FILE *fin = fopen(filename.c_str(), "r");
    chunk_size =
      fread(chunk, kEventLen, disk_chunk_size_ / kEventLen, fin) * kEventLen;
    fclose(fin);

    table.clear();
    CHECK_GT(chunk_size, 0);
    has_changed = false;
    for (int64_t i = chunk_size - kEventLen; i >= 0; i -= kEventLen) {
      if (is_tombstone(chunk + i)) {
       continue;
      }

      int64_t key = get_object_id(chunk + i);
      if (table.count(key) == 1) {
        to_tombstone(chunk + i);
        has_changed = true;
      } else {
        table.insert(key);
      }
    }

    new_chunk_size = merge_tombstones(chunk, chunk_size);
    if (new_chunk_size != chunk_size) {
      has_changed = true;
      bytes_saved_disk_[sid] += chunk_size - new_chunk_size;
    }

    if (has_changed) {
      pthread_mutex_lock(cur_chunk->mutex_ptr);
      FILE *fout = fopen(filename.c_str(), "w");
      fwrite(chunk, new_chunk_size, 1, fout);
      fclose(fout);
      pthread_mutex_unlock(cur_chunk->mutex_ptr);
    }

    if (new_chunk_size != chunk_size) {
      LOG(INFO) << "On-disk GC happened in " << filename
          << " saving " << chunk_size - new_chunk_size << " bytes.";
    } else {
      LOG(INFO) << "On-disk GC: nothing saved in " << filename;
    }

    // Then, garbage collect previous chunks.
    int64_t starting_chunk = last_chunk[sid] > 45 ? last_chunk[sid] - 45 : 0;
//    int64_t starting_chunk = 02
//    int64_t starting_chunk = last_chunk[sid];
    for (int64_t i = starting_chunk; i < last_chunk[sid]; ++i) {
      cur_chunk = chunk_summary_[sid][i];

      std::string filename = cur_chunk->filename;
      FILE *fin = fopen(filename.c_str(), "r");
      chunk_size =
        fread(chunk, kEventLen, disk_chunk_size_ / kEventLen, fin) * kEventLen;
      fclose(fin);

      has_changed = false;
      for (int64_t offset = 0; offset < chunk_size; offset += kEventLen) {
        if (is_data_event(chunk + offset) &&
            table.count(get_object_id(chunk + offset)) == 1) {
          to_tombstone(chunk + offset);
          has_changed = true;
        }
      }

      new_chunk_size = merge_tombstones(chunk, chunk_size);
      if (new_chunk_size != chunk_size) {
        has_changed = true;
        bytes_saved_disk_[sid] += chunk_size - new_chunk_size;
      }

      if (has_changed) {
        pthread_mutex_lock(cur_chunk->mutex_ptr);
        FILE *fout = fopen(filename.c_str(), "w");
        fwrite(chunk, new_chunk_size, 1, fout);
        fclose(fout);
        pthread_mutex_unlock(cur_chunk->mutex_ptr);
      }

      if (new_chunk_size != chunk_size) {
        LOG(INFO) << "On-disk GC happened in " << filename
          << " saving " << chunk_size - new_chunk_size << " bytes.";
      } else {
        LOG(INFO) << "On-disk GC: nothing saved in " << filename;
      }
    }

    ++last_chunk[sid];

    if (++stream_idx == my_streams.size()) {
      stream_idx = 0;
    }
  }
}

int64_t MultiTierStorage::merge_tombstones(uint8_t *chunk, int64_t size) {
  int64_t i = 0, j = 0;
  while (j < size) {
    if (is_tombstone(chunk + j)) {
      int64_t k = j;
      while (k < size && is_tombstone(chunk + k)) {
        k += kEventLen;
      }

      if (k != j) {
        memmove(chunk + j + 9, chunk + k - kEventLen + 9, 8);
      }

      if (i != j) {
        memmove(chunk + i, chunk + j, kEventLen);
      }
      j = k;
    } else {
      if (i != j) {
        memmove(chunk + i, chunk + j, kEventLen);
      }
      j += kEventLen;
    }
    i += kEventLen;
  }

  return i;
}

void MultiTierStorage::run_erasure_encoder() {
  LOG(INFO) << "Erasure encoder thread started.";
  uint8_t *ec_buffer = static_cast<uint8_t *>(dcalloc(1048576, 16));
  LOG(INFO) << "miaow";
  while (true) {
//    sem_wait(chunk_sem_);
    if (ec_watermark_ >= next_chunk_no_[0]) {
      pthread_yield();
      continue;
    }
    LOG(INFO) << "encode " + std::to_string(ec_watermark_);
//    std::string input_file = get_chunk_name(0, ec_watermark_);
    LOG(INFO) << "get_chunk_name " << 0 << " " << ec_watermark_;
    std::string input_file = "chunk-0-" + std::to_string(ec_watermark_);
    LOG(INFO) << "encoding " << input_file;

//    pthread_mutex_lock(chunk_summary_[0][ec_watermark_]->mutex_ptr);
    LOG(INFO) << "1";
    FILE *fin = fopen(input_file.c_str(), "r");
    LOG(INFO) << "2 " << (fin == NULL);
    int64_t chunk_size =
        fread(ec_buffer, kEventLen, disk_chunk_size_ / kEventLen, fin);
    LOG(INFO) << "3 " << chunk_size;
    fclose(fin);
    sleep(1);
//    pthread_mutex_unlock(chunk_summary_[0][ec_watermark_]->mutex_ptr);

    chunk_size *= kEventLen;

    if (fragments_ > 0) {
      int64_t fragment_size = chunk_size % fragments_ == 0 ? 0 : 1;
      fragment_size += chunk_size / fragments_;

      for (int i = 0; i < fragments_; ++i) {
        std::string output_file = input_file + "-" + std::to_string(i);
        FILE *fout = fopen(output_file.c_str(), "wb");
        fwrite(ec_buffer + i * fragment_size, 1, fragment_size, fout);
        fclose(fout);
      }
    } else {
      int64_t fragment_size = chunk_size / 2;
      for (int i = 0; i < 2; ++i) {
        std::string output_file = input_file + "-" + std::to_string(i);
        FILE *fout = fopen(output_file.c_str(), "wb");
        fwrite(ec_buffer + i * fragment_size, 1, fragment_size, fout);
        fclose(fout);
      }

      for (int64_t i = 0; i < fragment_size; ++i) {
        ec_buffer[i] ^= ec_buffer[i + fragment_size];
      }
      std::string output_file = input_file + "-2";
      FILE *fout = fopen(output_file.c_str(), "wb");
      fwrite(ec_buffer, 1, fragment_size, fout);
      fclose(fout);
    }

//    pthread_mutex_lock(chunk_summary_[0][ec_watermark_]->mutex_ptr);
    ++ec_watermark_;
    remove(input_file.c_str());
//    pthread_mutex_unlock(chunk_summary_[0][ec_watermark_]->mutex_ptr);
  }
}
