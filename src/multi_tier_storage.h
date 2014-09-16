#ifndef MULTI_TIER_STORAGE_H_
#define MULTI_TIER_STORAGE_H_

#include <pthread.h>
#include <stdint.h>

#include <deque>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>

#include "dmalloc.h"

// Stores events received by a Sprinkler node.  An in-memory buffer stores most
// recent events from every stream registered with the system, and an on-disk
// permanent storage unit keeps all the history for streams that should be kept
// at this node.
class MultiTierStorage {
 public:
  MultiTierStorage(int nstreams, int64_t mem_buf_size, int64_t disk_chunk_size,
      int gc_thread_count, int64_t min_events_to_gc,
      int64_t max_gc_table_size, int64_t max_gc_chunk_size)
    : nstreams_(nstreams),
      mutex_(nstreams), next_chunk_no_(nstreams, 0), used_chunk_no_(nstreams),
      max_gc_table_size_(max_gc_table_size),
      max_gc_chunk_size_(max_gc_chunk_size),
      gc_thread_count_(gc_thread_count), min_events_to_gc_(min_events_to_gc),
      gc_threads_(gc_thread_count), gc_hints_(gc_thread_count),
      bytes_saved_(nstreams) {
    // Set buffer/chunk sizes here since they are static.
    mem_buf_size_ = mem_buf_size;
    disk_chunk_size_ = disk_chunk_size;

    // This goes here since it needs mem_buf_size_.
    mem_store_ = std::vector<MemBuffer>(nstreams);

    // Initialize mutexes.
    for (int i = 0; i < nstreams; ++i) {
      int rc = pthread_mutex_init(&mutex_[i], NULL);
      if (rc) {
        LOG(ERROR) << "pthread_mutex_init failed with rc " << rc << ".";
      }
    }

    // Initialize garbage collection.
    init_gc();
  }

  // Add a block of raw events from a client.
  // "data" here contains only messages, no header is included.
  // Returns the smallest seq# that has not been assigned yet.
  int64_t put_raw_events(int sid, int64_t nevents, const uint8_t *data);

  // Add a block of formatted events from a peer proxy.
  // "data" here contains only messages, no header is included.
  // Returns the smallest seq# that has not been received yet.
  int64_t put_events(int sid, int64_t nevents, const uint8_t *data);

  // Retrieve events from a stream.
  // Return #events fetched into buffer; or a negative value indicating a type
  // of error.  See error code constants for error types.
  int64_t get_events(int sid, int64_t first_seq, int64_t max_events,
      uint8_t *buffer);

  // Error codes.
  static const int64_t kErrFuture = -1;  // Asked for a future seq#.
  static const int64_t kErrPast = -2;  // Asked for a seq# that is discarded.

  // Log level for this class.
  static const int kLogLevel = 8;

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
      CHECK_NOTNULL(chunk);
    }

    ~MemBuffer() {
      dfree(chunk);
    }
  };

  // Structure used to pass the context and thread id to a GC thread.
  struct GcHint {
    MultiTierStorage *ptr;
    int tid;
  };

  // Info about an on-going garbage collection on a stream.
  struct GcInfo {
    // The range of memory buffer in which events are used to garbage collect
    // older events.
    int64_t begin_hash_offset, end_hash_offset;
    // The range of memory buffer currently being garbage collected.
    int64_t begin_gc_offset, end_gc_offset;
    // Hash table of GC events.
    std::unordered_set<int64_t> table;
  };

  // Return the number of bytes in circular buffer range [begin, end).
  int64_t distance(int64_t begin, int64_t end);

  // Returns the amount of free space available in a MemBuffer, in bytes.
  int64_t get_used_space(const MemBuffer &membuf);

  // Returns the amount of free space available in a MemBuffer, in bytes.
  // This is always mem_buf_size_ - get_used_space().
  int64_t get_free_space(const MemBuffer &membuf);

  // Returns offset of the next/previous event in a MemBuffer.
  int64_t next_offset(int64_t offset);
  int64_t prev_offset(int64_t offset);

  // Returns if an offset falls into the region of valid events,
  // i.e. [begin_offset, end_offset), modulo mem_buf_size_.
  bool is_valid_offset(const MemBuffer &membuf, int64_t offset);

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
  std::string get_chunk_name(int sid, int64_t chunk_id);

  // Initialize data structures for garbage collection.
  void init_gc();

  // Entry point for a GC thread.  Every newly created GC thread starts from
  // this static function, gets its context, and do work in run_gc.
  // This is a workaround of std::thread, in which std::bind could be used and
  // this function is no longer needed.  However, as of Sep 2014, std::thread
  // is still not working under g++ 4.8.x.
  //
  // Args:
  //  arg: a pointer to a GcHint struct.
  static void *start_gc(void *arg);

  // This is where garbage collection work is done.
  void run_gc(int thread_id);

  // #streams.
  int nstreams_;
  // Mapping from stream id to array index for permanent storage.
  std::unordered_map<int, int> stream_index_;

  // Size of in-memory buffer for each stream in bytes.
  static int64_t mem_buf_size_;
  // In-memory buffer for all streams.
  std::vector<MemBuffer> mem_store_;
  // Mutex for each stream.
  std::vector<pthread_mutex_t> mutex_;

  // Maximum #events in the hash table used to match previous events.
  int64_t max_gc_table_size_;
  // Maximum buffer size to be GCed before pausing, in bytes.
  int64_t max_gc_chunk_size_;
  // Min #events in a stream to trigger GC.
  int64_t min_events_to_gc_;
  // #GC threads.
  int gc_thread_count_;
  // Pointers to garbage collection threads.
  std::vector<pthread_t> gc_threads_;
  // Context hints for each thread.
  std::vector<GcHint> gc_hints_;
  // Space saved by GC for each stream, in bytes.
  std::vector<int64_t> bytes_saved_;

  // Size of an on-disk data chunk in bytes.
  static int64_t disk_chunk_size_;
  // Next chunk# to assign for each stream stored on disk.
  std::vector<int64_t> next_chunk_no_;
  // Filenames for streams stored on this node.
  std::vector< std::deque<int64_t> > used_chunk_no_;
};

#endif  // MULTI_TIER_STORAGE_H_
