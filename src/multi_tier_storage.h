#ifndef MULTI_TIER_STORAGE_H_
#define MULTI_TIER_STORAGE_H_

#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>

#include <deque>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>

#include "dmalloc.h"

// Information about a chunk on disk.
struct ChunkInfo {
  std::string filename;
  int64_t begin_seq;
  int64_t end_seq;
  pthread_mutex_t *mutex_ptr;

  ChunkInfo(std::string filename, int64_t begin_seq, int64_t end_seq)
    : filename(filename), begin_seq(begin_seq), end_seq(end_seq) {
    mutex_ptr =
        static_cast<pthread_mutex_t *>(dcalloc(sizeof(pthread_mutex_t), 1));
    int rc = pthread_mutex_init(mutex_ptr, NULL);
    if (rc) {
      LOG(ERROR) << "pthread_mutex_init failed with rc " << rc << ".";
    }
  }
};

// Stores events received by a Sprinkler node.  An in-memory buffer stores most
// recent events from every stream registered with the system, and an on-disk
// permanent storage unit keeps all the history for streams that should be kept
// at this node.
class MultiTierStorage {
 public:
  MultiTierStorage(int nproxies, int nstreams,
      int64_t mem_buf_size, int64_t disk_chunk_size, int64_t pub_delay,
      int gc_thread_count, int64_t min_gc_pass, int64_t max_gc_pass,
      int64_t max_gc_table_size, int64_t max_gc_chunk_size,
      int gc_disk_thread_count, int fragments)
    : nstreams_(nstreams), mem_mutex_(nstreams), pub_delay_(pub_delay),
      next_chunk_no_(nstreams, 0), chunk_summary_(nstreams),
      max_gc_table_size_(max_gc_table_size),
      min_gc_pass_(min_gc_pass), max_gc_pass_(max_gc_pass),
      max_gc_chunk_size_(max_gc_chunk_size),
      gc_thread_count_(gc_thread_count),
      gc_threads_(gc_thread_count),
      gc_hints_(gc_thread_count + gc_disk_thread_count + 1),
      gc_disk_thread_count_(gc_disk_thread_count), meta_mutex_(nstreams),
      bytes_saved_disk_(nstreams, 0), fragments_(fragments) {
    // Set buffer/chunk sizes here since they are static.
    mem_buf_size_ = mem_buf_size;
    disk_chunk_size_ = disk_chunk_size;
    LOG(INFO) << "mem_buf_size_ = " << mem_buf_size_;
    LOG(INFO) << "disk_chunk_size_ = " << disk_chunk_size_;

    // This goes here since it needs mem_buf_size_.
    mem_store_ = std::vector<MemBuffer>(nstreams);

    // Initialize mutexes.
    for (int i = 0; i < nstreams; ++i) {
      int rc = pthread_mutex_init(&mem_mutex_[i], NULL);
      if (rc) {
        LOG(ERROR) << "pthread_mutex_init failed with rc " << rc << ".";
      }

      rc = pthread_mutex_init(&meta_mutex_[i], NULL);
      if (rc) {
        LOG(ERROR) << "pthread_mutex_init failed with rc " << rc << ".";
      }
    }

    // Init publish buffers.
    publish_buffer_ = std::vector< std::vector<PublishBuffer> >(nstreams);
    for (int i = 0; i < nproxies; ++i) {
      publish_buffer_[i] = std::vector<PublishBuffer>(nstreams);
    }

    // Init last_pub_timestamp_.
    last_pub_timestamp_ = std::vector< std::vector<int64_t> >(nstreams);
    for (int i = 0; i < nproxies; ++i) {
      last_pub_timestamp_[i] = std::vector<int64_t>(nstreams);
    }

    // Initialize garbage collection.
    init_gc();
  }

  // Add a block of raw events from a client.
  // "data" here contains only messages, no header is included.
  // Returns the smallest seq# that has not been assigned yet.
  int64_t put_raw_events(int sid, int64_t now,
      int64_t nevents, const uint8_t *data);

  // Add a block of formatted events from a peer proxy.
  // "data" here contains only messages, no header is included.
  // Returns the smallest seq# that has not been received yet.
  int64_t put_events(int sid, int64_t nevents, uint8_t *data);

  // Retrieve events from a stream.
  // Return #events fetched into buffer; or a negative value indicating a type
  // of error.  See error code constants for error types.
  int64_t get_events(int pid, int sid, int64_t now,
      int64_t first_seq, int64_t max_events, uint8_t *buffer);

  // Report the state of all streams/a stream: total events added, bytes wrote,
  // space saved by GC, and current used buffer range.
  void report_state();
  void report_state(int sid);

  // Grab all the mutex locks so that no GC thread could proceed.
  // This is called before a sprinkler node terminates itself.
  void grab_all_locks();

  // Error codes.
  static const int64_t kErrFuture = -1;  // Asked for a future seq#.
  static const int64_t kErrPast = -2;  // Asked for a seq# that is discarded.
  static const int64_t kErrDelay = -3;  // Minimum delay to publish not reached.

  // Log level for this class.
  static const int kLogLevel = 8;

 private:
  // In-memory buffer for a stream.
  struct MemBuffer {
    int64_t begin_seq, end_seq;
    int64_t begin_offset, end_offset;
    int64_t gc_begin_offset;
    int64_t gc_table_begin_offset, gc_table_end_offset;
    bool is_empty;
    uint8_t *chunk;
    // Amount of data wrote to this stream, in bytes.
    int64_t bytes_wrote;
    // Space saved by GC for this stream, in bytes.
    int64_t bytes_saved;

    MemBuffer() {
      begin_seq = end_seq = 1;
      begin_offset = end_offset = 0;
      gc_begin_offset = -1;
      gc_table_begin_offset = gc_table_end_offset = -1;
      is_empty = true;
      chunk = static_cast<uint8_t *>(dcalloc(mem_buf_size_, 1));
      bytes_wrote = bytes_saved = 0;
      CHECK_NOTNULL(chunk);
    }

    ~MemBuffer() {
      dfree(chunk);
    }
  };

  // Buffers events to be send in a single, continuous stream.
  struct PublishBuffer {
    int64_t offset, remainder;
    uint8_t *buffer;

    PublishBuffer() {
      offset = remainder = 0;
      LOG(INFO) << "disk_chunk_size_: " << disk_chunk_size_;
      buffer = static_cast<uint8_t *>(dcalloc(disk_chunk_size_, 1));
    }

    ~PublishBuffer() {
      dfree(buffer);
    }
  };

  // Structure used to pass the context and thread id to a GC thread.
  struct GcHint {
    MultiTierStorage *ptr;
    int tid;
    bool on_disk;
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

  // Returns if an offset falls in region [left, right) in a circular buffer.
  bool in_between(int64_t left, int64_t right, int64_t offset);

  // Returns the offset in an array of events such that seq fits into the
  // event at that offset, or -1 in case no event fits.
  int64_t adjust_offset_linear(int64_t seq, int64_t nevents,
      const uint8_t *chunk);

  // Returns the offset in a circular array (data chunk in a MemBuffer struct)
  // such that seq fits into the event at that offset, or -1 in case no event
  // fits.
  int64_t adjust_offset_circular(int64_t seq,
      int64_t begin, int64_t end, const uint8_t *chunk);

  // Returns the id of on-disk chunk that contains seq in stream sid.
  int get_chunk_id_by_seq(int sid, int64_t seq);

  // Flush a chunk of in-memory buffer to disk.
  void flush_to_disk(int sid);

  // Generate the name of next data chunk to be stored on disk.
  std::string get_chunk_name(int sid, int64_t chunk_id);

  // Initialize data structures for garbage collection.
  void init_gc();

  // Entry point for a GC thread.  Every newly created GC thread starts from
  // this static function, gets its context, and starts the main GC loop in
  // either run_gc_in_memory or run_gc_on_disk.
  // This is a workaround of std::thread, in which std::bind could be used and
  // this function is no longer needed.  However, as of Sep 2014, std::thread
  // is still not working under g++ 4.8.x.
  //
  // Args:
  //  arg: a pointer to a GcHint struct.
  static void *start_gc(void *arg);

  // Main loop for in-memory garbage collection.
  void run_gc_in_memory(int thread_id);

  // Main loop for on-disk garbage collection.
  void run_gc_on_disk(int thread_id);

  // Merge adjacent tombstone events within a disk chunk.
  // Returns the new chunk size after merging.
  int64_t merge_tombstones(uint8_t *chunk, int64_t size);

  // Entry point for erasure encoder.
  void run_erasure_encoder();

  // #streams.
  int nstreams_;
  // Mapping from stream id to array index for permanent storage.
  std::unordered_map<int, int> stream_index_;

  // Size of in-memory buffer for each stream in bytes.
  static int64_t mem_buf_size_;
  // In-memory buffer for all streams.
  std::vector<MemBuffer> mem_store_;
  // Mutex for each stream.
  std::vector<pthread_mutex_t> mem_mutex_;
  // Buffer to batch publishing on a continuous stream.
  std::vector< std::vector<PublishBuffer> > publish_buffer_;
  // Timestamp of the addition of the most recently published event on a
  // subscription, in milliseconds.
  std::vector< std::vector<int64_t> > last_pub_timestamp_;
  // Artificial delay before publishing an event in milliseconds, for the
  // purpose that the event could get more chance to be garbage collected
  // during the delay.
  int64_t pub_delay_;

  // Maximum #events in the hash table used to match previous events.
  int64_t max_gc_table_size_;
  // Min bytes in a stream to trigger GC.
  int64_t min_gc_pass_;
  // Maximum bytes to be GCed per pass.
  int64_t max_gc_pass_;
  // Maximum buffer size to be GCed before pausing, in bytes.
  int64_t max_gc_chunk_size_;
  // #GC threads.
  int gc_thread_count_;
  // Pointers to garbage collection threads.
  std::vector<pthread_t> gc_threads_;
  // Context hints for each thread.
  std::vector<GcHint> gc_hints_;

  // Size of an on-disk data chunk in bytes.
  static int64_t disk_chunk_size_;
  // Next chunk# to assign for each stream stored on disk.
  std::vector<int64_t> next_chunk_no_;
  // Filenames and start/end seq# for streams stored on this node.
  std::vector< std::deque<ChunkInfo*> > chunk_summary_;
  // Mutex for chunk metadata of each stream.
  std::vector<pthread_mutex_t> meta_mutex_;
  // #GC threads on disk.
  int64_t gc_disk_thread_count_;
  // Bytes saved by on-disk GC.
  std::vector<int64_t> bytes_saved_disk_;

  // Number of fragments per chunk; 0 means not using erasure code.
  int fragments_;
  // Semaphore that indicates how many chunks are waiting to be encoded.
  sem_t *chunk_sem_;
  // Next chunk to be erasure coded.
  int64_t ec_watermark_;
};

#endif  // MULTI_TIER_STORAGE_H_
