#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <grp.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <mach/mach_time.h>
#include <sys/event.h>
#include <sys/mman.h>
#include <os/lock.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <unistd.h>
#include <vmnet/vmnet.h>

#include "cli.h"
#include "log.h"

#if __MAC_OS_X_VERSION_MAX_ALLOWED < 101500
#error "Requires macOS 10.15 or later"
#endif

#define ARRAY_SIZE(a) (sizeof(a) / sizeof(a[0]))

// Maximum packets drained from vmnet.framework in one vmnet_read() call, and
// so also the most that _on_vmnet_packets_available() can publish as one
// batch. Defined here rather than next to its other user further down because
// that publish path needs it to size its batch arrays.
#define MAX_PACKET_COUNT_AT_ONCE 32

// Maximum packets on_accept() will batch into a single vmnet_write() call
// (and, symmetrically, a single writev() per destination conn when
// flooding that batch to other local clients).
#define WRITE_BATCH_MAX 32

// Default SO_SNDBUF/SO_RCVBUF applied to every accepted client connection.
// Measured to matter specifically once writes are batched below: batching
// increases how much data moves per write, and Darwin's default
// unix-domain-socket buffer sizes become the limiting factor once bursts
// get that much bigger -- tuned alone, without batching, this made no
// measurable difference. --sockbuf-size=N overrides; --sockbuf-size=0
// leaves the OS default untouched.
#define DEFAULT_SOCKBUF_SIZE (1024 * 1024)

// Default for --delivery-batch-size: how much one delivery thread coalesces
// into a single write to one target. Kept at the previous effective value so
// splitting it out of --sockbuf-size changes nothing by default; it exists so
// syscall amortization and head-of-line blocking can be swept independently.
#define DEFAULT_DELIVERY_BATCH_SIZE (1024 * 1024)


// Maximum length of a single frame, matching what the wire protocol's
// 4-byte length prefix is expected to carry in practice.
#define MAX_FRAME_SIZE 65536

// Each read() call in on_accept() is capped to this many new bytes,
// regardless of how much buffer space remains -- ingestion must stay
// bounded relative to the WRITE_BATCH_MAX-frame drain per cycle, or
// leftover can grow without bound (an earlier version of this let a read()
// request scale with whatever buffer space happened to remain, which could
// shrink the request to 0 bytes under sustained load; a 0-byte read()
// request returns 0 immediately, which is indistinguishable from EOF and
// tore the connection down). A bounded chunk size also caps how much
// latency one gathering cycle can add before a batch gets dispatched at
// all, instead of accumulating an unbounded amount before doing anything.
#define READ_CHUNK_SIZE MAX_FRAME_SIZE
// Scratch buffer: room for one full chunk of new data plus up to one
// max-size frame's worth of leftover from the previous cycle.
#define READ_BUF_SIZE (READ_CHUNK_SIZE + MAX_FRAME_SIZE)

// Per-delivery-thread private staging buffer: deliver_from_outbox() snapshots
// a bounded batch out of the (shared, concurrently-overwritten) source ring
// into this, then does its blocking write from here. Handing the kernel a
// pointer straight into the ring is NOT safe no matter how large the ring is:
// a write() can sit in the kernel for as long as the peer takes to drain
// while the producer keeps filling slots, and at multi-Gbit rates the
// producer laps the entire OUTBOX_RING_SLOTS ring in
// ~30-60ms. The kernel then pulls the tail of an already-overwritten slot into
// the middle of a frame it had already started sending -- the receiver doesn't
// lose a frame (which would be fine, that's the design), it gets half of one
// frame spliced onto half of another behind a valid-looking length prefix, and
// its framing desyncs permanently. Observed against a real Lima VM as
// "packet size 2845883679 exceeds read buffer size 32768".
// This does not change the loss semantics at all -- a target that falls behind
// still loses whole chunks at the ring's retention window, exactly as intended.
// It only makes the bytes already committed to one write immutable for the
// duration of that write.
#define DELIVERY_SCRATCH_SIZE (1024 * 1024)
_Static_assert(DELIVERY_SCRATCH_SIZE >= READ_BUF_SIZE,
               "one whole slot must always fit, or a large slot could never be delivered");

// --skip-vmnet-write is a diagnostic that deliberately breaks the daemon: with
// it, no frame reaches vmnet.framework, so DHCP, host access and external
// networking all stop working and only client-to-client traffic survives. It
// exists to size what the vmnet round trip costs. Build-time opt-in so a
// release build cannot be talked into it by a stray argument or signal.
#ifdef SOCKET_VMNET_DIAG
#define VMNET_WRITE_SUPPRESSED(state) atomic_load(&(state)->skip_vmnet_write)
#else
#define VMNET_WRITE_SUPPRESSED(state) (false)
#endif

// How many published batches back a slow target may fall behind a given
// source before it starts losing that source's oldest unread frames. This
// replaces the older design of writing directly into a peer's socket, which
// could block the whole daemon on one slow peer (lima-vm/socket_vmnet#173),
// with each conn publishing into its own bounded ring and every other conn
// pulling from it on its own thread,
// so one slow target can only ever fall behind and lose its own data, never
// block anyone else's delivery.
//
// Sized for ~10 Gbit/s sustained with ~20ms of absorption depth (enough to
// smooth over normal OS scheduling jitter between a source publishing and a
// healthy target's delivery thread getting CPU time, without turning into a
// multi-second latency buffer): 10 Gbit/s = 1.25 GB/s; 1.25 GB/s * 20ms =
// 25 MB; 25 MB / READ_BUF_SIZE(~128KB) =~190 slots, rounded up to 256. An
// earlier, much smaller value (16 slots =~1.2ms of absorption at this rate)
// measured ~90% loss between two perfectly healthy peers under sustained
// synthetic throughput -- not a zombie-peer problem, just not enough buffer
// to smooth over normal jitter once publishing stopped being throttled by a
// blocking write. 256 slots * READ_BUF_SIZE =~32 MB per conn's outbox
// (memory cost scales with conn count -- revisit if that stops being fine
// at realistic connection counts).
#define OUTBOX_RING_SLOTS 256

// Maximum number of simultaneously active client connections. IDs are reused
// after disconnect and index the per-target cursor arrays below.
#define MAX_CONNS 256

bool debug = false;

// One source's outbox: a small, bounded, single-writer/multi-reader ring of
// already-serialized batches ([4-byte BE length][frame], repeated, exactly
// the wire format already used for stream delivery). Publishing (one
// writer: this source's own read/batch loop, or the vmnet-packets-available
// handler for the synthetic vmnet source below) is a local memcpy + a
// sequence bump under a lock held only long enough for that copy -- it never
// touches a socket, so publishing can never block on any peer.
// _Alignas(64): without it, sizeof(struct outbox_slot) = 8 + READ_BUF_SIZE
// is not a multiple of the cache line size, so slots[0] could be aligned
// (if the array's own address is) while every later slot in the array
// drifts further off a cache-line boundary as the index grows -- _Alignas
// on the element type makes the compiler pad sizeof() up to a multiple of
// 64, so every element of an array of these is aligned, not just the
// first. Still needs the array's own base address aligned too -- see the
// posix_memalign calls at every allocation site (alloc_conn(),
// state.vmnet_outbox in main()).
struct outbox_slot {
  _Alignas(64) size_t byte_len; // 0 until first published
  uint8_t data[READ_BUF_SIZE];
};

struct outbox {
  // Serializes publisher-vs-publisher only. Readers (every other conn's
  // delivery thread) do NOT take this -- they use the seqlock protocol in
  // deliver_from_outbox() instead, so a reader can never block a publisher.
  os_unfair_lock lock;
  // Published with release ordering after the slot's data and byte_len are
  // in place; read with acquire ordering by the seqlock reader. Next publish
  // goes to slots[write_seq % OUTBOX_RING_SLOTS], then increments.
  _Atomic(uint64_t) write_seq;
  struct outbox_slot slots[OUTBOX_RING_SLOTS];
};
// outbox_slot's _Alignas(64) propagates automatically: a struct's own
// alignment requirement is the max of its members', with padding inserted
// as needed -- so slots[] forces struct outbox itself to be 64-aligned,
// which in turn forces the same on struct conn's embedded `outbox` field.
// No need to repeat _Alignas at every level; just prove it rather than
// trust it.
_Static_assert(_Alignof(struct outbox) == 64, "outbox_slot's _Alignas(64) should propagate here");

// Anonymous mmap rather than calloc/posix_memalign for the large, long-lived
// allocations (struct conn, which embeds a ~33 MB outbox, and the synthetic
// vmnet_outbox).
//
// Two reasons. Alignment: outbox_slot's _Alignas(64) only keeps each slot
// aligned *relative to the array's own start*, so the array's start has to
// land on a 64-byte boundary too -- malloc makes no such promise, while mmap
// returns page-aligned memory. And cost: calloc/memset would touch all ~33 MB
// up front, making every VM connect pay for faulting in and residency of a
// ring that is mostly never used at once. mmap hands back demand-zero pages,
// so slots materialize only as the ring actually wraps into them, which cuts
// both connect latency and idle RSS.
//
// Only used at connection setup and startup, never on a hot path.
static void *alloc_pages_zeroed(size_t size) {
  void *p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
  if (p == MAP_FAILED) {
    return NULL;
  }
  return p;
}

static void free_pages(void *p, size_t size) {
  if (p != NULL && munmap(p, size) != 0) {
    ERRORN("munmap");
  }
}

// Spin hint for the busy-poll loop. Deliberately NOT sched_yield(): that
// re-enters the scheduler, which is exactly the cost the spin exists to
// avoid.
#if defined(__aarch64__)
#define CPU_RELAX() __asm__ __volatile__("yield" ::: "memory")
#elif defined(__x86_64__)
#define CPU_RELAX() __asm__ __volatile__("pause" ::: "memory")
#else
#define CPU_RELAX() ((void)0)
#endif

// mach_absolute_time() ticks per microsecond, filled in once at startup.
// mach_absolute_time() is a register read on arm64, which matters when it is
// polled inside a spin loop; clock_gettime() would be far heavier.
static uint64_t g_ticks_per_usec = 1;

static void init_ticks_per_usec(void) {
  mach_timebase_info_data_t tb;
  if (mach_timebase_info(&tb) != KERN_SUCCESS || tb.numer == 0) {
    g_ticks_per_usec = 1;
    return;
  }
  // ns = ticks * numer / denom  =>  ticks per us = 1000 * denom / numer
  uint64_t t = (uint64_t)1000 * tb.denom / tb.numer;
  g_ticks_per_usec = t > 0 ? t : 1;
}

static const char *vmnet_strerror(vmnet_return_t v) {
  switch (v) {
  case VMNET_SUCCESS:
    return "VMNET_SUCCESS";
  case VMNET_FAILURE:
    return "VMNET_FAILURE";
  case VMNET_MEM_FAILURE:
    return "VMNET_MEM_FAILURE";
  case VMNET_INVALID_ARGUMENT:
    return "VMNET_INVALID_ARGUMENT";
  case VMNET_SETUP_INCOMPLETE:
    return "VMNET_SETUP_INCOMPLETE";
  case VMNET_INVALID_ACCESS:
    return "VMNET_INVALID_ACCESS";
  case VMNET_PACKET_TOO_BIG:
    return "VMNET_PACKET_TOO_BIG";
  case VMNET_BUFFER_EXHAUSTED:
    return "VMNET_BUFFER_EXHAUSTED";
  case VMNET_TOO_MANY_PACKETS:
    return "VMNET_TOO_MANY_PACKETS";
  default:
    return "(unknown status)";
  }
}

static void print_vmnet_start_param(xpc_object_t param) {
  if (param == NULL)
    return;
  xpc_dictionary_apply(param, ^bool(const char *key, xpc_object_t value) {
    xpc_type_t t = xpc_get_type(value);
    if (t == XPC_TYPE_UINT64)
      INFOF("* %s: %lld", key, xpc_uint64_get_value(value));
    else if (t == XPC_TYPE_INT64)
      INFOF("* %s: %lld", key, xpc_int64_get_value(value));
    else if (t == XPC_TYPE_STRING)
      INFOF("* %s: %s", key, xpc_string_get_string_ptr(value));
    else if (t == XPC_TYPE_UUID) {
      char uuid_str[36 + 1];
      uuid_unparse(xpc_uuid_get_bytes(value), uuid_str);
      INFOF("* %s: %s", key, uuid_str);
    } else
      INFOF("* %s: (unknown type)", key);
    return true;
  });
}

struct conn {
  // TODO: uint8_t mac[6];
  int socket_fd;
  struct conn *next;

  struct state *state; // back-pointer, needed by this conn's own delivery thread
  int id;               // stable, assigned at creation; indexes other conns'
                         // target_read_seq[] when THEY read from THIS conn
  uint64_t generation;  // distinguishes different conns that reuse one id
  struct outbox outbox; // this conn as a SOURCE (published by its read/batch loop)

  // This conn as a TARGET: one dedicated thread pulls from every other
  // conn's outbox (plus the synthetic vmnet_outbox) and writes to this
  // conn's own socket -- see conn_delivery_thread(). Blocking here only
  // ever delays delivery *to this one conn*, never anyone else's.
  pthread_t delivery_thread;
  atomic_bool delivery_should_stop;
  _Atomic(uint32_t) ref_count;           // owner plus delivery-loop snapshots
  uint64_t target_read_seq[MAX_CONNS];   // cursor into conns_by_id[i]->outbox
  uint64_t target_source_generation[MAX_CONNS];
  uint64_t target_read_seq_vmnet;        // cursor into state->vmnet_outbox
  uint8_t *delivery_scratch;             // DELIVERY_SCRATCH_SIZE, see that constant

  // Busy-poll instrumentation, per delivery thread. Plain (non-atomic) because
  // this conn's own delivery thread is the only writer; they are folded into
  // the process-wide totals once, when that thread exits. Keeping them out of
  // shared cache lines matters: the measurement apparatus must not itself
  // become a permanent contention point on the hot path just because an
  // experiment that has since been settled once needed the numbers.
  // Relaxed atomics rather than plain fields: still single-writer (this conn's
  // own delivery thread), so no contention -- each lives on that conn's own
  // cache line -- but SIGUSR2 reads them live from the main loop, and that
  // needs to be defined rather than merely working in practice.
  _Atomic(uint64_t) stat_spin_attempts;
  _Atomic(uint64_t) stat_spin_hits;
  _Atomic(uint64_t) stat_spin_ticks;
  _Atomic(uint64_t) stat_park_entries; // park protocol entered (always takes publish_mutex)
  _Atomic(uint64_t) stat_park_waits;   // subset that actually blocked on the condvar
  _Atomic(uint64_t) stat_slots_delivered;
  _Atomic(uint64_t) stat_bytes_delivered;
  _Atomic(uint64_t) stat_slots_dropped; // slots skipped because this target fell outside the window
  // Ingress side (written by this conn's own read loop).
  _Atomic(uint64_t) stat_slots_published;
  _Atomic(uint64_t) stat_bytes_published;
} _conn;
_Static_assert(offsetof(struct conn, outbox) % 64 == 0,
               "embedded outbox should land 64-aligned within struct conn too");

struct state {
  os_unfair_lock sem;
  dispatch_queue_t vms_queue;
  dispatch_queue_t host_queue;
  struct conn *conns; // TODO: avoid O(N) lookup

  // SO_SNDBUF/SO_RCVBUF applied to every accepted client connection; from
  // --sockbuf-size (default DEFAULT_SOCKBUF_SIZE), 0 to leave the OS default.
  int sockbuf_size;

  // Max bytes a delivery thread coalesces into one write to a target; from
  // --delivery-batch-size (default DEFAULT_DELIVERY_BATCH_SIZE). Always > 0.
  int delivery_batch_size;

  // Diagnostic: suppress every vmnet_write(). See --skip-vmnet-write.
#ifdef SOCKET_VMNET_DIAG
  // Atomic because SIGUSR1 flips it at runtime from the main loop while
  // connection threads are reading it -- that toggle is what makes an honest
  // A/B possible: same daemon, same VM boot, same everything, only this bit
  // different. Comparing across daemon restarts drags in whole-boot
  // differences that are easily larger than the effect being measured.
  _Atomic(bool) skip_vmnet_write;
#endif

  // --busy-poll=USEC: how long a delivery thread with nothing to do spins
  // watching publish_generation before falling back to parking on the
  // condvar. Named after Linux's net.core.busy_poll, which is the same idea
  // (spin briefly for new packets rather than block immediately). 0 disables
  // it, which is the historical behaviour.
  int busy_poll_usec;

  // Busy-poll instrumentation. Without these the experiment can look like a
  // win purely by moving the same cost out of __psynch_* rows and into a
  // spin loop, which a stack profile would happily reward. What matters is
  // the hit rate and I/O gained per CPU-second, so count both sides.
  // Process-wide totals, accumulated from each delivery thread as it exits.
  // stat_broadcasts is the exception and stays hot, incremented per broadcast:
  // it sits immediately before the publish_mutex acquisition it is counting,
  // so it shares a cache line's worth of cost with an operation that is
  // already far heavier, and separating it would buy nothing.
  _Atomic(uint64_t) stat_spin_attempts;
  _Atomic(uint64_t) stat_spin_hits;
  _Atomic(uint64_t) stat_spin_ticks;
  _Atomic(uint64_t) stat_park_entries;
  _Atomic(uint64_t) stat_park_waits;
  _Atomic(uint64_t) stat_broadcasts;

  // Synthetic source representing frames arriving from vmnet.framework
  // (vmnet->clients direction), published by _on_vmnet_packets_available()
  // instead of that function writing directly into every conn's socket --
  // same rationale as every other conn's own outbox. A pointer, heap
  // allocated in main() -- struct state itself is a stack local there, and
  // struct outbox is now tens of MB (see OUTBOX_RING_SLOTS); embedding it by
  // value would blow the stack the moment `struct state state = {0};` runs.
  struct outbox *vmnet_outbox;

  // Active IDs are reused after a stream connection closes. Besides bounding
  // cursor indexing, this prevents ordinary VM reconnects from exhausting a
  // process-lifetime counter.
  struct conn *conns_by_id[MAX_CONNS];
  uint64_t next_conn_generation; // protected by sem; zero means "no source seen"

  // Wakes every idle delivery thread the moment anything is published,
  // instead of each polling on a fixed usleep() interval -- measured via
  // `sample` that a 1ms poll interval cost delivery threads ~20% of their
  // time asleep even under sustained load.
  //
  // publish_generation is bumped on every publish; a delivery thread
  // compares it against the value it last saw to decide whether to wait at
  // all, so a publish landing between a thread's last scan and its wait
  // call is never missed.
  //
  // publish_waiters is what keeps that cheap. Taking publish_mutex and
  // broadcasting on *every* publish cost more CPU than the daemon's actual
  // I/O did: at multi-Gbit rates each source publishes ~9k times/sec, and a
  // `sample` profile showed __psynch_mutexwait + __psynch_cvbroad +
  // __psynch_mutexdrop together burning over half as many samples as
  // write()+sendmsg_x combined. So the mutex and the broadcast are now only
  // touched when a delivery thread is actually parked; while they are busy
  // (the common case under load) a publish is a single atomic increment and
  // the wakeups coalesce on their own.
  //
  // Ordering matters and is deliberate: a waiter increments publish_waiters
  // *before* re-reading publish_generation, and a publisher bumps
  // publish_generation *before* reading publish_waiters, both seq_cst. Any
  // interleaving therefore has either the waiter observing the new
  // generation (and not waiting) or the publisher observing the waiter (and
  // broadcasting). The bounded timedwait below is a backstop regardless.
  pthread_mutex_t publish_mutex;
  pthread_cond_t publish_cond;
  _Atomic(uint64_t) publish_generation;
  _Atomic(int) publish_waiters;
} _state;

static void *conn_delivery_thread(void *arg);

static void conn_retain(struct conn *conn) {
  atomic_fetch_add_explicit(&conn->ref_count, 1, memory_order_relaxed);
}

static void conn_release(struct conn *conn) {
  if (atomic_fetch_sub_explicit(&conn->ref_count, 1, memory_order_acq_rel) == 1) {
    free_pages(conn->delivery_scratch, DELIVERY_SCRATCH_SIZE);
    free_pages(conn, sizeof(*conn));
  }
}

// Registers conn in the conns list, assigns its stable id (see struct
// conn's id field), and spawns its delivery thread -- from this point on,
// other conns' delivery threads may start pulling from conn->outbox, and
// conn's own delivery thread starts pulling from everyone else's.
static bool state_add_conn(struct state *state, struct conn *conn) {
  atomic_init(&conn->ref_count, 1);
  conn->state = state;
  conn->delivery_scratch = alloc_pages_zeroed(DELIVERY_SCRATCH_SIZE);
  if (conn->delivery_scratch == NULL) {
    ERRORN("mmap(delivery_scratch)");
    return false;
  }

  // Start this conn's read cursors at every existing source's *current*
  // write_seq, not at 0. A conn that has just joined has no business
  // receiving the backlog still retained in everyone else's rings -- and
  // more than "no business": those cursors would immediately be clamped to
  // the retention window and the new conn would be blasted with
  // OUTBOX_RING_SLOTS slots (~16 MB) of other VMs' traffic at once, at the
  // exact moment it is least able to drain anything (a VM that has just
  // booted and hasn't brought its network up yet). Observed as a
  // just-started third VM immediately getting disconnected. Done before
  // the conn is linked into state->conns and before its delivery thread
  // starts, so nothing is delivering to or from it yet.
  conn->target_read_seq_vmnet =
      atomic_load_explicit(&state->vmnet_outbox->write_seq, memory_order_acquire);

  os_unfair_lock_lock(&state->sem);
  conn->generation = ++state->next_conn_generation;
  assert(conn->generation != 0); // a 64-bit process-lifetime counter must not wrap
  conn->id = -1;
  for (int id = 0; id < MAX_CONNS; id++) {
    if (state->conns_by_id[id] == NULL) {
      conn->id = id;
      state->conns_by_id[id] = conn;
      break;
    }
  }
  if (conn->id < 0) {
    ERRORF("MAX_CONNS (%d) active connections exceeded -- rejecting connection", MAX_CONNS);
    os_unfair_lock_unlock(&state->sem);
    free_pages(conn->delivery_scratch, DELIVERY_SCRATCH_SIZE);
    conn->delivery_scratch = NULL;
    return false;
  }
  for (struct conn *src = state->conns; src != NULL; src = src->next) {
    if (src->id < 0) {
      continue;
    }
    conn->target_read_seq[src->id] =
        atomic_load_explicit(&src->outbox.write_seq, memory_order_acquire);
    conn->target_source_generation[src->id] = src->generation;
  }

  int thread_error = pthread_create(&conn->delivery_thread, NULL, conn_delivery_thread, conn);
  if (thread_error != 0) {
    errno = thread_error; // pthread APIs return the error number; they do not set errno
    ERRORN("pthread_create(delivery_thread)");
    state->conns_by_id[conn->id] = NULL;
    os_unfair_lock_unlock(&state->sem);
    free_pages(conn->delivery_scratch, DELIVERY_SCRATCH_SIZE);
    conn->delivery_scratch = NULL;
    return false;
  }

  if (state->conns == NULL) {
    state->conns = conn;
  } else {
    struct conn *last;
    for (last = state->conns; last->next != NULL; last = last->next)
      ;
    last->next = conn;
  }
  os_unfair_lock_unlock(&state->sem);
  return true;
}

static struct conn *state_add_socket_fd(struct state *state, int socket_fd) {
  struct conn *conn = alloc_pages_zeroed(sizeof(*conn));
  if (conn == NULL) {
    ERRORN("mmap(conn)");
    return NULL;
  }
  conn->socket_fd = socket_fd;
  if (!state_add_conn(state, conn)) {
    free_pages(conn, sizeof(*conn));
    return NULL;
  }
  return conn;
}

// Wakes every *parked* delivery thread -- see the publish_mutex/publish_cond
// comment on struct state for why the waiter check is what makes this cheap,
// and for the ordering requirement between the two atomics here.
static void signal_publish(struct state *state) {
  atomic_fetch_add(&state->publish_generation, 1);
  if (atomic_load(&state->publish_waiters) == 0) {
    return; // nobody parked -- they'll see the new generation on their own
  }
  atomic_fetch_add_explicit(&state->stat_broadcasts, 1, memory_order_relaxed);
  pthread_mutex_lock(&state->publish_mutex);
  pthread_cond_broadcast(&state->publish_cond);
  pthread_mutex_unlock(&state->publish_mutex);
}

// Serializes `count` frames (headers_be[i] + iov[i] each) into the outbox's
// next slot as [4-byte BE length][frame]... back to back -- the same wire
// format stream delivery already needs -- and publishes it (bumps
// write_seq). This is the *only* thing that ever writes to an outbox; it's
// a local copy under a lock held only for the copy itself, never touching a
// socket, so publishing can never block on any peer. Every batch passed here
// // is already bounded to fit in one slot by its caller's own read/parse
// bounds -- see READ_BUF_SIZE and its callers -- the overflow guard below is
// defensive, not expected to ever trigger.
static void outbox_publish(struct state *state, struct outbox *outbox, uint32_t *headers_be,
                            struct iovec *iov, int count) {
  os_unfair_lock_lock(&outbox->lock);
  uint64_t seq = atomic_load_explicit(&outbox->write_seq, memory_order_relaxed);
  struct outbox_slot *slot = &outbox->slots[seq % OUTBOX_RING_SLOTS];
  size_t off = 0;
  for (int i = 0; i < count; i++) {
    size_t frame_len = iov[i].iov_len;
    if (off + 4 + frame_len > sizeof(slot->data)) {
      ERRORF("outbox_publish: batch too large for one slot (off=%zu, frame_len=%zu, cap=%zu) -- "
             "truncating batch",
             off, frame_len, sizeof(slot->data));
      break;
    }
    memcpy(slot->data + off, &headers_be[i], 4);
    memcpy(slot->data + off + 4, iov[i].iov_base, frame_len);
    off += 4 + frame_len;
  }
  slot->byte_len = off;
  // Release: everything written above must be visible to a reader that
  // observes this new sequence number.
  atomic_store_explicit(&outbox->write_seq, seq + 1, memory_order_release);
  os_unfair_lock_unlock(&outbox->lock);
  signal_publish(state);
}

// Pulls at most one cork-sized run of newly published slots from `src_outbox`
// (tracked via `*cursor`, this target's own read position into that specific
// source) and delivers it to `self`'s own socket. Bounding one visit prevents
// a continuously publishing source from monopolizing the target thread; the
// outer delivery loop returns to every other source before visiting this one
// again. Still a blocking write/sendto, but this is the
// *only* place it can block: a stuck `self` only ever delays further delivery
// *to itself*, never touches any other conn's outbox or any other target's
// delivery thread.
//
// Works in two strictly separated phases per batch:
//   1. snapshot -- copy a bounded run of published slots into self's own
//      private delivery_scratch, taking NO lock at all. Correctness comes
//      from a seqlock-style validation instead (see below), so a reader can
//      never block a publisher -- holding the outbox lock across this memcpy
//      measurably did, showing up as __ulock_wait2 in a `sample` profile.
//   2. write -- from delivery_scratch. The bytes being written are private
//      to this thread, so the producer lapping the ring mid-write can no
//      longer splice new data into the middle of a frame already being sent.
//      See DELIVERY_SCRATCH_SIZE for why that is not a theoretical concern.
//
// The seqlock argument, which is what makes phase 1 safe without a lock:
// read write_seq before the copy (W1) and again after it (W2). The producer
// can only have been writing slots in [W1, W2] during the copy, while the
// copy only touched slots in [cursor, W1). All of [cursor, W2] are distinct
// ring indices as long as W2 - cursor <= OUTBOX_RING_SLOTS - 1, so under that
// condition no slot we copied can have been the one being overwritten, and
// the snapshot is clean. If the condition fails we were lapped mid-copy, the
// bytes may be torn, and we throw the whole snapshot away and retry -- which
// is the same outcome as data aging out of the retention window, just
// detected later.
// Falling behind still loses whole chunks at the ring's retention window --
// that part is unchanged and intended.
static bool deliver_from_outbox(struct conn *self, struct outbox *src_outbox, uint64_t *cursor) {
  // How much to coalesce into one write. Its own knob (--delivery-batch-size),
  // not --sockbuf-size: the kernel buffer size and the userspace batch size
  // govern different things (what the kernel will hold, versus syscall
  // amortization traded against head-of-line blocking), and sharing one flag
  // made both untunable.
  size_t cork_cap = (size_t)self->state->delivery_batch_size;
  if (cork_cap > DELIVERY_SCRATCH_SIZE) {
    cork_cap = DELIVERY_SCRATCH_SIZE; // the staging buffer is the hard ceiling
  }
  // The producer is filling slots[write_seq % OUTBOX_RING_SLOTS] right now
  // (on_accept() read()s straight into it and only publishes afterwards), so
  // the oldest slot safe to read is write_seq - OUTBOX_RING_SLOTS + 1, not
  // write_seq - OUTBOX_RING_SLOTS: the latter is the same ring index as the
  // slot being written, whose byte_len still holds a stale-but-plausible
  // value from the previous lap while its data is already being overwritten.
  const uint64_t max_behind = OUTBOX_RING_SLOTS - 1;
  // Where to land when catching up after falling behind -- deliberately not
  // max_behind itself; see the comment at the catch-up site below.
  const uint64_t catchup_target = max_behind * 3 / 4;

  bool delivered_anything = false;
  int lapped_retries = 0;

  for (;;) {
    // ---- phase 1: lock-free snapshot, validated afterwards ----
    uint64_t w1 = atomic_load_explicit(&src_outbox->write_seq, memory_order_acquire);
    if (*cursor >= w1) {
      break; // nothing new
    }
    if (w1 - *cursor > max_behind) {
      // Catch up to `catchup_target` behind, not all the way out to
      // max_behind: landing exactly on the edge of the window means the
      // validation below fails on *any* single publish that happens during
      // the copy, so a congested target would burn several full memcpys in
      // a row, discard all of them and deliver nothing. The headroom left
      // here is enormous compared to what a producer can actually publish
      // during one snapshot (measured: well under one slot), so this costs
      // a little extra retention and buys forward progress under exactly
      // the congestion this design exists to handle.
      uint64_t dropped = w1 - catchup_target - *cursor;
      DEBUGF("delivery to fd %d fell behind by %llu batch(es), dropping to catch up", self->socket_fd,
             (unsigned long long)dropped);
      atomic_fetch_add_explicit(&self->stat_slots_dropped, dropped, memory_order_relaxed);
      *cursor = w1 - catchup_target;
    }
    uint64_t start = *cursor;
    size_t copied = 0;
    uint64_t slots = 0;
    bool torn = false;
    while (start + slots < w1) {
      struct outbox_slot *slot = &src_outbox->slots[(start + slots) % OUTBOX_RING_SLOTS];
      size_t len = slot->byte_len;
      // byte_len itself is read without synchronization, so a slot being
      // concurrently overwritten can yield a nonsense length. Bound it
      // before it reaches memcpy -- the validation below decides whether to
      // keep any of this, but the copy itself must stay in-bounds no matter
      // what was read.
      if (len > READ_BUF_SIZE) {
        torn = true;
        break;
      }
      if (len > 0) {
        if (copied + len > cork_cap && copied > 0) {
          break; // adding this slot would exceed the cork cap -- flush what we have
        }
        if (copied + len > DELIVERY_SCRATCH_SIZE) {
          break; // cannot fit (only reachable if copied > 0; one slot always fits)
        }
        memcpy(self->delivery_scratch + copied, slot->data, len);
        copied += len;
      }
      slots++;
    }

    // ---- validate: were we lapped while copying? ----
    // Full fence, not just an acquire load: the whole validation rests on
    // every read done by the memcpy loop above having completed *before*
    // this second look at write_seq. memory_order_acquire only constrains
    // accesses that follow it, so on its own it would permit those earlier
    // reads to sink past this load and invalidate the argument.
    atomic_thread_fence(memory_order_seq_cst);
    uint64_t w2 = atomic_load_explicit(&src_outbox->write_seq, memory_order_seq_cst);
    if (torn || w2 - start > max_behind) {
      if (++lapped_retries > 4) {
        // The producer is outrunning us persistently; stop spinning and let
        // the caller re-park. Same loss this design already takes at the
        // retention window -- and landing at catchup_target rather than at
        // the window edge, for the same reason as the catch-up above.
        *cursor = w2 > catchup_target ? w2 - catchup_target : 0;
        break;
      }
      continue; // discard the torn snapshot and take a fresh one
    }
    lapped_retries = 0;

    if (slots == 0) {
      break;
    }
    *cursor = start + slots;
    atomic_fetch_add_explicit(&self->stat_slots_delivered, slots, memory_order_relaxed);
    atomic_fetch_add_explicit(&self->stat_bytes_delivered, copied, memory_order_relaxed);
    delivered_anything = true;
    if (copied == 0) {
      break; // only empty slots -- nothing to send, but cursor advanced
    }

    // ---- phase 2: write, from private memory ----
    // Stream: plain blocking write of the whole snapshot. No send timeout,
    // no retry budget, no "this peer is too slow, disconnect it" heuristic:
    // congestion is already signalled by the ring itself -- a target that
    // can't keep up falls outside the retention window and loses whole
    // chunks at a clean batch boundary, which is exactly what a switch does
    // with a congested port. Layering a write-side timer on top of that
    // added no new signal and actively caused harm: it fired mid-write
    // (turning a slow peer into a partially-sent, framing-desyncing one),
    // and its retry budget then disconnected peers that were merely busy --
    // including a VM that had just booted and hadn't started draining yet.
    // Blocking here only ever stalls delivery to this one target; every
    // other target's delivery thread, and every producer, is untouched.
    size_t off = 0;
    while (off < copied) {
      ssize_t written = write(self->socket_fd, self->delivery_scratch + off, copied - off);
      if (written < 0) {
        if (errno == EINTR) {
          continue;
        }
        // A real error (EPIPE/ECONNRESET/...) means this peer is genuinely
        // gone, not slow. Stop delivering to it entirely and return rather
        // than just breaking out of this one write: the outer loop would
        // otherwise immediately snapshot the next batch (another full
        // memcpy) and fail again, spinning on a dead socket and spamming
        // the log until the reader thread happens to notice the shutdown.
        ERRORN("write");
        shutdown(self->socket_fd, SHUT_RDWR);
        atomic_store(&self->delivery_should_stop, true);
        return delivered_anything;
      }
      off += (size_t)written;
    }
    break;
  }
  return delivered_anything;
}

// This conn as a TARGET: pulls from every other known conn's outbox (plus
// the synthetic vmnet_outbox) and delivers to this conn's own socket. Runs
// for the conn's whole lifetime, until the connection closes and
// delivery_should_stop is set.
static void *conn_delivery_thread(void *arg) {
  struct conn *self = arg;
  struct state *state = self->state;

  while (!atomic_load(&self->delivery_should_stop)) {
    // Capture the generation *before* scanning, and use that as the baseline
    // for both the spin and the park below. Capturing it after an empty scan
    // would be a lost-wakeup bug: a publish landing between the scan and the
    // load would become the new baseline and then go unnoticed until the
    // timed-wait backstop fired.
    uint64_t gen_before_scan = atomic_load(&state->publish_generation);

    bool delivered_anything =
        deliver_from_outbox(self, state->vmnet_outbox, &self->target_read_seq_vmnet);

    // Snapshot stable references while holding the list lock. Traversing the
    // mutable linked list after dropping the lock was both a C data race on
    // `next` and incompatible with actually reclaiming disconnected conns.
    struct conn *sources[MAX_CONNS];
    size_t source_count = 0;
    os_unfair_lock_lock(&state->sem);
    for (struct conn *src = state->conns; src != NULL; src = src->next) {
      if (src == self || src->id < 0) {
        continue;
      }
      assert(source_count < ARRAY_SIZE(sources));
      conn_retain(src);
      sources[source_count++] = src;
    }
    os_unfair_lock_unlock(&state->sem);
    for (size_t i = 0; i < source_count; i++) {
      struct conn *src = sources[i];
      if (self->target_source_generation[src->id] != src->generation) {
        // This active ID belonged to a connection that has since closed.
        // Start at the replacement source's current head rather than using
        // the predecessor's unrelated cursor.
        self->target_read_seq[src->id] =
            atomic_load_explicit(&src->outbox.write_seq, memory_order_acquire);
        self->target_source_generation[src->id] = src->generation;
      }
      delivered_anything |=
          deliver_from_outbox(self, &src->outbox, &self->target_read_seq[src->id]);
      conn_release(src);
    }

    if (delivered_anything) {
      continue;
    }

    // Busy-poll before parking: park/unpark is a syscall pair, and at
    // multi-Gbit rates publishes arrive often enough that a thread which has
    // just drained everything is frequently woken again almost immediately.
    // Only the generation is checked here -- rescanning every outbox per
    // spin iteration would cost far more than the park it is trying to
    // avoid.
    if (state->busy_poll_usec > 0) {
      atomic_fetch_add_explicit(&self->stat_spin_attempts, 1, memory_order_relaxed);
      uint64_t started = mach_absolute_time();
      uint64_t deadline = started + (uint64_t)state->busy_poll_usec * g_ticks_per_usec;
      bool hit = false;
      for (unsigned i = 0;; i++) {
        if (atomic_load_explicit(&state->publish_generation, memory_order_acquire) !=
            gen_before_scan) {
          hit = true;
          break;
        }
        if (atomic_load(&self->delivery_should_stop)) {
          break;
        }
        CPU_RELAX();
        // Reading the clock every iteration would dominate the loop; check
        // the deadline periodically instead.
        if ((i & 0x3F) == 0x3F && mach_absolute_time() >= deadline) {
          break;
        }
      }
      atomic_fetch_add_explicit(&self->stat_spin_ticks, mach_absolute_time() - started,
                                memory_order_relaxed);
      if (hit) {
        atomic_fetch_add_explicit(&self->stat_spin_hits, 1, memory_order_relaxed);
        continue; // something was published -- rescan immediately
      }
    }

    // Park until the next publish. Registering as a waiter *before* checking
    // the generation is what makes this race-free against
    // signal_publish()'s waiter check: a publish that lands between the scan
    // above and this point either already bumped the generation (so the
    // check below sees it and this thread doesn't park at all) or observes
    // this thread's registration (so it takes the mutex and broadcasts). The
    // bounded timeout is a backstop for both, and also what lets a stopping
    // connection's delivery thread notice delivery_should_stop reasonably
    // promptly.
    atomic_fetch_add_explicit(&self->stat_park_entries, 1, memory_order_relaxed);
    atomic_fetch_add(&state->publish_waiters, 1);
    pthread_mutex_lock(&state->publish_mutex);
    if (atomic_load(&state->publish_generation) == gen_before_scan) {
      atomic_fetch_add_explicit(&self->stat_park_waits, 1, memory_order_relaxed);
      struct timespec timeout = {.tv_sec = 0, .tv_nsec = 50 * 1000 * 1000}; // 50ms
      pthread_cond_timedwait_relative_np(&state->publish_cond, &state->publish_mutex, &timeout);
    }
    pthread_mutex_unlock(&state->publish_mutex);
    atomic_fetch_sub(&state->publish_waiters, 1);
  }

  // Fold this thread's counters into the process-wide totals exactly once.
  atomic_fetch_add_explicit(&state->stat_spin_attempts, atomic_load(&self->stat_spin_attempts),
                            memory_order_relaxed);
  atomic_fetch_add_explicit(&state->stat_spin_hits, atomic_load(&self->stat_spin_hits), memory_order_relaxed);
  atomic_fetch_add_explicit(&state->stat_spin_ticks, atomic_load(&self->stat_spin_ticks), memory_order_relaxed);
  atomic_fetch_add_explicit(&state->stat_park_entries, atomic_load(&self->stat_park_entries),
                            memory_order_relaxed);
  atomic_fetch_add_explicit(&state->stat_park_waits, atomic_load(&self->stat_park_waits), memory_order_relaxed);
  return NULL;
}

static void state_remove_conn(struct state *state, struct conn *target) {
  os_unfair_lock_lock(&state->sem);
  if (state->conns != NULL) {
    if (state->conns == target) {
      state->conns = state->conns->next;
    } else {
      struct conn *conn;
      for (conn = state->conns; conn->next != NULL; conn = conn->next) {
        if (conn->next == target) {
          conn->next = conn->next->next;
          break;
        }
      }
    }
  }
  if (target->id >= 0 && state->conns_by_id[target->id] == target) {
    state->conns_by_id[target->id] = NULL;
  }
  target->next = NULL;
  os_unfair_lock_unlock(&state->sem);
}

static void _on_vmnet_packets_available(interface_ref iface, int64_t buf_count, int64_t max_bytes,
                                        struct state *state) {
  DEBUGF("Receiving from VMNET (buffer for %lld packets, max: %lld "
         "bytes)",
         buf_count, max_bytes);
  // TODO: use prealloced pool
  struct vmpktdesc *pdv = calloc(buf_count, sizeof(struct vmpktdesc));
  if (pdv == NULL) {
    ERRORN("calloc(estim_count, sizeof(struct vmpktdesc)");
    goto done;
  }
  for (int i = 0; i < buf_count; i++) {
    pdv[i].vm_flags = 0;
    pdv[i].vm_pkt_size = max_bytes;
    pdv[i].vm_pkt_iovcnt = 1, pdv[i].vm_pkt_iov = malloc(sizeof(struct iovec));
    if (pdv[i].vm_pkt_iov == NULL) {
      ERRORN("malloc(sizeof(struct iovec))");
      goto done;
    }
    pdv[i].vm_pkt_iov->iov_base = malloc(max_bytes);
    if (pdv[i].vm_pkt_iov->iov_base == NULL) {
      ERRORN("malloc(max_bytes)");
      goto done;
    }
    pdv[i].vm_pkt_iov->iov_len = max_bytes;
  }
  int received_count = buf_count;
  vmnet_return_t read_status = vmnet_read(iface, pdv, &received_count);
  if (read_status != VMNET_SUCCESS) {
    ERRORF("vmnet_read: [%d] %s", read_status, vmnet_strerror(read_status));
    goto done;
  }

  DEBUGF("Received from VMNET: %d packets (buffer was prepared for %lld packets)", received_count,
         buf_count);
  // Publish to the synthetic vmnet_outbox instead of writing directly into
  // every conn's socket -- every conn's own delivery thread picks this up
  // independently. See the outbox_publish()/deliver_from_outbox() comments.
  //
  // One vmnet_read() yields up to MAX_PACKET_COUNT_AT_ONCE packets, and they
  // are published as *one* slot rather than one slot each. Publishing per
  // packet was not just wasteful (an outbox lock acquisition, sequence bump
  // and wakeup per frame instead of per batch) -- it silently made this
  // direction's retention window 43x smaller than the client->vmnet one,
  // because a slot then held a single ~1514-byte frame instead of a whole
  // read chunk: 0.39 MB of buffering instead of 16.8 MB, i.e. ~0.7ms of
  // absorption at multi-Gbit rates rather than the ~20ms OUTBOX_RING_SLOTS is
  // sized for. Client<->client testing never exercised this; traffic arriving
  // from outside the host does.
  uint32_t headers_be[MAX_PACKET_COUNT_AT_ONCE];
  struct iovec iov[MAX_PACKET_COUNT_AT_ONCE];
  int batched = 0;
  size_t batched_bytes = 0;
  for (int i = 0; i < received_count; i++) {
    uint8_t dest_mac[6], src_mac[6];
    assert(pdv[i].vm_pkt_iov[0].iov_len > 12);
    const char *packet = (const char *)pdv[i].vm_pkt_iov[0].iov_base;
    memcpy(dest_mac, packet, sizeof(dest_mac));
    memcpy(src_mac, packet + 6, sizeof(src_mac));
    DEBUGF("[Handler i=%d] Dest %02X:%02X:%02X:%02X:%02X:%02X, Src "
           "%02X:%02X:%02X:%02X:%02X:%02X,",
           i, dest_mac[0], dest_mac[1], dest_mac[2], dest_mac[3], dest_mac[4], dest_mac[5],
           src_mac[0], src_mac[1], src_mac[2], src_mac[3], src_mac[4], src_mac[5]);

    size_t frame_len = pdv[i].vm_pkt_size;
    // Defensive split: ordinary MTU traffic puts all 32 packets in one slot
    // with room to spare, but never assemble past what one slot can hold.
    if (batched > 0 && batched_bytes + 4 + frame_len > READ_BUF_SIZE) {
      outbox_publish(state, state->vmnet_outbox, headers_be, iov, batched);
      batched = 0;
      batched_bytes = 0;
    }
    headers_be[batched] = htonl((uint32_t)frame_len);
    iov[batched] = (struct iovec){.iov_base = pdv[i].vm_pkt_iov[0].iov_base, .iov_len = frame_len};
    batched++;
    batched_bytes += 4 + frame_len;
  }
  if (batched > 0) {
    outbox_publish(state, state->vmnet_outbox, headers_be, iov, batched);
  }
done:
  if (pdv != NULL) {
    for (int i = 0; i < buf_count; i++) {
      if (pdv[i].vm_pkt_iov != NULL) {
        if (pdv[i].vm_pkt_iov->iov_base != NULL) {
          free(pdv[i].vm_pkt_iov->iov_base);
        }
        free(pdv[i].vm_pkt_iov);
      }
    }
    free(pdv);
  }
}

static void on_vmnet_packets_available(interface_ref iface, int64_t estim_count, int64_t max_bytes,
                                       struct state *state) {
  int64_t q = estim_count / MAX_PACKET_COUNT_AT_ONCE;
  int64_t r = estim_count % MAX_PACKET_COUNT_AT_ONCE;
  DEBUGF("estim_count=%lld, dividing by MAX_PACKET_COUNT_AT_ONCE=%d; q=%lld, "
         "r=%lld",
         estim_count, MAX_PACKET_COUNT_AT_ONCE, q, r);
  for (int i = 0; i < q; i++) {
    _on_vmnet_packets_available(iface, MAX_PACKET_COUNT_AT_ONCE, max_bytes, state);
  }
  if (r > 0)
    _on_vmnet_packets_available(iface, r, max_bytes, state);
}

static interface_ref start(struct state *state, struct cli_options *cliopt) {
  INFOF("Initializing vmnet.framework (mode %d)", cliopt->vmnet_mode);

  dispatch_semaphore_t sem = dispatch_semaphore_create(0);
  __block interface_ref iface = NULL;
  __block vmnet_return_t status = VMNET_FAILURE;
  __block uint64_t max_bytes = 0;
  vmnet_start_interface_completion_handler_t on_started =
      ^(vmnet_return_t x_status, xpc_object_t x_param) {
        status = x_status;
        if (x_status == VMNET_SUCCESS) {
          print_vmnet_start_param(x_param);
          max_bytes = xpc_dictionary_get_uint64(x_param, vmnet_max_packet_size_key);
        }
        dispatch_semaphore_signal(sem);
      };

  if (cliopt->vmnet_disable_dhcp) {
    // The DHCP server can only be disabled via the vmnet_network_configuration
    // API, which is macOS 26+. Guard at both compile time (SDK has the symbols)
    // and runtime (the host actually provides them).
#if defined(__MAC_OS_X_VERSION_MAX_ALLOWED) && __MAC_OS_X_VERSION_MAX_ALLOWED >= 260000
    if (__builtin_available(macOS 26.0, *)) {
      vmnet_return_t st = VMNET_FAILURE;
      vmnet_network_configuration_ref cfg =
          vmnet_network_configuration_create(cliopt->vmnet_mode, &st);
      if (cfg == NULL) {
        ERRORF("vmnet_network_configuration_create: [%d] %s", st, vmnet_strerror(st));
        return NULL;
      }
      if (cliopt->vmnet_interface != NULL) {
        INFOF("Using network interface \"%s\"", cliopt->vmnet_interface);
        st = vmnet_network_configuration_set_external_interface(cfg, cliopt->vmnet_interface);
        if (st != VMNET_SUCCESS) {
          ERRORF("vmnet_network_configuration_set_external_interface: [%d] %s", st,
                 vmnet_strerror(st));
          return NULL;
        }
      }
      if (cliopt->vmnet_gateway != NULL) {
        struct in_addr gateway, subnet, mask;
        if (!inet_aton(cliopt->vmnet_gateway, &gateway)) {
          ERRORF("invalid address \"%s\" was specified for --vmnet-gateway", cliopt->vmnet_gateway);
          return NULL;
        }
        if (!inet_aton(cliopt->vmnet_mask, &mask)) {
          ERRORF("invalid address \"%s\" was specified for --vmnet-mask", cliopt->vmnet_mask);
          return NULL;
        }
        subnet = gateway;
        subnet.s_addr &= mask.s_addr;
        vmnet_network_configuration_set_ipv4_subnet(cfg, &subnet, &mask);
      }
      if (cliopt->vmnet_nat66_prefix != NULL) {
        struct in6_addr prefix;
        if (inet_pton(AF_INET6, cliopt->vmnet_nat66_prefix, &prefix) != 1) {
          ERRORF("invalid IPv6 prefix \"%s\" for --vmnet-nat66-prefix", cliopt->vmnet_nat66_prefix);
          return NULL;
        }
        st = vmnet_network_configuration_set_ipv6_prefix(cfg, &prefix, 64);
        if (st != VMNET_SUCCESS) {
          ERRORF("vmnet_network_configuration_set_ipv6_prefix: [%d] %s", st, vmnet_strerror(st));
          return NULL;
        }
      }
      vmnet_network_configuration_disable_dhcp(cfg);
      vmnet_network_ref net = vmnet_network_create(cfg, &st);
      if (net == NULL) {
        ERRORF("vmnet_network_create: [%d] %s", st, vmnet_strerror(st));
        return NULL;
      }
      xpc_object_t desc = xpc_dictionary_create(NULL, NULL, 0);
      iface = vmnet_interface_start_with_network(net, desc, state->host_queue, on_started);
      dispatch_semaphore_wait(sem, DISPATCH_TIME_FOREVER);
      xpc_release(desc);
    } else {
      ERROR("--vmnet-disable-dhcp requires macOS 26.0 or later");
      return NULL;
    }
#else
    ERROR("--vmnet-disable-dhcp requires building against the macOS 26 SDK or later");
    return NULL;
#endif
  } else {
    xpc_object_t dict = xpc_dictionary_create(NULL, NULL, 0);
    xpc_dictionary_set_uint64(dict, vmnet_operation_mode_key, cliopt->vmnet_mode);
    if (cliopt->vmnet_interface != NULL) {
      INFOF("Using network interface \"%s\"", cliopt->vmnet_interface);
      xpc_dictionary_set_string(dict, vmnet_shared_interface_name_key, cliopt->vmnet_interface);
    }

    if (!uuid_is_null(cliopt->vmnet_network_identifier)) {
      xpc_dictionary_set_uuid(dict, vmnet_network_identifier_key, cliopt->vmnet_network_identifier);
    }

    if (cliopt->vmnet_gateway != NULL) {
      xpc_dictionary_set_string(dict, vmnet_start_address_key, cliopt->vmnet_gateway);
      xpc_dictionary_set_string(dict, vmnet_end_address_key, cliopt->vmnet_dhcp_end);
      xpc_dictionary_set_string(dict, vmnet_subnet_mask_key, cliopt->vmnet_mask);
    }

    xpc_dictionary_set_uuid(dict, vmnet_interface_id_key, cliopt->vmnet_interface_id);

    if (cliopt->vmnet_nat66_prefix != NULL) {
      xpc_dictionary_set_string(dict, vmnet_nat66_prefix_key, cliopt->vmnet_nat66_prefix);
    }

    iface = vmnet_start_interface(dict, state->host_queue, on_started);
    dispatch_semaphore_wait(sem, DISPATCH_TIME_FOREVER);
    xpc_release(dict);
  }

  if (status != VMNET_SUCCESS) {
    const char *start_api =
        cliopt->vmnet_disable_dhcp ? "vmnet_interface_start_with_network" : "vmnet_start_interface";
    ERRORF("%s: [%d] %s", start_api, status, vmnet_strerror(status));
    return NULL;
  }

  vmnet_interface_set_event_callback(
      iface, VMNET_INTERFACE_PACKETS_AVAILABLE, state->host_queue,
      ^(interface_event_t __attribute__((unused)) x_event_id, xpc_object_t x_event) {
        uint64_t estim_count =
            xpc_dictionary_get_uint64(x_event, vmnet_estimated_packets_available_key);
        on_vmnet_packets_available(iface, estim_count, max_bytes, state);
      });

  return iface;
}

static void stop(struct state *state, interface_ref iface) {
  if (iface == NULL) {
    return;
  }
  dispatch_semaphore_t sem = dispatch_semaphore_create(0);
  __block vmnet_return_t status;
  vmnet_stop_interface(iface, state->host_queue, ^(vmnet_return_t x_status) {
    status = x_status;
    dispatch_semaphore_signal(sem);
  });
  dispatch_semaphore_wait(sem, DISPATCH_TIME_FOREVER);
  if (status != VMNET_SUCCESS) {
    ERRORF("vmnet_stop_interface: [%d] %s", status, vmnet_strerror(status));
  }
}

static int socket_bindlisten(const char *socket_path, const char *socket_group) {
  int fd = -1;
  struct sockaddr_un addr = {0};

  unlink(socket_path); /* avoid EADDRINUSE */
  if ((fd = socket(PF_LOCAL, SOCK_STREAM, 0)) < 0) {
    ERRORN("socket");
    goto err;
  }
  addr.sun_family = PF_LOCAL;
  size_t socket_len = strlen(socket_path);
  if (socket_len + 1 > sizeof(addr.sun_path)) {
    ERRORF("the socket path is too long: %zu", socket_len);
    goto err;
  }
  strncpy(addr.sun_path, socket_path, sizeof(addr.sun_path) - 1);
  if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    ERRORN("bind");
    goto err;
  }
  if (listen(fd, 0) < 0) {
    ERRORN("listen");
    goto err;
  }
  if (socket_group != NULL) {
    errno = 0;
    struct group *grp = getgrnam(socket_group); /* Do not free */
    if (grp == NULL) {
      if (errno != 0)
        ERRORN("getgrnam");
      else
        ERRORF("unknown group name \"%s\"", socket_group);
      goto err;
    }
    /* fchown can't be used (EINVAL) */
    if (chown(socket_path, -1, grp->gr_gid) < 0) {
      ERRORN("chown");
      goto err;
    }
    if (chmod(socket_path, 0770) < 0) {
      ERRORN("chmod");
      goto err;
    }
  }
  return fd;
err:
  if (fd >= 0)
    close(fd);
  return -1;
}

static void remove_pidfile(const char *pidfile) {
  if (unlink(pidfile) != 0) {
    ERRORF("Failed to remove pidfile: \"%s\": %s", pidfile, strerror(errno));
    return;
  }
  INFOF("Removed pidfile \"%s\" for process %d", pidfile, getpid());
}

static int create_pidfile(const char *pidfile) {
  int flags = O_WRONLY | O_CREAT | O_EXLOCK | O_TRUNC | O_NONBLOCK;
  int fd = open(pidfile, flags, 0644);
  if (fd == -1) {
    ERRORF("Failed to open pidfile: \"%s\": %s", pidfile, strerror(errno));
    return -1;
  }

  char pid[20];
  snprintf(pid, sizeof(pid), "%u", getpid());
  ssize_t n = write(fd, pid, strlen(pid));
  if (n != (ssize_t)strlen(pid)) {
    if (n < 0) {
      ERRORF("Failed to write pidfile: \"%s\": %s", pidfile, strerror(errno));
    } else {
      // Should never happen, but if it does errno is not set.
      ERRORF("Short write to pidfile: \"%s\"", pidfile);
    }
    remove_pidfile(pidfile);
    close(fd);
    return -1;
  }

  INFOF("Created pidfile \"%s\" for process %d", pidfile, getpid());
  return fd;
}

static int setup_signals(int kq) {
  struct kevent changes[] = {
      {.ident = SIGHUP,  .filter = EVFILT_SIGNAL, .flags = EV_ADD},
      {.ident = SIGINT,  .filter = EVFILT_SIGNAL, .flags = EV_ADD},
      {.ident = SIGTERM, .filter = EVFILT_SIGNAL, .flags = EV_ADD},
#ifdef SOCKET_VMNET_DIAG
      {.ident = SIGUSR1, .filter = EVFILT_SIGNAL, .flags = EV_ADD},
#endif
      {.ident = SIGUSR2, .filter = EVFILT_SIGNAL, .flags = EV_ADD},
  };

  // Block signals we want to receive via kqueue.
  sigset_t mask;
  sigemptyset(&mask);
  for (size_t i = 0; i < ARRAY_SIZE(changes); i++) {
    sigaddset(&mask, changes[i].ident);
  }
  if (sigprocmask(SIG_BLOCK, &mask, NULL) != 0) {
    ERRORN("sigprocmask");
    return -1;
  }

  // We will receive EPIPE on the socket.
  signal(SIGPIPE, SIG_IGN);

  if (kevent(kq, changes, ARRAY_SIZE(changes), NULL, 0, NULL) != 0) {
    ERRORN("kevent");
    return -1;
  }
  return 0;
}

static int add_listen_fd(int kq, int fd) {
  struct kevent changes[] = {
      {.ident = fd, .filter = EVFILT_READ, .flags = EV_ADD},
  };
  if (kevent(kq, changes, ARRAY_SIZE(changes), NULL, 0, NULL) != 0) {
    ERRORN("kevent");
    return -1;
  }
  return 0;
}

// Live metrics snapshot, triggered by SIGUSR2. Sums the per-conn counters of
// every *currently connected* conn plus the totals already folded in by conns
// that have gone away, so the numbers are cumulative for the process rather
// than only visible at shutdown. Reading another thread's relaxed atomics
// gives a slightly skewed instant -- counters are bumped independently and
// there is no snapshot barrier -- which is fine for what this is for.
static void dump_metrics(struct state *state) {
  uint64_t att = atomic_load(&state->stat_spin_attempts), hit = atomic_load(&state->stat_spin_hits);
  uint64_t ticks = atomic_load(&state->stat_spin_ticks);
  uint64_t pe = atomic_load(&state->stat_park_entries), pw = atomic_load(&state->stat_park_waits);
  uint64_t deliv = 0, bytes = 0, dropped = 0;
  uint64_t pub = 0, pubbytes = 0;
  int conns = 0;

  os_unfair_lock_lock(&state->sem);
  for (struct conn *c = state->conns; c != NULL; c = c->next) {
    conns++;
    att += atomic_load(&c->stat_spin_attempts);
    hit += atomic_load(&c->stat_spin_hits);
    ticks += atomic_load(&c->stat_spin_ticks);
    pe += atomic_load(&c->stat_park_entries);
    pw += atomic_load(&c->stat_park_waits);
    deliv += atomic_load(&c->stat_slots_delivered);
    bytes += atomic_load(&c->stat_bytes_delivered);
    dropped += atomic_load(&c->stat_slots_dropped);
    pub += atomic_load(&c->stat_slots_published);
    pubbytes += atomic_load(&c->stat_bytes_published);
  }
  os_unfair_lock_unlock(&state->sem);

  INFOF("metrics: conns=%d vmnet_write=%s", conns,
        VMNET_WRITE_SUPPRESSED(state) ? "SUPPRESSED" : "enabled");
  INFOF("metrics: delivered %llu batches / %.1f MB, dropped %llu batches (%.3f%%)",
        (unsigned long long)deliv, (double)bytes / 1e6, (unsigned long long)dropped,
        (deliv + dropped) ? 100.0 * (double)dropped / (double)(deliv + dropped) : 0.0);
  INFOF("metrics: published %llu slots / %.1f MB = %.2f KB per slot",
        (unsigned long long)pub, (double)pubbytes / 1e6,
        pub ? (double)pubbytes / (double)pub / 1024.0 : 0.0);
  INFOF("metrics: park-entries %llu (%llu waited), broadcasts %llu",
        (unsigned long long)pe, (unsigned long long)pw,
        (unsigned long long)atomic_load(&state->stat_broadcasts));
  if (state->busy_poll_usec > 0) {
    INFOF("metrics: busy-poll(%dus) %llu attempts, %llu hits (%.1f%%), %.1f ms spun",
          state->busy_poll_usec, (unsigned long long)att, (unsigned long long)hit,
          att ? 100.0 * (double)hit / (double)att : 0.0,
          (double)ticks / (double)g_ticks_per_usec / 1000.0);
  }
}

static void on_accept(struct state *state, int accept_fd, interface_ref iface);

int main(int argc, char *argv[]) {
  debug = getenv("DEBUG") != NULL;
  int rc = 1;
  int listen_fd = -1;
  int pidfile_fd = -1;
  int kq = -1;
  __block interface_ref iface = NULL;

  struct state state = {0};
  init_ticks_per_usec();

  struct cli_options *cliopt = cli_options_parse(argc, argv);
  assert(cliopt != NULL);

  state.vmnet_outbox = alloc_pages_zeroed(sizeof(*state.vmnet_outbox));
  if (state.vmnet_outbox == NULL) {
    ERRORN("mmap(vmnet_outbox)");
    goto done;
  }
  // Not relying on `state = {0}` happening to match PTHREAD_MUTEX_INITIALIZER
  // / PTHREAD_COND_INITIALIZER's bit patterns -- init explicitly.
  pthread_mutex_init(&state.publish_mutex, NULL);
  pthread_cond_init(&state.publish_cond, NULL);

  if (geteuid() != 0) {
    WARN("Running without root. This is very unlikely to work: See README.md");
  }
  if (geteuid() != getuid()) {
    WARN("Seems running with SETUID. This is insecure and highly discouraged: See README.md");
  }

  kq = kqueue();
  if (kq == -1) {
    ERRORN("kqueue");
    goto done;
  }

  // Setup signals beofre creating the pidfile to ensure removal of the pidfile
  // when terminating by signal.
  if (setup_signals(kq)) {
    goto done;
  }

  if (cliopt->pidfile != NULL) {
    pidfile_fd = create_pidfile(cliopt->pidfile);
    if (pidfile_fd == -1) {
      goto done; // error already logged.
    }
  }

  DEBUGF("Opening socket \"%s\" (for UNIX group \"%s\")", cliopt->socket_path,
         cliopt->socket_group);
  listen_fd = socket_bindlisten(cliopt->socket_path, cliopt->socket_group);
  if (listen_fd < 0) {
    ERRORN("socket_bindlisten");
    goto done;
  }

  state.sockbuf_size = cliopt->sockbuf_size < 0 ? DEFAULT_SOCKBUF_SIZE : cliopt->sockbuf_size;

  state.sem = OS_UNFAIR_LOCK_INIT;
  state.delivery_batch_size = cliopt->delivery_batch_size < 0 ? DEFAULT_DELIVERY_BATCH_SIZE
                                                             : cliopt->delivery_batch_size;
  state.busy_poll_usec = cliopt->busy_poll_usec;
#ifdef SOCKET_VMNET_DIAG
  atomic_store(&state.skip_vmnet_write, cliopt->skip_vmnet_write);
  if (state.skip_vmnet_write) {
    WARN("--skip-vmnet-write: DIAGNOSTIC MODE. No frame will reach vmnet.framework, so DHCP, host "
         "access and external networking are all broken. Only client-to-client traffic works.");
  }
#endif

  // Queue for vm connections, allowing processing vms requests in parallel.
  state.vms_queue =
      dispatch_queue_create("io.github.lima-vm.socket_vmnet.vms", DISPATCH_QUEUE_CONCURRENT);

  // Queue for processing vmnet events.
  state.host_queue =
      dispatch_queue_create("io.github.lima-vm.socket_vmnet.host", DISPATCH_QUEUE_SERIAL);

  iface = start(&state, cliopt);
  if (iface == NULL) {
    // Error already logged.
    goto done;
  }

  if (add_listen_fd(kq, listen_fd)) {
    goto done;
  }
  while (1) {
    struct kevent events[1];
    int n = kevent(kq, NULL, 0, events, 1, NULL);
    if (n < 0) {
      if (errno == EINTR) {
        // A signal delivery (e.g. a debugger attach/continue cycle) can
        // interrupt this syscall without it being a real failure -- retry
        // rather than treating it as fatal. A real signal we care about
        // (SIGHUP/SIGINT/SIGTERM) still arrives as an EVFILT_SIGNAL *event*
        // from kevent(), handled below, not as this syscall itself failing.
        continue;
      }
      ERRORN("kevent");
      goto done;
    }

    if (events[0].filter == EVFILT_SIGNAL) {
      if ((int)events[0].ident == SIGUSR2) {
        dump_metrics(&state);
        continue;
      }
#ifdef SOCKET_VMNET_DIAG
      if ((int)events[0].ident == SIGUSR1) {
        bool now = !atomic_load(&state.skip_vmnet_write);
        atomic_store(&state.skip_vmnet_write, now);
        INFOF("SIGUSR1: vmnet_write() is now %s", now ? "SUPPRESSED (diagnostic)" : "enabled");
        continue;
      }
#endif
      INFOF("Received signal %s", strsignal(events[0].ident));
      break;
    }

    if (events[0].filter == EVFILT_READ && (int)events[0].ident == listen_fd) {
      int accept_fd = accept(listen_fd, NULL, NULL);
      if (accept_fd < 0) {
        ERRORN("accept");
        goto done;
      }
      struct state *state_p = &state;
      dispatch_async(state.vms_queue, ^{
        on_accept(state_p, accept_fd, iface);
      });
    }
  }
  rc = 0;
done:
  DEBUGF("shutting down with rc=%d", rc);
  {
    uint64_t att = atomic_load(&state.stat_spin_attempts);
    uint64_t hit = atomic_load(&state.stat_spin_hits);
    uint64_t ticks = atomic_load(&state.stat_spin_ticks);
    INFOF("delivery: %llu park-entries (%llu actually waited), %llu broadcasts",
          (unsigned long long)atomic_load(&state.stat_park_entries),
          (unsigned long long)atomic_load(&state.stat_park_waits),
          (unsigned long long)atomic_load(&state.stat_broadcasts));
    if (state.busy_poll_usec > 0) {
      INFOF("busy-poll(%dus): %llu attempts, %llu hits (%.1f%%), %.1f ms spun",
            state.busy_poll_usec, (unsigned long long)att, (unsigned long long)hit,
            att ? 100.0 * (double)hit / (double)att : 0.0,
            (double)ticks / (double)g_ticks_per_usec / 1000.0);
    }
  }
  if (iface != NULL) {
    stop(&state, iface);
  }
  if (listen_fd != -1) {
    close(listen_fd);
  }
  if (pidfile_fd != -1) {
    remove_pidfile(cliopt->pidfile);
    close(pidfile_fd);
  }
  if (state.vms_queue != NULL)
    dispatch_release(state.vms_queue);
  if (state.host_queue != NULL)
    dispatch_release(state.host_queue);
  if (kq != -1) {
    close(kq);
  }
  cli_options_destroy(cliopt);
  return rc;
}

static void on_accept(struct state *state, int accept_fd, interface_ref iface) {
  INFOF("Accepted a connection (fd %d)", accept_fd);
  struct conn *self_conn = state_add_socket_fd(state, accept_fd);
  if (self_conn == NULL) {
    INFOF("Rejecting connection (fd %d): unable to initialize delivery state", accept_fd);
    close(accept_fd);
    return;
  }
  if (state->sockbuf_size > 0) {
    if (setsockopt(accept_fd, SOL_SOCKET, SO_SNDBUF, &state->sockbuf_size,
                   sizeof(state->sockbuf_size)) < 0) {
      ERRORN("setsockopt(SO_SNDBUF)");
    }
    if (setsockopt(accept_fd, SOL_SOCKET, SO_RCVBUF, &state->sockbuf_size,
                   sizeof(state->sockbuf_size)) < 0) {
      ERRORN("setsockopt(SO_RCVBUF)");
    }
  }
  // No SO_SNDTIMEO here deliberately -- congestion is signalled by the ring
  // (a target that falls outside the retention window loses whole chunks at
  // a clean boundary), not by a write-side timer. See the write loop in
  // deliver_from_outbox() for why adding one made things strictly worse.

  // Reads straight into this conn's own outbox slots -- no separate scratch
  // buffer and no copy to publish a batch. An earlier version staged frames
  // in a scratch buffer and copied them in to publish; that copy let this
  // side publish faster than the delivery side could drain, and measured 87%
  // frame loss. One slot at
  // a time is filled directly by read(), parsed in place, and "published"
  // by just recording how many of its bytes are a complete batch and
  // bumping write_seq -- the data was already sitting there.
  size_t leftover = 0;
  uint64_t write_slot = 0; // which slot of self_conn->outbox we're filling

  struct iovec batch_iov[WRITE_BATCH_MAX];
  struct vmpktdesc batch_pdv[WRITE_BATCH_MAX];

  for (uint64_t i = 0;; i++) {
    DEBUGF("[Socket-to-VMNET i=%lld] Receiving from the socket %d", i, accept_fd);
    void *slot_data = self_conn->outbox.slots[write_slot % OUTBOX_RING_SLOTS].data;

    // Fill whatever's already buffered in one syscall, capped to
    // READ_CHUNK_SIZE (not "whatever room happens to be left" -- see the
    // comment on that constant), then parse as many complete
    // length-prefixed frames out of it as are present below, carrying over
    // any trailing partial frame to combine with the next read.
    size_t room = READ_BUF_SIZE - leftover;
    size_t want = room < READ_CHUNK_SIZE ? room : READ_CHUNK_SIZE;
    ssize_t new_bytes = 0;
    if (want > 0) {
      new_bytes = read(accept_fd, (uint8_t *)slot_data + leftover, want);
      if (new_bytes < 0) {
        ERRORN("read");
        goto done;
      }
      if (new_bytes == 0) {
        // EOF according to man page of read.
        INFOF("Connection closed by peer (fd %d)", accept_fd);
        goto done;
      }
    }
    // want == 0 means the previous cycle's leftover already fills a full
    // chunk's worth of buffer -- skip the read this time (a 0-byte read()
    // request isn't meaningful and shouldn't be confused with EOF) and just
    // parse what's already sitting there; that drains some of it and frees
    // room for the next cycle's read.
    size_t total = leftover + (size_t)new_bytes;
    size_t cursor = 0;
    int batch_count = 0;
    while (batch_count < WRITE_BATCH_MAX && total - cursor >= 4) {
      uint32_t hdr_be;
      memcpy(&hdr_be, (uint8_t *)slot_data + cursor, 4);
      uint32_t hdr = ntohl(hdr_be);
      assert(hdr <= MAX_FRAME_SIZE);
      if (total - cursor - 4 < hdr) {
        break; // incomplete frame -- wait for more data next read
      }
      void *frame_ptr = (uint8_t *)slot_data + cursor + 4;
      batch_iov[batch_count] = (struct iovec){.iov_base = frame_ptr, .iov_len = hdr};
      batch_pdv[batch_count] = (struct vmpktdesc){
          .vm_pkt_size = hdr, .vm_pkt_iov = &batch_iov[batch_count], .vm_pkt_iovcnt = 1, .vm_flags = 0};
      cursor += 4 + hdr;
      batch_count++;
    }
    if (batch_count == 0) {
      // Not even one complete frame yet -- e.g. a single frame close to
      // MAX_FRAME_SIZE spanning more than one chunk read, or just the
      // start of a connection. Keep reading into this same (still
      // unpublished) slot rather than publishing an empty batch.
      leftover = total;
      continue;
    }

    int written_count = batch_count;
    DEBUGF("[Socket-to-VMNET i=%lld] Sending to VMNET: %d packet(s)", i, batch_count);
    vmnet_return_t write_status =
        VMNET_WRITE_SUPPRESSED(state) ? VMNET_SUCCESS
                                             : vmnet_write(iface, batch_pdv, &written_count);
    if (write_status != VMNET_SUCCESS) {
      ERRORF("vmnet_write: [%d] %s", write_status, vmnet_strerror(write_status));
      goto done;
    }
    DEBUGF("[Socket-to-VMNET i=%lld] Sent to VMNET: %d packet(s)", i, batch_count);

    // Publish: this slot's first `cursor` bytes are a complete,
    // ready-to-deliver batch, already in the wire format delivery needs
    // (see deliver_from_outbox()) -- nothing to copy, just make it visible.
    self_conn->outbox.slots[write_slot % OUTBOX_RING_SLOTS].byte_len = cursor;
    // Release: the slot's data (written by read() above) and its byte_len
    // must both be visible to any reader that observes this sequence number.
    // No lock needed -- this conn's outbox has exactly one publisher, this
    // thread, and readers are lock-free (see deliver_from_outbox()).
    atomic_store_explicit(&self_conn->outbox.write_seq, write_slot + 1, memory_order_release);
    atomic_fetch_add_explicit(&self_conn->stat_slots_published, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&self_conn->stat_bytes_published, cursor, memory_order_relaxed);
    signal_publish(state);

    // Move to the next slot -- this one is now visible to readers and must
    // not be written to again. Any trailing partial frame has to move with
    // us: a small, bounded (< MAX_FRAME_SIZE) copy, same as this file
    // always needed to carry a partial frame across a read cycle, just
    // between two slots' buffers now instead of within one reused scratch.
    write_slot++;
    void *next_slot_data = self_conn->outbox.slots[write_slot % OUTBOX_RING_SLOTS].data;
    if (cursor < total) {
      memcpy(next_slot_data, (uint8_t *)slot_data + cursor, total - cursor);
      leftover = total - cursor;
    } else {
      leftover = 0;
    }
  }
done:
  INFOF("Closing a connection (fd %d)", accept_fd);
  // Unblock the delivery thread if it's mid-write to this same fd or
  // waiting on publish_cond, then wait for it to actually exit before
  // freeing anything it might still touch.
  atomic_store(&self_conn->delivery_should_stop, true);
  shutdown(accept_fd, SHUT_RDWR);
  signal_publish(state);
  // Unconditional: state_add_conn() only hands back a conn once its delivery
  // thread has actually been created, and rejects the connection outright
  // otherwise -- so there is always exactly one thread to join here.
  pthread_join(self_conn->delivery_thread, NULL);
  state_remove_conn(state, self_conn);
  close(accept_fd);
  conn_release(self_conn);
}
