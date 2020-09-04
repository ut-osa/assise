#ifndef RDMA_GLOBALS_H
#define RDMA_GLOBALS_H

#include <stdio.h>
#include <inttypes.h>

// FIXME: cacheline size should not be a compile-time constant.
#if defined(__x86_64__) || defined(__i386__)
#  define CACHELINE_BYTES 64
#elif defined(__arm__)
#  define CACHELINE_BYTES 32
#elif defined(__aarch64__)
#  define CACHELINE_BYTES 64
#else
# error set CACHELINE_BYTES appropriately
#endif

// XXX NOTE: MAX_BUFFER must be a power of 2 (necessary for atomicity of buffer acquisitions)
#define MAX_DEVICES 2 // max # of RDMA devices/ports
#define MAX_CONNECTIONS 1000 // max # of RDMA connections per peer
#define MAX_MR 10 // max # of memory regions per connection
#define MAX_PENDING 500 // max # of pending app responses per connection
#define MAX_BUFFER 8 // max # of msg buffers per connection
#define MAX_SEND_QUEUE_SIZE 1024 // depth of rdma send queue
#define MAX_RECV_QUEUE_SIZE 1024 // depth of rdma recv queue

#define POLLING_TIMEOUT 1000 // timeout (in us) for polling messages

// max # of rdma operations that can be batched together
// must be < MAX_SEND_QUEUE_SIZE
#define MAX_BATCH_SIZE 50

typedef uint64_t addr_t;

#endif
