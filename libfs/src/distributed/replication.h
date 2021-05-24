#ifndef _REPLICATION_H_
#define _REPLICATION_H_

#include "concurrency/synchronization.h"
#include "filesystem/shared.h"
#include "global/global.h"
#include "global/types.h"
#include "global/util.h"
#include "ds/list.h"
#include "ds/stdatomic.h"
#include "agent.h"

#define N_RSYNC_THREADS 2

#ifdef KERNFS
extern struct replication_context *g_sync_ctx[g_n_max_libfs + g_n_nodes];
#else
extern struct replication_context *g_sync_ctx[g_n_nodes];
#endif
extern int g_rsync_chunk;
extern int g_enable_rpersist;
extern int g_rsync_rf;

//TODO: [refactor] use fs_log struct for peer (too many common elements between peer_metadata & fs_log).

// metadata for remote peers
typedef struct peer_metadata {
	int id;

	struct peer_id *info;

	// start addr for copying from local log (i.e. last blk synced + 1)
	addr_t local_start;

	// start addr for persisting at remote log (i.e. remote next_avail_header)
	addr_t remote_start;

	// last viable addr at remote log (before wrap around)
	addr_t remote_end;

	// start addr for digesting at remote log
	addr_t start_digest;

	// base addr for remote log
	addr_t base_addr;

	uint32_t start_version;

	uint32_t avail_version;

	// outstanding rsyncs.
	uint8_t outstanding;

	//pthread_t is unsigned long
	unsigned long replication_thread_id;

	// # of log headers to digest at remote (used for convenience/sanity; can be replaced by n_used_blk)
	atomic_uint n_digest;

	// # of log headers requested for digestion in the last request
	uint32_t n_digest_req;

	// # of log headers that are pending transmission or inflight
	atomic_uint n_pending;

	// # of log headers reserved at remote (used for convenience/sanity; can be replaced by n_used_blk)
	atomic_uint n_used;

	// # of blocks reserved at remote log
	atomic_ulong n_used_blk;

	//1 if peer is currently digesting; 0 otherwise
	int digesting;

	//1 if peer is currently syncing data (one flag per each replication thread)
	int syncing[N_RSYNC_THREADS+1]; 

	// # of unsynced log headers (used for convenience/sanity; can be replaced by n_unsync_blk)
	atomic_uint n_unsync;

	// # of unsynced log blocks
	atomic_ulong n_unsync_blk;

	// used for concurrently updating *_start variables
	pthread_mutex_t *shared_rsync_addr_lock;
	
	// used for concurrently updating n_unsync_* variables
	pthread_mutex_t *shared_rsync_n_lock;

	// used to enforce rsync order (acts as an optimization for coalescing; otherwise unused)
	pthread_mutex_t *shared_rsync_order;
	pthread_cond_t *shared_rsync_cond_wait;
	atomic_ulong cond_check;


} peer_meta_t;

// replication globals
struct replication_context {
	struct peer_id *self;
	addr_t begin;
	addr_t size;
	atomic_ulong *end; //last blknr before wrap around
	addr_t base_addr;

	// remote peer metadata; can add as many elements
	// as necessary in case of multiple replicas
	peer_meta_t *peer;
};

// ephemeral sync metadata
struct rdma_meta_entry {
	int local_mr;
	int remote_mr;
	int rotated;
	rdma_meta_t *meta;
	struct list_head head;
};

struct sync_list {
	uint32_t count;
	addr_t n_blk;
	addr_t remote_start;
	struct list_head head;
};

typedef struct sync_interval {
	addr_t start;
	addr_t end;
	struct list_head head;
} sync_interval_t;

typedef struct sync_meta {
	int id;
	addr_t local_start;
	addr_t remote_start;
	addr_t remote_end;
	addr_t peer_base_addr;
	addr_t end; //snapshot of last blknr before wrap around
	uint32_t n_unsync; //# of unsynced log hdrs
	uint32_t n_tosync; //# of log hdrs to sync (after coalescing)
	addr_t n_unsync_blk; //# of unsynced log blks
	addr_t n_tosync_blk; //# of log blks to sync (after coalescing)
	uint32_t n_digest;
	int rotated; //whether or not log has wrapped around during session
	struct sync_list *intervals;
	struct list_head rdma_entries;

} sync_meta_t;

typedef void(*conn_cb_fn)(char* ip);
typedef void(*disc_cb_fn)(char* ip);
typedef void (*signal_cb_fn)(struct app_context *);

#define next_loghdr(x,y) next_loghdr_blknr(x,y->nr_log_blocks,g_sync_ctx[session->id]->begin,session->end)

static inline void set_peer_digesting(peer_meta_t *peer)
{
	mlfs_printf("set digesting for peer %d\n", peer->id);
	while (1) {
		//if (!xchg_8(&g_fs_log->digesting, 1)) 
		if (!cmpxchg(&peer->digesting, 0, 1)) 
			return;

		while (peer->digesting) 
			cpu_relax();
	}
}

static inline void clear_peer_digesting(peer_meta_t *peer)
{
	mlfs_printf("clear digesting for peer %d\n", peer->id);
	//while (1) {
		//if (!xchg_8(&g_fs_log->digesting, 1)) 
		if (cmpxchg(&peer->digesting, 1, 0)) 
			return;

		//while (peer->digesting) 
		//	cpu_relax();
	//}
}

static inline void set_peer_syncing(peer_meta_t *peer, int seqn)
{
	mlfs_printf("set syncing for peer %d seqn %d\n", peer->id, seqn);
	while (1) {
		//if (!xchg_8(&g_fs_log->digesting, 1)) 
		if (!cmpxchg(&peer->syncing[seqn], 0, 1)) 
			return;

		while (peer->syncing[seqn]) 
			cpu_relax();
	}
}

static inline void clear_peer_syncing(peer_meta_t *peer, int seqn)
{
	mlfs_printf("clear syncing for peer %d seqn %d\n", peer->id, seqn);
	//while (1) {
		//if (!xchg_8(&g_fs_log->digesting, 1)) 
		if (cmpxchg(&peer->syncing[seqn], 1, 0))
			return;

		//while (peer->digesting) 
		//	cpu_relax();
	//}
}

void init_replication(int id, struct peer_id *peer, addr_t begin, addr_t size,
		addr_t addr, atomic_ulong *end);
int make_replication_request_async(uint32_t n_blk);
int make_replication_request_sync(peer_meta_t *peer);
int mlfs_do_rsync_forward(int node_id, uint32_t imm);
uint32_t create_rsync(int id, struct list_head **rdma_entries, uint32_t n_blk, int force_digest);
void update_peer_sync_state(peer_meta_t *peer, uint16_t nr_log_blocks, int rotated);
void update_peer_digest_state(peer_meta_t *peer, addr_t start_digest, int n_digested, int rotated);
void wait_on_peer_digesting(peer_meta_t *peer);
void wait_on_peer_replicating(peer_meta_t *peer, int seqn);
void wait_till_digest_checkpoint(peer_meta_t *peer, int version, addr_t block_nr);

uint32_t addr_to_logblk(int id, addr_t addr);
uint32_t generate_rsync_metadata(int id, uint16_t seq_n, addr_t size, int rotated, int ack);
uint32_t next_rsync_metadata(uint32_t meta);
int decode_rsync_metadata(uint32_t meta, uint16_t *seq_n, addr_t *n_log_blk, int *steps,
		int *requester_id, int *sync, int *ack);
peer_meta_t* get_next_peer();
static int start_rsync_session(peer_meta_t *peer, uint32_t n_blk);
void end_rsync_session(peer_meta_t *peer);
static void generate_rsync_msgs();
static void generate_rsync_msgs_using_replay(struct replay_list *replay_list, int destroy);
static void coalesce_log_and_produce_replay(struct replay_list *replay_list);
static void coalesce_log_entry(addr_t loghdr_blkno, loghdr_t *loghdr, struct replay_list *replay_list);
void move_log_ptr(int id, addr_t *start, addr_t addr_trans, addr_t size);
static void reset_log_ptr(addr_t *log_ptr);
static void rdma_add_sge(addr_t size);
static void rdma_finalize();
addr_t nr_blks_between_ptrs(int id, addr_t start, addr_t target); 
static addr_t nr_blks_before_wrap(int id, addr_t start, addr_t addr_trans, addr_t size);
static void print_peer_meta_struct(peer_meta_t *meta); 
static void print_sync_meta_struct(sync_meta_t *meta); 
static void print_sync_intervals_list(sync_meta_t *meta);

#endif
