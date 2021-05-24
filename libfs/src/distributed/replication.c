#include <time.h>

#include "replication.h"
#include "concurrency/thpool.h"

#ifdef KERNFS
#include "fs.h"
#else
#include "filesystem/fs.h"
#endif

#ifdef DISTRIBUTED

int g_rsync_chunk = 256;	//default: 1 MB replication chunk
int g_enable_rpersist = 0;	//default: persistence disabled
int g_rsync_rf = g_n_nodes;	//default: replicate to all nodes

int log_idx = 0;

#ifdef KERNFS
struct replication_context *g_sync_ctx[g_n_max_libfs + g_n_nodes];
#else
struct replication_context *g_sync_ctx[g_n_nodes];
#endif

__thread sync_meta_t *session;

threadpool thread_pool;

static void replication_worker(void *arg);

void init_replication(int remote_log_id, struct peer_id *peer, addr_t begin, addr_t size, addr_t addr, atomic_ulong *end)
{
	int ret, idx;
	const char *env;
	pthread_mutexattr_t attr;

#ifdef KERNFS
	idx = remote_log_id;
#else
	idx = 0;
#endif

	g_sync_ctx[idx] = (struct replication_context *)mlfs_zalloc(sizeof(struct replication_context));
	g_sync_ctx[idx]->begin = begin;
	g_sync_ctx[idx]->size = size;
	g_sync_ctx[idx]->end = end;
	g_sync_ctx[idx]->base_addr = addr - (disk_sb[g_log_dev].log_start << g_block_size_shift);
	
	g_sync_ctx[idx]->peer = (peer_meta_t *)mlfs_zalloc(sizeof(peer_meta_t));
	g_sync_ctx[idx]->peer->id = idx;
	g_sync_ctx[idx]->peer->info = peer;
	g_sync_ctx[idx]->peer->local_start = begin;
	g_sync_ctx[idx]->peer->remote_start = begin;
	g_sync_ctx[idx]->peer->start_digest = g_sync_ctx[idx]->peer->remote_start;

	mlfs_info("initializing replication module (begin: %lu | end: %lu | log size: %lu | base_addr: %lu)\n",
			g_sync_ctx[idx]->begin, atomic_load(g_sync_ctx[idx]->end), g_sync_ctx[idx]->size, g_sync_ctx[idx]->base_addr);

	atomic_init(&g_sync_ctx[idx]->peer->cond_check, begin);
	atomic_init(&g_sync_ctx[idx]->peer->n_digest, 0); //we assume that replica's log is empty
	atomic_init(&g_sync_ctx[idx]->peer->n_pending, 0);
	atomic_init(&g_sync_ctx[idx]->peer->n_used, 0);
	atomic_init(&g_sync_ctx[idx]->peer->n_used_blk, 0);
	atomic_init(&g_sync_ctx[idx]->peer->n_unsync, 0);
	atomic_init(&g_sync_ctx[idx]->peer->n_unsync_blk, 0);

	thread_pool = thpool_init(N_RSYNC_THREADS);

	// locks used for updating rsync metadata
	g_sync_ctx[idx]->peer->shared_rsync_addr_lock = (pthread_mutex_t *)mlfs_zalloc(sizeof(pthread_mutex_t));
	pthread_mutexattr_init(&attr);
	pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	pthread_mutex_init(g_sync_ctx[idx]->peer->shared_rsync_addr_lock, &attr);

	g_sync_ctx[idx]->peer->shared_rsync_n_lock = (pthread_mutex_t *)mlfs_zalloc(sizeof(pthread_mutex_t));
	pthread_mutexattr_init(&attr);
	pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	pthread_mutex_init(g_sync_ctx[idx]->peer->shared_rsync_n_lock, &attr);

	g_sync_ctx[idx]->peer->shared_rsync_order = (pthread_mutex_t *)mlfs_zalloc(sizeof(pthread_mutex_t));
	pthread_mutexattr_init(&attr);
	pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	pthread_mutex_init(g_sync_ctx[idx]->peer->shared_rsync_order, &attr);

	g_sync_ctx[idx]->peer->shared_rsync_cond_wait = (pthread_cond_t *)mlfs_zalloc(sizeof(pthread_cond_t));

	// This is just a temporary workaround to remove RDMA dependencies for local FS configs
	// TODO: Refactor!
	if(g_n_nodes > 1) {
		g_sync_ctx[idx]->peer->base_addr = mr_remote_addr(g_sync_ctx[idx]->peer->info->sockfd[SOCK_IO], MR_NVM_LOG)
			- (disk_sb[g_log_dev].log_start << g_block_size_shift);
	}
	else
		g_sync_ctx[idx]->peer->base_addr = 0;
	//assert(g_sync_ctx[idx]->peer->base_addr != 0);

	env = getenv("MLFS_RF");

	if(env)
		g_rsync_rf = atoi(env);

	env = getenv("MLFS_CHUNK_SIZE");

	if(env)
		g_rsync_chunk = atoi(env);

	env = getenv("MLFS_ENABLE_RPERSIST");

	if(env)
		g_enable_rpersist = atoi(env);

	assert(g_rsync_rf <= g_n_nodes);
}

uint32_t addr_to_logblk(int id, addr_t addr)
{
	uint32_t logblk = ((addr - g_sync_ctx[id]->peer->base_addr) >> g_block_size_shift);
	return logblk;
}

uint32_t next_rsync_metadata(uint32_t meta)
{
	uint32_t remaining = (uint8_t) ((meta & 0x00000300uL) >> 8) - 1;
	assert(remaining >= 0);
	return (remaining << 8) | (meta & 0xFFFFFCFFuL);
	
}

uint32_t generate_rsync_metadata(int peer_id, uint16_t seqn, addr_t size, int rotated, int ack)
{
	uint32_t block_nr = ((ALIGN(size, g_block_size_bytes)) >> g_block_size_shift);
	mlfs_rpc("generating rsync metadata: peer_id %d, seqn %u block_nr %u rotated %u ack %u\n",
			peer_id, seqn, block_nr, rotated, ack);
	return (((uint32_t) seqn << 30) | ((uint32_t) block_nr << 10) | ((uint8_t) (g_rsync_rf - 1) << 8)
			| ((uint16_t) peer_id << 2) | ((uint8_t) rotated << 1) | ((uint8_t) ack));
}

int decode_rsync_metadata(uint32_t meta, uint16_t *seq_n, addr_t *n_log_blk, int *steps, int *requester_id, int *rotated, int *ack)
{
	if(seq_n)
		*seq_n = (uint16_t) ((meta & 0xC0000000uL) >> 30);

	if(n_log_blk)
		*n_log_blk = (addr_t) ((meta & 0x3FFFFC00uL) >> 10);

	if(steps)
		*steps = (uint8_t) ((meta & 0x00000300uL) >> 8);

	if(requester_id)
		*requester_id = (uint8_t) ((meta & 0x000000FCuL) >> 2);

	if(rotated)
		*rotated = (int) ((meta & 2) >> 1);

	if(ack)
		*ack = (int) (meta & 1);

	mlfs_printf("decoding rsync metadata: peer_id %u seqn %u block_nr %lu rotated %u ack %u\n",
			*requester_id, *seq_n, *n_log_blk, *rotated, *ack);

	return 0;
}

void update_peer_sync_state(peer_meta_t *peer, uint16_t nr_log_blocks, int rotated)
{
	if(!peer)
		return;

	//FIXME: the order of increments matter: peer's n_unsync
	//should be incremented before the local n_digest; otherwise,
	//we might run into an issue where log transactions are
	//digested before being synced
	pthread_mutex_lock(peer->shared_rsync_n_lock);
	atomic_fetch_add(&peer->n_unsync_blk, nr_log_blocks);
	atomic_fetch_add(&peer->n_unsync, 1);

	if(rotated) {
		peer->avail_version++;
		peer->remote_end = peer->remote_start - 1;
		peer->local_start = g_sync_ctx[peer->id]->begin;
		peer->remote_start = g_sync_ctx[peer->id]->begin;
		mlfs_info("-- remote log tail is rotated: start %lu end %lu\n",
				peer->remote_start, peer->remote_end);
	}

	pthread_mutex_unlock(peer->shared_rsync_n_lock);

	//mlfs_printf("update peer %d sync state: n_unsync_blk %lu\n", peer->id, atomic_load(&peer->n_unsync_blk));
}

void update_peer_digest_state(peer_meta_t *peer, addr_t start_digest, int n_digested, int rotated)
{
	if(!peer) {
		panic("Invalid peer for digest response\n");
	}

	if (peer->n_digest_req == n_digested)  {
		mlfs_info("%s", "digest is done correctly\n");
		mlfs_info("%s", "-----------------------------------\n");
	} else {
		mlfs_printf("[D] digest is done insufficiently: req %u | done %u\n",
				peer->n_digest_req, n_digested);
		panic("Digest was incorrect!\n");
	}

	//printf("%s\n", "received digestion notification from slave");
	//pthread_mutex_lock(peer->shared_rsync_addr_lock);

	// rotations can sometimes be recorded by LibFS; detect them here
	if(peer->start_digest > start_digest)
		rotated = 1;

	peer->start_digest = start_digest;
	clear_peer_digesting(peer);
	//peer->digesting = 0;

	//atomic_fetch_sub(&g_sync_ctx->peer->n_used, n_digested);

	//FIXME: do not set to zero (this is incorrect); instead return and subtract n_digested_blks
	//atomic_store(&peer->n_used_blk, 0);

	if(rotated && (++peer->start_version == peer->avail_version)) {
		mlfs_info("-- remote log tail is rotated: start %lu end %lu\n",
				peer->remote_start, 0L);
		peer->remote_end = 0;
	}

	mlfs_printf("update peer %d digest state: version %u block_nr %lu\n", peer->id, peer->start_version, peer->start_digest);
	//pthread_mutex_unlock(peer->shared_rsync_addr_lock);
}

void wait_on_peer_digesting(peer_meta_t *peer)
{
	mlfs_info("%s\n", "waiting till peer finishes digesting");
	uint64_t tsc_begin, tsc_end;

	if(!peer)
		return;

#ifdef LIBFS
	if (enable_perf_stats)
		tsc_begin = asm_rdtsc();
#endif

	while(peer->digesting)
		cpu_relax();

	mlfs_info("%s\n", "ending wait for peer digest");

#ifdef LIBFS
	if (enable_perf_stats) {
		tsc_end = asm_rdtsc();
		g_perf_stats.digest_wait_tsc += (tsc_end - tsc_begin);
		g_perf_stats.digest_wait_nr++;
	}
#endif
}

void wait_till_digest_checkpoint(peer_meta_t *peer, int version, addr_t block_nr)
{
	mlfs_printf("wait till digest current (v:%d,tail:%lu) required (v:%d,tail:%lu)\n",
			peer->start_version, peer->start_digest, version, block_nr);

#if 0
	while(peer->digesting || peer->start_version < version ||
	    peer->start_version == version && block_nr > peer->start_digest) {
		//mlfs_printf("[WAIT] digest current (v:%d,tail:%lu) required (v:%d,tail:%lu) digesting:%s\n",
		//	peer->start_version, peer->start_digest, version, block_nr, peer->digesting?"YES":"NO");
		cpu_relax();
	}
#else
	// skip if we no more data needs to be digested
	//if(peer->start_version > version || (peer->start_version == version && peer->start_digest >= block_nr))
	//	return;

	while(peer->digesting || peer->start_version < version ||
	    peer->start_version == version && block_nr >= peer->start_digest) {
		cpu_relax();
	}

	//while(peer->digesting)
	//	cpu_relax();

	// otherwise, wait for digestion to complete

	/*
	if(block_nr) {
	while(!peer->digesting)
		cpu_relax();

	while(peer->digesting)
		cpu_relax();
	}
	*/
#endif

}

void wait_on_peer_replicating(peer_meta_t *peer, int seqn)
{

	if(!peer)
		return;

	mlfs_info("%s\n", "waiting till peer finishes replication");
	/*
	uint64_t tsc_begin, tsc_end;
	if (enable_perf_stats)
		tsc_begin = asm_rdtsc();
	*/

	while(peer->syncing[seqn])
		cpu_relax();
	mlfs_info("%s\n", "ending wait for peer replication");

	/*
	if (enable_perf_stats) {
		tsc_end = asm_rdtsc();
		g_perf_stats.digest_wait_tsc += (tsc_end - tsc_begin);
		g_perf_stats.digest_wait_nr++;
	}
	*/
}

peer_meta_t* get_next_peer()
{
	if(g_n_nodes > 1)
		return g_sync_ctx[0]->peer;
	else
		return NULL;
}

// forward rsync to the next node in the chain
int mlfs_do_rsync_forward(int node_id, uint32_t imm)
{
	struct list_head *rdma_entries;
	uint64_t tsc_begin;
	uint32_t n_loghdrs;

	pthread_mutex_lock(g_sync_ctx[node_id]->peer->shared_rsync_addr_lock);

	//generate rsync metadata
	n_loghdrs = create_rsync(node_id, &rdma_entries, 0, 0);

	//generate mock rsync metadata (useful for debugging)-----
	//ret = create_rsync_mock(&rdma_entries, g_fs_log->start_blk);
	//--------------------------------------------------------

	//no data to rsync
	if(!n_loghdrs) {
		mlfs_debug("%s\n", "Ignoring rsync call; slave node is up-to-date");
		pthread_mutex_unlock(g_sync_ctx[node_id]->peer->shared_rsync_addr_lock);
		return -1;
	}

#ifdef LIBFS
	if (enable_perf_stats) {
	        g_perf_stats.rsync_ops += 1;	
		tsc_begin = asm_rdtscp();
	}
#endif

	// generate next imm value
	uint32_t next_imm = next_rsync_metadata(imm);

	rpc_replicate_log_async(g_sync_ctx[node_id]->peer, rdma_entries, next_imm);

	atomic_fetch_add(&g_sync_ctx[node_id]->peer->n_digest, n_loghdrs);
	atomic_fetch_sub(&g_sync_ctx[node_id]->peer->n_pending, n_loghdrs);

	pthread_mutex_unlock(g_sync_ctx[node_id]->peer->shared_rsync_addr_lock);

#ifdef LIBFS
	if (enable_perf_stats)
		g_perf_stats.rdma_write_time_tsc += (asm_rdtscp() - tsc_begin);
#endif
	
	//if (enable_perf_stats) 
	//	show_libfs_stats();

	return 0;
}

int make_replication_request_async(uint32_t n_blk)
{
	uint8_t n = 0;

	if(g_n_nodes > 1 && atomic_load(&get_next_peer()->n_unsync_blk) > 0) {
#if 0
		while(n < N_RSYNC_THREADS && cmpxchg(&peer->outstanding, n, n+1) != n) {
			n = peer->outstanding;
		}

		if(n < N_RSYNC_THREADS) {
			mlfs_muffled("inc - remaining rsyncs: %d\n", n+1);
			thpool_add_work(thread_pool, replication_worker, (void *)peer);
			return 0;
		} else
			return -EBUSY;
#else
		thpool_add_work(thread_pool, replication_worker, (void *)&n_blk);
		return 0;
#endif
	}
	else
		return -EBUSY;

#if 0
	if (peer->outstanding < N_RSYNC_THREADS && atomic_load(&peer->n_unsync_blk) > 0) {
		peer->outstanding++;

	} else
		return -EBUSY;
#endif
}

int make_replication_request_sync(peer_meta_t *peer)
{
	struct list_head *rdma_entries;
	uint64_t tsc_begin;
	uint32_t n_loghdrs;

	// no need to replicate if we only have a single node 
	if(g_n_nodes == 1)
		return -EBUSY;

	pthread_mutex_lock(g_sync_ctx[peer->id]->peer->shared_rsync_addr_lock);

	//generate rsync metadata
	n_loghdrs = create_rsync(peer->id, &rdma_entries, 0, 0);

	//generate mock rsync metadata (useful for debugging)-----
	//ret = create_rsync_mock(&rdma_entries, g_fs_log->start_blk);
	//--------------------------------------------------------

	//no data to rsync
	if(!n_loghdrs) {
		mlfs_debug("%s\n", "Ignoring rsync call; slave node is up-to-date");
		pthread_mutex_unlock(g_sync_ctx[peer->id]->peer->shared_rsync_addr_lock);
		return -1;
	}

#ifdef LIBFS
	if (enable_perf_stats) {
	        g_perf_stats.rsync_ops += 1;	
		tsc_begin = asm_rdtscp();
	}
#endif

	rpc_replicate_log_sync(peer, rdma_entries, 0);

	atomic_fetch_add(&g_sync_ctx[peer->id]->peer->n_digest, n_loghdrs);
	atomic_fetch_sub(&g_sync_ctx[peer->id]->peer->n_pending, n_loghdrs);

	pthread_mutex_unlock(g_sync_ctx[peer->id]->peer->shared_rsync_addr_lock);

	mlfs_rpc("DEBUG sync n_digest %u\n", atomic_load(&g_sync_ctx[peer->id]->peer->n_digest));

#ifdef LIBFS
	if (enable_perf_stats)
		g_perf_stats.rdma_write_time_tsc += (asm_rdtscp() - tsc_begin);
#endif
	
	//if (enable_perf_stats) 
	//	show_libfs_stats();

	return 0;
}

uint32_t create_rsync(int id, struct list_head **rdma_entries, uint32_t n_blk, int force_digest)
{
	uint64_t tsc_begin;
	addr_t current_local_start, current_remote_start, cond_value;

	if (enable_perf_stats)
		tsc_begin = asm_rdtscp();

	//pthread_mutex_lock(g_sync_ctx[id]->peer->shared_rsync_addr_lock);

	//take a snapshot of sync parameters (necessary since rsyncs can be issued concurrently)
	int ret = start_rsync_session(g_sync_ctx[id]->peer, n_blk);

	//TODO: check for two conditions:
	//case 1: if g_sync_ctx->n_used_blk + g_sync_ctx->peer->n_unsync_blk > g_sync_ctx->size
	//		block until a digestion notification is sent & ack received
	//case 2: g_sync_ctx->n_used_blk > 'digest threshold'
	//		trigger remote digest notification (non-blocking)

	//if(!g_sync_ctx->peer->digesting && g_sync_ctx->peer->n_used_blk_nonatm > g_sync_ctx->size/5) {
	/* 
	if(!g_sync_ctx->peer->digesting && force_digest) {
		char cmd_buf[RPC_MSG_BYTES] = {0};
		int percent = 100; //statically set to 100% digest
		
		sprintf(cmd_buf, "|digest |%d|%u|%lu", percent, g_sync_ctx->peer->n_used_nonatm, g_sync_ctx->peer->remote_end);
		trigger_signal((char*)cmd_buf);

		g_sync_ctx->peer->digesting = 1;

		mlfs_info("trigger remote digest: percent[%d] n_digest[%d] remote_end[%lu]\n",
				percent, g_sync_ctx->peer->n_used_nonatm, g_sync_ctx->peer->remote_end);
		
		//zero out counters
		atomic_store(&g_sync_ctx->peer->n_used, 0);
		atomic_store(&g_sync_ctx->peer->n_used_blk, 0);
	}
	else if(g_sync_ctx->peer->digesting && (g_sync_ctx->peer->n_used_blk_nonatm
		       	+ g_sync_ctx->peer->n_unsync_blk_nonatm > g_sync_ctx->size*9/10)) {
		panic("[error] unable to rsync: remote log is full\n");
	}
	*/

	//nothing to rsync
	if(ret) {
		//mlfs_info("nothing to rsync. n_unsync = %u n_threshold = %u\n", session->n_unsync_blk, g_rsync_chunk);
		//pthread_mutex_unlock(g_sync_ctx[session->id]->peer->shared_rsync_addr_lock);
		end_rsync_session(g_sync_ctx[session->id]->peer);
		return 0;
	}

	//we save the value of local_start before rsync to escape conditional mutex (in case of coalescing)
	cond_value = session->local_start;

	//ensure correctness of local_start for rsync; sometimes, it'll be invalid if the
       	//append at local_start resulted in a wrap around
	if(session->end && session->local_start > session->end) {
		reset_log_ptr(&session->local_start);
		reset_log_ptr(&g_sync_ctx[id]->peer->local_start);
	}

	current_local_start = session->local_start;
	current_remote_start = session->remote_start;

	//update peer metadata - 3 globals (local_start, n_unsync_blk, n_unsync)
	//we know their values in advance
	move_log_ptr(session->id, &g_sync_ctx[id]->peer->local_start, 0, session->n_unsync_blk);
	atomic_fetch_sub(&g_sync_ctx[id]->peer->n_unsync_blk, session->n_unsync_blk);
	atomic_fetch_sub(&g_sync_ctx[id]->peer->n_unsync, session->n_unsync);

	assert(atomic_load(&g_sync_ctx[id]->peer->n_unsync) >= 0);


#ifdef COALESCING
//--------------------[Coalescing]-------------------------
//---------------------------------------------------------

	//pthread_mutex_unlock(g_sync_ctx[id]->peer->shared_rsync_addr_lock);
	
	struct replay_list *replay_list = (struct replay_list *) mlfs_zalloc(
			sizeof(struct replay_list));

	coalesce_log_and_produce_replay(replay_list);

#ifdef LIBFS
	if(enable_perf_stats) {
		g_perf_stats.coalescing_log_time_tsc += asm_rdtscp() - tsc_begin;
		tsc_begin = asm_rdtscp();
	}
#endif

	//------Ordering barrier------
	//
	//the follwoing conditional wait insures that threads will exit in the same order they have acquired
	//the shared_rsync_addr_lock; this allows g_sync_ctx->peer metadata to be updated correctly

	//Example on race condition:
	//
	//Starting state:
	//local_start = remote_start = 1
	//n_unsync = n_tosync = 4 (for both threads)
	//
	//Execution:
	//Thread 1 - Updates (local_start) 1-->5
	//Thread 2 - Updates (local_start) 5-->9
	//Thread 2 - Coalesces & Updates (remote_start) 1-->5
	//Thread 1 - Coalesces & Updates (remote_start) 5-->9
	//
	//Outcome:
	//Thread 1 sends log segment [1,5) to be appended to remote node in region [5,9)
	//Thread 2 sends log segment [5,9) to be appended to remote node in region [1,5)
	//
	//Local and remote logs now have different transaction ordering and are inconsistent

	mlfs_debug("[%d] checking wait condition. cond_value: %lu, cond_check: %lu\n",
			tid, cond_value, atomic_load(&g_sync_ctx[id]->peer->cond_check));
	//TODO: is there a more efficient way to implement this?
	pthread_mutex_lock(g_sync_ctx[id]->peer->shared_rsync_order);
	while (cond_value != atomic_load(&g_sync_ctx[id]->peer->cond_check))
		pthread_cond_wait(g_sync_ctx[id]->peer->shared_rsync_cond_wait, g_sync_ctx[id]->peer->shared_rsync_order);

	pthread_mutex_unlock(g_sync_ctx[id]->peer->shared_rsync_order);
	//---------End barrier--------
	//After escaping the barrier we use the remote_start value set by the previous thread
	session->remote_start = g_sync_ctx[id]->peer->remote_start;

	generate_rsync_msgs_using_replay(replay_list, 1);

	//update the 4 remaining globals (only known after coalescing)
	//atomic_fetch_add(&g_sync_ctx->peer->n_digest, *n_digest_update);
	atomic_fetch_add(&g_sync_ctx[id]->peer->n_used, session->n_tosync);
	atomic_fetch_add(&g_sync_ctx[id]->peer->n_used_blk, session->n_tosync_blk);
	g_sync_ctx[id]->peer->remote_start = session->remote_start;
	if(session->remote_end) {
		g_sync_ctx[id]->peer->remote_end = session->remote_end;
		g_sync_ctx[id]->peer->avail_version++;
	}

	//updating cond_check will allow the thread immediately behind us to get across the 'Ordering barrier'
	//note: we need to update the rest of the globals BEFORE we perform this update
	pthread_mutex_lock(g_sync_ctx[id]->peer->shared_rsync_order);
	mlfs_debug("[%d] updating cond_check to %lu\n", tid, session->local_start);
	atomic_store(&g_sync_ctx[id]->peer->cond_check, session->local_start);
	pthread_cond_broadcast(g_sync_ctx[id]->peer->shared_rsync_cond_wait);
	pthread_mutex_unlock(g_sync_ctx[id]->peer->shared_rsync_order);

#else
//------------------[No coalescing]------------------------
//---------------------------------------------------------
	generate_rsync_msgs(0, 0);

	mlfs_assert(session->n_unsync_blk == session->n_tosync_blk);
	mlfs_assert(session->local_start == session->remote_start);

	//printf("debug: remote_log_used %u remote_log_total %u to_send %u\n", atomic_load(&g_sync_ctx->peer->n_used_blk), g_sync_ctx->size - 1, session->n_tosync_blk); 

	//update the 4 remaining globals before unlocking
	//atomic_fetch_add(&g_sync_ctx->peer->n_digest, *n_digest_update);
	atomic_fetch_add(&g_sync_ctx[id]->peer->n_used, session->n_tosync);
	atomic_fetch_add(&g_sync_ctx[id]->peer->n_used_blk, session->n_tosync_blk);
	g_sync_ctx[id]->peer->remote_start = session->remote_start;
	if(session->remote_end) {
		g_sync_ctx[id]->peer->remote_end = session->remote_end;
		g_sync_ctx[id]->peer->avail_version++;
	}

	//pthread_mutex_unlock(g_sync_ctx[id]->peer->shared_rsync_addr_lock);

#endif
	assert(session->intervals->count == 0);

	*rdma_entries = &session->rdma_entries;

	//mlfs_debug("rsync local |%lu|%u\n", current_local_start, session->n_unsync);
	mlfs_rpc("rsync |%lu|%u\n", current_remote_start, session->n_tosync);

#ifdef LIBFS
	if (enable_perf_stats) {
		g_perf_stats.n_rsync_blks += session->n_unsync_blk;
		g_perf_stats.n_rsync_blks_skipped += (session->n_unsync_blk - session->n_tosync_blk);
		g_perf_stats.calculating_sync_interval_time_tsc += asm_rdtscp() - tsc_begin;
	}
#endif

	uint32_t n_loghdrs = session->n_tosync;

	end_rsync_session(g_sync_ctx[id]->peer);

	return n_loghdrs;	
}

static void generate_rsync_msgs_using_replay(struct replay_list *replay_list, int destroy)
{
	mlfs_debug("local_start: %lu, remote_start: %lu, sync_size: %lu\n", session->local_start, session->remote_start,
			session-> n_unsync_blk);
	struct list_head *l, *tmp;
	loghdr_t *current_loghdr;
	loghdr_t *prev_loghdr;
	addr_t current_loghdr_blk;
	addr_t prev_loghdr_blk;
	uint8_t *node_type;
	addr_t blocks_to_sync = 0;
	addr_t blocks_skipped = 0;
	int initialized = 0;

	int id = session->id;
	
	prev_loghdr_blk = g_sync_ctx[id]->begin-1;

	list_for_each_safe(l, tmp, &replay_list->head) {
		node_type = (uint8_t *)l + sizeof(struct list_head);
		mlfs_assert(*node_type < 6);
	
		switch(*node_type) {
			case NTYPE_I: {
				i_replay_t *item;
				item = (i_replay_t *)container_of(l, i_replay_t, list);
				current_loghdr_blk = item->blknr;
				if(destroy){
					HASH_DEL(replay_list->i_digest_hash, item);
					list_del(l);
					mlfs_free(item);
				}
				break;
			}
			case NTYPE_D: {
				panic("deprecated operation: NTYPE_D\n");
				break;
			}
			case NTYPE_F: {
				f_replay_t *item;
				item = (f_replay_t *)container_of(l, f_replay_t, list);
				if(destroy) {
					f_iovec_t *f_iovec, *f_tmp;
					HASH_DEL(replay_list->f_digest_hash, item);
					list_for_each_entry_safe(f_iovec, f_tmp, 
						&item->iovec_list, iov_list) {
					//printf("item->key.inum: %u | item->key.ver: %u | f_iovec->node_type: %d\n",
					//		item->key.inum, item->key.ver, f_iovec->node_type);
					list_del(&f_iovec->iov_list);
					//no need to free here; will call later on
					//mlfs_free(f_iovec);
					}
					list_del(l);
					mlfs_free(item);
				}	
				break;
			}
			case NTYPE_U: {
				u_replay_t *item;
				item = (u_replay_t *)container_of(l, u_replay_t, list);
				current_loghdr_blk = item->blknr;
				if(destroy) {
					HASH_DEL(replay_list->u_digest_hash, item);
					list_del(l);
					mlfs_free(item);
				}
				break;
			}
			case NTYPE_V: {
				f_iovec_t *item;
				item = (f_iovec_t *)container_of(l, f_iovec_t, list);
				current_loghdr_blk = item->blknr;
				if(destroy) {
					list_del(l);
					mlfs_free(item);
				}
				break;
			}
			default:
				panic("unsupported node type!\n");
		}

		//file type inodes aren't linked to log entries; only their iovecs. Skip!
		//we also ignore nodes pointing to the same log entries
		if(*node_type == NTYPE_F || current_loghdr_blk == prev_loghdr_blk)
			continue;

		//log header pointers should never point to superblock
		assert(current_loghdr_blk != g_sync_ctx[id]->begin-1);

		current_loghdr = (loghdr_t *)(g_sync_ctx[id]->base_addr + 
				(current_loghdr_blk << g_block_size_shift));

		if(!initialized) {
			blocks_skipped += nr_blks_between_ptrs
				(session->id, session->local_start, current_loghdr_blk);
			session->local_start = current_loghdr_blk;
			initialized = 1;
		}
		else if(next_loghdr(prev_loghdr_blk, prev_loghdr) != current_loghdr_blk) {
			mlfs_debug("detecting a log break; prev_blk: %lu, prev_loghdr->next_blk:"
				       "%lu, current_loghdr: %lu\n", prev_loghdr_blk, 
				        next_loghdr(prev_loghdr_blk, prev_loghdr), current_loghdr_blk);
			generate_rsync_msgs(blocks_to_sync, 1);

			//update local_start due to gap resulting from coalesced txs
			blocks_skipped += nr_blks_between_ptrs
				(session->id, session->local_start, current_loghdr_blk);
			session->local_start = current_loghdr_blk;
			//reset blocks_to_sync
			blocks_to_sync = 0;
		}

		blocks_to_sync += current_loghdr->nr_log_blocks;

		prev_loghdr_blk = current_loghdr_blk;
		prev_loghdr = current_loghdr;
	}

	//process any remaining blocks and convert to rdma msg metadata
	generate_rsync_msgs(blocks_to_sync, 0);

	mlfs_debug("shifting loghdr pointer to final position. shift by (n_unsync_blk[%lu]"
	       " - n_tosync_blk[%lu] - blocks_skipped[%lu]) = %lu\n", session->n_unsync_blk, 
	       session->n_tosync_blk,  blocks_skipped, (session->n_unsync_blk - session->n_tosync_blk
		       - blocks_skipped));
	
	assert((int)(session->n_unsync_blk - session->n_tosync_blk - blocks_skipped) >= 0);

	//move local_start to end
	move_log_ptr(session->id, &session->local_start, 0, session->n_unsync_blk - 
			session->n_tosync_blk - blocks_skipped);

	//verify that # of coalesced blocks is <= uncoalesced blocks
	assert(session->n_tosync_blk <= session->n_unsync_blk);

	//if (enable_perf_stats) {
	//	g_perf_stats.n_rsync_blks += session->n_tosync_blk;
	//	g_perf_stats.n_rsync_blks_skipped += (session->n_unsync_blk - session->n_tosync_blk);
	//}

	if(destroy) {
		id_version_t *id_v, *tmp;
		HASH_ITER(hh, replay_list->id_version_hash, id_v, tmp) {
			HASH_DEL(replay_list->id_version_hash, id_v);
			mlfs_free(id_v);	
		}
		HASH_CLEAR(hh, replay_list->i_digest_hash);
		HASH_CLEAR(hh, replay_list->f_digest_hash);
		HASH_CLEAR(hh, replay_list->u_digest_hash);
		HASH_CLEAR(hh, replay_list->id_version_hash);

		list_del(&replay_list->head);
	}
}

void coalesce_log_and_produce_replay(struct replay_list *replay_list)
{
	mlfs_debug("%s\n", "coalescing log segment");

	replay_list->i_digest_hash = NULL;
	replay_list->f_digest_hash = NULL;
	replay_list->u_digest_hash = NULL;
	replay_list->id_version_hash = NULL;

	INIT_LIST_HEAD(&replay_list->head);

	addr_t loghdr_blk = session->local_start;
	loghdr_t *loghdr;
	addr_t counted_blks = 0;
	uint16_t logtx_size;

	// digest log entries
	while (counted_blks < session->n_unsync_blk) {
		//pid_t tid = syscall(SYS_gettid);
		loghdr = (loghdr_t *)(g_sync_ctx[session->id]->base_addr + 
					(loghdr_blk << g_block_size_shift));	

		if (loghdr->inuse != LH_COMMIT_MAGIC)
			panic("CRITICAL ISSUE: undefined log entry during replay list processing");

		counted_blks += loghdr->nr_log_blocks;

		coalesce_log_entry(loghdr_blk, loghdr, replay_list);
		
		loghdr_blk = next_loghdr(loghdr_blk, loghdr);
	}

	assert(session->n_unsync_blk == counted_blks);
}

#if 0
// count # of log transactions inside a log segment
static uint16_t nrs_loghdrs(addr_t start, addr_t size)
{
	addr_t acc_size;
	addr_t loghdr_counter;
	uint16_t current_size;
	addr_t current_blk;

	assert(start + size <= g_sync_ctx->size);
	mlfs_debug("counting loghdrs starting from %lu in the next %lu blks\n", start, size);
	loghdr_counter = 1;
	current_blk = start;

	loghdr_t *current_loghdr = (loghdr_t *)(g_sync_ctx->base_addr + 
			(current_blk << g_block_size_shift));

	acc_size = current_loghdr->nr_log_blocks;

	//count # of log tx beginning from 'start' blk
	while(size > acc_size) {
		current_blk = next_loghdr(current_blk, current_loghdr);
		current_loghdr = (loghdr_t *)(g_sync_ctx->base_addr + 
			(current_blk << g_block_size_shift));

		if (current_loghdr->inuse != LH_COMMIT_MAGIC) {
			mlfs_debug("Possible inconsistency in loghdr %lu\n", current_blk);
			assert(current_loghdr->inuse == 0);
			mlfs_free(current_loghdr);
			break;
		}
		acc_size += current_loghdr->nr_log_blocks;
		//mlfs_debug("acc_size: %lu\n", acc_size);
		loghdr_counter++;
	}

	if(acc_size != size) {
		mlfs_debug("acc_size: %lu | size: %lu | current_blk: %lu | next_blk: %lu\n",
				acc_size, size, current_blk, next_loghdr(current_blk, current_loghdr));
		panic("unable to count loghdrs - log ended prematurely\n");

	}
	assert(acc_size == size);
	return loghdr_counter;	
}

#endif

//shifts loghdr blk_nr to new location by 'size' blks, taking into account wraparound
//note: for remote logs, we use the 'lag' parameter to account for lag between local & 
//remote log ptrs (in case of coalescing).
void move_log_ptr(int id, addr_t *log_ptr, addr_t lag, addr_t size)
{

	if(size == 0)
		return;
	
	mlfs_debug("moving log blk %lu with lag[%lu] by %lu blks\n", *log_ptr, lag, size);

	/* 		 
	 *
	 *  Here we have two cases:
	 *
	 *  (i) log_ptr + size > ctx->end
	 *
	 *  <ctx->begin ........ log_ptr(new) ....... log_ptr(old) ........ ctx->end> *WRAP*
	 *		  after			            	    before
	 *   
	 *   N.B. size = before + after
	 *
	 *  (ii) log_ptr + size <= ctx->end
	 *
	 *  <ctx->begin ....... log_ptr(old) ........ log_ptr(new) ........ ctx->end>
	 *				       size
	 *
	 */

	//compute remaining blk count from 'start' until wrap around
	addr_t before = nr_blks_before_wrap(id, *log_ptr, lag, size);
	mlfs_debug("log id %d before = %lu\n", id, before);
	if(*log_ptr + size > g_sync_ctx[session->id]->size) {
		*log_ptr = g_sync_ctx[session->id]->begin + (size - before);
		mlfs_debug("log blk moved to %lu\n", *log_ptr);
	}
	else {
		*log_ptr = *log_ptr + size;
		mlfs_debug("log blk moved to %lu\n", *log_ptr);
	}
}

static void reset_log_ptr(addr_t *blk_nr)
{
	*blk_nr = g_sync_ctx[session->id]->begin;
}

addr_t nr_blks_between_ptrs(int id, addr_t start, addr_t target)
{
	//FIXME: the sum of blocks between two pointers still includes uncommitted logs; can this lead to inconsistencies?
	addr_t blks_in_between;

	if(start == target)
		return 0UL;

	//check if there's a wrap around
	if(target < start) {
		//get block size till cut-off point
		blks_in_between = nr_blks_before_wrap(id, start, 0, g_sync_ctx[id]->size - start + 2);

		blks_in_between += (target - g_sync_ctx[id]->begin);
	}
	else
		blks_in_between = (target - start);

	mlfs_debug("%lu blks in between %lu and %lu\n", blks_in_between, start, target);
	return blks_in_between;
}

//TODO: rework function so that nr of blocks replicated == n_blk (currently it's >=)
static int start_rsync_session(peer_meta_t *peer, uint32_t n_blk)
{
	int id = peer->id;
	//setvbuf(stdout, (char*)NULL, _IONBF, 0);
	session  = (sync_meta_t*) mlfs_zalloc(sizeof(sync_meta_t));
	session->id = id;
	session->intervals = (struct sync_list*) mlfs_zalloc(sizeof(struct sync_list));
	session->end = atomic_load(g_sync_ctx[id]->end);
	session->local_start = peer->local_start;
	session->remote_start = peer->remote_start;
	session->intervals->remote_start = session->remote_start;

	//make sure both of these values are synchronized for sanity checking
	pthread_mutex_lock(g_sync_ctx[id]->peer->shared_rsync_n_lock);
	session->n_unsync = atomic_load(&peer->n_unsync);
	session->n_unsync_blk = atomic_load(&peer->n_unsync_blk);
	pthread_mutex_unlock(g_sync_ctx[id]->peer->shared_rsync_n_lock);

	//TODO: merge with log_alloc (defined in log.c)
	addr_t nr_used_blk = 0;
	if (peer->avail_version == peer->start_version) {
		mlfs_assert(peer->remote_start >= peer->start_digest);
		nr_used_blk = peer->remote_start - peer->start_digest; 
	} else {
		mlfs_assert(peer->remote_start <= peer->start_digest);
		nr_used_blk = (g_sync_ctx[id]->size - peer->start_digest);
		nr_used_blk += (peer->remote_start - g_sync_ctx[id]->begin);
	}

	//check if remote log has undigested data that could be overwritten
	if(nr_used_blk + session->n_unsync_blk + g_sync_ctx[id]->begin > g_sync_ctx[id]->size) {
		// This condition shouldn't occur, since we should always send digest requests
		// to peer before its log gets full.
		if(!g_sync_ctx[id]->peer->digesting) {
			panic("can't rsync; peer log is full, yet it is not digesting.\n");
		}

		wait_on_peer_digesting(get_next_peer());
	}

	//mlfs_printf("try rsync: chunk_blks %lu remote_used_blks %lu remote_log_size %lu\n",
	//	session->n_unsync_blk, nr_used_blk, g_sync_ctx->size - g_sync_ctx->begin + 1);

	if(session->n_unsync_blk && session->n_unsync_blk >= n_blk) {
		mlfs_debug("%s\n", " +++ start rsync session");

		session->n_tosync = session->n_unsync; //start at ceiling and decrement with coalescing

		session->peer_base_addr = peer->base_addr;
		INIT_LIST_HEAD(&session->intervals->head);
		INIT_LIST_HEAD(&session->rdma_entries);

		mlfs_debug("log info : begin %lu size %lu base %lx\n",
				g_sync_ctx[id]->begin, g_sync_ctx[id]->size, g_sync_ctx[id]->base_addr);
		mlfs_debug("sync info: n_unsync %u n_unsync_blk %lu local_start: %lu remote_start: %lu\n",
				session->n_unsync, session->n_unsync_blk, session->local_start,
				session->remote_start);
		return 0;
	}
	else
		return -1;
}

void end_rsync_session(peer_meta_t *peer)
{
	if(session->n_tosync) {
		atomic_fetch_add(&g_sync_ctx[session->id]->peer->n_pending, session->n_tosync);
		mlfs_debug("rsync complete: local_start %lu remote_start %lu n_digest %u\n",
				session->local_start, session->remote_start,
				atomic_load(&g_sync_ctx[session->id]->peer->n_digest));
		mlfs_debug("%s\n", "--- end rsync session");
	}

	free(session);
	session = NULL;
}

static void generate_rsync_msgs(addr_t sync_size, int loop_mode)
{
	addr_t interval_size, local_interval_size, remote_interval_size;
	addr_t size;

	if(sync_size)
		size = sync_size;
	else
		size = session->n_unsync_blk;

	while(size > 0) {

		//only create contiguous sync intervals; limit interval_size if
		//wrap around(s) occur at local and/or remote log
		local_interval_size = nr_blks_before_wrap
			(session->id, session->local_start, 0, size);
		remote_interval_size = nr_blks_before_wrap
			(session->id, session->remote_start, session->local_start - session->remote_start, size);

		interval_size = min(local_interval_size, remote_interval_size);

		mlfs_debug("intervals: local->%lu remote->%lu size:%lu\n",
				session->local_start, session->remote_start, interval_size);

		//assert(interval_size != 0);

#if 0
		//we hit wrap around on local log; wrap local log pointer
		if(local_interval_size == 0)
			session->local_start = g_sync_ctx->begin;

		//can't find any space to fill from remote pointer until end of log;
		//this occurs when there is space available at remote log but the
		//next log tx is too big to be appended contiguously
		if(remote_interval_size < size) {
			//wrap remote log pointer and convert any existing intervals
			//to rdma msg metadata; this is done since rdma-writes can't
			//perform scatter writes (i.e. write into non-cont. locations
			//at the remote end)

			//TODO need to modify this logic to support rdma verbs that
			//allow scatter writes
			session->remote_start = g_sync_ctx->begin;
			rdma_finalize();
		}

		if(interval_size == 0)
			continue;
#endif
		//adding interval [session->local_start, session->local_start+interval_size] to sync list
		rdma_add_sge(interval_size); 

		//convert any existing sge elements to rdma msg. occurs when:
		//(1) remote log wrap around detected (make sure interval != 0; otherwise msg is empty)
		//(2) num of sync_intervals is equal to MAX_RDMA_SGE (i.e. max allowed by rdma write)
		if((remote_interval_size == interval_size && interval_size < size && interval_size != 0)
		//			|| (size == interval_size && !loop_mode)
					|| session->intervals->count == MAX_RDMA_SGE)
			rdma_finalize();
	
	
		//update sync metadata (if we wrapped around, reset *_start to beginning of log)
		//remote pointer is modified first, since it relies on value of current local start
		if(remote_interval_size == interval_size && interval_size < size) {
			//before we reset remote_start, calculate remote_end for remote digests
			session->remote_end = session->remote_start + interval_size - 1;	
			reset_log_ptr(&session->remote_start);
			mlfs_printf("-- remote log tail is rotated: start %lu end %lu\n",
					session->remote_start, session->remote_end);
			session->rotated = 1;
		}
		else
			move_log_ptr(session->id, &session->remote_start, (session->local_start
					- session->remote_start), interval_size);

		if(local_interval_size == interval_size && interval_size < size)
			reset_log_ptr(&session->local_start);	
		else
			move_log_ptr(session->id, &session->local_start, 0, interval_size);

		session->n_tosync_blk += interval_size;
		
		//decrement # of blks to sync
		size -= interval_size;
	}
	assert(size == 0);

	//convert any remaining intervals to rdma msg if we're not calling this function in a loop
 	if(!loop_mode)
		rdma_finalize();
}


static void coalesce_log_entry(addr_t loghdr_blkno, loghdr_t *loghdr, struct replay_list *replay_list)
{
	int i, ret;
	addr_t blknr;
	uint16_t nr_entries;

	nr_entries = loghdr->n;

	for (i = 0; i < nr_entries; i++) {
		//for rsyncs, we set the blknr of replay_list elements to loghdr_blkno
		blknr = loghdr_blkno;
		switch(loghdr->type[i]) {
			case L_TYPE_INODE_CREATE:
			case L_TYPE_INODE_UPDATE: {
				i_replay_t search, *item;
				memset(&search, 0, sizeof(i_replay_t));

				search.key.inum = loghdr->inode_no[i];

				//USE HASH MAP INSTEAD OF ARRAY FOR VERSION TABLE
				id_version_t *id_v;
				HASH_FIND_INT(replay_list->id_version_hash, &search.key.inum, id_v);

				if (loghdr->type[i] == L_TYPE_INODE_CREATE) { 
					//inode_version_table[search.key.inum]++;
					if(id_v == NULL) {
						id_v = (id_version_t *)mlfs_zalloc(sizeof(id_version_t));
						id_v->inum = search.key.inum;
						id_v->ver = 1;
						HASH_ADD_INT(replay_list->id_version_hash, inum, id_v);
					}
					else
						id_v->ver++;	
				}

				//search.key.ver = inode_version_table[search.key.inum];
				
				if(id_v == NULL)
					search.key.ver = 0;
				else
					search.key.ver = id_v->ver;

				HASH_FIND(hh, replay_list->i_digest_hash, &search.key,
						sizeof(replay_key_t), item);
				if (!item) {
					item = (i_replay_t *)mlfs_zalloc(sizeof(i_replay_t));
					item->key = search.key;
					item->node_type = NTYPE_I;
					list_add_tail(&item->list, &replay_list->head);

					// tag the inode coalecing starts from inode creation.
					// This is crucial information to decide whether 
					// unlink can skip or not.
					if (loghdr->type[i] == L_TYPE_INODE_CREATE) 
						item->create = 1;
					else
						item->create = 0;

					HASH_ADD(hh, replay_list->i_digest_hash, key,
							sizeof(replay_key_t), item);
					mlfs_debug("[INODE] inum %u (ver %u) - create %d\n",
							item->key.inum, item->key.ver, item->create);
				}
				// move blknr to point the up-to-date inode snapshot in the log.
				item->blknr = blknr;
#ifdef LIBFS
				if (enable_perf_stats)
					g_perf_stats.n_rsync++;
#endif
				break;
			}
			case L_TYPE_DIR_ADD:
			case L_TYPE_DIR_DEL: 
			case L_TYPE_DIR_RENAME: 
			case L_TYPE_FILE: {
				f_replay_t search, *item;
				f_iovec_t *f_iovec;
				f_blklist_t *_blk_list;
				lru_key_t k;
				offset_t iovec_key;
				int found = 0;

				memset(&search, 0, sizeof(f_replay_t));
				search.key.inum = loghdr->inode_no[i];
				//search.key.ver = inode_version_table[loghdr->inode_no[i]];

				//USE HASH MAP INSTEAD OF ARRAY FOR VERSION TABLE
				id_version_t *id_v;
				HASH_FIND_INT(replay_list->id_version_hash, &search.key.inum, id_v);
				if(id_v == NULL)
					search.key.ver = 0;
				else
					search.key.ver = id_v->ver;

				HASH_FIND(hh, replay_list->f_digest_hash, &search.key,
						sizeof(replay_key_t), item);
				if (!item) {
					item = (f_replay_t *)mlfs_zalloc(sizeof(f_replay_t));
					item->key = search.key;

					HASH_ADD(hh, replay_list->f_digest_hash, key,
							sizeof(replay_key_t), item);
					
					INIT_LIST_HEAD(&item->iovec_list);
					item->node_type = NTYPE_F;
					item->iovec_hash = NULL;
					list_add_tail(&item->list, &replay_list->head);

					mlfs_debug("[FILE-NEW] inum %u (ver %u)\n",
								item->key.inum, item->key.ver);
				}

#ifdef IOMERGE
				// IO data is merged if the same offset found.
				// Reduce amount IO when IO data has locality such as Zipf dist.
				// FIXME: currently iomerge works correctly when IO size is 
				// 4 KB and aligned.
				iovec_key = ALIGN_FLOOR(loghdr->data[i], g_block_size_bytes);

				if (loghdr->data[i] % g_block_size_bytes !=0 ||
						loghdr->length[i] != g_block_size_bytes) 
					panic("IO merge is not support current IO pattern\n");

				HASH_FIND(hh, item->iovec_hash, 
						&iovec_key, sizeof(offset_t), f_iovec);

				if (f_iovec && 
						(f_iovec->length == loghdr->length[i])) {
					f_iovec->offset = iovec_key;
					f_iovec->blknr = blknr;
					// TODO: merge data from loghdr->blocks to f_iovec buffer.
					found = 1;
				}

				if (!found) {
					f_iovec = (f_iovec_t *)mlfs_zalloc(sizeof(f_iovec_t));
					f_iovec->length = loghdr->length[i];
					f_iovec->offset = loghdr->data[i];
					f_iovec->blknr = blknr;
					f_iovec->node_type = NTYPE_V;
					INIT_LIST_HEAD(&f_iovec->list);
					INIT_LIST_HEAD(&f_iovec->iov_list);
					list_add_tail(&f_iovec->iov_list, &item->iovec_list);
					list_add_tail(&f_iovec->list, &replay_list->head);

					f_iovec->hash_key = iovec_key;
					HASH_ADD(hh, item->iovec_hash, hash_key,
							sizeof(offset_t), f_iovec);
				}
#else
				f_iovec = (f_iovec_t *)mlfs_zalloc(sizeof(f_iovec_t));
				f_iovec->length = loghdr->length[i];
				f_iovec->offset = loghdr->data[i];
				f_iovec->blknr = blknr;
				f_iovec->node_type = NTYPE_V;
				INIT_LIST_HEAD(&f_iovec->list);
				INIT_LIST_HEAD(&f_iovec->iov_list);
				list_add_tail(&f_iovec->iov_list, &item->iovec_list);
				list_add_tail(&f_iovec->list, &replay_list->head);
				
#endif	//IOMERGE
				mlfs_debug("[FILE-DATA] inum %u (ver %u) - data %lu\n",
							item->key.inum, item->key.ver, f_iovec->blknr);
#ifdef LIBFS
				if (enable_perf_stats)
					g_perf_stats.n_rsync++;
#endif
				break;
			}
			case L_TYPE_UNLINK: {
				// Got it! Kernfs can skip digest of related items.
				// clean-up inode, directory, file digest operations for the inode.
				uint32_t inum = loghdr->inode_no[i];
				i_replay_t i_search, *i_item;
				f_replay_t f_search, *f_item;
				u_replay_t u_search, *u_item;
				//d_replay_key_t d_key;
				f_iovec_t *f_iovec, *tmp;

				replay_key_t key;
				
				key.inum = loghdr->inode_no[i];
				id_version_t *id_v;

				//USE HASH MAP INSTEAD OF ARRAY FOR VERSION TABLE
				HASH_FIND_INT(replay_list->id_version_hash, &key.inum, id_v);
				if(id_v == NULL)
					key.ver = 0;
				else
					key.ver = id_v->ver;

				// This is required for structure key in UThash.
				memset(&i_search, 0, sizeof(i_replay_t));
				memset(&f_search, 0, sizeof(f_replay_t));
				memset(&u_search, 0, sizeof(u_replay_t));

				mlfs_debug("%s\n", "-------------------------------");

				// check inode digest info can skip.
				i_search.key.inum = key.inum;
				i_search.key.ver = key.ver;
				HASH_FIND(hh, replay_list->i_digest_hash, &i_search.key,
						sizeof(replay_key_t), i_item);
				//printf("UNLINK STATUS: i_item->: %d\n", i_item->create);
				if (i_item && i_item->create) {
					mlfs_debug("[INODE] inum %u (ver %u) --> SKIP\n", 
							i_item->key.inum, i_item->key.ver);
					// the unlink can skip and erase related i_items
					HASH_DEL(replay_list->i_digest_hash, i_item);
					list_del(&i_item->list);
					mlfs_free(i_item);
					session->n_tosync--;
#ifdef LIBFS
					if (enable_perf_stats)
						g_perf_stats.n_rsync_skipped++;
#endif
				} else {
					// the unlink must be applied. create a new unlink item.
					u_item = (u_replay_t *)mlfs_zalloc(sizeof(u_replay_t));
					u_item->key = key;
					u_item->node_type = NTYPE_U;
					u_item->blknr = blknr; 
					HASH_ADD(hh, replay_list->u_digest_hash, key,
							sizeof(replay_key_t), u_item);
					list_add_tail(&u_item->list, &replay_list->head);
					mlfs_debug("[ULINK] inum %u (ver %u)\n", 
							u_item->key.inum, u_item->key.ver);
					session->n_tosync--;
#ifdef LIBFS
					if (enable_perf_stats)
						g_perf_stats.n_rsync++;
#endif
				}

				// delete file digest info.
				f_search.key.inum = key.inum;
				f_search.key.ver = key.ver;

				HASH_FIND(hh, replay_list->f_digest_hash, &f_search.key,
						sizeof(replay_key_t), f_item);
				if (f_item) {
					list_for_each_entry_safe(f_iovec, tmp, 
							&f_item->iovec_list, iov_list) {
						list_del(&f_iovec->list);
						list_del(&f_iovec->iov_list);
						mlfs_free(f_iovec);
						session->n_tosync--;
#ifdef LIBFS
						if (enable_perf_stats)
							g_perf_stats.n_rsync_skipped++;
#endif
					}

					HASH_DEL(replay_list->f_digest_hash, f_item);
					list_del(&f_item->list);
					mlfs_free(f_item);
				}

				mlfs_debug("%s\n", "-------------------------------");
				break;
			}
			default: {
				printf("%s: digest type %d\n", __func__, loghdr->type[i]);
				panic("unsupported type of operation\n");
				break;
			}
		}
	}
}


static void rdma_add_sge(addr_t size)
{
	//size should always be a +ve value
	if(size <= 0)
		return;

	sync_interval_t *interval  = (sync_interval_t *) mlfs_zalloc(sizeof(sync_interval_t));
	interval->start = session->local_start;
	interval->end = interval->start + size - 1;

	//make sure interval is contiguous
	if(interval->end > g_sync_ctx[session->id]->size) {
		mlfs_debug("intrv start: %lu | intrv end: %lu | log end: %lu\n",
				interval->start, interval->end, g_sync_ctx[session->id]->size);
		panic("error producing sync interval");
	}

	INIT_LIST_HEAD(&interval->head);
	list_add_tail(&interval->head, &session->intervals->head);

	//update intervals list metadata (counters, etc.)
	//if this is the first gather element in rdma msg, set initial remote address
	if(session->intervals->count == 0)
		session->intervals->remote_start = session->remote_start;
	session->intervals->count++;
	session->intervals->n_blk += size;
}

static void rdma_finalize()
{
	//ignore calls when there are no sge elements
	if(session->intervals->count == 0)
		return;

	mlfs_debug("+++ create rdma - remote_start %lu n_blk %lu n_sge %u\n",
			session->intervals->remote_start, session->intervals->n_blk,
			session->intervals->count);
	struct rdma_meta_entry *rdma_entry = (struct rdma_meta_entry *)
		mlfs_zalloc(sizeof(struct rdma_meta_entry));
	rdma_entry->meta = (rdma_meta_t *) mlfs_zalloc(sizeof(rdma_meta_t)
			+ session->intervals->count * sizeof(struct ibv_sge));

	rdma_entry->meta->addr = session->peer_base_addr + 
		(session->intervals->remote_start << g_block_size_shift);

	rdma_entry->meta->length = session->intervals->n_blk << g_block_size_shift;
	rdma_entry->meta->sge_count = session->intervals->count;
	rdma_entry->meta->next = NULL;

	if(session->rotated) {
		rdma_entry->rotated = 1;
		session->rotated = 0;
	}

	//printf("peer base address = %lu \n", session->peer_base_addr);
	//printf("remote_start << g_block_size_shift = %lu\n", (session->intervals->remote_start << g_block_size_shift));
	//printf("rdma_entry->meta->address = %lu \n", rdma_entry->meta->address);
	//printf("rdma_entry->meta->total_len = %lu \n", (session->intervals->n_blk << g_block_size_shift));
	
	sync_interval_t *interval, *tmp;
	int i = 0;

	list_for_each_entry_safe(interval, tmp, &session->intervals->head, head) {
		assert(interval->end <= g_sync_ctx[session->id]->size);
		assert(interval->start <= interval->end);
		mlfs_debug("sge%d - local_start %lu n_blk %lu\n",
				i, interval->start, interval->end - interval->start + 1);
		rdma_entry->meta->sge_entries[i].addr = (uint64_t) (g_sync_ctx[session->id]->base_addr 
			+ (interval->start << g_block_size_shift));
		rdma_entry->meta->sge_entries[i].length = (interval->end
				- interval->start + 1) << g_block_size_shift;
		mlfs_debug("sge - addr %lu length %u\n", rdma_entry->meta->sge_entries[i].addr,
				rdma_entry->meta->sge_entries[i].length);
		list_del(&interval->head);
		mlfs_free(interval);
		i++;
	}
	mlfs_debug("%s\n", "--- end rdma");

	list_add_tail(&rdma_entry->head, &session->rdma_entries);
	
	//reset list counters
	session->intervals->count = 0;
	session->intervals->n_blk = 0;
}

static addr_t nr_blks_before_wrap(int id, addr_t start, addr_t lag, addr_t size)
{
	mlfs_debug("log traverse: type %s start_blk %lu size %lu\n", lag?"remote":"local", start+lag, size);

	//check for wrap around and compute block sizes till cut-off
	if(start + size > g_sync_ctx[id]->size) {
		//computationally intensive for larger #s of loghdrs
		//but only happens when wraparounds occur - which is infrequent
		//TODO: need to optimize if prior assumption becomes invalid
		
		int step = 0;
		addr_t nr_blks = 0;
		addr_t current_blk = start + lag;

		loghdr_t *current_loghdr = (loghdr_t *)(g_sync_ctx[id]->base_addr + 
				(current_blk << g_block_size_shift));
	
		//compute all log tx sizes beginning from 'start' blk until we reach end
		while((start + nr_blks + current_loghdr->nr_log_blocks <= g_sync_ctx[id]->size)
				&& nr_blks + current_loghdr->nr_log_blocks <= size) {
			/*
			mlfs_debug("current_blk: %lu | current_size: %d | "
					"current_blk + current_size: %lu | original size: %lu | "
					"log_size: %lu | log_end: %lu | nr_blks: %lu\n",
					current_blk, current_loghdr->nr_log_blocks, current_blk
					+ current_loghdr->nr_log_blocks, size, g_sync_ctx->size + lag,
				       	atomic_load(g_sync_ctx->end), nr_blks); */

			step++;

			if (current_loghdr->inuse != LH_COMMIT_MAGIC) {
				mlfs_debug("cond check inuse != LH_COMMIT_MAGIC for loghdr %lu\n", current_blk);
				assert(nr_blks == size);
				break;
			}

			nr_blks += current_loghdr->nr_log_blocks;

			mlfs_debug("step%d: current_blk %lu nr_blks %u acc_size %lu\n",
				step, current_blk, current_loghdr->nr_log_blocks, nr_blks);

			//we are done here
			if(nr_blks == size)	
				break;

			current_blk = next_loghdr(current_blk, current_loghdr);
	
			current_loghdr = (loghdr_t *)(g_sync_ctx[id]->base_addr + 
				(current_blk << g_block_size_shift));

			/*
			mlfs_debug("current_blk: %lu | current_size: %d | "
					"current_blk + current_size: %lu | original size: %lu | "
					"log_size: %lu | log_end: %lu | nr_blks: %lu\n",
					current_blk, current_loghdr->nr_log_blocks, current_blk
					+ current_loghdr->nr_log_blocks, size, g_sync_ctx->size + lag,
				       	atomic_load(g_sync_ctx->end), nr_blks); */
		}
		return nr_blks;
	}
	else
		return size;
}

#if 0
//FIXME: no reason to do this calculation; loghdrs should instead carry
//a size attribute in place of pointers
static uint16_t get_logtx_size(loghdr_t *loghdr, addr_t blk_nr)
{
	uint16_t tx_size;
	if(loghdr->next_loghdr_blkno != 0 && loghdr->next_loghdr_blkno > blk_nr)
		tx_size = loghdr->next_loghdr_blkno - blk_nr;
	else {
		//we can't rely on next_loghdr_blkno to compute current log tx size
		tx_size = 1; //accounts for logheader block
		for (int i = 0; i < loghdr->n; i++)
			tx_size += (loghdr->length[i] >> g_block_size_shift);
	}
	mlfs_debug("[get_logtx_size] loghdr %lu size is %u blk(s)\n", blk_nr, tx_size);
	return tx_size;
}
#endif

// ------ LIBFS ONLY ---------------------
// ---------------------------------------
static void replication_worker(void *arg)
{
	//FIXME: pass peer_id
	struct list_head *rdma_entries;
	uint64_t tsc_begin;
	uint32_t n_loghdrs = 0;
	uint32_t n_blk = 0;

	if(arg)
		n_blk = *((uint32_t*)arg);

	pthread_mutex_lock(g_sync_ctx[0]->peer->shared_rsync_addr_lock);

	//generate rsync metadata
	n_loghdrs = create_rsync(0, &rdma_entries, n_blk,  0);

	//generate mock rsync metadata (useful for debugging)-----
	//ret = create_rsync_mock(&rdma_entries, g_fs_log->start_blk);
	//--------------------------------------------------------

	//no data to rsync
	if(!n_loghdrs) {
		mlfs_debug("%s\n", "Ignoring rsync call; slave node is up-to-date");
		pthread_mutex_unlock(g_sync_ctx[0]->peer->shared_rsync_addr_lock);
		return;
	}

	//if (enable_perf_stats) {
	//        g_perf_stats.rsync_ops += 1;	
	//	tsc_begin = asm_rdtscp();
	//}

	rpc_replicate_log_sync(get_next_peer(), rdma_entries, 0);

	atomic_fetch_add(&g_sync_ctx[0]->peer->n_digest, n_loghdrs);
	atomic_fetch_sub(&g_sync_ctx[0]->peer->n_pending, n_loghdrs);

	pthread_mutex_unlock(g_sync_ctx[0]->peer->shared_rsync_addr_lock);

	mlfs_rpc("DEBUG sync n_digest %u\n", atomic_load(&g_sync_ctx[0]->peer->n_digest));

	//if (enable_perf_stats)
	//	g_perf_stats.rdma_write_time_tsc += (asm_rdtscp() - tsc_begin);
	
	//if (enable_perf_stats) 
	//	show_libfs_stats();

	return;

#if 0
	uint8_t n = 0;

	while(!n || cmpxchg(&get_peer()->outstanding, n, n-1) != n) {
		n = get_peer()->outstanding;
	}

	//get_peer()->outstanding--;
	mlfs_muffled("dec - remaining rsyncs: %d\n", n-1);
#endif
}

//--------------USED FOR DEBUGGING/PROFILING----------------------
//----------------------------------------------------------------

#if 0
int create_rsync_mock(int id, struct list_head **rdma_entries, addr_t start_blk)
{
	uint64_t tsc_begin;
	if (enable_perf_stats)
		tsc_begin = asm_rdtscp();

	int ret = start_rsync_session(g_sync_ctx[id]->peer, 0);

	//nothing to rsync
	if(ret) {
		end_rsync_session(g_sync_ctx->peer);
		return -1;
	}

	mlfs_debug("n_unsync_blk: %lu\n", session->n_unsync_blk);
	atomic_fetch_sub(&g_sync_ctx[id]->peer->n_unsync_blk, session->n_unsync_blk);
	atomic_fetch_sub(&g_sync_ctx[id]->peer->n_unsync, session->n_unsync);

#ifdef COALESCING
	session->local_start = start_blk;
	session->remote_start = start_blk;
	
	struct replay_list *replay_list = (struct replay_list *) mlfs_zalloc(
			sizeof(struct replay_list));

	coalesce_log_and_produce_replay(replay_list);

	if(enable_perf_stats) {
		g_perf_stats.coalescing_log_time_tsc += asm_rdtscp() - tsc_begin;
		tsc_begin = asm_rdtscp();
	}

	//set coalescing factor 'div' and # of sge elements 'sge';
	int div = 2;
	int sge = 2;

	generate_rsync_msgs_using_replay_mock(replay_list, 1, div, sge);

#else
	session->local_start = g_sync_ctx[id]->begin;
	session->remote_start = g_sync_ctx[id]->begin;
	session->n_tosync_blk = session->n_unsync_blk;
	rdma_add_sge(session->n_unsync_blk); 	
	rdma_finalize();
#endif


	assert(session->intervals->count == 0);

	*rdma_entries = &session->rdma_entries;


	if (enable_perf_stats) {
		g_perf_stats.n_rsync_blks += session->n_unsync_blk;
	}

	end_rsync_session(g_sync_ctx[id]->peer);

	return 0;
}

void generate_rsync_msgs_using_replay_mock(struct replay_list *replay_list, int destroy, int div, int sge)
{
	mlfs_debug("------starting---%lu---\n", session->local_start);

	//printf("local_start: %lu, remote_start: %lu, sync_size: %lu\n", session->local_start, session->remote_start,
	//		session-> n_unsync_blk);
	struct list_head *l, *tmp;
	loghdr_t *current_loghdr;
	loghdr_t *prev_loghdr;
	addr_t current_loghdr_blk;
	addr_t prev_loghdr_blk;
	uint8_t *node_type;
	addr_t blocks_to_sync = 0;
	addr_t blocks_skipped = 0;
	int initialized = 0;
	
	prev_loghdr_blk = g_sync_ctx[id]->begin-1;

	list_for_each_safe(l, tmp, &replay_list->head) {
		node_type = (uint8_t *)l + sizeof(struct list_head);
		mlfs_assert(*node_type < 6);
	
		switch(*node_type) {
			case NTYPE_I: {
				i_replay_t *item;
				item = (i_replay_t *)container_of(l, i_replay_t, list);
				current_loghdr_blk = item->blknr;
				if(destroy){
					HASH_DEL(replay_list->i_digest_hash, item);
					list_del(l);
					mlfs_free(item);
				}
				break;
			}
			case NTYPE_D: {
				d_replay_t *item;
				item = (d_replay_t *)container_of(l, d_replay_t, list);
				current_loghdr_blk = item->blknr;
				if(destroy)
					HASH_DEL(replay_list->d_digest_hash, item);
					list_del(l);
					mlfs_free(item);

				break;
			}
			case NTYPE_F: {
				f_replay_t *item;
				item = (f_replay_t *)container_of(l, f_replay_t, list);
				if(destroy) {
					f_iovec_t *f_iovec, *f_tmp;
					HASH_DEL(replay_list->f_digest_hash, item);
					list_for_each_entry_safe(f_iovec, f_tmp, 
						&item->iovec_list, iov_list) {
					//printf("item->key.inum: %u | item->key.ver: %u | f_iovec->node_type: %d\n",
					//		item->key.inum, item->key.ver, f_iovec->node_type);
					list_del(&f_iovec->iov_list);
					//no need to free here; will call later on
					//mlfs_free(f_iovec);
					}
					list_del(l);
					mlfs_free(item);
				}	
				break;
			}
			case NTYPE_U: {
				u_replay_t *item;
				item = (u_replay_t *)container_of(l, u_replay_t, list);
				current_loghdr_blk = item->blknr;
				if(destroy) {
					HASH_DEL(replay_list->u_digest_hash, item);
					list_del(l);
					mlfs_free(item);
				}
				break;
			}
			case NTYPE_V: {
				f_iovec_t *item;
				item = (f_iovec_t *)container_of(l, f_iovec_t, list);
				current_loghdr_blk = item->blknr;
				if(destroy) {
					list_del(l);
					mlfs_free(item);
				}
				break;
			}
			default:
				panic("unsupported node type!\n");
		}
		prev_loghdr_blk = current_loghdr_blk;
		prev_loghdr = current_loghdr;
	}

	session->local_start = g_sync_ctx[id]->begin;
	session->remote_start = g_sync_ctx[id]->begin;
	//printf("DEBUG size is n_unsync:%lu final_size:%lu\n", session->n_unsync_blk, ((session->n_unsync_blk/div)/sge));

	addr_t size_remaining, size_to_sync;

	//only coalesce if we have 'div' or more loghdrs, otherwise do nothing
	if(session->n_unsync >= div) {
		size_remaining = session->n_unsync_blk/div;
		sge = min(sge, max(session->n_unsync_blk/div,1));
	}
	else {
		size_remaining = session->n_unsync_blk;
		sge = 1;
	}

	//addr_t size_remaining = session->n_unsync_blk/div;
	session->n_tosync_blk = size_remaining;

	size_to_sync = max(size_remaining/sge,1);

	for(int i=0; i<sge; i++) {
		//printf("[rsync-sge] size_to_sync: %lu | total: %lu | sge: %d\n", size_to_sync, session->n_tosync_blk, sge);
		if(size_to_sync == 0)
			break;
		rdma_add_sge(size_to_sync);
		session->local_start += size_to_sync;
		//session->remote_start += size_to_sync;
		size_remaining -= size_to_sync;

		//if(i == sge - 2)
		//	size_to_sync = size_remaining;
		//else
		size_to_sync = min(size_to_sync, size_remaining);
	}

	//if (enable_perf_stats) {
	//	g_perf_stats.n_rsync_blks += session->n_tosync_blk;
	//	g_perf_stats.n_rsync_blks_skipped += (session->n_unsync_blk - session->n_tosync_blk);
	//}

	rdma_finalize();

	if(destroy) {
		id_version_t *id_v, *tmp;
		HASH_ITER(hh, replay_list->id_version_hash, id_v, tmp) {
			HASH_DEL(replay_list->id_version_hash, id_v);
			mlfs_free(id_v);	
		}
		HASH_CLEAR(hh, replay_list->i_digest_hash);
		HASH_CLEAR(hh, replay_list->d_digest_hash);
		HASH_CLEAR(hh, replay_list->f_digest_hash);
		HASH_CLEAR(hh, replay_list->u_digest_hash);
		HASH_CLEAR(hh, replay_list->id_version_hash);

		list_del(&replay_list->head);
	}

}

// call this function to start a nanosecond-resolution timer
struct timespec timer_start(){
	struct timespec start_time;
	clock_gettime(CLOCK_REALTIME, &start_time);
	return start_time;
}

// call this function to end a timer, returning nanoseconds elapsed as a long
long timer_end(struct timespec start_time) {
	struct timespec end_time;
	long sec_diff, nsec_diff, nanoseconds_elapsed;

	clock_gettime(CLOCK_REALTIME, &end_time);

	sec_diff =  end_time.tv_sec - start_time.tv_sec;
	nsec_diff = end_time.tv_nsec - start_time.tv_nsec;

	if(nsec_diff < 0) {
		sec_diff--;
		nsec_diff += (long)1e9;
	}

	nanoseconds_elapsed = sec_diff * (long)1e9 + nsec_diff;

	return nanoseconds_elapsed;
}
#endif

static void print_peer_meta_struct(peer_meta_t *meta) {	
	mlfs_debug("local start: %lu | remote start: %lu | n_used: %u | n_used_blk: %lu | n_digest: %u | n_unsync_blk: %lu | "
			"mutex_check: %lu\n", meta->local_start, meta->remote_start, atomic_load(&meta->n_used),
		       	atomic_load(&meta->n_used_blk), atomic_load(&meta->n_digest), atomic_load(&meta->n_unsync_blk),
			atomic_load(&meta->cond_check));
}

static void print_sync_meta_struct(sync_meta_t *meta) {
	mlfs_debug("local start: %lu | remote start: %lu | n_unsync_blk: %lu | n_tosync_blk: %lu\n"
			"intervals->count: %u | intervals->n_blk: %lu | intervals->remote_start: %lu | "
			"peer_base_addr: %lu\n",
		       	meta->local_start, meta->remote_start, meta->n_unsync_blk, meta->n_tosync_blk,
			meta->intervals->count, meta->intervals->n_blk, meta->intervals->remote_start,
			meta->peer_base_addr);
}

static void print_sync_intervals_list(sync_meta_t *meta) {
	sync_interval_t *interval, *tmp;
	int i = 0;
	mlfs_printf("------ printing sync list ----%lu-\n", 0UL);
	list_for_each_entry_safe(interval, tmp, &meta->intervals->head, head) {
		mlfs_printf("[%d] start: %lu | end: %lu | len: %lu | log size: %lu\n", i, interval->start,
			       	interval->end, interval->end-interval->start+1, g_sync_ctx[meta->id]->size);
		i++;
	}
	mlfs_printf("------------ end list -------%lu-\n", 0UL);
}

//---------------------------------------------------------------
//---------------------------------------------------------------

#endif
