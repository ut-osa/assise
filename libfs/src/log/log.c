#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <pthread.h>
#include <unistd.h>

#include "distributed/posix_wrapper.h"
//#include "posix/posix_interface.h"
#include "mlfs/mlfs_user.h"
#include "log/log.h"
#include "concurrency/thread.h"
#include "filesystem/fs.h"
#include "filesystem/slru.h"
#include "io/block_io.h"
#include "global/mem.h"
#include "global/util.h"
#include "mlfs/mlfs_interface.h"
#include "storage/storage.h"
#include "concurrency/thpool.h"

#if MLFS_LEASE
#include "experimental/leases.h"
#endif

/**
 A system call should call start_log_tx()/commit_log_tx() to mark
 its start and end. Usually start_log_tx() just increments
 the count of in-progress FS system calls and returns.

 Log appends are synchronous.
 For crash consistency, log blocks are persisted first and
 then log header is perstisted in commit_log_tx().
 Digesting happens as a unit of a log block group
 (each logheader for each group).
 n in the log header indicates # of live log blocks.
 After digesting all log blocks in a group, the digest thread
 unsets inuse bit, indicating the group  can be garbage-collected.
 Note that the log header must be less than
 a block size for crash consistency.

 On-disk format of log area
 [ log superblock | log header | log blocks | log header | log blocks ... ]
 [ log header | log blocks ] is a transaction made by a system call.

 Each logheader describes a log block group created by
 a single transaction. Different write syscalls use different log group.
 start_log_tx() start a new log group and commit_log_tx()
 serializes writing multiple log groups to log area.

 TODO: This mechanism is currently unnecessary. Crash consistency guarantees
 are now tied to fsync calls. We no longer need to persist log headers for each
 and every system call. commit_log_tx() can be called after an fsync and transaction
 groups can be arbitrarily sized (not bounded at 3). This will help reduce space
 amplification (not a big deal) but most importantly the # of rdma operations that
 need to be posted to replicate the logs efficiently.

 We still need to think about workloads that trigger fsyncs after small writes.
 This potentially wastes space if a logheader needs to consume an entire block.
 */

volatile struct fs_log *g_fs_log;
volatile struct log_superblock *g_log_sb;

// for communication with kernel fs.
//int g_sock_fd;

#ifdef DISTRIBUTED
//FIXME: temporary fix; remove asap
int pending_sock_fd;

static void persist_replicated_logs(int dev, addr_t n_log_blk);
#endif

//static struct sockaddr_un g_srv_addr, g_addr;

// The 30% is ad-hoc parameter: In genernal, 30% ~ 40% shows good performance
// in all workloads
static int g_digest_threshold = 30;

static void read_log_superblock(struct log_superblock *log_sb);
static void write_log_superblock(struct log_superblock *log_sb);
static void update_log_superblock(struct log_superblock *log_sb);
static void commit_log(void);
static void digest_log(void);

pthread_mutex_t *g_log_mutex_shared;

//pthread_t is unsigned long
static unsigned long digest_thread_id[g_n_devices];
//Thread entry point
void *digest_thread(void *arg);
static threadpool thread_pool;

mlfs_time_t start_time;
mlfs_time_t end_time;
int started = 0;


void init_log()
{
	int ret;
	//int volatile done = 0;
	pthread_mutexattr_t attr;
	char* env;

	if (sizeof(struct logheader) > g_block_size_bytes) {
		printf("log header size %lu block size %lu\n",
				sizeof(struct logheader), g_block_size_bytes);
		panic("initlog: too big logheader");
	}

	if (g_log_dev < 0 || g_log_dev > g_n_devices) {
		printf("log dev %d should be between %d and %d\n",
				g_log_dev, 0, g_n_devices);
		panic("init_log: invalid log dev");
	}

	g_fs_log = (struct fs_log *)mlfs_zalloc(sizeof(struct fs_log));
	g_log_sb = (struct log_superblock *)mlfs_zalloc(sizeof(struct log_superblock));

	g_fs_log->log_sb_blk = disk_sb[g_log_dev].log_start + g_self_id * g_log_size;

	//FIXME: this is not the actual log size (rename variable!)
	g_fs_log->size = g_fs_log->log_sb_blk + g_log_size;

	// FIXME: define usage of log dev
	g_fs_log->dev = g_log_dev;
	g_fs_log->id = g_self_id;
	g_fs_log->nloghdr = 0;

	ret = pipe((int*)g_fs_log->digest_fd);
	if (ret < 0) 
		panic("cannot create pipe for digest\n");

	read_log_superblock((struct log_superblock *)g_log_sb);

	g_fs_log->log_sb = g_log_sb;

	// Assuming all logs are digested by recovery.
	//g_fs_log->next_avail_header = disk_sb[dev].log_start + 1; // +1: log superblock
	//g_fs_log->start_blk = disk_sb[dev].log_start + 1;

	g_fs_log->next_avail_header = g_fs_log->log_sb_blk + 1;
	g_fs_log->start_blk = g_fs_log->log_sb_blk + 1;

	mlfs_debug("end of the log %lx\n", g_fs_log->size);

	g_log_sb->start_digest = g_fs_log->next_avail_header;
	g_log_sb->start_persist = g_fs_log->next_avail_header;

	write_log_superblock((struct log_superblock *)g_log_sb);

	atomic_init(&g_log_sb->n_digest, 0);
	atomic_init(&g_log_sb->end, 0);

	//g_fs_log->outstanding = 0;
	g_fs_log->start_version = g_fs_log->avail_version = 0;

	pthread_spin_init(&g_fs_log->log_lock, PTHREAD_PROCESS_SHARED);
	
	// g_log_mutex_shared is shared mutex between parent and child.
	g_log_mutex_shared = (pthread_mutex_t *)mlfs_zalloc(sizeof(pthread_mutex_t));
	pthread_mutexattr_init(&attr);
	pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	pthread_mutex_init(g_log_mutex_shared, &attr);

	g_fs_log->shared_log_lock = (pthread_mutex_t *)mlfs_zalloc(sizeof(pthread_mutex_t));
	pthread_mutexattr_init(&attr);
	pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	pthread_mutex_init(g_fs_log->shared_log_lock, &attr);

#ifdef LIBFS
	//digest_thread_id[dev] = mlfs_create_thread(digest_thread, &dev);
	thread_pool = thpool_init(4);

	// enable/disable statistics for log
	enable_perf_stats = 0;

	env = getenv("MLFS_DIGEST_TH");

	if(env)
		g_digest_threshold = atoi(env);

	/* wait until the digest thread get started */
	//while(!g_fs_log->ready);
#endif

	printf("init log dev %d start_blk %lu end %lu\n", g_log_dev,
			g_fs_log->start_blk, g_fs_log->size);
}

void shutdown_log()
{
	mlfs_info("Shutting down log%s", "\n");

	int dev=0;
#if defined(DISTRIBUTED) && !defined(MASTER)
	//Slave node should recalculate n_digest
	//this value might not be updated since slaves never call commit_log
	//update_log_superblock(g_log_sb);

#if 1
	mlfs_time_t sub;
	timersub(&start_time, &end_time, &sub)
        mlfs_printf("MAX TS = %ld\n", start_time.tv_sec);
        mlfs_printf("MIN TS = %ld\n", end_time.tv_sec);
    	mlfs_printf("Elapsed time (s): %ld %ld\n", sub.tv_sec, sub.tv_usec);
#endif

#endif

	// wait until the digest_thread finishes job.
	if (g_fs_log->digesting) {
		mlfs_info("%s", "[L] Wait finishing on-going digest\n");
		wait_on_digesting();
	}

#if MLFS_MASTER && g_n_nodes > 1
	// wait until the peer finishes all outstanding replication requests.
	if (get_next_peer()->outstanding) {
		mlfs_info("%s", "[L] Wait finishing on-going peer replication\n");
		for(int i=0; i<N_RSYNC_THREADS+1; i++)
			wait_on_peer_replicating(get_next_peer(), i);
	}

	// wait until the peer's digest_thread finishes job.
	if (get_next_peer()->digesting) {
		mlfs_info("%s", "[L] Wait finishing on-going peer digest\n");
		wait_on_peer_digesting(get_next_peer());
	}
#endif

	if (atomic_load(&g_fs_log->log_sb->n_digest)) {
		mlfs_info("%s", "[L] Digesting remaining log data\n");
		while(make_digest_request_async(100) != -EBUSY);
		m_barrier();
		wait_on_digesting();
#if MLFS_MASTER
		wait_on_peer_digesting(get_next_peer());
#endif
	}
}

static loghdr_t *read_log_header(addr_t blkno)
{
       int ret;
       struct buffer_head *bh;
       loghdr_t *hdr_data = mlfs_alloc(sizeof(struct logheader));

       bh = bh_get_sync_IO(g_log_dev, blkno, BH_NO_DATA_ALLOC);
       bh->b_size = sizeof(struct logheader);
       bh->b_data = (uint8_t *)hdr_data;

       bh_submit_read_sync_IO(bh);

       return hdr_data;
}

static void read_log_superblock(struct log_superblock *log_sb)
{
	int ret;
	struct buffer_head *bh;

	bh = bh_get_sync_IO(g_log_dev, g_fs_log->log_sb_blk, 
			BH_NO_DATA_ALLOC);

	bh->b_size = sizeof(struct log_superblock);
	bh->b_data = (uint8_t *)log_sb;

	bh_submit_read_sync_IO(bh);

	bh_release(bh);

	return;
}

static void write_log_superblock(struct log_superblock *log_sb)
{
	int ret;
	struct buffer_head *bh;

	bh = bh_get_sync_IO(g_log_dev, g_fs_log->log_sb_blk, 
			BH_NO_DATA_ALLOC);

	bh->b_size = sizeof(struct log_superblock);
	bh->b_data = (uint8_t *)log_sb;

	mlfs_write(bh);

	bh_release(bh);

	return;
}

// deprecated. TODO: cleanup
static void update_log_superblock(struct log_superblock *log_sb)
{
	loghdr_t *loghdr;
	addr_t loghdr_blkno = log_sb->start_digest;

	int wrap = 0;
	uint8_t n_digest = 0;

	assert(loghdr_blkno > disk_sb[g_log_dev].log_start);

	while(loghdr_blkno != 0)
	{
		loghdr = read_log_header(loghdr_blkno);
		//FIXME: possible issue if LH_COMMIT_MAGIC matches despite a non-existent loghdr
		if (loghdr->inuse != LH_COMMIT_MAGIC) {
			if(log_sb->start_digest > disk_sb[g_log_dev].log_start + 1 && !wrap) {
				atomic_store(&log_sb->end, loghdr_blkno - 1);
				loghdr_blkno = disk_sb[g_log_dev].log_start + 1;
				wrap = 1;
			}
			else
				break;
		}
		else {
			n_digest++;
			loghdr_blkno += loghdr->nr_log_blocks;
			assert(loghdr_blkno <= g_fs_log->size);
		}
	}

	//check that we wrapped around correctly
	if(wrap) {
		addr_t remaining_blocks = g_fs_log->size - atomic_load(&log_sb->end);
		loghdr = read_log_header(g_fs_log->log_sb_blk + 1);
		assert(loghdr->nr_log_blocks > remaining_blocks);
	}

	atomic_store(&log_sb->n_digest, n_digest);
	//we don't care about persisting log_sb; since we digest everything anyways
	//write_log_superblock(g_log_sb);
}

inline addr_t log_alloc(uint32_t nr_blocks)
{
	int ret;


	uint64_t start_tsc_tmp;
	if (enable_perf_stats)
		start_tsc_tmp = asm_rdtscp();

	/* g_fs_log->start_blk : header
	 * g_fs_log->next_avail_header : tail 
	 *
	 * There are two cases:
	 *
	 * <log_begin ....... log_start .......  next_avail_header .... log_end>
	 *	      digested data      available data       empty space
	 *
	 *	                    (ver 10)              (ver 9)
	 * <log_begin ...... next_avail_header ....... log_start ...... log_end>
	 *	       available data       digested data       available data
	 *
	 */
	//mlfs_assert(g_fs_log->avail_version - g_fs_log->start_version < 2);

	// Log is getting full. make asynchronous digest request.
	if (!g_fs_log->digesting) {
		addr_t nr_used_blk = 0;
		if (g_fs_log->avail_version == g_fs_log->start_version) {
			mlfs_assert(g_fs_log->next_avail_header >= g_fs_log->start_blk);
			nr_used_blk = g_fs_log->next_avail_header - g_fs_log->start_blk; 
		} else {
			nr_used_blk = (g_fs_log->size - g_fs_log->start_blk);
			nr_used_blk += (g_fs_log->next_avail_header - g_fs_log->log_sb_blk);
		}

		if (nr_used_blk > ((g_digest_threshold * g_log_size) / 100)) {
			mlfs_info("Exceeded digest threshold. nr_used_blk: %lu (threshold: %lu)\n",
					nr_used_blk, g_digest_threshold * g_log_size / 100);
			while(make_digest_request_async(100) != -EBUSY)
			mlfs_info("%s", "[L] log is getting full. asynchronous digest!\n");
		}
	}

	pthread_mutex_lock(g_fs_log->shared_log_lock);

	// next_avail_header reaches the end of log. 
	//if (g_fs_log->next_avail_header + nr_blocks > g_fs_log->log_sb_blk + g_fs_log->size) {
	if (g_fs_log->next_avail_header + nr_blocks > g_fs_log->size) {
		atomic_store(&g_log_sb->end, g_fs_log->next_avail_header - 1);
		g_fs_log->next_avail_header = g_fs_log->log_sb_blk + 1;	

		atomic_add(&g_fs_log->avail_version, 1);

		mlfs_info("-- log tail is rotated: new start %lu new end %lu start_version %u avail_version %u\n",
				g_fs_log->next_avail_header, atomic_load(&g_log_sb->end),
				g_fs_log->start_version, g_fs_log->avail_version);
	}

	addr_t next_log_blk = 
		__sync_fetch_and_add(&g_fs_log->next_avail_header, nr_blocks);

	pthread_mutex_unlock(g_fs_log->shared_log_lock);

	// This has many policy questions.
	// Current implmentation is very converative.
	// Pondering the way of optimization.
retry:
	if (g_fs_log->avail_version > g_fs_log->start_version) {
		if (g_fs_log->start_blk - g_fs_log->next_avail_header
				< (g_log_size/ 5)) {
			mlfs_info("%s", "\x1B[31m [L] synchronous digest request and wait! \x1B[0m\n");
			while (make_digest_request_async(95) != -EBUSY);

			m_barrier();
			wait_on_digesting();
		}
	}

	if (g_fs_log->avail_version > g_fs_log->start_version) {
		if (g_fs_log->next_avail_header > g_fs_log->start_blk) 
			goto retry;
	}

	if (0) {
		int i;
		for (i = 0; i < nr_blocks; i++) {
			mlfs_info("alloc %lu\n", next_log_blk + i);
		}
	}

	//if (enable_perf_stats)
	//	g_perf_stats.tmp_tsc += (asm_rdtscp() - start_tsc_tmp);

	return next_log_blk;
}

// allocate logheader meta and attach logheader.
static inline struct logheader_meta *loghd_alloc(struct logheader *lh)
{
	struct logheader_meta *loghdr_meta;

	loghdr_meta = (struct logheader_meta *)
		mlfs_zalloc(sizeof(struct logheader_meta));

	if (!loghdr_meta)
		panic("cannot allocate logheader_meta");

	INIT_LIST_HEAD(&loghdr_meta->link);

	loghdr_meta->loghdr = lh;

	return loghdr_meta;
}

// Write in-memory log header to disk.
// This is the true point at which the
// current transaction commits.
static void persist_log_header(struct logheader_meta *loghdr_meta,
		addr_t hdr_blkno)
{
	struct logheader *loghdr = loghdr_meta->loghdr;
	struct buffer_head *io_bh;
	int i;
	uint64_t start_tsc;

	if (enable_perf_stats)
		start_tsc = asm_rdtscp();

	io_bh = bh_get_sync_IO(g_log_dev, hdr_blkno, BH_NO_DATA_ALLOC);

	if (enable_perf_stats) {
		g_perf_stats.bcache_search_tsc += (asm_rdtscp() - start_tsc);
		g_perf_stats.bcache_search_nr++;
	}
	//pthread_spin_lock(&io_bh->b_spinlock);

	mlfs_get_time(&loghdr->mtime);
	io_bh->b_data = (uint8_t *)loghdr;
	io_bh->b_size = sizeof(struct logheader);

	mlfs_write(io_bh);

	mlfs_debug("pid %u [log header] inuse %d blkno %lu next_hdr_blockno %lu\n", 
			getpid(),
			loghdr->inuse, io_bh->b_blocknr, 
			next_loghdr_blknr(loghdr_meta->hdr_blkno,
				loghdr->nr_log_blocks, g_fs_log->log_sb_blk+1,
				atomic_load(&g_log_sb->end)));

	if (loghdr_meta->ext_used) {
		io_bh->b_data = loghdr_meta->loghdr_ext;
		io_bh->b_size = loghdr_meta->ext_used;
		io_bh->b_offset = sizeof(struct logheader);
		mlfs_write(io_bh);
	}

	bh_release(io_bh);

	//pthread_spin_unlock(&io_bh->b_spinlock);
}

// called at the start of each FS system call.
void start_log_tx(void)
{
	struct logheader_meta *loghdr_meta;

#ifndef CONCURRENT
	pthread_mutex_lock(g_log_mutex_shared);
	g_fs_log->outstanding++;

	mlfs_debug("start log_tx %u\n", g_fs_log->outstanding);
	mlfs_assert(g_fs_log->outstanding == 1);
#endif

	loghdr_meta = get_loghdr_meta();

#ifdef LOG_OPT
	struct logheader *prev_loghdr = loghdr_meta->previous_loghdr;
	addr_t prev_hdr_blkno = loghdr_meta->prev_hdr_blkno;
#endif

	memset(loghdr_meta, 0, sizeof(struct logheader_meta));

#ifdef LOG_OPT
	loghdr_meta->previous_loghdr = prev_loghdr;
	loghdr_meta->prev_hdr_blkno = prev_hdr_blkno;
#endif

	if (!loghdr_meta)
		panic("cannot locate logheader_meta\n");

	loghdr_meta->hdr_blkno = 0;
	INIT_LIST_HEAD(&loghdr_meta->link);

	/*
	if (g_fs_log->outstanding == 0 ) {
		mlfs_debug("outstanding %d\n", g_fs_log->outstanding);
		panic("outstanding\n");
	}
	*/
}

void abort_log_tx(void)
{
	struct logheader_meta *loghdr_meta;

	loghdr_meta = get_loghdr_meta();

	if (loghdr_meta->is_hdr_allocated)
		mlfs_free(loghdr_meta->loghdr);

#ifndef CONCURRENT
	pthread_mutex_unlock(g_log_mutex_shared);
	g_fs_log->outstanding--;
#endif

	return;
}

// called at the end of each FS system call.
// commits if this was the last outstanding operation.
void commit_log_tx(void)
{
	int do_commit = 0;

	/*
	if(g_fs_log->outstanding > 0) {
		do_commit = 1;
	} else {
		panic("commit when no outstanding tx\n");
	}
	*/

	do_commit = 1;

	if(do_commit) {
		uint64_t tsc_begin;
		struct logheader_meta *loghdr_meta;

		if (enable_perf_stats)
			tsc_begin = asm_rdtscp();

		commit_log();

		loghdr_meta = get_loghdr_meta();


#if LOG_OPT
	if (loghdr_meta->is_hdr_allocated) {
		if(loghdr_meta->previous_loghdr)
			mlfs_free(loghdr_meta->previous_loghdr);

		loghdr_meta->previous_loghdr = loghdr_meta->loghdr;
		loghdr_meta->prev_hdr_blkno = loghdr_meta->hdr_blkno;
	}
#else
	if (loghdr_meta->is_hdr_allocated)
		mlfs_free(loghdr_meta->loghdr);
#endif
	

#ifndef CONCURRENT
		g_fs_log->outstanding--;
		pthread_mutex_unlock(g_log_mutex_shared);
		mlfs_debug("commit log_tx %u\n", g_fs_log->outstanding);
#endif
		if (enable_perf_stats) {
			g_perf_stats.log_commit_tsc += (asm_rdtscp() - tsc_begin);
			g_perf_stats.log_commit_nr++;
		}
	} else {
		panic("it has a race condition\n");
	}
}

static int persist_log_inode(struct logheader_meta *loghdr_meta, uint32_t idx)
{
	struct inode *ip;
	addr_t logblk_no;
	uint32_t nr_logblocks = 0;
	struct buffer_head *log_bh;
	struct logheader *loghdr = loghdr_meta->loghdr;
	uint64_t start_tsc, start_tsc_tmp;

	if (enable_perf_stats)
		start_tsc_tmp = asm_rdtscp();

	logblk_no = loghdr_meta->hdr_blkno + loghdr_meta->pos;
	loghdr->blocks[idx] = loghdr_meta->pos;
	loghdr_meta->pos++;

	mlfs_assert(loghdr_meta->pos <= loghdr->nr_log_blocks);

	if (enable_perf_stats)
		start_tsc = asm_rdtscp();

	log_bh = bh_get_sync_IO(g_log_dev, logblk_no, BH_NO_DATA_ALLOC);

	if (enable_perf_stats) {
		g_perf_stats.bcache_search_tsc += (asm_rdtscp() - start_tsc);
		g_perf_stats.bcache_search_nr++;
	}

	//loghdr->blocks[idx] = logblk_no;

	ip = icache_find(loghdr->inode_no[idx]);
	mlfs_assert(ip);

	log_bh->b_data = (uint8_t *)ip->_dinode;
	log_bh->b_size = sizeof(struct dinode);
	log_bh->b_offset = 0;

	if (ip->flags & I_DELETING) {
		// icache_del(ip);
		// ideleted_add(ip);
		ip->flags &= ~I_VALID;
		bitmap_clear(sb[g_root_dev]->s_inode_bitmap, ip->inum, 1);
	}

	nr_logblocks = 1;

	//mlfs_assert(log_bh->b_blocknr < g_fs_log->next_avail_header);
	mlfs_assert(log_bh->b_dev == g_fs_log->dev);

	mlfs_debug("inum %u offset %lu @ blockno %lx\n",
				loghdr->inode_no[idx], loghdr->data[idx], logblk_no);

	mlfs_write(log_bh);

	bh_release(log_bh);

#if MLFS_LEASE
	//surrender_lease(ip->inum);
#endif

	//if (enable_perf_stats) {
	//		g_perf_stats.tmp_tsc += (asm_rdtscp() - start_tsc_tmp);
	//}
	//mlfs_assert((log_bh->b_blocknr + nr_logblocks) == g_fs_log->next_avail);

	return 0;
}

/* This is a critical path for write performance.
 * Stay optimized and need to be careful when modifying it */
static int persist_log_file(struct logheader_meta *loghdr_meta, 
		uint32_t idx, uint8_t n_iovec)
{
	uint32_t k, l, size;
	offset_t key;
	struct fcache_block *fc_block;
	addr_t logblk_no;
	uint32_t nr_logblocks = 0;
	struct buffer_head *log_bh;
	struct logheader *loghdr = loghdr_meta->loghdr;
	uint32_t io_size;
	struct inode *inode;
	lru_key_t lru_entry;
	uint64_t start_tsc, start_tsc_tmp;
	int ret;


	if (enable_perf_stats)
		start_tsc_tmp = asm_rdtscp();

	inode = icache_find(loghdr->inode_no[idx]);

	mlfs_assert(inode);

	size = loghdr_meta->io_vec[n_iovec].size;

	// Handling small write (< 4KB).
	if (size < g_block_size_bytes) {
		/* fc_block invalidation and coalescing.

		   1. find fc_block -> if not exist, allocate fc_block and perform writes
				    -> if exist, fc_block may or may not be valid.

		   2. if fc_block is valid, then do coalescing (disabled for now)
		   Note: Reasons for disabling coalescing:
			(a) the 'blocks' array in the loghdr now stores the relative block #
			    to the loghdr blk # (and not absolute). In other words, We don't
			    have a way of pointing at an older log entry.
			(b) current coalescing implementation doesn't account for gaps in partial
		            block writes. Loghdr metadata isn't expressive enough.
			(c) coalecsing can result in data loss when rsyncing (if the block we're
			    coalescing to has already been rsynced.
		   TODO: Address the aforementioned issues and re-enable coalescing

		   3. if fc_block is not valid, then skip coalescing and update fc_block.
		*/

		uint32_t offset_in_block;

		key = (loghdr->data[idx] >> g_block_size_shift);
		offset_in_block = (loghdr->data[idx] % g_block_size_bytes);

		if (enable_perf_stats)
			start_tsc = asm_rdtscp();

		fc_block = fcache_find(inode, key);

		if (enable_perf_stats) {
			g_perf_stats.l0_search_tsc += (asm_rdtscp() - start_tsc);
			g_perf_stats.l0_search_nr++;
		}

		logblk_no = loghdr_meta->hdr_blkno + loghdr_meta->pos;
		loghdr->blocks[idx] = loghdr_meta->pos;
		loghdr_meta->pos++;

//disabling coalescing
#if 0
		if (fc_block) {
			ret = check_log_invalidation(fc_block);
			// fc_block is invalid. update it
			if (ret) {
				fc_block->log_version = g_fs_log->avail_version;
				fc_block->log_addr = logblk_no;
			}
			// fc_block is valid
			else {
				if (fc_block->log_addr)  {
					logblk_no = fc_block->log_addr;
					fc_block->log_version = g_fs_log->avail_version;
					mlfs_debug("write is coalesced %lu @ %lu\n", loghdr->data[idx], logblk_no);
				}
			}
		}
#endif

		if(fc_block)
			ret = check_log_invalidation(fc_block);

		if (!fc_block) {
			//mlfs_assert(loghdr_meta->pos <= loghdr->nr_log_blocks);

			fc_block = fcache_alloc_add(inode, key);
		}

		fc_block->log_version = g_fs_log->avail_version;

		if(inode->itype == T_DIR)
			fcache_log_replace(fc_block, logblk_no, offset_in_block, size, g_fs_log->avail_version);
		else
			fcache_log_add(fc_block, logblk_no, offset_in_block, size, g_fs_log->avail_version);

		if (enable_perf_stats)
			start_tsc = asm_rdtscp();

		log_bh = bh_get_sync_IO(g_log_dev, logblk_no, BH_NO_DATA_ALLOC);

		if (enable_perf_stats) {
			g_perf_stats.bcache_search_tsc += (asm_rdtscp() - start_tsc);
			g_perf_stats.bcache_search_nr++;
		}

		// the logblk_no could be either a new block or existing one (patching case).
		//loghdr->blocks[idx] = logblk_no;

		// case 1. the IO fits into one block.
		if (offset_in_block + size <= g_block_size_bytes)
			io_size = size;
		// case 2. the IO incurs two blocks write (unaligned).
		else 
			panic("do not support this case yet\n");

		log_bh->b_data = loghdr_meta->io_vec[n_iovec].base;
		log_bh->b_size = io_size;
		log_bh->b_offset = offset_in_block;

		mlfs_assert(log_bh->b_dev == g_fs_log->dev);

		mlfs_debug("inum %u offset %lu @ blockno %lx (partial io_size=%u)\n",
				loghdr->inode_no[idx], loghdr->data[idx], logblk_no, io_size);

		
        mlfs_write(log_bh);

		bh_release(log_bh);

    	//if (enable_perf_stats)
	    //	g_perf_stats.tmp_tsc += (asm_rdtscp() - start_tsc_tmp);

#if 0
		// write sanity check
		struct buffer_head *debug_bh;
		char *debug_str = (char*) mlfs_alloc(io_size);
		debug_bh = bh_get_sync_IO(g_log_dev, logblk_no, BH_NO_DATA_ALLOC);
		debug_bh->b_data = (uint8_t *) debug_str;
		debug_bh->b_size = io_size;
		debug_bh->b_offset = offset_in_block;
		m_barrier();
		bh_submit_read_sync_IO(debug_bh);
		mlfs_debug("Sanity check. Reread log block after write. Output str: %.*s\n",
				io_size, debug_str);
		bh_release(debug_bh);
		mlfs_free(debug_str);
#endif

	} 
	// Handling large (possibly multi-block) write.
	else {
		offset_t cur_offset;

		//if (enable_perf_stats)
		//	start_tsc_tmp = asm_rdtscp();

		cur_offset = loghdr->data[idx];

		/* logheader of multi-block is always 4K aligned.
		 * It is guaranteed by mlfs_file_write() */
		mlfs_assert((loghdr->data[idx] % g_block_size_bytes) == 0);
		mlfs_assert((size % g_block_size_bytes) == 0);

		nr_logblocks = size >> g_block_size_shift; 

		mlfs_assert(nr_logblocks > 0);

		logblk_no = loghdr_meta->hdr_blkno + loghdr_meta->pos;
		loghdr->blocks[idx] = loghdr_meta->pos;
		loghdr_meta->pos += nr_logblocks;

		mlfs_assert(loghdr_meta->pos <= loghdr->nr_log_blocks);

		if (enable_perf_stats)
			start_tsc = asm_rdtscp();

		log_bh = bh_get_sync_IO(g_log_dev, logblk_no, BH_NO_DATA_ALLOC);

		if (enable_perf_stats) {
			g_perf_stats.bcache_search_tsc += (asm_rdtscp() - start_tsc);
			g_perf_stats.bcache_search_nr++;
		}

		log_bh->b_data = loghdr_meta->io_vec[n_iovec].base;
		log_bh->b_size = size;
		log_bh->b_offset = 0;

		//loghdr->blocks[idx] = logblk_no;

		// Update log address hash table.
		// This is performance bottleneck of sequential write.
#if 1
		for (k = 0, l = 0; l < size; l += g_block_size_bytes, k++) {
			key = (cur_offset + l) >> g_block_size_shift;
			//mlfs_debug("updating log fcache: inode %u blknr %lu\n", inode->inum, key);
			mlfs_assert(logblk_no);

			if (enable_perf_stats)
				start_tsc = asm_rdtscp();

			fc_block = fcache_find(inode, key);

			if (enable_perf_stats) {
				g_perf_stats.l0_search_tsc += (asm_rdtscp() - start_tsc);
				g_perf_stats.l0_search_nr++;
			}

			if (!fc_block)
				fc_block = fcache_alloc_add(inode, key);


			fcache_log_del_all(fc_block); //delete any existing log patches (overwrite)

			fc_block->log_version = g_fs_log->avail_version;
			fcache_log_add(fc_block, logblk_no + k, 0, g_block_size_bytes, g_fs_log->avail_version);
		}
#endif

		mlfs_debug("inum %u offset %lu size %u @ blockno %lx (aligned)\n",
				loghdr->inode_no[idx], cur_offset, size, logblk_no);

		mlfs_write(log_bh);

		bh_release(log_bh);

	}

	//if (enable_perf_stats) {
	//		g_perf_stats.tmp_tsc += (asm_rdtscp() - start_tsc_tmp);
	//}


	return 0;
}

static uint32_t compute_log_blocks(struct logheader_meta *loghdr_meta)
{
	struct logheader *loghdr = loghdr_meta->loghdr; 
	uint8_t type, n_iovec; 
	uint32_t nr_log_blocks = 0;
	int i;

	for (i = 0, n_iovec = 0; i < loghdr->n; i++) {
		type = loghdr->type[i];

		switch(type) {
			case L_TYPE_UNLINK:
			case L_TYPE_INODE_CREATE:
			case L_TYPE_INODE_UPDATE:
			case L_TYPE_ALLOC: {
				nr_log_blocks++;
				break;
			} 
			case L_TYPE_DIR_ADD:
			case L_TYPE_DIR_RENAME:
			case L_TYPE_DIR_DEL:
			case L_TYPE_FILE: {
				uint32_t size;
				size = loghdr_meta->io_vec[n_iovec].size;

				if (size < g_block_size_bytes)
					nr_log_blocks++;
				else
					nr_log_blocks += 
						(size >> g_block_size_shift);
				n_iovec++;
				break;
			}
			default: {
				panic("unsupported log type\n");
				break;
			}
		}
	}

	return nr_log_blocks;
}

// Copy modified blocks from cache to log.
static void persist_log_blocks(struct logheader_meta *loghdr_meta)
{ 
	struct logheader *loghdr = loghdr_meta->loghdr; 
	uint32_t i, nr_logblocks = 0; 
	uint8_t type, n_iovec; 
	addr_t logblk_no;

	uint64_t start_tsc_tmp;

    //mlfs_assert(hdr_blkno >= g_fs_log->start);



	for (i = 0, n_iovec = 0; i < loghdr->n; i++) {
		type = loghdr->type[i];

		//mlfs_printf("commit log %u of type %u block_nr %lu\n", i, type, loghdr_meta->hdr_blkno);
		switch(type) {
			case L_TYPE_UNLINK:
			case L_TYPE_INODE_CREATE:
			case L_TYPE_INODE_UPDATE: {
				persist_log_inode(loghdr_meta, i);
				break;
			} 
				/* Directory information is piggy-backed in 
				 * log header */
			case L_TYPE_DIR_ADD:
			case L_TYPE_DIR_RENAME:
			case L_TYPE_DIR_DEL:
#if MLFS_LEASE
				//m_barrier();
				//mark_lease_revocable(loghdr->inode_no[i]);
#endif
			case L_TYPE_FILE: {
				persist_log_file(loghdr_meta, i, n_iovec);
				n_iovec++;
				break;
			}
			case L_TYPE_ALLOC:
				break;
			default: {
				panic("unsupported log type\n");
				break;
			}
		}
	}
}

static void commit_log(void)
{
	struct logheader_meta *loghdr_meta;
	struct logheader *loghdr, *previous_loghdr;
	uint64_t tsc_begin, tsc_end, tsc_begin_tmp, tsc_end_tmp;

	//if (enable_perf_stats)
	//	tsc_begin_tmp = asm_rdtscp();

	// loghdr_meta is stored in TLS.
	loghdr_meta = get_loghdr_meta();
	mlfs_assert(loghdr_meta);

	/* There was no log update during transaction */
	if (!loghdr_meta->is_hdr_allocated)
		return;

	mlfs_assert(loghdr_meta->loghdr);
	loghdr = loghdr_meta->loghdr;
	previous_loghdr = loghdr_meta->previous_loghdr;

	if (loghdr->n <= 0)
		panic("empty log header\n");

	if (loghdr->n > 0) {
		uint32_t nr_log_blocks;
		uint32_t pos = 1;
		uint8_t do_coalesce = 0;
		uint32_t nr_extra_blocks = 0;

		// Pre-compute required log blocks for atomic append.
		nr_log_blocks = compute_log_blocks(loghdr_meta);
		nr_log_blocks++; // +1 for a next log header block;

#ifndef CONCURRENT
		pthread_mutex_lock(g_fs_log->shared_log_lock);
#endif
		//FIXME: temporarily changing to spinlocks
		//pthread_spin_lock(&g_fs_log->log_lock);

#ifdef LOG_OPT
		// check if loghdr entry can be coalesced
		if(!loghdr_meta->is_hdr_locked && loghdr->n == 1 &&
				loghdr->type[0] == L_TYPE_FILE && previous_loghdr &&
				previous_loghdr->type[0] == L_TYPE_FILE &&
				previous_loghdr->inode_no[0] == loghdr->inode_no[0]) {

			// two cases for coalescing: overwrite or append
			// TODO: support overwrites at non-identical offsets
			// TODO: Do not coalesce if previous_loghdr has been fsynced and/or replicated

			// overwrites
			if(previous_loghdr->data[0] == loghdr->data[0]) {	
				if(previous_loghdr->length[0] >= loghdr->length[0])
					nr_extra_blocks = 0;
				else
					nr_extra_blocks = (loghdr->length[0] - previous_loghdr->length[0]
								>> g_block_size_shift);

				mlfs_debug("coalesce - overwrite existing log entry - block %lu nr_extra_blocks %u\n",
						loghdr_meta->prev_hdr_blkno, nr_extra_blocks);
			}
			// appends
			else if(previous_loghdr->data[0] + previous_loghdr->length[0] == loghdr->data[0]) {
				pos += (loghdr->data[0] - previous_loghdr->data[0] >> g_block_size_shift);
				nr_extra_blocks = (loghdr->data[0] + loghdr->length[0] >> g_block_size_shift) -
					(previous_loghdr->data[0] + previous_loghdr->length[0] >> g_block_size_shift);

				mlfs_debug("coalesce - append to existing log entry - block %lu nr_extra_blocks %u pos %u\n",
						loghdr_meta->prev_hdr_blkno, nr_extra_blocks, pos);
			}
			else {
				nr_extra_blocks = -1;
			}

			//FIXME: this currently does not support multi-threading
			if (nr_extra_blocks >= 0 && g_fs_log->next_avail_header + nr_extra_blocks <= g_fs_log->size) {
				do_coalesce = 1;
			}
			
		}

#endif
		// atomic log allocation.

		if (enable_perf_stats)
			tsc_begin = asm_rdtscp();

		if(do_coalesce) {
			nr_log_blocks = nr_extra_blocks;	
			log_alloc(nr_log_blocks);
			loghdr_meta->hdr_blkno = loghdr_meta->prev_hdr_blkno;
		}
		else
			loghdr_meta->hdr_blkno = log_alloc(nr_log_blocks);
		//loghdr_meta->nr_log_blocks = nr_log_blocks;
		// loghdr_meta->pos = 0 is used for log header block.

		loghdr_meta->pos = pos;

		//loghdr_meta->hdr_blkno = g_fs_log->next_avail_header;
		//g_fs_log->next_avail_header = loghdr_meta->hdr_blkno + loghdr_meta->nr_log_blocks;

		loghdr->nr_log_blocks = nr_log_blocks;
		loghdr->inuse = LH_COMMIT_MAGIC;

#ifndef CONCURRENT
		pthread_mutex_unlock(g_fs_log->shared_log_lock);
#endif
		//pthread_spin_unlock(&g_fs_log->log_lock);

		//if (enable_perf_stats)
		//	g_perf_stats.tmp_tsc += (asm_rdtscp() - tsc_begin_tmp);

		mlfs_debug("pid %u [commit] log block %lu nr_log_blocks %u\n",
				getpid(), loghdr_meta->hdr_blkno, loghdr->nr_log_blocks);
		mlfs_debug("pid %u [commit] current header %lu next header %lu\n", 
				getpid(), loghdr_meta->hdr_blkno, g_fs_log->next_avail_header);

		// TODO: try to optimize this.
		// this guarantee is no longer necessary since
		// consistency is now tied to fsyncs

		/* Crash consistent order: log blocks write
		 * is followed by log header write */

    	if (enable_perf_stats)
	    	tsc_begin_tmp = asm_rdtscp();


		persist_log_blocks(loghdr_meta);

    	if (enable_perf_stats) {
                tsc_end_tmp = asm_rdtscp();
	    		g_perf_stats.tmp_tsc += (tsc_end_tmp - tsc_begin_tmp);
    	}

		if (enable_perf_stats) {
			tsc_end = asm_rdtscp();
			g_perf_stats.log_write_tsc += (tsc_end - tsc_begin);
		}

#if 0
		if(loghdr->next_loghdr_blkno != g_fs_log->next_avail_header) {
			printf("loghdr_blkno %lu, next_avail %lu\n",
					loghdr->next_loghdr_blkno, g_fs_log->next_avail_header);
			panic("loghdr->next_loghdr_blkno is tainted\n");
		}
#endif

		if (enable_perf_stats)
			tsc_begin = asm_rdtscp();

		// Write log header to log area (real commit)
		persist_log_header(loghdr_meta, loghdr_meta->hdr_blkno);

		if (enable_perf_stats) {
			tsc_end = asm_rdtscp();
			g_perf_stats.loghdr_write_tsc += (tsc_end - tsc_begin);
		}

#if defined(DISTRIBUTED) && defined(MASTER)
		//if (enable_perf_stats)
		//	tsc_begin = asm_rdtscp();

		update_peer_sync_state(get_next_peer(), loghdr->nr_log_blocks, 0);

		//trigger asynchronous replication if we exceed our chunk size
 		if(g_n_nodes > 1 && g_rsync_chunk && atomic_load(&get_next_peer()->n_unsync_blk) > g_rsync_chunk) {
			make_replication_request_async(g_rsync_chunk);
#ifdef LOG_OPT
			struct logheader_meta *loghdr_meta;
			loghdr_meta = get_loghdr_meta();
			mlfs_assert(loghdr_meta);
			loghdr_meta->is_hdr_locked = true;
#endif
			mlfs_info("%s", "[L] received remote replication signal. asynchronous replication!\n");
		}

		//if (enable_perf_stats)
		//	g_perf_stats.tmp_tsc += (asm_rdtscp() - tsc_begin);
#endif
		atomic_fetch_add(&g_log_sb->n_digest, 1);
		//mlfs_printf("n_digest %u\n", atomic_load(&g_log_sb->n_digest));
	}
}

/* FIXME: Use of parameter and name of that are very confusing.
 * data: 
 *	file_inode - offset in file
 *	dir_inode - parent inode number
 * length: 
 *	file_inode - file size 
 *	dir_inode - offset in directory
 */
void add_to_loghdr(uint8_t type, struct inode *inode, offset_t data, 
		uint32_t length, void *extra, uint16_t extra_len)
{
	uint32_t i;
	struct logheader *loghdr;
	struct logheader_meta *loghdr_meta;

#if 1
	if(!started) {
		mlfs_get_time(&start_time);
		started = 1;
	}
	else {
		mlfs_get_time(&end_time);
	}
#endif

	loghdr_meta = get_loghdr_meta();

	mlfs_assert(loghdr_meta);

	if (!loghdr_meta->is_hdr_allocated) {
		loghdr = (struct logheader *)mlfs_zalloc(sizeof(*loghdr));

		loghdr_meta->loghdr = loghdr;
		loghdr_meta->is_hdr_allocated = 1;
	}

	loghdr = loghdr_meta->loghdr;

	if (loghdr->n >= g_fs_log->size)
		panic("too big a transaction for log");

	/*
		 if (g_fs_log->outstanding < 1)
		 panic("add_to_loghdr: outside of trans");
	*/

	i = loghdr->n;

	if (i >= g_max_blocks_per_operation)
		panic("log header is too small\n");

	loghdr->type[i] = type;
	loghdr->inode_no[i] = inode->inum;

	if (type == L_TYPE_FILE ||
			type == L_TYPE_DIR_ADD ||
			type == L_TYPE_DIR_RENAME ||
			type == L_TYPE_DIR_DEL ||
			type == L_TYPE_ALLOC)
		// offset in file.
		loghdr->data[i] = (offset_t)data;
	else
		loghdr->data[i] = 0;

	loghdr->length[i] = length;
	loghdr->n++;

	if (extra_len) {
		uint16_t ext_used = loghdr_meta->ext_used;

		loghdr_meta->loghdr_ext[ext_used] = '0' + i;
		ext_used++;
		memmove(&loghdr_meta->loghdr_ext[ext_used], extra, extra_len);
		ext_used += extra_len;
		strncat((char *)&loghdr_meta->loghdr_ext[ext_used], "|", 1);
		ext_used++;
		loghdr_meta->loghdr_ext[ext_used] = '\0';
		loghdr_meta->ext_used = ext_used;
	
		mlfs_assert(ext_used <= 2048);
	}

	mlfs_debug("add_to_loghdr [%s] inum %u\n", (type == L_TYPE_FILE? "FILE" :
				type == L_TYPE_DIR_ADD? "DIR_ADD" : type == L_TYPE_DIR_RENAME? "DIR_RENAME" :
				type == L_TYPE_DIR_DEL? "DIR_DEL" : type == L_TYPE_INODE_CREATE? "INODE_CREATE" :
				type == L_TYPE_INODE_UPDATE? "INODE_UDPATE" : type == L_TYPE_UNLINK? "UNLINK" :
				type == L_TYPE_ALLOC? "ALLOC" : "UNKNOWN"), inode->inum);

	/*
		 if (type != L_TYPE_FILE)
		 mlfs_debug("[loghdr-add] dev %u, type %u inum %u data %lu\n",
		 inode->dev, type, inode->inum, data);
	 */
}

int mlfs_do_rdigest(uint32_t n_digest)
{
#if defined(DISTRIBUTED) && defined(MASTER)

	if(g_n_nodes == 1)
		return 0;

	//mlfs_do_rsync();
	//make_replication_request_async(0);

	//MP_AWAIT_PENDING_WORK_COMPLETIONS(get_next_peer()->info->sockfd[SOCK_IO]);
	//m_barrier();

	//mlfs_assert(get_next_peer()->start_digest <= get_next_peer()->remote_start);
	//struct rpc_pending_io *pending[g_n_nodes];
	//for(int i=0; i<g_n_nodes; i++)
	rpc_remote_digest_async(get_next_peer()->info->id, get_next_peer(), n_digest, 0);

	//for(int i=0; i<g_n_nodes; i++)

	//if (enable_perf_stats)
	//	show_libfs_stats();

#endif
	return 0;
}

int mlfs_do_rsync()
{
#if defined(DISTRIBUTED) && defined(MASTER)

#if !defined(CC_OPT)
	make_replication_request_sync(get_next_peer());
#endif

#ifdef LOG_OPT
	struct logheader_meta *loghdr_meta;
	loghdr_meta = get_loghdr_meta();
	mlfs_assert(loghdr_meta);
	loghdr_meta->is_hdr_locked = true;
#endif

#endif
	return 0;
}

//////////////////////////////////////////////////////////////////////////
// Libmlfs signal callback (used to handle signaling between replicas)

#ifdef DISTRIBUTED
void signal_callback(struct app_context *msg)
{
	char cmd_hdr[12];

	if(msg->data) {
		sscanf(msg->data, "|%s |", cmd_hdr);
		mlfs_debug("received rpc with body: %s on sockfd %d\n", msg->data, msg->sockfd);
	}
	else {
		cmd_hdr[0] = 'i';
	}

	// master/slave callbacks
	// handles 2 message types (digest)

	if (cmd_hdr[0] == 'b') {
		uint32_t peer_id;
		sscanf(msg->data, "|%s |%u", cmd_hdr, &peer_id);
		g_self_id = peer_id;
		mlfs_printf("Assigned LibFS ID=%u\n", g_self_id);
	}
#ifndef MASTER
	//slave callback
	//handles 2 types of messages (read, immediate)

	//read command
	else if(cmd_hdr[0] == 'r') {
		//trigger mlfs read and send back response
		//TODO: check if block is up-to-date (one way to do this is by propogating metadata updates
		//to slave as soon as possible and checking if blocks are up-to-date)
		char path[MAX_REMOTE_PATH];
		uintptr_t dst;
		loff_t offset;
		uint32_t io_size;
		//uint8_t * buf;
		int fd, ret;
		int flags = 0;

		sscanf(msg->data, "|%s |%s |%ld|%u|%lu", cmd_hdr, path, &offset, &io_size, &dst);
		mlfs_debug("received remote read RPC with path: %s | offset: %ld | io_size: %u | dst: %lu\n",
				path, offset, io_size, dst);
 		//buf = (uint8_t*) mlfs_zalloc(io_size << g_block_size_shift);

		struct mlfs_reply *reply = rpc_get_reply_object(msg->sockfd, (uint8_t*)dst, msg->id);

		fd = mlfs_posix_open(path, flags, 0); //FIXME: mode is currently unused - setting to 0
		if(fd < 0)
			panic("remote_read: failed to open file\n");

		ret = mlfs_rpc_pread64(fd, reply, io_size, offset);
		if(ret < 0)
			panic("remote_read: failed to read file\n");
	}
	else if(cmd_hdr[0] == 'r') { //replicate and digest
		addr_t n_log_blk;
		int node_id, ack, steps, persist;
		uint16_t seqn;

		mlfs_debug("received log data from replica %d | n_log_blk: %u | steps: %u | ack_required: %s | persist: %s\n",
				node_id, n_log_blk, steps, ack?"yes":"no", persist?"yes":"no");	

	}
	else
		panic("unidentified remote signal\n");


#else
	//master callback
	//handles 1 type of messages (complete, lease, replicate)

	//digestion completion notification
	else if (cmd_hdr[0] == 'c') {
		//FIXME: this callback currently does not support coalescing; it assumes remote
		// and local kernfs digests the same amount of data
		int log_id, dev, rotated, lru_updated;
		uint32_t n_digested;
		addr_t start_digest;

		mlfs_printf("peer recv: %s\n", msg->data);
		sscanf(msg->data, "|%s |%d|%d|%d|%lu|%d|%d|", cmd_hdr, &log_id, &dev,
				&n_digested, &start_digest, &rotated, &lru_updated);


		// local kernfs
		if(g_rpc_socks[msg->sockfd]->peer->id == g_kernfs_id)
			handle_digest_response(msg->data);
		else
			update_peer_digest_state(get_next_peer(), start_digest, n_digested, rotated);

#if 0
		if(dev == g_log_dev) {
			handle_digest_response(msg->data);
		}
		else
			update_peer_digest_state(get_next_peer(), start_digest, n_digested, rotated);
#endif
		//handle_digest_response(msg->data);
	}
	else if(cmd_hdr[0] == 'l') {
#if 0
		char path[MAX_PATH];
		int type;
		sscanf(msg->data, "|%s |%s |%d", cmd_hdr, path, &type);
		mlfs_debug("received remote lease acquire with path %s | type[%d]\n", path, type);
		resolve_lease_conflict(msg->sockfd, path, type, msg->id);
#else
		panic("invalid code path\n");
#endif
	}
	else if(cmd_hdr[0] == 'f') { // flush cache / revoke lease
#if MLFS_LEASE
		int digest;
		uint32_t inum;
		uint32_t n_digest;
		sscanf(msg->data, "|%s |%u|%d", cmd_hdr, &inum, &digest);

		revoke_lease(msg->sockfd, msg->id, inum);

#if 1
		if(digest) {
			// wait until the digest_thread finishes job.
			if (g_fs_log->digesting) {
				mlfs_info("%s", "[L] Wait finishing on-going digest\n");
				wait_on_digesting();
			}

			while(make_digest_request_async(100) == -EBUSY)
				cpu_relax();

			mlfs_printf("%s", "[L] lease contention detected. asynchronous digest!\n");
		}
#endif

		purge_dir_caches(inum);
		//rpc_send_ack(msg->sockfd, msg->id);
#else
		panic("invalid code path\n");
#endif
	}
	else if(cmd_hdr[0] == 'e') { // lease error
#if MLFS_LEASE
		uint32_t inum;
		sscanf(msg->data, "|%s |%u", cmd_hdr, &inum);
		report_lease_error(inum);
#else
		panic("invalid code path\n");
#endif
	}
	else if(cmd_hdr[0] == 'i') { //immediate completions are replication notifications
		clear_peer_syncing(get_next_peer(), msg->id);
	}
	else if(cmd_hdr[0] == 'a') //ack (ignore)
		return;
	else
		panic("unidentified remote signal\n");

#endif
}
#endif

/////////////////////////////////////////////////////////////////////////

// Libmlfs digest thread.

void wait_on_digesting()
{
	uint64_t tsc_begin, tsc_end;
	if (enable_perf_stats) 
		tsc_begin = asm_rdtsc();

	while(g_fs_log->digesting)
		cpu_relax();

	if (enable_perf_stats) {
		tsc_end = asm_rdtsc();
		g_perf_stats.digest_wait_tsc += (tsc_end - tsc_begin);
		g_perf_stats.digest_wait_nr++;
	}
}

// FIXME: function is currently not asynchronous
int make_digest_request_async(int percent)
{
	mlfs_log("try digest async | digesting %d n_digest %lu\n",
			g_fs_log->digesting, atomic_load(&g_log_sb->n_digest));
	if (!g_fs_log->digesting && atomic_load(&g_log_sb->n_digest) > 0) {
		set_digesting();
		//mlfs_debug("Send digest command: %s\n", cmd_buf);
		//ret = write(g_fs_log->digest_fd[1], cmd_buf, MAX_CMD_BUF);
		make_digest_request_sync(100);
#ifdef LOG_OPT
	struct logheader_meta *loghdr_meta;
	loghdr_meta = get_loghdr_meta();
	mlfs_assert(loghdr_meta);
	loghdr_meta->is_hdr_locked = true;
#endif
		return 0;
	} else
		return -EBUSY;
}

uint32_t make_digest_request_sync(int percent)
{
	int ret, i;
	char cmd[RPC_MSG_BYTES];
	uint32_t digest_count = 0, n_digest;
	loghdr_t *loghdr;
	struct inode *ip;

	// update start_digest (in case its pointing to a value higher than 'end')
	// reason: start_digest is returned from digest response and might not take wraparound
	// into account

	addr_t end = atomic_load(&g_log_sb->end);  
	if(end && g_fs_log->start_blk > end) {
		g_fs_log->start_blk = g_fs_log->log_sb_blk + 1;
		atomic_add(&g_fs_log->start_version, 1);
	}

	//addr_t loghdr_blkno = g_fs_log->start_blk;
	g_log_sb->start_digest = g_fs_log->start_blk;
	write_log_superblock((struct log_superblock *)g_log_sb);

#if defined(DISTRIBUTED) && defined(MASTER)
	//sync log before digesting (to avoid data loss)
	//FIXME: (1) Should we modify this to adhere to the specified 'percent'?
	//FIXME: (2) This delays the digestion process and blocks its thread;
	// 		contemplating possible optimizations

	//n_digest = atomic_load(&get_next_peer()->n_digest);
	n_digest = atomic_load(&g_log_sb->n_digest);

	mlfs_debug("sanity check: n_digest (local) %u n_digest (remote) %u\n",
			atomic_load(&g_log_sb->n_digest), n_digest);

	g_fs_log->n_digest_req = (percent * n_digest) / 100;

	//if(!atomic_compare_exchange_strong(&get_next_peer()->n_digest, &n_digest, 0))
	//	assert("race condition on remote n_digest");

	// sync up if we are behind
	// FIXME: implement mlfs_do_rsync that replicates up to a certain number of log txs
	if(g_n_nodes > 1 && atomic_load(&get_next_peer()->n_digest) < g_fs_log->n_digest_req)
		mlfs_do_rsync();

	for(int i=0; i<N_RSYNC_THREADS+1; i++)
		wait_on_peer_replicating(get_next_peer(), i);

	//FIXME: just for testing; remove this line
	//g_fs_log->n_digest_req = 10 * n_digest / 100;
	if(g_n_nodes > 1) {
		mlfs_assert(atomic_load(&get_next_peer()->n_digest) >= g_fs_log->n_digest_req);
		atomic_fetch_sub(&get_next_peer()->n_digest, g_fs_log->n_digest_req);
	}

#else
	n_digest = atomic_load(&g_log_sb->n_digest);
	g_fs_log->n_digest_req = (percent * n_digest) / 100;
#endif
	socklen_t len = sizeof(struct sockaddr_un);
	sprintf(cmd, "|digest |%d|%d|%u|%lu|%lu|%lu",
			g_self_id, g_log_dev, g_fs_log->n_digest_req, g_log_sb->start_digest,
		       	 g_fs_log->log_sb_blk + 1, atomic_load(&g_log_sb->end));

	mlfs_printf("%s\n", cmd);

	rpc_forward_msg(g_kernfs_peers[g_kernfs_id]->sockfd[SOCK_BG], cmd);

	mlfs_do_rdigest(g_fs_log->n_digest_req);

	return n_digest;
}

static void cleanup_lru_list(int lru_updated)
{
	lru_node_t *node, *tmp;
	int i = 0;

	pthread_rwlock_wrlock(shm_lru_rwlock);

	list_for_each_entry_safe_reverse(node, tmp, &lru_heads[g_log_dev], list) {
		HASH_DEL(lru_hash, node);
		list_del(&node->list);
		mlfs_free_shared(node);
	}

	pthread_rwlock_unlock(shm_lru_rwlock);
}

void handle_digest_response(char *ack_cmd)
{	
	char ack[10] = {0};
	addr_t next_hdr_of_digested_hdr;
	int dev, libfs_id, n_digested, rotated, lru_updated;
	struct inode *inode, *tmp;

	sscanf(ack_cmd, "|%s |%d|%d|%d|%lu|%d|%d|", ack, &libfs_id, &dev, &n_digested, 
			&next_hdr_of_digested_hdr, &rotated, &lru_updated);

	mlfs_printf("%s\n", ack_cmd);

	mlfs_assert(g_self_id == libfs_id);

	if (g_fs_log->n_digest_req == n_digested)  {
		mlfs_info("%s", "digest is done correctly\n");
		mlfs_info("%s", "-----------------------------------\n");
	} else {
		mlfs_printf("[D] digest is done insufficiently: req %u | done %u\n",
				g_fs_log->n_digest_req, n_digested);
		panic("Digest was incorrect!\n");
	}

	mlfs_debug("g_fs_log->start_blk %lx, next_hdr_of_digested_hdr %lx\n",
			g_fs_log->start_blk, next_hdr_of_digested_hdr);

	if (rotated) {
		atomic_add(&g_fs_log->start_version, 1);
		mlfs_printf("-- log head is rotated: new start %lu new end %lu start_version %u avail_version %u\n",
				g_fs_log->next_avail_header, atomic_load(&g_log_sb->end),
				g_fs_log->start_version, g_fs_log->avail_version);
		if(g_fs_log->start_version == g_fs_log->avail_version)
			atomic_store(&g_log_sb->end, 0);
	}

	// change start_blk
	g_fs_log->start_blk = next_hdr_of_digested_hdr;
	g_log_sb->start_digest = next_hdr_of_digested_hdr;

	// adjust g_log_sb->n_digest properly
	atomic_fetch_sub(&g_log_sb->n_digest, n_digested);

	//Start cleanup process after digest is done.

	//cleanup_lru_list(lru_updated);

	//FIXME: uncomment this line!
	//update our inode cache
	sync_all_inode_ext_trees();

	// persist log superblock.
	write_log_superblock((struct log_superblock *)g_log_sb);

	mlfs_info("clear digesting state for log%s", "\n");
	clear_digesting();

	if (enable_perf_stats) 
		show_libfs_stats();
}

