#include <sys/epoll.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "mlfs/mlfs_user.h"
#include "global/global.h"
#include "global/util.h"
#include "global/defs.h"
#include "io/balloc.h"
#include "kernfs_interface.h"
#include "concurrency/thpool.h"
#include "fs.h"
#include "io/block_io.h"
#include "storage/storage.h"
#include "extents.h"
#include "extents_bh.h"
#include "filesystem/slru.h"
#include "migrate.h"

#ifdef DISTRIBUTED
#include "distributed/rpc_interface.h"
#include "distributed/replication.h"
#endif

#define _min(a, b) ({\
		__typeof__(a) _a = a;\
		__typeof__(b) _b = b;\
		_a < _b ? _a : _b; })

#define ALLOC_SIZE (1<<15)

#define NOCREATE 0
#define CREATE 1

int log_fd = 0;
int shm_fd = 0;
uint8_t *shm_base;

#if 0
void mlfs_get_time(mlfs_time_t *a) {}
#else
void mlfs_get_time(mlfs_time_t *t)
{
	gettimeofday(t, NULL);
	return;
}
#endif


#if MLFS_LEASE
SharedTable *lease_table;
#endif

pthread_spinlock_t icache_spinlock;
pthread_spinlock_t dcache_spinlock;

pthread_mutex_t digest_mutex;

struct inode *inode_hash;

struct disk_superblock disk_sb[g_n_devices + 1];
struct super_block *sb[g_n_devices + 1];

ncx_slab_pool_t *mlfs_slab_pool;
ncx_slab_pool_t *mlfs_slab_pool_shared;

kernfs_stats_t g_perf_stats;
uint8_t enable_perf_stats;

uint16_t *inode_version_table;
threadpool thread_pool;
threadpool thread_pool_ssd;
#ifdef FCONCURRENT
threadpool file_digest_thread_pool;
#endif

struct timespec first_digest_time;
double persist_sec = 0;
double digest_sec = 0;
double run_sec = 0;
int started = 0;

#if MLFS_LEASE
struct sockaddr_un all_cli_addr[g_n_hot_rep];
int addr_idx = 0;
#endif

DECLARE_BITMAP(g_log_bitmap, g_n_max_libfs);

int digest_unlink(uint8_t from_dev, uint8_t to_dev, int libfs_id, uint32_t inum);

#define NTYPE_I 1
#define NTYPE_D 2
#define NTYPE_F 3
#define NTYPE_U 4

#if 0
typedef struct replay_node_key
{
	uint32_t inum;
	uint16_t ver;
} replay_key_t;

/* It is important to place node_type right after struct list_head
 * since digest_log_from_replay_list compute node_type like this:
 * node_type = &list + sizeof(struct list_head)
 */
typedef struct inode_replay {
	replay_key_t key;
	addr_t blknr; 
	mlfs_hash_t hh;
	struct list_head list;
	uint8_t node_type;
	uint8_t create;
} i_replay_t;

typedef struct d_replay_key {
	uint32_t inum;
	uint16_t ver;
	uint8_t type;
} d_replay_key_t;

typedef struct directory_replay {
	d_replay_key_t key;
	int n;
	uint32_t dir_inum;
	uint32_t dir_size;
	addr_t blknr;
	mlfs_hash_t hh;
	struct list_head list;
	uint8_t node_type;
} d_replay_t;

typedef struct block_list {
	struct list_head list;
	uint32_t n;
	addr_t blknr;
} f_blklist_t;

typedef struct file_io_vector {
	offset_t hash_key;
	struct list_head list;
	offset_t offset;
	uint32_t length;
	addr_t blknr; 
	uint32_t n_list;
	struct list_head iov_blk_list;
	mlfs_hash_t hh;
} f_iovec_t;

typedef struct file_replay {
	replay_key_t key;
	struct list_head iovec_list;
	f_iovec_t *iovec_hash;
	mlfs_hash_t hh;
	struct list_head list;
	uint8_t node_type;
} f_replay_t;

typedef struct unlink_replay {
	replay_key_t key;
	mlfs_hash_t hh;
	struct list_head list;
	uint8_t node_type;
} u_replay_t;

struct replay_list {
	i_replay_t *i_digest_hash;
	d_replay_t *d_digest_hash;
	f_replay_t *f_digest_hash;
	u_replay_t *u_digest_hash;
	struct list_head head;
};
#endif

struct digest_arg {
	int sock_fd;
#ifdef DISTRIBUTED
	uint32_t seqn;
#endif
	struct sockaddr_un cli_addr;
	char msg[MAX_CMD_BUF];
};

struct f_digest_worker_arg {
	uint8_t from_dev;
	uint8_t to_dev;
	f_replay_t *f_item;
	int libfs_id;
};


void show_kernfs_stats(void)
{
	float clock_speed_mhz = get_cpu_clock_speed();
	uint64_t n_digest = g_perf_stats.n_digest == 0 ? 1.0 : g_perf_stats.n_digest;

	printf("\n");
	//printf("CPU clock : %.3f MHz\n", clock_speed_mhz);
	printf("----------------------- kernfs statistics\n");
	printf("digest       : %.3f ms\n",
			g_perf_stats.digest_time_tsc / (clock_speed_mhz * 1000.0));
	printf("- replay     : %.3f ms\n",
			g_perf_stats.replay_time_tsc / (clock_speed_mhz * 1000.0));
	printf("- apply      : %.3f ms\n",
			g_perf_stats.apply_time_tsc / (clock_speed_mhz * 1000.0));
	printf("-- inode digest : %.3f ms\n",
			g_perf_stats.digest_inode_tsc / (clock_speed_mhz * 1000.0));
	printf("-- dir digest   : %.3f ms\n",
			g_perf_stats.digest_dir_tsc / (clock_speed_mhz * 1000.0));
	printf("-- file digest  : %.3f ms\n",
			g_perf_stats.digest_file_tsc / (clock_speed_mhz * 1000.0));
	printf("- persist    : %.3f ms\n",
			g_perf_stats.persist_time_tsc / (clock_speed_mhz * 1000.0));
	printf("n_digest        : %lu\n", g_perf_stats.n_digest);
	printf("n_digest_skipped: %lu (%.1f %%)\n", 
			g_perf_stats.n_digest_skipped, 
			((float)g_perf_stats.n_digest_skipped * 100.0) / (float)n_digest);
	printf("path search    : %.3f ms\n",
			g_perf_stats.path_search_tsc / (clock_speed_mhz * 1000.0));
	printf("total migrated : %lu MB\n", g_perf_stats.total_migrated_mb);
#ifdef MLFS_LEASE
	printf("nr lease rpc (local)	: %lu\n", g_perf_stats.lease_rpc_local_nr);
	printf("nr lease rpc (remote)	: %lu\n", g_perf_stats.lease_rpc_remote_nr);
	printf("nr lease migration	: %lu\n", g_perf_stats.lease_migration_nr);
	printf("nr lease contention	: %lu\n", g_perf_stats.lease_contention_nr);
#endif
	printf("--------------------------------------\n");
}

void show_storage_stats(void)
{
	printf("------------------------------ storage stats\n");
	printf("NVM - %.2f %% used (%.2f / %.2f GB) - # of used blocks %lu\n", 
			(100.0 * (float)sb[g_root_dev]->used_blocks) /
			(float)disk_sb[g_root_dev].ndatablocks,
			(float)(sb[g_root_dev]->used_blocks << g_block_size_shift) / (1 << 30),
			(float)(disk_sb[g_root_dev].ndatablocks << g_block_size_shift) / (1 << 30),
			sb[g_root_dev]->used_blocks);

#ifdef USE_SSD
	printf("SSD - %.2f %% used (%.2f / %.2f GB) - # of used blocks %lu\n", 
			(100.0 * (float)sb[g_ssd_dev]->used_blocks) /
			(float)disk_sb[g_ssd_dev].ndatablocks,
			(float)(sb[g_ssd_dev]->used_blocks << g_block_size_shift) / (1 << 30),
			(float)(disk_sb[g_ssd_dev].ndatablocks << g_block_size_shift) / (1 << 30),
			sb[g_ssd_dev]->used_blocks);
#endif

#ifdef USE_HDD
	printf("HDD - %.2f %% used (%.2f / %.2f GB) - # of used blocks %lu\n", 
			(100.0 * (float)sb[g_hdd_dev]->used_blocks) /
			(float)disk_sb[g_hdd_dev].ndatablocks,
			(float)(sb[g_hdd_dev]->used_blocks << g_block_size_shift) / (1 << 30),
			(float)(disk_sb[g_hdd_dev].ndatablocks << g_block_size_shift) / (1 << 30),
			sb[g_hdd_dev]->used_blocks);
#endif

		mlfs_info("--- lru_list count %lu, %lu, %lu\n",
			  g_lru[g_root_dev].n, g_lru[g_ssd_dev].n, g_lru[g_hdd_dev].n);
}

loghdr_meta_t *read_log_header(uint8_t from_dev, addr_t hdr_addr)
{
	int ret, i;
	loghdr_t *_loghdr;
	loghdr_meta_t *loghdr_meta;

	loghdr_meta = (loghdr_meta_t *)mlfs_zalloc(sizeof(loghdr_meta_t));
	if (!loghdr_meta) 
		panic("cannot allocate logheader\n");

	INIT_LIST_HEAD(&loghdr_meta->link);

	/* optimization: instead of reading log header block to kernel's memory,
	 * buffer head points to memory address for log header block.
	 */
	_loghdr = (loghdr_t *)(g_bdev[from_dev]->map_base_addr + 
		(hdr_addr << g_block_size_shift));

	loghdr_meta->loghdr = _loghdr;
	loghdr_meta->hdr_blkno = hdr_addr;
	loghdr_meta->is_hdr_allocated = 1;

	mlfs_log("%s", "--------------------------------\n");
	mlfs_log("%d\n", _loghdr->n);
	mlfs_log("ts %ld.%06ld\n", _loghdr->mtime.tv_sec, _loghdr->mtime.tv_usec);
	mlfs_log("blknr %lu\n", hdr_addr);
	mlfs_log("size %u\n", _loghdr->nr_log_blocks);
	mlfs_log("inuse %x\n", _loghdr->inuse);
	for(int i=0; i< _loghdr->n; i++) {
		mlfs_log("type[%d]: %u\n", i, _loghdr->type[i]);
		mlfs_log("inum[%d]: %u\n", i, _loghdr->inode_no[i]);
	}

	/*
	for (i = 0; i < _loghdr->n; i++) {
		mlfs_debug("types %d blocks %lx\n", 
				_loghdr->type[i], _loghdr->blocks[i] + loghdr_meta->hdr_blkno);
	}
	*/

	return loghdr_meta;
}

// FIXME: to_dev is now used. change APIs
int digest_inode(uint8_t from_dev, uint8_t to_dev, int libfs_id, 
		uint32_t inum, addr_t blknr)
{
	struct buffer_head *bh;
	struct dinode *src_dinode;
	struct inode *inode;
	int ret;

	bh = bh_get_sync_IO(from_dev, blknr, BH_NO_DATA_ALLOC);
	bh->b_size = sizeof(struct dinode);
	bh->b_data = mlfs_alloc(bh->b_size);

	bh_submit_read_sync_IO(bh);
	mlfs_io_wait(from_dev, 1);

	src_dinode = (struct dinode *)bh->b_data;

	mlfs_debug("[INODE] dev %u type %u inum %u size %lu\n",
			g_root_dev, src_dinode->itype, inum, src_dinode->size);

	// Inode exists only in NVM layer.
	to_dev = g_root_dev;

	inode = icache_find(inum);

	if (!inode) 
		inode = ialloc(src_dinode->itype, inum);

	mlfs_assert(inode);

	if (inode->flags & I_DELETING) {
		// reuse deleting inode.
		// digest_unlink cleaned up old contents already.

		inode->flags &= ~I_DELETING;
		inode->flags |= I_VALID;
		inode->itype = src_dinode->itype;
		inode->i_sb = sb;
		inode->i_generation = 0;
		inode->i_data_dirty = 0;
	}

	if (inode->itype == T_FILE || inode->itype == T_DIR) {
		struct mlfs_extent_header *ihdr;
		handle_t handle = {.libfs =libfs_id, .dev = to_dev};

		ihdr = ext_inode_hdr(&handle, inode);

		// The first creation of dinode of file
		if (ihdr->eh_magic != MLFS_EXT_MAGIC) {
			mlfs_ext_tree_init(&handle, inode);

			// For testing purpose, those data are hard-coded.
			inode->i_writeback = NULL;
			memset(inode->i_uuid, 0xCC, sizeof(inode->i_uuid));
			inode->i_csum = mlfs_crc32c(~0, inode->i_uuid, sizeof(inode->i_uuid));
			inode->i_csum =
				mlfs_crc32c(inode->i_csum, &inode->inum, sizeof(inode->inum));
			inode->i_csum = mlfs_crc32c(inode->i_csum, &inode->i_generation,
					sizeof(inode->i_generation));
		}

		if (inode->itype == T_FILE) {
			// ftruncate (shrink length)
			if (src_dinode->size < inode->size) {
				handle_t handle;
				handle.dev = g_root_dev;
				handle.libfs = libfs_id;

				// fix truncate segmentation fault by bad end block calculation
				mlfs_lblk_t start_blk = (src_dinode->size >> g_block_size_shift) +
					((src_dinode->size & g_block_size_mask) != 0);
				mlfs_lblk_t end_blk = (inode->size >> g_block_size_shift);
				
				ret = mlfs_ext_truncate(&handle, inode, start_blk, end_blk);

				mlfs_assert(!ret);
			}
		}
	}

	inode->size = src_dinode->size;

	mlfs_debug("[INODE] (%d->%d) inode inum %u type %d, size %lu\n",
			from_dev, to_dev, inode->inum, inode->itype, inode->size);

	mlfs_mark_inode_dirty(libfs_id, inode);

	clear_buffer_uptodate(bh);

	mlfs_free(bh->b_data);
	bh_release(bh);

#if 0
	// mark inode digested
	if(inode->lstate == LEASE_DIGEST_TO_ACQUIRE || inode->lstate == LEASE_DIGEST_TO_READ) {
	  inode->lstate = LEASE_FREE;
	}
#endif
	return 0;
}

int digest_file(uint8_t from_dev, uint8_t to_dev, int libfs_id, uint32_t file_inum, 
		offset_t offset, uint32_t length, addr_t blknr)
{
	int ret;
	uint32_t offset_in_block = 0;
	struct inode *file_inode;
	struct buffer_head *bh_data, *bh;
	uint8_t *data;
	struct mlfs_ext_path *path = NULL;
	struct mlfs_map_blocks map;
	uint32_t nr_blocks = 0, nr_digested_blocks = 0;
	offset_t cur_offset;
	handle_t handle = {.libfs = libfs_id, .dev = to_dev};

	mlfs_debug("[FILE] (%d->%d) inum %d offset %lu(0x%lx) length %u\n", 
			from_dev, to_dev, file_inum, offset, offset, length);

	if (length < g_block_size_bytes)
		nr_blocks = 1;
	else {
		nr_blocks = (length >> g_block_size_shift);

		if (length % g_block_size_bytes != 0) 
			nr_blocks++;
	}

	mlfs_assert(nr_blocks > 0);

	if ((from_dev == g_ssd_dev) || (from_dev == g_hdd_dev))
		panic("does not support this case yet\n");

	// Storage to storage copy.
	// FIXME: this does not work if migrating block from SSD to NVM.
	data = g_bdev[from_dev]->map_base_addr + (blknr << g_block_size_shift);
	file_inode = icache_find(file_inum);
	if (!file_inode) {
		struct dinode dip;
		file_inode = icache_alloc_add(file_inum);

		read_ondisk_inode(file_inum, &dip);
		mlfs_assert(dip.itype != 0);

		sync_inode_from_dinode(file_inode, &dip);
	}

	// update file inode length and mtime.
	if (file_inode->size < offset + length) {
		/* Inode size should be synchronized among other layers.
		 * So, update both inodes */
		file_inode->size = offset + length;
		mlfs_mark_inode_dirty(libfs_id, file_inode);
	}

	nr_digested_blocks = 0;
	cur_offset = offset;
	offset_in_block = offset % g_block_size_bytes;
	
	// case 1. a single block writing: small size (< 4KB) 
	// or a heading block of unaligned starting offset.
	if ((length < g_block_size_bytes) || offset_in_block != 0) {
		int _len = _min(length, (uint32_t)g_block_size_bytes - offset_in_block);

		map.m_lblk = (cur_offset >> g_block_size_shift);
		map.m_pblk = 0;
		map.m_len = 1;
		map.m_flags = 0;
		ret = mlfs_ext_get_blocks(&handle, file_inode, &map, 
				MLFS_GET_BLOCKS_CREATE);

		mlfs_assert(ret == 1);
		bh_data = bh_get_sync_IO(to_dev, map.m_pblk, BH_NO_DATA_ALLOC); 

		mlfs_assert(bh_data);

		bh_data->b_data = data + offset_in_block;
		bh_data->b_size = _len;
		bh_data->b_offset = offset_in_block;

#ifdef MIGRATION
		lru_key_t k = {
		  .dev = to_dev,
		  .block = map.m_pblk,
		};
		lru_val_t v = {
		  .inum = file_inum,
		  .lblock = map.m_lblk,
		};
		update_slru_list_from_digest(to_dev, k, v);
#endif
		//mlfs_debug("File data : %s\n", bh_data->b_data);

		ret = mlfs_write_opt(bh_data);
		mlfs_assert(!ret);

		bh_release(bh_data);

		mlfs_debug("inum %d, offset %lu len %u (dev %d:%lu) -> (dev %d:%lu)\n", 
				file_inode->inum, cur_offset, _len, 
				from_dev, blknr, to_dev, map.m_pblk);

		nr_digested_blocks++;
		cur_offset += _len;
		data += _len;
	}

	// case 2. multiple trial of block writing.
	// when extent tree has holes in a certain offset (due to data migration),
	// an extent is split at the hole. Kernfs should call mlfs_ext_get_blocks()
	// with setting m_lblk to the offset having a the hole to fill it.
	while (nr_digested_blocks < nr_blocks) {
		int nr_block_get = 0, i;

		mlfs_assert((cur_offset % g_block_size_bytes) == 0);

		map.m_lblk = (cur_offset >> g_block_size_shift);
		map.m_pblk = 0;
		map.m_len = nr_blocks - nr_digested_blocks;
		map.m_flags = 0;

		// find block address of offset and update extent tree
		if (to_dev == g_ssd_dev || to_dev == g_hdd_dev) {
			//make kernelFS do log-structured update.
			//map.m_flags |= MLFS_MAP_LOG_ALLOC;
			nr_block_get = mlfs_ext_get_blocks(&handle, file_inode, &map, 
					MLFS_GET_BLOCKS_CREATE_DATA);
		} else {
			nr_block_get = mlfs_ext_get_blocks(&handle, file_inode, &map, 
					MLFS_GET_BLOCKS_CREATE_DATA);
		}

		mlfs_assert(map.m_pblk != 0);

		mlfs_assert(nr_block_get <= (nr_blocks - nr_digested_blocks));
		mlfs_assert(nr_block_get > 0);

		nr_digested_blocks += nr_block_get;

		// update data block
		bh_data = bh_get_sync_IO(to_dev, map.m_pblk, BH_NO_DATA_ALLOC);

		bh_data->b_data = data;
		bh_data->b_size = nr_block_get * g_block_size_bytes;
		bh_data->b_offset = 0;

#ifdef MIGRATION
		//TODO: important note: this scheme only works if LRU_ENTRY_SIZE is
		//equal to g_block_size_bytes. To use larger LRU_ENTRY_SIZES and keep
		//the g_lru_hash map small, we should align the keys by LRU_ENTRY_SIZE
		for (i = 0; i < nr_block_get; i++) { 
			lru_key_t k = {
				.dev = to_dev,
				.block = map.m_pblk + i,
			};
			lru_val_t v = {
				.inum = file_inum,
				.lblock = map.m_lblk + i,
			};
			update_slru_list_from_digest(to_dev, k, v);
		}
#endif

		//mlfs_debug("File data : %s\n", bh_data->b_data);

		ret = mlfs_write_opt(bh_data);
		mlfs_assert(!ret);
		clear_buffer_uptodate(bh_data);
		bh_release(bh_data);

		if (0) {
			struct buffer_head *bh;
			uint8_t tmp_buf[4096];
			bh = bh_get_sync_IO(to_dev, map.m_pblk, BH_NO_DATA_ALLOC);

			bh->b_data = tmp_buf;
			bh->b_size = g_block_size_bytes;
			bh_submit_read_sync_IO(bh);
			mlfs_io_wait(bh->b_dev, 1);

			GDB_TRAP;

			bh_release(bh);
		}

		mlfs_debug("inum %d, offset %lu len %u (dev %d:%lu) -> (dev %d:%lu)\n", 
				file_inode->inum, cur_offset, bh_data->b_size, 
				from_dev, blknr, to_dev, map.m_pblk);

		cur_offset += nr_block_get * g_block_size_bytes;
		data += nr_block_get * g_block_size_bytes;
	}

	mlfs_assert(nr_blocks == nr_digested_blocks);

	if (file_inode->size < offset + length)
		file_inode->size = offset + length;

	return 0;
}

int digest_allocate(uint8_t from_dev, uint8_t to_dev, int libfs_id, uint32_t inum, uint64_t length)
{
	struct inode *ip;
	struct mlfs_map_blocks map;
	handle_t handle;
	uint64_t first_pblk;
	uint64_t blk_alloced;
	uint64_t blk_length;
	int ret;

	mlfs_printf("[ALLOCATE] from %u to %u, inum %u, size %lu\n", from_dev, to_dev, inum, length);

	ip = icache_find(inum);
	if (!ip) {
			struct dinode dip;
		ip = icache_alloc_add(inum);
		read_ondisk_inode(inum, &dip);
		mlfs_assert(dip.itype != 0);
		sync_inode_from_dinode(ip, &dip);
	}

	if (ip->size >= length)
		panic("error: fallocate to smaller size!");

	blk_length = length >> g_block_size_shift;
	if (length % g_block_size_bytes)
		blk_length++;

	mlfs_printf("allocating %lu blocks!\n", blk_length);

	handle.dev = to_dev;
	handle.libfs = libfs_id;
	blk_alloced = 0;
	while (blk_alloced < blk_length) {
			map.m_lblk = blk_alloced;
		map.m_pblk = 0;
		map.m_flags = 0;

		if ((blk_length - blk_alloced) > ALLOC_SIZE)
			map.m_len = ALLOC_SIZE;
		else
			map.m_len = blk_length - blk_alloced;

		mlfs_printf("try allocate %u blocks, lblk start %u\n", map.m_len,  map.m_lblk);
		ret = mlfs_ext_get_blocks(&handle, ip, &map, MLFS_GET_BLOCKS_CREATE);
		mlfs_printf("allocated %d blocks from %lu\n", ret, map.m_pblk);

		if (ret <= 0) {
				mlfs_printf("mlfs_ext_get_blocks failed, ret %d!", ret);
			panic("fallocate");
		}

		if (map.m_lblk == 0)
			first_pblk = map.m_pblk;
		else if (map.m_pblk != first_pblk + blk_alloced) {
				mlfs_printf("non-contiguous allocation! segment start %lu, first pblk %lu, contiguous length %lu", map.m_pblk, first_pblk, blk_alloced);
			panic("fallocate");
		}

		blk_alloced += ret;

	}

	mlfs_mark_inode_dirty(libfs_id, ip);
	return 0;
}


//FIXME: this function is not synchronized with up-to-date
//changes. Refer digest_file to update this function.
int digest_file_iovec(uint8_t from_dev, uint8_t to_dev, int libfs_id, 
		uint32_t file_inum, f_iovec_t *iovec)
{
	int ret;
	uint32_t offset_in_block = 0;
	struct inode *file_inode;
	struct buffer_head *bh_data, *bh;
	uint8_t *data;
	struct mlfs_ext_path *path = NULL;
	struct mlfs_map_blocks map;
	uint32_t nr_blocks = 0, nr_digested_blocks = 0;
	offset_t cur_offset;
	uint32_t length = iovec->length;
	offset_t offset = iovec->offset;
	f_blklist_t *_blk_list;
	handle_t handle = {.libfs = libfs_id, .dev = to_dev};

	mlfs_debug("[FILE] (%d->%d) inum %d offset %lu(0x%lx) length %u\n", 
			from_dev, to_dev, file_inum, offset, offset, length);

	if (length < g_block_size_bytes)
		nr_blocks = 1;
	else {
		nr_blocks = (length >> g_block_size_shift);

		if (length % g_block_size_bytes != 0) 
			nr_blocks++;
	}

	mlfs_assert(nr_blocks > 0);

	if (from_dev == g_ssd_dev)
		panic("does not support this case\n");

	file_inode = icache_find(file_inum);
	if (!file_inode) {
		struct dinode dip;
		file_inode = icache_alloc_add(file_inum);

		read_ondisk_inode(file_inum, &dip);
		mlfs_assert(dip.itype != 0);

		sync_inode_from_dinode(file_inode, &dip);
	}

#ifdef USE_SSD
	// update file inode length and mtime.
	if (file_inode->size < offset + length) {
		/* Inode size should be synchronized among NVM and SSD layer.
		 * So, update both inodes */
		uint8_t sync_dev = 3 - to_dev;
		handle_t handle;
		struct inode *sync_file_inode = icache_find(file_inum);
		if (!sync_file_inode) {
			struct dinode dip;
			sync_file_inode = icache_alloc_add(file_inum);

			read_ondisk_inode(file_inum, &dip);

			mlfs_assert(dip.itype != 0);
			sync_inode_from_dinode(sync_file_inode, &dip);

			file_inode->i_sb = sb;
		}

		file_inode->size = offset + length;
		sync_file_inode->size = file_inode->size;

		handle.libfs = libfs_id;

		handle.dev = to_dev;
		mlfs_mark_inode_dirty(libfs_id, file_inode);

		handle.dev = sync_dev;
		mlfs_mark_inode_dirty(libfs_id, sync_file_inode);
	}
#endif

	nr_digested_blocks = 0;
	cur_offset = offset;
	offset_in_block = offset % g_block_size_bytes;

	_blk_list = list_first_entry(&iovec->iov_blk_list, f_blklist_t, list);

	// case 1. a single block writing: small size (< 4KB) 
	// or a heading block of unaligned starting offset.
	if ((length < g_block_size_bytes) || offset_in_block != 0) {
		int _len = _min(length, (uint32_t)g_block_size_bytes - offset_in_block);

		//mlfs_assert(_blk_list->n == 1);
		data = g_bdev[from_dev]->map_base_addr + (_blk_list->blknr << g_block_size_shift);

		map.m_lblk = (cur_offset >> g_block_size_shift);
		map.m_pblk = 0;
		map.m_len = 1;

		ret = mlfs_ext_get_blocks(&handle, file_inode, &map, 
				MLFS_GET_BLOCKS_CREATE);

		mlfs_assert(ret == 1);

		bh_data = bh_get_sync_IO(to_dev, map.m_pblk, BH_NO_DATA_ALLOC);

		bh_data->b_data = data + offset_in_block;
		bh_data->b_size = _len;
		bh_data->b_offset = offset_in_block;

		//mlfs_debug("File data : %s\n", bh_data->b_data);

		ret = mlfs_write_opt(bh_data);
		mlfs_assert(!ret);
		clear_buffer_uptodate(bh_data);
		bh_release(bh_data);

		mlfs_debug("inum %d, offset %lu (dev %d:%lx) -> (dev %d:%lx)\n", 
				file_inode->inum, cur_offset, from_dev, 
				iovec->blknr, to_dev, map.m_pblk);

		nr_digested_blocks++;
		cur_offset += _len;
		data += _len;
	}

	// case 2. multiple trial of block writing.
	// when extent tree has holes in a certain offset (due to data migration),
	// an extent is split at the hole. Kernfs should call mlfs_ext_get_blocks()
	// with setting m_lblk to the offset having a the hole to fill it.
	while (nr_digested_blocks < nr_blocks) {
		int nr_block_get = 0, i, j;
		int rand_val; 

		mlfs_assert((cur_offset % g_block_size_bytes) == 0);

		map.m_lblk = (cur_offset >> g_block_size_shift);
		map.m_pblk = 0;
		map.m_len = min(nr_blocks - nr_digested_blocks, (1 << 15));

		// find block address of offset and update extent tree
		nr_block_get = mlfs_ext_get_blocks(&handle, file_inode, &map, 
				MLFS_GET_BLOCKS_CREATE);

		mlfs_assert(map.m_pblk != 0);

		mlfs_assert(nr_block_get > 0);

		nr_digested_blocks += nr_block_get;

		if (_blk_list->n > nr_block_get) {
			data = g_bdev[from_dev]->map_base_addr + 
				(_blk_list->blknr << g_block_size_shift);

			mlfs_debug("inum %d, offset %lu len %u (dev %d:%lu) -> (dev %d:%lu)\n", 
					file_inode->inum, cur_offset, _blk_list->n << g_block_size_shift, 
					from_dev, _blk_list->blknr, to_dev, map.m_pblk);

			bh_data = bh_get_sync_IO(to_dev, map.m_pblk, BH_NO_DATA_ALLOC);

			bh_data->b_data = data;
			bh_data->b_size = (_blk_list->n << g_block_size_shift);
			bh_data->b_offset = 0;

			//mlfs_debug("File data : %s\n", bh_data->b_data);

			ret = mlfs_write_opt(bh_data);
			mlfs_assert(!ret);
			clear_buffer_uptodate(bh_data);
			bh_release(bh_data);

			cur_offset += (nr_block_get << g_block_size_shift);

			_blk_list->n -= nr_block_get;
			_blk_list->blknr += nr_block_get;

			continue;
		}

		for (i = 0, j = _blk_list->n; j <= nr_block_get; 
				i += _blk_list->n, j += _blk_list->n) {
			data = g_bdev[from_dev]->map_base_addr + 
				(_blk_list->blknr << g_block_size_shift);

			mlfs_debug("inum %d, offset %lu len %u (dev %d:%lu) -> (dev %d:%lu)\n", 
					file_inode->inum, cur_offset, _blk_list->n << g_block_size_shift, 
					from_dev, _blk_list->blknr, to_dev, map.m_pblk + i);

			// update data block
			bh_data = bh_get_sync_IO(to_dev, map.m_pblk + i, BH_NO_DATA_ALLOC);

			bh_data->b_data = data;
			bh_data->b_blocknr = map.m_pblk + i;
			bh_data->b_size = (_blk_list->n << g_block_size_shift);
			bh_data->b_offset = 0;

			//mlfs_debug("File data : %s\n", bh_data->b_data);

			ret = mlfs_write_opt(bh_data);
			mlfs_assert(!ret);
			clear_buffer_uptodate(bh_data);
			bh_release(bh_data);

			cur_offset += (_blk_list->n << g_block_size_shift);

			_blk_list = list_next_entry(_blk_list, list);

			if (&_blk_list->list == &iovec->iov_blk_list)
				break;
		}
	}

	mlfs_assert(nr_blocks == nr_digested_blocks);

	if (file_inode->size < offset + length)
		file_inode->size = offset + length;

	return 0;
}

int digest_unlink(uint8_t from_dev, uint8_t to_dev, int libfs_id, uint32_t inum)
{
	struct buffer_head *bh;
	struct inode *inode;
	struct dinode dinode;
	int ret = 0;

	mlfs_assert(to_dev == g_root_dev);

	mlfs_debug("[UNLINK] (%d->%d) inum %d\n", from_dev, to_dev, inum);

	inode = icache_find(inum);
	if(!inode) {
		struct dinode dip;
		inode = icache_alloc_add(inum);

		read_ondisk_inode(inum, &dip);
		sync_inode_from_dinode(inode, &dip);
	}

	mlfs_assert(inode);

	if (inode->size > 0) {
		handle_t handle = {.libfs = libfs_id, .dev = to_dev};
		mlfs_lblk_t end = (inode->size) >> g_block_size_shift;

		ret = mlfs_ext_truncate(&handle, inode, 0, end == 0 ? end : end - 1);
		mlfs_assert(!ret);
	}

	memset(inode->_dinode, 0, sizeof(struct dinode));

	inode->flags = 0;
	inode->flags |= I_DELETING;
	inode->size = 0;
	inode->itype = 0;
	
	mlfs_mark_inode_dirty(libfs_id, inode);

	return 0;
}

static void digest_each_log_entries(uint8_t from_dev, int libfs_id, loghdr_meta_t *loghdr_meta)
{
	int i, ret;
	loghdr_t *loghdr;
	uint16_t nr_entries;
	uint64_t tsc_begin;

	nr_entries = loghdr_meta->loghdr->n;
	loghdr = loghdr_meta->loghdr;

	for (i = 0; i < nr_entries; i++) {
		if (enable_perf_stats)
			g_perf_stats.n_digest++;

		//mlfs_printf("digesting log entry with inum %d peer_id %d\n", loghdr->inode_no[i], libfs_id);
		// parse log entries on types.
		switch(loghdr->type[i]) {
			case L_TYPE_INODE_CREATE: 
			// ftruncate is handled by this case.
			case L_TYPE_INODE_UPDATE: {
				if (enable_perf_stats) 
					tsc_begin = asm_rdtscp();

				ret = digest_inode(from_dev,
						g_root_dev,
						libfs_id,
						loghdr->inode_no[i], 
						loghdr->blocks[i] + loghdr_meta->hdr_blkno);
				mlfs_assert(!ret);

				if (enable_perf_stats)
					g_perf_stats.digest_inode_tsc +=
						asm_rdtscp() - tsc_begin;
				break;
			}
			case L_TYPE_DIR_ADD: 
			case L_TYPE_DIR_RENAME: 
			case L_TYPE_DIR_DEL:
			case L_TYPE_FILE: {
				uint8_t dest_dev = g_root_dev;
				int rand_val;
				lru_key_t k;

				if (enable_perf_stats) 
					tsc_begin = asm_rdtscp();
#ifdef USE_SSD
				// for NVM bypassing test
				//dest_dev = g_ssd_dev;
#endif
				ret = digest_file(from_dev, 
						dest_dev,
						libfs_id,
						loghdr->inode_no[i], 
						loghdr->data[i], 
						loghdr->length[i],
						loghdr->blocks[i] + loghdr_meta->hdr_blkno);
				mlfs_assert(!ret);

				if (enable_perf_stats)
					g_perf_stats.digest_file_tsc +=
						asm_rdtscp() - tsc_begin;
				break;
			}
			case L_TYPE_UNLINK: {
				if (enable_perf_stats) 
					tsc_begin = asm_rdtscp();

				ret = digest_unlink(from_dev,
						g_root_dev,
						libfs_id, 
						loghdr->inode_no[i]);
				mlfs_assert(!ret);

				if (enable_perf_stats) 
					g_perf_stats.digest_inode_tsc +=
						asm_rdtscp() - tsc_begin;
				break;
			}
			case L_TYPE_ALLOC: {
				ret = digest_allocate(from_dev,
						g_root_dev,
					        libfs_id,
						loghdr->inode_no[i],
						loghdr->data[i]);
				mlfs_assert(!ret);
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

static void digest_replay_and_optimize(uint8_t from_dev, 
		loghdr_meta_t *loghdr_meta, struct replay_list *replay_list)
{
	int i, ret;
	loghdr_t *loghdr;
	uint16_t nr_entries;

	nr_entries = loghdr_meta->loghdr->n;
	loghdr = loghdr_meta->loghdr;

	for (i = 0; i < nr_entries; i++) {
		switch(loghdr->type[i]) {
			case L_TYPE_INODE_CREATE:
			case L_TYPE_INODE_UPDATE: {
				i_replay_t search, *item;
				memset(&search, 0, sizeof(i_replay_t));

				search.key.inum = loghdr->inode_no[i];

				if (loghdr->type[i] == L_TYPE_INODE_CREATE) 
						inode_version_table[search.key.inum]++;
				search.key.ver = inode_version_table[search.key.inum];

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
					item->blknr = loghdr->blocks[i] + loghdr_meta->hdr_blkno;
					if (enable_perf_stats)
						g_perf_stats.n_digest++;
					break;
				}
				case L_TYPE_DIR_DEL: 
				case L_TYPE_DIR_RENAME: 
				case L_TYPE_DIR_ADD: 
				case L_TYPE_FILE: {
					f_replay_t search, *item;
					f_iovec_t *f_iovec;
					f_blklist_t *_blk_list;
					lru_key_t k;
					offset_t iovec_key;
					int found = 0;

					memset(&search, 0, sizeof(f_replay_t));
					search.key.inum = loghdr->inode_no[i];
					search.key.ver = inode_version_table[loghdr->inode_no[i]];

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
					}

	#ifndef EXPERIMENTAL
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
						f_iovec->blknr = loghdr->blocks[i] + loghdr_meta->hdr_blkno;
						// TODO: merge data from loghdr->blocks to f_iovec buffer.
						found = 1;
					}

					if (!found) {
						f_iovec = (f_iovec_t *)mlfs_zalloc(sizeof(f_iovec_t));
						f_iovec->length = loghdr->length[i];
						f_iovec->offset = loghdr->data[i];
						f_iovec->blknr = loghdr->blocks[i] + loghdr_meta->hdr_blkno;
						INIT_LIST_HEAD(&f_iovec->list);
						list_add_tail(&f_iovec->list, &item->iovec_list);

						f_iovec->hash_key = iovec_key;
						HASH_ADD(hh, item->iovec_hash, hash_key,
								sizeof(offset_t), f_iovec);
					}
	#else
					f_iovec = (f_iovec_t *)mlfs_zalloc(sizeof(f_iovec_t));
					f_iovec->length = loghdr->length[i];
					f_iovec->offset = loghdr->data[i];
					f_iovec->blknr = loghdr->blocks[i] + loghdr_meta->hdr_blkno;
					INIT_LIST_HEAD(&f_iovec->list);
					list_add_tail(&f_iovec->list, &item->iovec_list);
	#endif	//IOMERGE

	#else //EXPERIMENTAL
					// Experimental feature: merge contiguous small writes to
					// a single write one.
					mlfs_debug("new log block %lu\n", loghdr->blocks[i] + loghdr_meta->hdr_blkno);
					_blk_list = (f_blklist_t *)mlfs_zalloc(sizeof(f_blklist_t));
					INIT_LIST_HEAD(&_blk_list->list);

					// FIXME: Now only support 4K aligned write.
					_blk_list->n = (loghdr->length[i] >> g_block_size_shift);
					_blk_list->blknr = loghdr->blocks[i] + loghdr_meta->hdr_blkno;

					if (!list_empty(&item->iovec_list)) {
						f_iovec = list_last_entry(&item->iovec_list, f_iovec_t, list);

						// Find the case where io_vector can be coalesced.
						if (f_iovec->offset + f_iovec->length == loghdr->data[i]) {
							f_iovec->length += loghdr->length[i];
							f_iovec->n_list++;

							mlfs_debug("block is merged %u\n", _blk_list->blknr);
							list_add_tail(&_blk_list->list, &f_iovec->iov_blk_list);
						} else {
							mlfs_debug("new f_iovec %lu\n",  loghdr->data[i]); 
							// cannot coalesce io_vector. allocate new one.
							f_iovec = (f_iovec_t *)mlfs_zalloc(sizeof(f_iovec_t));
							f_iovec->length = loghdr->length[i];
							f_iovec->offset = loghdr->data[i];
							f_iovec->blknr = loghdr->blocks[i] + loghdr_meta->hdr_blkno;
							INIT_LIST_HEAD(&f_iovec->list);
							INIT_LIST_HEAD(&f_iovec->iov_blk_list);

							list_add_tail(&_blk_list->list, &f_iovec->iov_blk_list);
							list_add_tail(&f_iovec->list, &item->iovec_list);
						}
					} else {
						f_iovec = (f_iovec_t *)mlfs_zalloc(sizeof(f_iovec_t));
						f_iovec->length = loghdr->length[i];
						f_iovec->offset = loghdr->data[i];
						f_iovec->blknr = loghdr->blocks[i] + loghdr_meta->hdr_blkno;
						INIT_LIST_HEAD(&f_iovec->list);
						INIT_LIST_HEAD(&f_iovec->iov_blk_list);

						list_add_tail(&_blk_list->list, &f_iovec->iov_blk_list);
						list_add_tail(&f_iovec->list, &item->iovec_list);
					}
	#endif

					if (enable_perf_stats)
						g_perf_stats.n_digest++;
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

					replay_key_t key = {
						.inum = loghdr->inode_no[i],
						.ver = inode_version_table[loghdr->inode_no[i]],
					};

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

					if (i_item && i_item->create) {
						mlfs_debug("[INODE] inum %u (ver %u) --> SKIP\n", 
							i_item->key.inum, i_item->key.ver);
					// the unlink can skip and erase related i_items
					HASH_DEL(replay_list->i_digest_hash, i_item);
					list_del(&i_item->list);
					mlfs_free(i_item);
					if (enable_perf_stats)
						g_perf_stats.n_digest_skipped++;
				} else {
					// the unlink must be applied. create a new unlink item.
					u_item = (u_replay_t *)mlfs_zalloc(sizeof(u_replay_t));
					u_item->key = key;
					u_item->node_type = NTYPE_U;
					HASH_ADD(hh, replay_list->u_digest_hash, key,
							sizeof(replay_key_t), u_item);
					list_add_tail(&u_item->list, &replay_list->head);
					mlfs_debug("[ULINK] inum %u (ver %u)\n", 
							u_item->key.inum, u_item->key.ver);
					if (enable_perf_stats)
						g_perf_stats.n_digest++;
				}

#if 0
				HASH_FIND(hh, replay_list->u_digest_hash, &key,
						sizeof(replay_key_t), u_item);
				if (u_item) {
					// previous unlink can skip. 
					mlfs_debug("[ULINK] inum %u (ver %u) --> SKIP\n", 
							u_item->key.inum, u_item->key.ver);
					HASH_DEL(replay_list->u_digest_hash, u_item);
					list_del(&u_item->list);
					mlfs_free(u_item);
				} 
#endif

				// delete file digest info.
				f_search.key.inum = key.inum;
				f_search.key.ver = key.ver;

				HASH_FIND(hh, replay_list->f_digest_hash, &f_search.key,
						sizeof(replay_key_t), f_item);

				if (f_item) {
					list_for_each_entry_safe(f_iovec, tmp, 
							&f_item->iovec_list, list) {
						list_del(&f_iovec->list);
						mlfs_free(f_iovec);
						
						if (enable_perf_stats)
							g_perf_stats.n_digest_skipped++;
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

#ifdef FCONCURRENT
static void file_digest_worker(void *arg)
{
	struct f_digest_worker_arg *_arg = (struct f_digest_worker_arg *)arg;
	f_iovec_t *f_iovec, *iovec_tmp;
	f_replay_t *f_item;
	lru_key_t k;

	f_item = _arg->f_item;

	list_for_each_entry_safe(f_iovec, iovec_tmp, 
			&f_item->iovec_list, list) {
#ifndef EXPERIMENTAL
		digest_file(_arg->from_dev, _arg->to_dev, _arg->libfs_id,
				f_item->key.inum, f_iovec->offset, 
				f_iovec->length, f_iovec->blknr);
		mlfs_free(f_iovec);
#else
		digest_file_iovec(_arg->from_dev, _arg->to_dev, _arg->libfs_id, 
				f_item->key.inum, f_iovec);
#endif //EXPERIMENTAL
	}

	mlfs_free(_arg);
}
#endif

// digest must be applied in order since unlink and creation cannot commute.
static void digest_log_from_replay_list(uint8_t from_dev, int libfs_id, struct replay_list *replay_list)
{
	struct list_head *l, *tmp;
	uint8_t *node_type;
	f_iovec_t *f_iovec, *iovec_tmp;
	uint64_t tsc_begin;

	list_for_each_safe(l, tmp, &replay_list->head) {
		node_type = (uint8_t *)l + sizeof(struct list_head);
		mlfs_assert(*node_type < 5);

		switch(*node_type) {
			case NTYPE_I: {
				i_replay_t *i_item;
				i_item = (i_replay_t *)container_of(l, i_replay_t, list);

				if (enable_perf_stats) 
					tsc_begin = asm_rdtscp();

				digest_inode(from_dev, g_root_dev, libfs_id, i_item->key.inum, i_item->blknr);

				HASH_DEL(replay_list->i_digest_hash, i_item);
				list_del(l);
				mlfs_free(i_item);

				if (enable_perf_stats)
					g_perf_stats.digest_inode_tsc += asm_rdtscp() - tsc_begin;
				break;
			}
			case NTYPE_D: {
				panic("deprecated operation: NTYPE_D\n");
				break;
			}
			case NTYPE_F: {
				uint8_t dest_dev = g_root_dev;
				f_replay_t *f_item, *t;
				f_item = (f_replay_t *)container_of(l, f_replay_t, list);
				lru_key_t k;

				if (enable_perf_stats) 
					tsc_begin = asm_rdtscp();

#ifdef FCONCURRENT
				HASH_ITER(hh, replay_list->f_digest_hash, f_item, t) {
					struct f_digest_worker_arg *arg;

					// Digest worker thread will free the arg.
					arg = (struct f_digest_worker_arg *)mlfs_alloc(
							sizeof(struct f_digest_worker_arg));

					arg->from_dev = from_dev;
					arg->to_dev = g_root_dev;
					arg->f_item = f_item;
					arg->libfs_id = libfs_id;

					thpool_add_work(file_digest_thread_pool,
							file_digest_worker, (void *)arg);
				}

				//if (thpool_num_threads_working(file_digest_thread_pool))
				thpool_wait(file_digest_thread_pool);

				HASH_ITER(hh, replay_list->f_digest_hash, f_item, t) {
					HASH_DEL(replay_list->f_digest_hash, f_item);
					mlfs_free(f_item);
				}
#else
				list_for_each_entry_safe(f_iovec, iovec_tmp, 
						&f_item->iovec_list, list) {

#ifndef EXPERIMENTAL
					digest_file(from_dev, dest_dev, libfs_id,
							f_item->key.inum, f_iovec->offset, 
							f_iovec->length, f_iovec->blknr);
					mlfs_free(f_iovec);
#else
					digest_file_iovec(from_dev, dest_dev, libfs_id,
							f_item->key.inum, f_iovec);
#endif //EXPERIMETNAL
					if (dest_dev == g_ssd_dev)
						mlfs_io_wait(g_ssd_dev, 0);
				}

				HASH_DEL(replay_list->f_digest_hash, f_item);
				mlfs_free(f_item);
#endif //FCONCURRENT

				list_del(l);

				if (enable_perf_stats)
					g_perf_stats.digest_file_tsc += asm_rdtscp() - tsc_begin;
				break;
			}
			case NTYPE_U: {
				u_replay_t *u_item;
				u_item = (u_replay_t *)container_of(l, u_replay_t, list);

				if (enable_perf_stats) 
					tsc_begin = asm_rdtscp();

				digest_unlink(from_dev, g_root_dev, libfs_id, u_item->key.inum);

				HASH_DEL(replay_list->u_digest_hash, u_item);
				list_del(l);
				mlfs_free(u_item);

				if (enable_perf_stats)
					g_perf_stats.digest_inode_tsc += asm_rdtscp() - tsc_begin;
				break;
			}
			default:
				panic("unsupported node type!\n");
		}
	}

	HASH_CLEAR(hh, replay_list->i_digest_hash);
	HASH_CLEAR(hh, replay_list->f_digest_hash);
	HASH_CLEAR(hh, replay_list->u_digest_hash);

#if 0
	i_replay_t *i_item, *i_tmp;
	f_replay_t *f_item, *f_tmp;
	f_iovec_t *f_iovec, *iovec_tmp;

	HASH_ITER(hh, replay_list->i_digest_hash, i_item, i_tmp) {
		digest_inode(from_dev, g_root_dev, i_item->inum, i_item->blknr);
		mlfs_free(i_item);
	}

	HASH_ITER(hh, replay_list->f_digest_hash, f_item, f_tmp) {
		list_for_each_entry_safe(f_iovec, iovec_tmp, &f_item->iovec_list, list) {
			digest_file(from_dev, g_root_dev, f_item->inum, f_iovec->offset,
					f_iovec->length, f_iovec->blknr);
			mlfs_free(f_iovec);
		}
		mlfs_free(f_item);
	}
#endif
}

static int persist_dirty_objects_nvm(int log_id) 
{
	struct rb_node *node;

	mlfs_debug("persisting dirty objects for peer %d\n", log_id);

	// flush extent tree changes
	sync_all_buffers(g_bdev[g_root_dev]);

	// save dirty inodes
	//pthread_mutex_lock(&inode_dirty_mutex);
	//pthread_spin_lock(&inode_dirty_mutex);

	// flush writes to NVM (only relevant if writes are issued asynchronously using a DMA engine)
	//mlfs_commit(g_root_dev);

	for (node = rb_first(&sb[g_root_dev]->s_dirty_root[log_id]); 
			node; node = rb_next(node)) {
		struct inode *ip = rb_entry(node, struct inode, i_rb_node);
		mlfs_debug("[dev %d] write dirty inode %d size %lu for log_id %d\n",
				g_root_dev, ip->inum, ip->size, log_id);
		rb_erase(&ip->i_rb_node, &get_inode_sb(g_root_dev, ip)->s_dirty_root[log_id]);
		write_ondisk_inode(ip);
	}


	//pthread_spin_unlock(&inode_dirty_mutex);

	// save block allocation bitmap
	store_all_bitmap(g_root_dev, sb[g_root_dev]->s_blk_bitmap);
	
	return 0;
}

static int persist_dirty_objects_ssd(void) 
{
	struct rb_node *node;

	sync_all_buffers(g_bdev[g_ssd_dev]);

	store_all_bitmap(g_ssd_dev, sb[g_ssd_dev]->s_blk_bitmap);

	return 0;
}

static int persist_dirty_objects_hdd(void) 
{
	struct rb_node *node;

	sync_all_buffers(g_bdev[g_hdd_dev]);

	store_all_bitmap(g_hdd_dev, sb[g_hdd_dev]->s_blk_bitmap);

	return 0;
}

int digest_logs(uint8_t from_dev, int libfs_id, int n_hdrs, addr_t start_blkno,
	       	addr_t end_blkno, addr_t *loghdr_to_digest, int *rotated)
{
	loghdr_meta_t *loghdr_meta;
	int i, n_digest;
	uint64_t tsc_begin;
	uint16_t size;
	addr_t current_loghdr_blk, next_loghdr_blk;
	struct replay_list replay_list = {
		.i_digest_hash = NULL,
		.f_digest_hash = NULL,
		.u_digest_hash = NULL,
	};

	INIT_LIST_HEAD(&replay_list.head);

	//memset(inode_version_table, 0, sizeof(uint16_t) * NINODES);

	current_loghdr_blk = next_loghdr_blk = *loghdr_to_digest;

	// digest log entries
	for (i = 0 ; i < n_hdrs; i++) {
		loghdr_meta = read_log_header(from_dev, *loghdr_to_digest);
		size = loghdr_meta->loghdr->nr_log_blocks;
		
		if (loghdr_meta->loghdr->inuse != LH_COMMIT_MAGIC) {
			mlfs_assert(loghdr_meta->loghdr->inuse == 0);
			mlfs_free(loghdr_meta);
			break;
		}

#ifdef DIGEST_OPT
		if (enable_perf_stats)	
			tsc_begin = asm_rdtscp();
		digest_replay_and_optimize(from_dev, loghdr_meta, &replay_list);
		if (enable_perf_stats)	
			g_perf_stats.replay_time_tsc += asm_rdtscp() - tsc_begin;
#else
		digest_each_log_entries(from_dev, libfs_id, loghdr_meta);
#endif

		// rotated when next log header jumps to beginning of the log.
		// FIXME: instead of this condition, it would be better if 
		// *loghdr_to_digest > the lost block of application log.

		next_loghdr_blk = next_loghdr_blknr(current_loghdr_blk, size, start_blkno, end_blkno);
		if (current_loghdr_blk > next_loghdr_blk) {
			mlfs_debug("current header %lu, next header %lu\n",
					current_loghdr_blk, next_loghdr_blk);
			*rotated = 1;
		}

		*loghdr_to_digest = next_loghdr_blk;

		current_loghdr_blk = *loghdr_to_digest;

		mlfs_free(loghdr_meta);
	}

#ifdef DIGEST_OPT
	if (enable_perf_stats)	
		tsc_begin = asm_rdtscp();
	digest_log_from_replay_list(from_dev, libfs_id, &replay_list);
	if (enable_perf_stats)	
		g_perf_stats.apply_time_tsc += asm_rdtscp() - tsc_begin;
#endif

	n_digest = i;

	if (0) {
		ncx_slab_stat_t slab_stat;
		ncx_slab_stat(mlfs_slab_pool, &slab_stat);
	}

	return n_digest;
}

static void handle_digest_request(void *arg)
{

#ifndef CONCURRENT
	pthread_mutex_lock(&digest_mutex);
#endif

	int dev_id, log_id;
	int sock_fd;
	struct digest_arg *digest_arg;
	char *buf, response[MAX_CMD_BUF];
	char cmd_header[12];
	int rotated = 0;
	int lru_updated = 0;
	addr_t digest_blkno, start_blkno, end_blkno;
	uint32_t digest_count;
	addr_t next_hdr_of_digested_hdr;

#if 1
	struct timespec end_time;

	if(!started) {
		clock_gettime(CLOCK_MONOTONIC, &first_digest_time);
	}

	started = 1;
#endif

	memset(cmd_header, 0, 12);

	digest_arg = (struct digest_arg *)arg;

	sock_fd = digest_arg->sock_fd;
	buf = digest_arg->msg;

	// parsing digest request
	sscanf(buf, "|%s |%d|%d|%u|%lu|%lu|%lu|", 
			cmd_header, &log_id, &dev_id, &digest_count, &digest_blkno, &start_blkno, &end_blkno);

	mlfs_debug("%s\n", cmd_header);
	if (strcmp(cmd_header, "digest") == 0) {
		mlfs_info("digest command: dev_id %u, digest_blkno %lx, digest_count %u,"
				"start_blkno %lx, end_blkno %lx\n",
				dev_id, digest_blkno, digest_count, start_blkno, end_blkno);

	// check LibFS is referencing the defined NVM log device
	mlfs_assert(dev_id == g_log_dev);

	if (enable_perf_stats) {
		g_perf_stats.digest_time_tsc = asm_rdtscp();
		g_perf_stats.persist_time_tsc = asm_rdtscp();
		g_perf_stats.path_search_tsc = 0;
		g_perf_stats.replay_time_tsc = 0;
		g_perf_stats.apply_time_tsc= 0;
		g_perf_stats.digest_dir_tsc = 0;
		g_perf_stats.digest_file_tsc = 0;
		g_perf_stats.digest_inode_tsc = 0;
		g_perf_stats.n_digest_skipped = 0;
		g_perf_stats.n_digest = 0;
	}

#ifdef DISTRIBUTED
	int requester_kernfs = local_kernfs_id(log_id);
	int is_last_kernfs = (((requester_kernfs + g_n_nodes - 1) % g_n_nodes) == g_self_id);

	// update peer digesting state
	set_peer_digesting(g_sync_ctx[log_id]->peer);

	g_sync_ctx[log_id]->peer->n_digest_req = digest_count;

	//if(log_id) {
	if(log_id && requester_kernfs != g_self_id) {
		// not the last request in the chain; forward to next kernfs
		if(!is_last_kernfs) {
			mlfs_info("forward digest to next KernFS with ID=%u\n", (g_self_id+1) % g_n_nodes);
			rpc_forward_msg(g_peers[(g_self_id + 1) % g_n_nodes]->sockfd[SOCK_BG], (char*) buf);
		}
	}
#endif
	digest_count = digest_logs(dev_id, log_id, digest_count, start_blkno, end_blkno, &digest_blkno, &rotated);

	mlfs_debug("-- Total used block %d\n", 
			bitmap_weight((uint64_t *)sb[g_root_dev]->s_blk_bitmap->bitmap,
				sb[g_root_dev]->ondisk->ndatablocks));


#ifdef MIGRATION	
	try_migrate_blocks(g_root_dev, g_ssd_dev, 0, 0, 1);
#endif

	if (enable_perf_stats)
		g_perf_stats.persist_time_tsc = asm_rdtscp();

	// flush writes to NVM (only relevant if writes are issued asynchronously using a DMA engine)
	mlfs_commit(g_root_dev);


	persist_dirty_objects_nvm(log_id);
#ifdef USE_SSD
	//persist_dirty_objects_ssd();
#endif
//#ifdef USE_HDD
//	persist_dirty_objects_hdd();
//#endif	

	if (enable_perf_stats)	
		g_perf_stats.persist_time_tsc = 
			(asm_rdtscp() - g_perf_stats.persist_time_tsc);

#ifdef DISTRIBUTED
		// this is a local peer; respond via unix socket
	//if(requester_kernfs == g_self_id) {
	if(log_id && local_kernfs_id(log_id) == g_self_id) {
		memset(response, 0, MAX_CMD_BUF);
#if 0
		sprintf(response, "|ACK |%d|%d|%d|%lu|%d|%d|", 
				log_id, dev_id, digest_count, digest_blkno, rotated, lru_updated);
		mlfs_info("Write %s to libfs\n", response);
		sendto(sock_fd, response, MAX_SOCK_BUF, 0, 
				(struct sockaddr *)&digest_arg->cli_addr, sizeof(struct sockaddr_un));
#else
		update_peer_digest_state(g_sync_ctx[g_rpc_socks[sock_fd]->peer->id]->peer, digest_blkno, digest_count, rotated);
		sprintf(response, "|complete |%d|%d|%d|%lu|%d|%d|", 
				log_id, dev_id, digest_count, digest_blkno, rotated, lru_updated);
		mlfs_info("Write %s to libfs\n", response);
		rpc_forward_msg(sock_fd, response);
#endif
	}
	else {
		//if I'm not last in chain; wait for next kernfs to respond first
		if(!is_last_kernfs) {
			mlfs_info("wait for KernFS %d to digest\n", (g_self_id + 1) % g_n_nodes);
			wait_on_peer_digesting(g_sync_ctx[log_id]->peer);
		}
		else {
			update_peer_digest_state(g_sync_ctx[log_id]->peer, digest_blkno, digest_count, rotated);
		}
		mlfs_info("digest response to Peer with ID = %d\n", log_id);
		rpc_remote_digest_response(digest_arg->sock_fd, log_id, dev_id,
				digest_blkno, digest_count, rotated, digest_arg->seqn);
	}

#if MLFS_LEASE
	clear_lease_checkpoints(log_id, g_sync_ctx[log_id]->peer->start_version, g_sync_ctx[log_id]->peer->start_digest);
#endif

#if 0
	// last kernfs in the chain; respond to peer
	else if(((peer_kernfs + g_n_nodes - 1) % g_n_nodes) == g_self_id)
		//NOTE: passing seqn = 0
		rpc_remote_digest_response(log_id, dev_id, digest_blkno, digest_count, rotated, 0);
	// forward to next kernfs in chain
	else
		rpc_forward_msg(g_peers[(g_self_id + 1) % g_n_nodes]->sockfd[SOCK_BG], (char*) arg);
#endif
#else
	memset(response, 0, MAX_CMD_BUF);
	sprintf(response, "|ACK |%d|%d|%d|%lu|%d|%d|", 
			log_id, dev_id, digest_count, digest_blkno, rotated, lru_updated);
	mlfs_info("Write %s to libfs\n", response);
	sendto(sock_fd, response, MAX_CMD_BUF, 0, 
			(struct sockaddr *)&digest_arg->cli_addr, sizeof(struct sockaddr_un));
#endif


#if MLFS_LEASE
	/*
	sprintf(response, "|NOTIFY |%d|", 1);

	// notify any other local libfs instances
	for(int i=0; i<g_n_hot_rep; i++) {

		if(sockaddr_cmp((struct sockaddr *)&digest_arg->cli_addr,
				       (struct sockaddr *)&all_cli_addr[i])) {
			sendto(sock_fd, response, MAX_SOCK_BUF, 0, 
				(struct sockaddr *)&all_cli_addr[i], sizeof(struct sockaddr_un));
		}
	}
	*/
#endif

	if (enable_perf_stats)	
		g_perf_stats.digest_time_tsc = 
			(asm_rdtscp() - g_perf_stats.digest_time_tsc);

	show_storage_stats();

	if (enable_perf_stats)	
		show_kernfs_stats();

	} else if (strcmp(cmd_header, "lru") == 0) {
		// only used for debugging.
		if (0) {
			lru_node_t *l;
			list_for_each_entry(l, &lru_heads[g_log_dev], list) 
				mlfs_info("%u - %lu\n", 
						l->val.inum, l->val.lblock);
		}
	} else {
		panic("invalid command\n");
	}

#ifndef CONCURRENT
	pthread_mutex_unlock(&digest_mutex);
#endif

#if 1
	clock_gettime(CLOCK_MONOTONIC, &end_time);

	double start_sec = (double)(first_digest_time.tv_sec * 1000000000.0 + (double)first_digest_time.tv_nsec) / 1000000000.0;
	double end_sec = (double)(end_time.tv_sec * 1000000000.0 + (double)end_time.tv_nsec) / 1000000000.0;
	run_sec = end_sec - start_sec;

    	mlfs_printf("Total runtime (s): %lf\n", run_sec);
#endif

	mlfs_free(arg);
}

#define MAX_EVENTS 4
static void wait_for_event(void)
{
#if 0
	int sock_fd, epfd, flags, n, ret;
	struct sockaddr_un addr;
	struct epoll_event epev = {0};
	struct epoll_event *events;
	int i;

	if ((sock_fd = socket(AF_UNIX, SOCK_DGRAM, 0)) < 0)
		panic ("socket error");

	memset(&addr, 0, sizeof(addr));

	addr.sun_family = AF_UNIX;
	strncpy(addr.sun_path, SRV_SOCK_PATH, sizeof(addr.sun_path));

	unlink(SRV_SOCK_PATH);

	if (bind(sock_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0)
		panic("bind error");
	
	// make it non-blocking
	flags = fcntl(sock_fd, F_GETFL, 0);
	flags |= O_NONBLOCK;
	ret = fcntl(sock_fd, F_SETFL, flags);
	if (ret < 0)
		panic("fail to set non-blocking mode\n");

	epfd = epoll_create(1);
	epev.data.fd = sock_fd;
	epev.events = EPOLLIN | EPOLLRDHUP | EPOLLHUP;
	ret = epoll_ctl(epfd, EPOLL_CTL_ADD, sock_fd, &epev);
	if (ret < 0)
		panic("fail to connect epoll fd\n");

	events = mlfs_zalloc(sizeof(struct epoll_event) * MAX_EVENTS);

	while(1) {
		n = epoll_wait(epfd, events, MAX_EVENTS, -1);

		if (n < 0 && errno != EINTR)
			panic("epoll has error\n");

		for (i = 0; i < n; i++) {
			/*
			if ((events[i].events & EPOLLERR) ||
					(events[i].events & EPOLLHUP) ||
					(!(events[i].events & EPOLLIN)))
			{
				fprintf (stderr, "epoll error\n");
				continue;
			}
			*/

			if ((events[i].events & EPOLLIN) &&
					events[i].data.fd == sock_fd) {
				int ret;
				char buf[MAX_CMD_BUF];
				char cmd_header[12];
				uint32_t dev_id;
				uint32_t digest_count;
				addr_t digest_blkno, start_blkno, end_blkno;
				struct sockaddr_un cli_addr;
				socklen_t len = sizeof(struct sockaddr_un);
				struct digest_arg *digest_arg;

				ret = recvfrom(sock_fd, buf, MAX_CMD_BUF, 0,
						(struct sockaddr *)&cli_addr, &len);

				// When clients hang up, the recvfrom returns 0 (EOF).
				if (ret == 0) 
					continue;

				mlfs_info("GET: %s\n", buf);

				memset(cmd_header, 0, 12);
				sscanf(buf, "|%s |%d|%u|%lu|%lu|%lu|", 
						cmd_header, &dev_id, &digest_count, &digest_blkno, &start_blkno, &end_blkno);

				if(cmd_header[0] == 'd') {
					digest_arg = (struct digest_arg *)mlfs_alloc(sizeof(struct digest_arg));
					digest_arg->sock_fd = sock_fd;
					digest_arg->cli_addr = cli_addr;
					memmove(digest_arg->msg, buf, MAX_CMD_BUF);

#ifdef CONCURRENT
					thpool_add_work(thread_pool, handle_digest_request, (void *)digest_arg);
#else
					handle_digest_request((void *)digest_arg);
#endif

#ifdef MIGRATION
					/*
					thpool_wait(thread_pool);
					thpool_wait(thread_pool_ssd);
					*/

					//try_writeback_blocks();
					//try_migrate_blocks(g_root_dev, g_ssd_dev, 0, 0, 1);
					//try_migrate_blocks(g_root_dev, g_hdd_dev, 0);
#endif
				}
#if MLFS_LEASE
				else if(cmd_header[0] == 'a') {
					//store client address
					all_cli_addr[addr_idx] = cli_addr;
					addr_idx++;
				}
#endif
				else {
					printf("received cmd: %s\n", cmd_header);
					panic("unidentified code path!");
				}
			} else {
				mlfs_info("%s\n", "Huh?");
			}
		}
	}

	close(epfd);

#else
	pause();
#endif
}

void shutdown_fs(void)
{
	printf("Finalize FS\n");

	device_shutdown();
	mlfs_commit(g_root_dev);
	return ;
}

#ifdef USE_SLAB
void mlfs_slab_init(uint64_t pool_size)
{
	uint8_t *pool_space;

	// Transparent huge page allocation.
	pool_space = (uint8_t *)mmap(0, pool_size, PROT_READ|PROT_WRITE,
			MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE, -1, 0);

	mlfs_assert(pool_space);

	if(madvise(pool_space, pool_size, MADV_HUGEPAGE) < 0)
		panic("cannot do madvise for huge page\n");

	mlfs_slab_pool = (ncx_slab_pool_t *)pool_space;
	mlfs_slab_pool->addr = pool_space;
	mlfs_slab_pool->min_shift = 3;
	mlfs_slab_pool->end = pool_space + pool_size;

	ncx_slab_init(mlfs_slab_pool);
}
#endif

void debug_init(void)
{
#ifdef MLFS_LOG
	log_fd = open(LOG_PATH, O_RDWR|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);
#endif
}

static void shared_memory_init(void)
{
	int ret;

	shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
	if (shm_fd == -1)
		panic("cannot create shared memory\n");

	ret = ftruncate(shm_fd, SHM_SIZE);
	if (ret == -1)
		panic("cannot ftruncate shared memory\n");

	shm_base = (uint8_t *)mmap(SHM_START_ADDR, 
			SHM_SIZE + 4096, 
			PROT_READ| PROT_WRITE, 
			MAP_SHARED | MAP_POPULATE | MAP_FIXED,
			shm_fd, 0);

	printf("shm mmap base %p\n", shm_base);
	if (shm_base == MAP_FAILED) {
		printf("error: %s\n", strerror(errno));
		panic("cannot map shared memory\n");
	}

	// the first 4 KB is reserved.
	mlfs_slab_pool_shared = (ncx_slab_pool_t *)(shm_base + 4096);
	mlfs_slab_pool_shared->addr = shm_base + 4096;
	mlfs_slab_pool_shared->min_shift = 3;
	mlfs_slab_pool_shared->end = shm_base + SHM_SIZE - 4096;

	ncx_slab_init(mlfs_slab_pool_shared);

	bandwidth_consumption = (uint64_t *)shm_base;
	lru_heads = (struct list_head *)shm_base + 128;

	return;
}

void init_device_lru_list(void)
{
	int i;

	for (i = 1; i < g_n_devices + 1; i++) {
		memset(&g_lru[i], 0, sizeof(struct lru));
		INIT_LIST_HEAD(&g_lru[i].lru_head);
		g_lru_hash[i] = NULL;

#ifdef MLFS_REPLICA
		memset(&g_stage_lru[i], 0, sizeof(struct lru));
		INIT_LIST_HEAD(&g_stage_lru[i].lru_head);

		memset(&g_swap_lru[i], 0, sizeof(struct lru));
		INIT_LIST_HEAD(&g_swap_lru[i].lru_head);
#endif
	}

	pthread_spin_init(&lru_spinlock, PTHREAD_PROCESS_SHARED);

	return;
}

void locks_init(void)
{
	pthread_mutexattr_t attr;

	pthread_mutexattr_init(&attr);
	pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);

	pthread_mutex_init(&block_bitmap_mutex, &attr);
	//pthread_mutex_init(&inode_dirty_mutex, &attr);
 	pthread_spin_init(&inode_dirty_mutex, PTHREAD_PROCESS_PRIVATE);

#ifndef CONCURRENT
	pthread_mutex_init(&digest_mutex, &attr); //prevent concurrent digests
#endif
}

void init_fs(void)
{
	int i;
	const char *perf_profile;

#ifdef USE_SLAB
	mlfs_slab_init(3UL << 30); 
#endif

	device_init();

	debug_init();

	init_device_lru_list();

	//shared_memory_init();

	cache_init(g_root_dev);

	locks_init();

	for (i = 0; i < g_n_devices + 1; i++) 
		sb[i] = mlfs_zalloc(sizeof(struct super_block));

	read_superblock(g_root_dev);
	read_root_inode(g_root_dev);
	balloc_init(g_root_dev, sb[g_root_dev], 0);

#ifdef USE_SSD
	read_superblock(g_ssd_dev);
	balloc_init(g_ssd_dev, sb[g_ssd_dev], 0);
#endif

#ifdef USE_HDD
	read_superblock(g_hdd_dev);
	balloc_init(g_hdd_dev, sb[g_hdd_dev], 0);
#endif

	// read superblock for log if it's on a separate device
	if(g_log_dev != g_root_dev)
		read_superblock(g_log_dev);

	mlfs_assert(g_log_size * g_n_max_libfs <= disk_sb[g_log_dev].nlog);

	memset(&g_perf_stats, 0, sizeof(kernfs_stats_t));

	inode_version_table = 
		(uint16_t *)mlfs_zalloc(sizeof(uint16_t) * NINODES); 

	perf_profile = getenv("MLFS_PROFILE");

	if (perf_profile)
		enable_perf_stats = 1;
	else
		enable_perf_stats = 0;

	mlfs_debug("%s\n", "LIBFS is initialized");

	thread_pool = thpool_init(8);
	// A fixed thread for using SPDK.
	thread_pool_ssd = thpool_init(1);
#ifdef FCONCURRENT
	file_digest_thread_pool = thpool_init(48);
#endif

#ifdef DISTRIBUTED
	bitmap_set(g_log_bitmap, 0, g_n_max_libfs);

	assert(disk_sb[g_log_dev].nlog >= g_log_size * g_n_hot_rep);
	//for(int i=0; i<g_n_hot_rep; i++)
	//	init_log(i);

	//set memory regions to be registered by rdma device
	int n_regions = 2;
	struct mr_context *mrs = (struct mr_context *) mlfs_zalloc(sizeof(struct mr_context) * n_regions);

	// TODO: Optimize registrations. current implementation registers unused and sometimes overlapping memory regions
	for(int i=0; i<n_regions; i++) {
		switch(i) {
			case MR_NVM_LOG: {
				mrs[i].type = MR_NVM_LOG;
				//mrs[i].addr = (uint64_t)g_bdev[g_log_dev]->map_base_addr;
				mrs[i].addr = (uint64_t)g_bdev[g_log_dev]->map_base_addr + (disk_sb[g_log_dev].log_start << g_block_size_shift);
				//mrs[i].length = dev_size[g_root_dev];
				mrs[i].length = ((disk_sb[g_log_dev].nlog) << g_block_size_shift);
				break;
			}
			case MR_NVM_SHARED: {
				mrs[i].type = MR_NVM_SHARED;
				mrs[i].addr = (uint64_t)g_bdev[g_root_dev]->map_base_addr;
				//mrs[i].length = dev_size[g_root_dev];
				//mrs[i].length = (1UL << 30);
				//mrs[i].length = (sb[g_root_dev]->ondisk->size << g_block_size_shift);
				mrs[i].length = (disk_sb[g_root_dev].datablock_start - disk_sb[g_root_dev].inode_start << g_block_size_shift);
				break;
			}
			/*
			case MR_DRAM_CACHE: {
				mrs[i].type = MR_DRAM_CACHE;
				mrs[i].addr = (uint64_t) g_fcache_base;
				mrs[i].length = (g_max_read_cache_blocks << g_block_size_shift);
				break;
			}
			*/
			case MR_DRAM_BUFFER:
				//do nothing
			default:
				break;
		}
	}

	// initialize rpc module
	char *port = getenv("PORTNO");

	if(!port) {
		// initialize rpc module
		port = (char *)mlfs_zalloc(sizeof(char)*10);
		sprintf(port, "%d", 12345);
	}
	init_rpc(mrs, n_regions, port, signal_callback);
#endif

	wait_for_event();
}

//////////////////////////////////////////////////////////////////////////
// KernFS signal callback (used to handle signaling between replicas)

#ifdef DISTRIBUTED
void signal_callback(struct app_context *msg)
{
	char cmd_hdr[12];
	// handles 4 message types (bootstrap, log, digest, lease)
	if(msg->data) {
		sscanf(msg->data, "|%s |", cmd_hdr);
		mlfs_rpc("peer recv: %s\n", msg->data);
	}
	else {
		cmd_hdr[0] = 'i';
	}

	// digest request
	if (cmd_hdr[0] == 'd') {
		//int dev, percent, steps;
		//uint32_t n_digest;
		//addr_t start_blk, log_end;
		
		printf("peer recv: %s\n", msg->data);

		struct digest_arg *digest_arg = (struct digest_arg *)mlfs_alloc(sizeof(struct digest_arg));
		digest_arg->sock_fd = msg->sockfd;
		digest_arg->seqn = msg->id;
		//digest_arg->sock_fd = msg->sockfd;
		//digest_arg->cli_addr = cli_addr;
		memmove(digest_arg->msg, msg->data, MAX_CMD_BUF);
		//sscanf(msg->data, "|%s |%d|%d|%lu|%u|%lu|%u", cmd_hdr, &dev, &percent, &start_blk, &n_digest, &log_end, &steps);
		//sscanf(msg->data, "|%s |%d|%d|%u|%lu|%lu|%lu|", 
		//	cmd_hdr, &log_id, &dev_id, &digest_count, &digest_blkno, &start_blkno, &end_blkno);
		//g_fs_log[dev]->start_blk = start_blk;
		//g_log_sb->start_digest = start_digest;
 		//atomic_store(&g_log_sb[dev]->n_digest, n_digest);
		//atomic_store(&g_log_sb[dev]->end, log_end);

#ifdef CONCURRENT
		//thpool_add_work(thread_pool, handle_digest_request, (void*) digest_arg);
		handle_digest_request(digest_arg);
#else
		handle_digest_request(digest_arg);
#endif

#ifdef MIGRATION		
		//try_migrate_blocks(g_root_dev, g_ssd_dev, 0, 0, 1);
#endif
	}
	//digestion completion notification
	else if (cmd_hdr[0] == 'c') {
		//FIXME: this callback currently does not support coalescing; it assumes remote
		// and local kernfs digests the same amount of data
		int log_id, dev, rotated, lru_updated;
		uint32_t n_digested;
		addr_t start_digest;

		printf("peer recv: %s\n", msg->data);
		sscanf(msg->data, "|%s |%d|%d|%d|%lu|%d|%d|", cmd_hdr, &log_id, &dev,
				&n_digested, &start_digest, &rotated, &lru_updated);
		update_peer_digest_state(g_sync_ctx[log_id]->peer, start_digest, n_digested, rotated);
		//handle_digest_response(msg->data);
	}
#if 0
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
#endif
	else if(cmd_hdr[0] == 'i') { //immediate completions are replication notifications
		addr_t n_log_blk;
		int node_id, rotated, steps, ack;
		uint16_t seqn;
		decode_rsync_metadata(msg->id, &seqn, &n_log_blk, &steps, &node_id, &rotated, &ack);

		mlfs_rpc("peer recv: rsync from libfs-%d | n_log_blk: %lu | steps: %u | rotated: %s | ack: %s\n",
				node_id, n_log_blk, steps, rotated?"yes":"no", ack?"yes":"no");	

		//FIXME: this currently updates nr of loghdrs incorrectly (assumes it's just 1)
		update_peer_sync_state(g_sync_ctx[node_id]->peer, n_log_blk, rotated);

		//replicate asynchronously to the next node in the chain
		// As a latency-hiding optimization, we perform this before persisting

		int peer_kernfs = local_kernfs_id(node_id);
		int last_in_chain = (((peer_kernfs + g_n_nodes - 1) % g_n_nodes) == g_self_id);

		// forward to next kernfs in chain
		if(!last_in_chain)
			mlfs_do_rsync_forward(node_id, msg->id);
	
#if 0
		//FIXME: pass the correct dev id
		persist_replicated_logs(1, n_log_blk);
#endif

		// last kernfs in the chain; respond to peer
		if(last_in_chain && ack) {
			mlfs_info("last_in_chain -> send replication response to peer %d on sockfd %u\n",
					node_id, g_peers[node_id]->sockfd[SOCK_IO]);
			rpc_send_ack(g_peers[node_id]->sockfd[SOCK_IO], seqn);
		}

	}
#if 0
	//digestion completion notification
	else if (cmd_hdr[0] == 'c') {
		uint32_t n_digested, rotated;
		addr_t start_digest;

		printf("peer recv: %s\n", msg->data);
		sscanf(msg->data, "|%s |%lu|%d|%d", cmd_hdr, &start_digest, &n_digested, &rotated);
		update_peer_digest_state(get_next_peer(), start_digest, n_digested, rotated);
	}
#endif
	else if(cmd_hdr[0] == 'l') {
#if MLFS_LEASE
		//char path[MAX_PATH];
		int type;
		uint32_t req_id;
		uint32_t inum;
		uint32_t version;
		addr_t blknr;
		sscanf(msg->data, "|%s |%u|%u|%d|%u|%lu", cmd_hdr, &req_id, &inum, &type, &version, &blknr);
		//mlfs_debug("received remote lease acquire with inum %u | type[%d]\n", inum, type);

		int ret = modify_lease_state(req_id, inum, type, version, blknr);

		// If ret < 0 due to incorrect lease manager
		// (a) For read/write lease RPCs, return 'invalid lease request' to LibFS
		// (b) For lease revocations, simply forward to correct lease manager
		if(type != LEASE_FREE) {
			//mlfs_printf("lease granted to %d\n", g_rpc_socks[msg->sockfd]->peer->id);

			if(ret < 0)
				rpc_lease_invalid(msg->sockfd, g_rpc_socks[msg->sockfd]->peer->id, inum, msg->id);
			else
				rpc_send_ack(msg->sockfd, msg->id);
		}
		else {
			if(ret < 0)
				rpc_lease_change(abs(ret), req_id, inum, type, version, blknr, 0);
		}	

#else
		panic("invalid code path\n");
#endif
	}
	else if(cmd_hdr[0] == 'm') { // migrate lease (enforce)
#if MLFS_LEASE
		int digest;
		uint32_t inum;
		uint32_t new_kernfs_id;
		sscanf(msg->data, "|%s |%u|%d", cmd_hdr, &inum, &new_kernfs_id);
		update_lease_manager(inum, new_kernfs_id);
		//rpc_send_ack(msg->sockfd, msg->id);
#else
		panic("invalid code path\n");
#endif
	}	
	else if (cmd_hdr[0] == 'b') {
		mlfs_assert(g_self_id == 0);
		uint32_t pid;
		
		sscanf(msg->data, "|%s |%u", cmd_hdr, &pid);

		// libfs bootstrap calls are forwarded to all other KernFS instances
		register_peer_log(g_rpc_socks[msg->sockfd]->peer, 1);
		for(int i=1; i<g_n_nodes; i++) {
			struct rpc_pending_io *pending = rpc_register_log(g_kernfs_peers[i % g_n_nodes]->sockfd[0],
					g_rpc_socks[msg->sockfd]->peer);
			rpc_await(pending);
		}

		rpc_bootstrap_response(msg->sockfd, msg->id);
	}
	else if(cmd_hdr[0] == 'p' && g_self_id) { //log registration
		int id;
		uint32_t pid;
		char ip[NI_MAXHOST];
		
		sscanf(msg->data, "|%s |%d|%u|%s", cmd_hdr, &id, &pid, ip);

		struct peer_id *peer = _find_peer(ip, pid);
		mlfs_assert(peer);
		peer->id = id;
		register_peer_log(peer, 0);
		rpc_register_log_response(msg->sockfd, msg->id);
	}
	else if(cmd_hdr[0] == 'p' && !g_self_id) { //log registration response (ignore)
		return;
	}
	else if(cmd_hdr[0] == 'a') //ack (ignore)
		return;
	else
		panic("unidentified remote signal\n");
}
#endif

void persist_replicated_logs(int id, addr_t n_log_blk)
{
#ifdef DISTRIBUTED
	//addr_t size = nr_blks_between_ptrs(g_log_sb->start_persist, end_blk+1);
	int flags = 0;

	flags |= (PMEM_PERSIST_FLUSH | PMEM_PERSIST_DRAIN);

	uint32_t size = 0;

	//wrap around
	//FIXME: pass log star_blk
 	if(g_peers[id]->log_sb->start_persist + n_log_blk > g_log_size)
		g_peers[id]->log_sb->start_persist = g_sync_ctx[id]->begin;

	//move_log_ptr(&g_log_sb->start_persist, 0, n_log_blk);

	//mlfs_commit(id, g_peers[id]->log_sb->start_persist, 0, (n_log_blk << g_block_size_shift), flags);

	g_peers[id]->log_sb->start_persist += n_log_blk; 



	//TODO: apply to individual logs to avoid flushing any unused memory
	/*
	addr_t last_blk =0;
	addr_t loghdr_blk = 0;
	struct buffer_head *io_bh;
	loghdr_t *loghdr = read_log_header(g_fs_log->dev, start_blk);

	while(end != last_blk) {	
		io_bh = bh_get_sync_IO(g_log_dev, loghdr_blk, BH_NO_DATA_ALLOC);

		io_bh->b_data = (uint8_t *)loghdr;
		io_bh->b_size = sizeof(struct logheader);

		mlfs_commit(io_bh);

		loghdr_blk = next_loghdr_blknr(loghdr->hdr_blkno,loghdr->nr_log_blocks,
				g_fs_log->log_sb_blk+1, atomic_load(&g_log_sb->end));
		loghdr = read_log_header(g_fs_log->dev, loghdr_blk);
	}
	*/
#endif
}


#if 0

int rpc_bootstrap(int sockfd)
{
	struct app_context *msg;
	int buffer_id = rc_acquire_buffer(sockfd, &msg);
#ifdef KERNFS
	snprintf(msg->data, MAX_SIGNAL_BUF, "|bootstrap |%u|%d", getpid(), 1);
#else
	snprintf(msg->data, MAX_SIGNAL_BUF, "|bootstrap |%u|%d", getpid(), 0);
#endif
	msg->id = generate_rpc_seqn(g_rpc_socks[sockfd]);

	IBV_WRAPPER_SEND_MSG_ASYNC(sockfd, buffer_id, 1);
}

int rpc_bootstrap_response(int sockfd, uint32_t seq_n)
{
	struct app_context *msg;
	int buffer_id = rc_acquire_buffer(sockfd, &msg);

	snprintf(msg->data, MAX_SIGNAL_BUF, "|bootstrap |%u", g_rpc_socks[sockfd]->peer->id);
	msg->id = seq_n; //set immediate to sequence number in order for requester to match it

	IBV_WRAPPER_SEND_MSG_ASYNC(sockfd, buffer_id, 0);
}

int rpc_register_log(int sockfd, struct peer_id *peer)
{
	struct app_context *msg;
	int buffer_id = rc_acquire_buffer(sockfd, &msg);
	snprintf(msg->data, MAX_SIGNAL_BUF, "|log %d|%u|%s", peer->id, peer->pid, peer->ip);
	msg->id = generate_rpc_seqn(g_rpc_socks[sockfd]);

	IBV_WRAPPER_SEND_MSG_ASYNC(sockfd, buffer_id, 1);
}


int rpc_register_log_response(int sockfd, uint32_t seq_n)
{
	struct app_context *msg;
	int buffer_id = rc_acquire_buffer(sockfd, &msg);

	snprintf(msg->data, MAX_SIGNAL_BUF, "|log |registered");
	msg->id = seq_n; //set immediate to sequence number in order for requester to match it

	IBV_WRAPPER_SEND_MSG_ASYNC(sockfd, buffer_id, 0);
}

#endif

#if 0
void signal_callback(struct app_context *msg)
{
	char cmd_hdr[8];

	if(msg->data) {
		sscanf(msg->data, "|%s |", cmd_hdr);
		mlfs_debug("received rpc with body: %s on sockfd %d\n", msg->data, msg->sockfd);
	}
	else {
		cmd_hdr[0] = 'i';
		mlfs_debug("received imm with id %u on sockfd %d\n", msg->id, msg->sockfd);
	}

	// master/slave callbacks
	// handles 1 message type (digest)
	if (cmd_hdr[0] == 'd') {
		int dev, percent;
		uint32_t n_digest;
		addr_t start_blk, log_end;
		//FIXME: temporary fix for sockfd
		pending_sock_fd = msg->sockfd;
		
		printf("peer recv: %s\n", msg->data);
		sscanf(msg->data, "|%s |%d|%d|%lu|%u|%lu", cmd_hdr, &dev, &percent, &start_blk, &n_digest, &log_end);
		g_fs_log[dev]->start_blk = start_blk;
		//g_log_sb->start_digest = start_digest;
 		atomic_store(&g_log_sb[dev]->n_digest, n_digest);
		atomic_store(&g_log_sb[dev]->end, log_end);
		while(make_digest_seg_request_async(dev, 100) != -EBUSY)
			mlfs_info("%s", "[L] received a remote log digest signal. asynchronous digest!\n");
	}
#if 0
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
#endif
	}
	else if(cmd_hdr[0] == 'i') { //immediate completions are replication notifications
		addr_t n_log_blk;
		int node_id, ack, steps, persist;
		uint16_t seqn;
		decode_rsync_metadata(msg->id, &seqn, &n_log_blk, &steps, &node_id, &ack, &persist);

		mlfs_debug("received log data from replica %d | n_log_blk: %u | steps: %u | ack_required: %s | persist: %s\n",
				node_id, n_log_blk, steps, ack?"yes":"no", persist?"yes":"no");	

		//FIXME: this currently updates nr of loghdrs incorrectly (assumes it's just 1)
		update_peer_sync_state(get_peer(), n_log_blk);

		//replicate asynchronously to the next node in the chain
		// As a latency-hiding optimization, we perform this before persisting
		if(steps > 1) {
			//make_replication_request_async(0);
			mlfs_do_rsync_forward(node_id, msg->id);
			//mlfs_info("%s", "[L] received remote replication signal. asynchronous replication!\n");
		}

		if(persist) {
			//FIXME: pass the correct dev id
			persist_replicated_logs(1, n_log_blk);
		}

		if(ack || steps <= 1) {
			rpc_replication_response(peer_sockfd(node_id, SOCK_IO), seqn);
		}

	}
	//digestion completion notification
	else if (cmd_hdr[0] == 'c') {
		uint32_t n_digested, rotated;
		addr_t start_digest;

		printf("peer recv: %s\n", msg->data);
		sscanf(msg->data, "|%s |%lu|%d|%d", cmd_hdr, &start_digest, &n_digested, &rotated);
		update_peer_digest_state(get_peer(), start_digest, n_digested, rotated);
	}
	else if(cmd_hdr[0] == 'l') {
#if MLFS_LEASE
		char path[MAX_PATH];
		int type;
		sscanf(msg->data, "|%s |%s |%d", cmd_hdr, path, &type);
		mlfs_debug("received remote lease acquire with path %s | type[%d]\n", path, type);
		resolve_lease_conflict(msg->sockfd, path, type, msg->id);
#else
		panic("invalid code path\n");
#endif
	}
	else if(cmd_hdr[0] == 'r') {
#if MLFS_LEASE
		int type;
		sscanf(msg->data, "|%s |%d", cmd_hdr, &type);
		mlfs_debug("%s\n", "received replication notification; all lease aquisitions will wait");
		notify_replication_start();
#else
		panic("invalid code path\n");
#endif
	}
	else if(cmd_hdr[0] == 'i') //rdma immediate notification (ignore)
		return;
	else
		panic("unidentified remote signal\n");

}
#endif


void cache_init(uint8_t dev)
{
	int i;
	inode_hash = NULL;

	pthread_spin_init(&icache_spinlock, PTHREAD_PROCESS_SHARED);
	pthread_spin_init(&dcache_spinlock, PTHREAD_PROCESS_SHARED);

#if MLFS_LEASE
	lease_table = SharedTable_mock();
#endif
}

void read_superblock(uint8_t dev)
{
	int ret;
	struct buffer_head *bh;

	bh = bh_get_sync_IO(dev, 1, BH_NO_DATA_ALLOC);
	bh->b_size = g_block_size_bytes;
	bh->b_data = mlfs_zalloc(g_block_size_bytes);

	bh_submit_read_sync_IO(bh);
	mlfs_io_wait(dev, 1);

	if (!bh)
		panic("cannot read superblock\n");

	mlfs_debug("size of superblock %ld\n", sizeof(struct disk_superblock));

	memmove(&disk_sb[dev], bh->b_data, sizeof(struct disk_superblock));

	mlfs_info("superblock: size %lu nblocks %lu ninodes %u\n"
			"[inode start %lu bmap start %lu datablock start %lu log start %lu]\n",
			disk_sb[dev].size, 
			disk_sb[dev].ndatablocks, 
			disk_sb[dev].ninodes,
			disk_sb[dev].inode_start, 
			disk_sb[dev].bmap_start, 
			disk_sb[dev].datablock_start,
			disk_sb[dev].log_start);

	sb[dev]->ondisk = &disk_sb[dev];

	// set all rb tree roots to NULL
	for(int i = 0; i < (g_n_max_libfs + g_n_nodes); i++)
		sb[dev]->s_dirty_root[i] = RB_ROOT;

	sb[dev]->last_block_allocated = 0;

	// The partition is GC unit (1 GB) in SSD.
	// disk_sb[dev].size : total # of blocks 
#if 0
	sb[dev]->n_partition = disk_sb[dev].size >> 18;
	if (disk_sb[dev].size % (1 << 18))
		sb[dev]->n_partition++;
#endif

	sb[dev]->n_partition = 1;

	//single partitioned allocation, used for debugging.
	mlfs_info("dev %u: # of segment %u\n", dev, sb[dev]->n_partition);
	sb[dev]->num_blocks = disk_sb[dev].ndatablocks;
	sb[dev]->reserved_blocks = disk_sb[dev].datablock_start;
	sb[dev]->s_bdev = g_bdev[dev];

	mlfs_free(bh->b_data);
	bh_release(bh);
}

#if 0
int reserve_log(struct peer_id *peer)
{
	int id = find_next_zero_bit(g_log_bitmap, g_n_max_libfs, 0);

	if (sizeof(struct logheader) > g_block_size_bytes) {
		printf("log header size %lu block size %lu\n",
				sizeof(struct logheader), g_block_size_bytes);
		panic("initlog: too big logheader");
	}

	if (dev < 0 || dev > g_n_devices) {
		printf("log dev %d should be between %d and %d\n",
				dev, 0, g_n_devices);
		panic("initlog: invalid log dev id");
	}

	g_log_sb[dev] = (struct log_superblock *)mlfs_zalloc(sizeof(struct log_superblock));

	// the first block stores the log metadata
	g_log_sb[dev]->start_digest = disk_sb[g_log_dev].log_start + id * g_log_size + 1;

	g_log_sb[dev]->start_persist = g_log_sb[dev]->start_digest;

	write_log_superblock(dev, g_log_sb[dev]);

	atomic_init(&g_log_sb[dev]->n_digest, 0);
	atomic_init(&g_log_sb[dev]->end, 0);

	//maintian log identifiers
	//return dev_id, log_id
}
#endif
