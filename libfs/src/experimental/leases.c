#include "leases.h"
#include "distributed/rpc_interface.h"
//#include "filesystem/fs.h"
//#include "filesystem/file.h"
//#include "posix/posix_interface.h"
#include "mlfs/mlfs_interface.h"
#include "filesystem/stat.h"
#include "concurrency/thread.h"

#ifdef KERNFS
#include "fs.h"
#else
#include "filesystem/fs.h"
#include "log/log.h"
#endif

#if MLFS_LEASE

double revoke_sec=0;
//struct timeval start_time, end_time;

static SharedGuts *SharedGuts_create(void *base, size_t size)
{
	SharedGuts *guts = base;
	//pthread_mutexattr_t attr;
	//int rc = pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	//assert(rc == 0);
	int ret = pthread_spin_init(&guts->mutex, PTHREAD_PROCESS_SHARED);
	assert(ret == 0);
	guts->base = base;
	guts->size = size;
	guts->next_allocation = (char *)(guts + 1);
	guts->hash = NULL;
	return guts;
}

static SharedGuts *SharedGuts_mock()
{
	SharedGuts *guts = mlfs_zalloc(sizeof(SharedGuts));
	int ret = pthread_spin_init(&guts->mutex, PTHREAD_PROCESS_PRIVATE);
	assert(ret == 0);
	return guts;
}

SharedTable *SharedTable_create(const char *name, size_t arena_size)
{
	SharedTable *t = mlfs_zalloc(sizeof(SharedTable));
	t->fd = shm_open(name, O_CREAT | O_RDWR, 0666);
	assert(t->fd > 0);
	t->size = arena_size;
	ftruncate(t->fd, t->size);
	t->base = mmap(0, t->size, PROT_READ | PROT_WRITE, MAP_SHARED, t->fd, 0);
	assert(t->base != MAP_FAILED);
	memset(t->base, 0, t->size);
	t->guts = SharedGuts_create(t->base, t->size);
	return t;
}

SharedTable *SharedTable_mock()
{
	SharedTable *t = mlfs_zalloc(sizeof(SharedTable));
	t->guts = SharedGuts_mock();

}

SharedTable *SharedTable_subscribe(const char *name)
{
	SharedTable *t = mlfs_zalloc(sizeof(SharedTable));
	t->fd = shm_open(name, O_CREAT | O_RDWR, 0666);
	assert(t->fd > 0);
	SharedGuts *guts = mmap(NULL, sizeof (SharedGuts), PROT_READ | PROT_WRITE, MAP_SHARED, t->fd, 0);
	assert(guts != MAP_FAILED);

	pthread_spin_lock(&guts->mutex);

	//void *where = guts->base;
	void *where = 0;
	t->size = guts->size;
	munmap(guts, sizeof (SharedGuts));
	t->base = mmap(where, t->size, PROT_READ | PROT_WRITE, MAP_SHARED, t->fd, 0);
	t->guts = t->base;
	//printf("---- DEBUGM --- t->guts->base %p t->base %p\n", t->guts->base, t->base);
	//assert(t->guts->base == t->base);
	assert(t->guts->size == t->size);

	pthread_spin_unlock(&t->guts->mutex);
	return t;
}

void SharedTable_unsubscribe(SharedTable *t)
{
	munmap(t->base, t->size);
	close(t->fd);
}

static void *SharedTable_malloc(SharedTable *t, size_t size)
{
	void *ptr = t->guts->next_allocation;
	if ((ptr - t->base) + size <= t->size) {
		t->guts->next_allocation += size;
	} else {
		assert(0 && "out of memory");
		ptr = NULL;
	}
	return ptr;
}

// TODO: implement frees for SharedTable
static void SharedTable_free(SharedTable *t, void *p)
{
	// no-op
}

static inline mlfs_lease_t *lcache_find(uint32_t inum)
{
	mlfs_lease_t *ls;

	//pthread_spin_lock(&lcache_spinlock);

	HASH_FIND(hash_handle, lease_table->guts->hash, &inum,
        		sizeof(uint32_t), ls);

	//pthread_spin_unlock(&lcache_spinlock);

	return ls;
}

static inline mlfs_lease_t *lcache_alloc_add(uint32_t inum)
{
	mlfs_lease_t *ls;

#ifdef __cplusplus
	ls = static_cast<mlfs_lease_t *>(mlfs_zalloc(sizeof(mlfs_lease_t)));
#else
	ls = mlfs_zalloc(sizeof(mlfs_lease_t));
#endif
	pthread_spin_init(&ls->mutex, PTHREAD_PROCESS_PRIVATE);
	//pthread_mutex_init(&ls->mutex, NULL);

	if (!ls)
		panic("Fail to allocate lease\n");

	ls->inum = inum;

	HASH_ADD(hash_handle, lease_table->guts->hash, inum,
	 		sizeof(uint32_t), ls);

	return ls;
}

static inline mlfs_lease_t *lcache_add(mlfs_lease_t *ls)
{
	uint32_t inum = ls->inum;

	pthread_spin_lock(&lease_table->guts->mutex);

	HASH_ADD(hash_handle, lease_table->guts->hash, inum,
	 		sizeof(uint32_t), ls);

	pthread_spin_unlock(&lease_table->guts->mutex);

	return ls;
}

static inline int lcache_del(mlfs_lease_t *ls)
{
	pthread_spin_lock(&lease_table->guts->mutex);

	HASH_DELETE(hash_handle, lease_table->guts->hash, ls);
	free(ls);

	pthread_spin_unlock(&lease_table->guts->mutex);
	return 0;
}

void lupdate(mlfs_lease_t *ls)
{
}

// Find the inode with number inum on device dev
// and return the in-memory copy. Does not lock
// the inode and does not read it from disk.
mlfs_lease_t* lget(uint32_t inum)
{
	mlfs_lease_t *ls;

	pthread_spin_lock(&lease_table->guts->mutex);

	ls = lcache_find(inum);

	if (ls) {
		pthread_spin_unlock(&lease_table->guts->mutex);
		return ls;
	}

	ls = lcache_alloc_add(inum);

	pthread_spin_unlock(&lease_table->guts->mutex);

	return ls;
}

// Allocate a new inode with the given type on device dev.
// A free inode has a type of zero.
mlfs_lease_t* lalloc(uint8_t type, uint32_t inode_nr)
{
	uint32_t inum;
	int ret;
	//struct buffer_head *bp;
	mlfs_lease_t *ls;


	ls = lcache_find(inode_nr);

	if (!ls)
		ls = lget(inode_nr);

#if 0

	ip->_dinode = (struct dinode *)ip;

	if (!(ip->flags & I_VALID)) {
		read_ondisk_inode(dev, inode_nr, &dip);

		if (dip.itype == 0) {
			memset(&dip, 0, sizeof(struct dinode));
			dip.itype = type;
			dip.dev = dev;
		}

		sync_inode_from_dinode(ip, &dip);
		ip->flags |= I_VALID;
	}

	ip->i_sb = sb;
	ip->i_generation = 0;
	ip->i_data_dirty = 0;
	ip->nlink = 1;
#endif

	return ls;
}


int read_ondisk_lease(uint32_t inum, mlfs_lease_t *ls)
{
	int ret;
	struct buffer_head *bh;
	addr_t lease_block;

	lease_block = get_lease_block(g_root_dev, inum);
	mlfs_debug("read lease block: %lu\n", lease_block);
	bh = bh_get_sync_IO(g_root_dev, lease_block, BH_NO_DATA_ALLOC);

	bh->b_size = sizeof(mlfs_lease_t);
	bh->b_data = (uint8_t *)ls;
	bh->b_offset = sizeof(mlfs_lease_t) * (inum % LPB);
	bh_submit_read_sync_IO(bh);
	mlfs_io_wait(g_root_dev, 1);

	return 0;
}

int write_ondisk_lease(mlfs_lease_t *ls)
{
	int ret;
	struct buffer_head *bh;
	addr_t lease_block;

	lease_block = get_lease_block(g_root_dev, ls->inum);
	mlfs_debug("write lease block: %lu\n", lease_block);
	bh = bh_get_sync_IO(g_root_dev, lease_block, BH_NO_DATA_ALLOC);

	bh->b_size = sizeof(mlfs_lease_t);
	bh->b_data = (uint8_t *)ls;
	bh->b_offset = sizeof(mlfs_lease_t) * (ls->inum % LPB);
	ret = mlfs_write(bh);
	mlfs_io_wait(g_root_dev, 0);

	return ret;
}

#ifdef LIBFS
int acquire_family_lease(uint32_t inum, int type, char *path)
{
	mlfs_printf("trying to acquire child/parent leases of type %d for inum %u\n",
			type, inum);
	// child should always be acquired first, otherwise we deadlock
	// due to ordering of namei
	acquire_lease(inum, type, path);
	acquire_parent_lease(inum, type, path);

	return 1;
}

int acquire_parent_lease(uint32_t inum, int type, char *path)
{
	mlfs_printf("trying to acquire parent lease of type %d for inum %u\n",
			type, inum);
	char name[DIRSIZ], parent_path[DIRSIZ];
	struct inode *pid;

	pid = nameiparent(path, name);
	get_parent_path(path, parent_path, name);

	acquire_lease(pid->inum, type, parent_path);

	return 1;
}

int report_lease_error(uint32_t inum)
{
	mlfs_lease_t* ls = lcache_find(inum);

	if (!ls)
		panic("failed to find lease\n");

	ls->errcode = 1;
	return 1;
}

// invoked by KernFS upcall
int revoke_lease(int sockfd, uint32_t seq_n, uint32_t inum)
{
	mlfs_printf("\x1B[31m [L] KernFS requesting lease revocation for inum %u \x1B[0m\n", inum);

	mlfs_lease_t* ls = lcache_find(inum);

	if (!ls) {
		ls = lcache_alloc_add(inum);
	}

	if (!ls)
		panic("failed to allocate lease\n");

	pthread_spin_lock(&ls->mutex); //never calls this if we're using shared locks (causes a deadlock)

	if(ls->hid != g_self_id)
		panic("kernfs trying to revoke an unowned lease\n");

	while(ls->holders > 0) {
#if 0
		int holders;
		current = time(NULL);
		if((current - start) > timeout) {
			holders = ls->holders;

			if(cmpxchg(&ls->holders, holders, 0) == holders)
				goto retry;
		}
#else
		cpu_relax();
#endif
	}

	//rpc_lease_flush_response(sockfd, seq_n, inum, ls->lversion, ls->lblock);
	rpc_lease_change(ls->mid, g_self_id, ls->inum, LEASE_FREE, ls->lversion, ls->lblock, 0);

	ls->state = LEASE_FREE;
	ls->hid = ls->mid;

	pthread_spin_unlock(&ls->mutex);

	mlfs_printf("lease revoked for inum %u\n", inum);
}

int mark_lease_revocable(uint32_t inum)
{
	uint64_t start_tsc;

	mlfs_lease_t* ls = lcache_find(inum);

	if (!ls) {
		mlfs_info("[Warning] failed to surrender uncached lease for inum: %u\n", inum);
		return 0;
	}
		//panic("failed to allocate lease\n");

	mlfs_info("surrendering lease (state: %d) for inum: %u\n",
			ls->state, inum);

	//rpc_lease_change(ls->mid, ls->inum, LEASE_FREE, g_fs_log->start_version,
	//			g_log_sb[0]->start_digest);

	if(ls->holders == 0) {
		mlfs_info("%s", "[Warning] trying to give up unowned lease.\n");
		return -1;
	}

	struct logheader_meta *loghdr_meta;
	loghdr_meta = get_loghdr_meta();

	//uint32_t n_digest = atomic_load(&g_log_sb[0]->n_digest);

	ls->lversion = g_fs_log->avail_version;
	ls->lblock = loghdr_meta->hdr_blkno;

	mlfs_printf("marking lease revocable: inum %u block_nr %lu version %u\n", inum, ls->lblock, ls->lversion);

	if (enable_perf_stats)
		start_tsc = asm_rdtscp();

	if(ls->holders == 1) {
#ifndef LAZY_SURRENDER
			rpc_lease_change(ls->mid, g_self_id, ls->inum, LEASE_FREE, ls->lversion, ls->lblock, 0);
			ls->state = LEASE_FREE;		
#endif

#if 0
			while(make_digest_request_async(100) == -EBUSY)
				cpu_relax();

			mlfs_info("%s", "[L] Giving up lease. asynchronous digest!\n");

#endif
			ls->holders--;
	}
	else
		ls->holders--;

	if (enable_perf_stats)
		g_perf_stats.lease_revoke_wait_tsc += (asm_rdtscp() - start_tsc);

	mlfs_info("lease surrendered (state: %d) for inum: %u (holder = %u)\n",
			ls->state, inum, ls->hid);
}

int acquire_lease(uint32_t inum, int type, char *path)
{
	mlfs_printf("LIBFS ID= %d trying to acquire lease of type %d for inum %u\n", g_self_id, type, inum);

	mlfs_lease_t* ls = lcache_find(inum);

	int i_dirty = 0;

	uint64_t start_tsc;

	if (!ls) {
		ls = lcache_alloc_add(inum);
	}

	if (!ls)
		panic("failed to allocate lease\n");

	if (enable_perf_stats)
		start_tsc = asm_rdtscp();

	pthread_spin_lock(&ls->mutex);

retry:

	if(type > ls->state || ls->hid != g_self_id) {
		rpc_lease_change(ls->mid, g_self_id, ls->inum, type, 0, 0, 1);

		// received invalid response
		if(ls->errcode) {
			ls->errcode = 0; // clear error code
			goto retry;
		}

		ls->holders++;
		// FIXME: remove this; inode cache should be cleared whenever leases
		// are revoked

		i_dirty = 1;

#if 0
		// update size after acquiring inode
		// necessary to prevent issues with directory allocation
		struct inode *ip;
		ip = icache_find(inum);

		ilock(ip);
		// no need to update size for dirty inodes
		//if (ip && !(ip->flags & I_DIRTY)) {
		//	struct dinode dip;
		//	read_ondisk_inode(inum, &dip);

		//	ip->size = dip.size;
		//}
		sync_inode_ext_tree(ip);
		iunlock(ip);
#endif
		mlfs_printf("LIBFS ID = %d lease acquired path %s new state %d manager ID = %u\n",
				g_self_id, path, type, ls->mid);
	}
	else {
		ls->holders++;
		mlfs_printf("lease already exists (inum %u state %d). No RPCs triggered.\n", inum, ls->state);
	}

	ls->state = type;
	ls->hid = g_self_id;

	mlfs_info("[DEBUG] after ls->inum %u ls->holders = %d\n", ls->inum, ls->holders);

	pthread_spin_unlock(&ls->mutex);

#if 1
	if(i_dirty) {
		// update size after acquiring inode
		// necessary to prevent issues with directory allocation

		struct dinode dip;
		struct inode *ip;
		ip = icache_find(inum);

		ilock(ip);

		//XXX: check for potential concurrency issues if reading while dinode is being updated by kernfs
		read_ondisk_inode(inum, &dip);
		sync_inode_from_dinode(ip, &dip);

		iunlock(ip);
	}
#endif

	if (enable_perf_stats)
		g_perf_stats.lease_lpc_wait_tsc += (asm_rdtscp() - start_tsc);


	return 1;
}

// necessary when dropping a lease
// FIXME: prevent unnecessary cache wipes by only calling this when a lease is in conflict
int purge_dir_caches(uint32_t inum)
{
	//purge node's de_cache entry from inode's parent
	//purge dcache entry of inode
	//purge dcache entries of all children files
	//purge decache entries of all files

	mlfs_info("purging cache for inum: %u\n", inum);


#if 0
	struct inode* inode = icache_find(g_root_dev, inum);

	if (!inode)
		return 0;

	mlfs_info("purging caches for inum: %u\n", inode->inum);

#if 0
	struct inode *id, *pid;
	uint8_t *dirent_array;
	struct mlfs_dirent de = {0};
	char name[DIRSIZ];
	offset_t off = 0;

	pid = nameiparent(path, name);

	if ((id = dir_lookup(pid, name, &off)) != NULL)  {
		get_dirent(pid, &de, off);

		mlfs_assert(strcmp(de.name, name) == 0);
		de_cache_del(pid, name);
	}
	
	dlookup_del(path);
#endif
	if(inode->itype == T_DIR)
		de_cache_del_all(inode);

	fcache_del_all(inode);
	icache_del(inode);
	free(inode);

	//TODO: should also delete path from lookup table
	// is there an easy way to do this using just the inum?
	//dlookup_del(inode->dev, path);
#endif
	return 1;
}
#endif


#ifdef LIBFS
void update_remote_ondisk_lease(uint8_t node_id, mlfs_lease_t *ls)
{
	mlfs_debug("writing remote ondisk lease inum %u to peer %u\n", ls->inum, node_id);
	addr_t offset = (get_lease_block(g_root_dev, ls->inum) << g_block_size_shift) + sizeof(mlfs_lease_t) * (ls->inum %LPB);
	addr_t src = ((uintptr_t)g_bdev[g_root_dev]->map_base_addr) + offset;
	addr_t dst = mr_remote_addr(g_peers[node_id]->sockfd[0], MR_NVM_SHARED) + offset;
	rdma_meta_t *meta = create_rdma_meta(src, dst, sizeof(mlfs_lease_t));

	IBV_WRAPPER_WRITE_ASYNC(g_peers[node_id]->sockfd[0], meta, MR_NVM_SHARED, MR_NVM_SHARED);
}

// For lease acquisitions, lease must be locked beforehand.
int modify_lease_state(int req_id, int inum, int new_state, int version, addr_t logblock)
{
	uint64_t start_tsc;

	// get lease from cache
	mlfs_lease_t* ls = lget(inum);

	if (!ls)
		panic("failed to allocate lease\n");


	mlfs_info("modifying lease inum[%u] from %d to %d Req. ID = %d Holder ID = %d (num holders = %u)\n",
			inum, ls->state, new_state, req_id, ls->hid, ls->holders);

	if(new_state == LEASE_READ) {
		panic("read path not implemented!\n");
	}
	else if(new_state == LEASE_FREE) {
		ls->lversion = version;
		ls->lblock = logblock;
		
		if(ls->holders > 0)
			ls->holders--;
		else
			panic("lease surrender failed. Lease has no current owners!\n");
		//ls->holders--;
		return 1;
	}

	else {

		//pthread_mutex_lock(&ls->mutex);

		if(ls->holders == 0) {

			goto acquire_lease;
		}
		else {	


			if (enable_perf_stats)
				start_tsc = asm_rdtscp();

			// wait until original lease owner relinquishes its lease
			while(ls->holders > 0)
				cpu_relax();

			if (enable_perf_stats)
				g_perf_stats.local_contention_tsc += (asm_rdtscp() - start_tsc);

			//FIXME: need to implement this call; we currently assume that the digest checkpoint was reached
			//wait_till_digest_checkpoint(g_sync_ctx[ls->hid]->peer, ls->lversion, ls->lblock);

acquire_lease:
			if (enable_perf_stats)
				start_tsc = asm_rdtscp();

			// wait until lease reaches its designated digest checkpoint
			// KernFS sets lease version and lblock to zero when this condition is met
			while(ls->lversion | ls->lblock)
				cpu_relax();

			if (enable_perf_stats)
				g_perf_stats.local_digestion_tsc += (asm_rdtscp() - start_tsc);

			ls->holders++;
			ls->state = new_state;
			ls->hid = req_id;
			//ls->holders--;

			goto PERSIST;
		}


	}

PERSIST:

	write_ondisk_lease(ls);
	
	for(int i=0; i<g_n_nodes;i++) {
		if(i == g_kernfs_id)
			continue;
		mlfs_printf("updating remote lease for inum %u\n", ls->inum);
		update_remote_ondisk_lease(i, ls);
	}

	//pthread_mutex_unlock(&ls->mutex);
	
	return 1;
}

void shutdown_lease_protocol()
{
	//mlfs_printf("Elapsed time for revocations: %lf\n", revoke_sec);
}

#else
int modify_lease_state(int req_id, int inum, int new_state, int version, addr_t logblock)
{
	int do_migrate = 0;

	// get lease from cache
	mlfs_lease_t* ls = lget(inum);

#if 0
	if (!ls) {
		ls = lcache_alloc_add(g_root_dev, inum);
	}
#endif

	if (!ls)
		panic("failed to allocate lease\n");

	if (enable_perf_stats) {
		//mlfs_printf("local_kernfs_id for peer %d is %d\n", req_id, local_kernfs_id(req_id));
		//if(g_self_id == local_kernfs_id(req_id))
		//	g_perf_stats.lease_rpc_local_nr++;
		//else
			g_perf_stats.lease_rpc_remote_nr++;
	}

#ifdef LEASE_MIGRATION
	// this KernFS is not the lease manager
	if (ls->mid != g_self_id) {
		return -ls->mid;
	}
#endif

	mlfs_printf("modifying lease inum[%u] from %d to %d Req. ID = %d Holder ID = %d (num holders = %u)\n",
			inum, ls->state, new_state, req_id, ls->hid, ls->holders);

	if(new_state == LEASE_READ) {
		panic("read path not implemented!\n");
	}
	else if(new_state == LEASE_FREE) {
		// if previous state is write, update log params
		//if(ls->state == LEASE_WRITE) {
			// confirm that LibFS has actually held this lease
			//mlfs_assert(ls->hid == req_id);

			//ls->state = LEASE_FREE;
			ls->lversion = version;
			ls->lblock = logblock;
		//}
		if(ls->holders > 0)
			ls->holders--;
		else
			panic("lease surrender failed. Lease has no current owners!\n");
		//ls->holders--;
		return 1;
	}

	else {

		pthread_spin_lock(&ls->mutex);

#ifdef LEASE_MIGRATION
		// this KernFS is not the lease manager
		if (ls->mid != g_self_id) {
			return -ls->mid;
		}
#endif

		//goto PERSIST;
		time_t start = time(NULL);
		time_t current = start;
		time_t timeout = 1; // timeout interval in seconds
retry:
		if(ls->holders == 0) {

			goto acquire_lease;
		}
		else {	
	
			//ls->holders++;

			//if(ls->hid == req_id)
			//	goto acquire_lease;

			mlfs_printf("%s", "\x1B[31m [L] Conflict detected: synchronous digest request and wait! \x1B[0m\n");

busy_wait:
			//FIXME: temporarily commenting this out
			//while(ls->holders > 1)
			//	cpu_relax();

			//if(cmpxchg(&ls->holders, 1, 2) != 1)
			//	goto busy_wait;

			rpc_lease_flush(ls->hid, inum, true);

			// wait until original lease owner relinquishes its lease
			while(ls->holders > 0)
				cpu_relax();

			wait_till_digest_checkpoint(g_sync_ctx[ls->hid]->peer, ls->lversion, ls->lblock);

acquire_lease:
			//FIXME: temporarily commenting this out
			ls->holders++;
			//ls->holders = 1;
			ls->state = new_state;
			ls->hid = req_id;
			//ls->holders--;

			goto PERSIST;
		}
#if 0
		while(ls->holders > 0) {
#if 0
			int holders;
			current = time(NULL);
			if((current - start) > timeout) {
				holders = ls->holders;

				if(cmpxchg(&ls->holders, holders, 0) == holders)
					goto retry;
			}
#else
			cpu_relax();
#endif
		}
#endif

	}

PERSIST:

#ifdef LEASE_MIGRATION
	// We are trying to acquire the lease; update ltime and check if we need to migrate

	// update lease acquisition time if it's being accessed from a different node
	//if(local_kernfs_id(ls->hid) != local_kernfs_id(req_id))
	//	mlfs_get_time(&ls->time);


	// consecutive lease requests from same LibFS & requester's local kernfs isn't manager
	if(local_kernfs_id(req_id) != g_self_id) {
		struct timespec now;
		//time_stats elaps;
		//mlfs_get_time(&ls->time);
		//mlfs_get_time(&now);
		clock_gettime(CLOCK_MONOTONIC, &now);

		//mlfs_printf("time (before): %ld.%06ld s \n", ls->time.tv_sec, ls->time.tv_usec);
		//mlfs_printf("time (after): %ld.%06ld s \n", now.tv_sec, now.tv_usec);
		
		double start_sec = (double)(ls->time.tv_sec * 1000000000.0 + (double)ls->time.tv_nsec) / 1000000000.0;
		double end_sec = (double)(now.tv_sec * 1000000000.0 + (double)now.tv_nsec) / 1000000000.0;
		double sec = end_sec - start_sec;

		//timersub(&now, &ls->time, &elaps);
		//double sec = (double)(elaps.tv_sec * 1000000.0 + (double)elaps.tv_usec) / 1000000.0;
		//double sec = (double)elaps.tv_sec;
		mlfs_printf("Lease elapsed time: %f\n", sec);

		if(sec >= 1) {
			do_migrate = 1;
			// migrate lease management to local kernfs
			ls->mid = local_kernfs_id(req_id);

			// record migration time (to prevent frequent migrations)
			//mlfs_get_time(&ls->time);
			clock_gettime(CLOCK_MONOTONIC, &ls->time);

			mlfs_printf("Migrating lease to KernFS ID = %d\n", ls->mid);
#if 0
			// flush all caches for inode
			int pos = 0;
			while(!bitmap_empty(ip->libfs_ls_bitmap, g_n_max_libfs)) {
				pos = find_next_bit(ip->libfs_ls_bitmap, g_n_max_libfs, pos);
				if(pos != libfs_id) {
					rpc_lease_flush(pos, ip->inum, false);
				}
				bitmap_clear(ip->libfs_ls_bitmap, pos, 1);
			}
#endif
		}
	}

#endif

	write_ondisk_lease(ls);
	
	for(int i=0; i<g_n_nodes;i++) {
		if(i == g_self_id)
			continue;
		update_remote_ondisk_lease(i, ls);
	}

	if(do_migrate) {
		if (enable_perf_stats) {
			g_perf_stats.lease_migration_nr++;
		}

		for(int i=0; i<g_n_nodes;i++) {
			if(i == g_self_id)
				continue;
			rpc_lease_migrate(i, ls->inum, ls->mid);
		}
	}

	pthread_spin_unlock(&ls->mutex);
	
	return 1;
}

int clear_lease_checkpoints(int req_id, int version, addr_t log_block)
{
	mlfs_lease_t *item, *tmp;

	mlfs_printf("clearing outdated digest checkpoints for leases belonging to peer ID = %d\n", req_id);

	pthread_spin_lock(&lease_table->guts->mutex);

	//FIXME: keep a list of leases held by req_id to make this go faster
	HASH_ITER(hash_handle, lease_table->guts->hash, item, tmp) {
		//HASH_DELETE(hash_handle, lease_hash, item);
		mlfs_debug("[DEBUG] hash item: lease inum %u state %u\n", item->inum, item->state);
		if(item->hid == req_id && ((version == item->lversion && log_block >= item->lblock)
				|| version > item->lversion)) {

				if(item->lversion || item->lblock) {
					item->lversion = 0;
					item->lblock = 0;
					mlfs_printf("lease digestion checkpoint cleared for inum %u\n", item->inum);
				}
		}

		//mlfs_free(item);
	}

	pthread_spin_unlock(&lease_table->guts->mutex);
}

int discard_leases(int req_id)
{
//#ifdef LAZY_SURRENDER
	mlfs_lease_t *item, *tmp;

	mlfs_printf("discarding all leases for peer ID = %d\n", req_id);

	//pthread_spin_lock(&lcache_spinlock);

	HASH_ITER(hash_handle, lease_table->guts->hash, item, tmp) {
		//HASH_DELETE(hash_handle, lease_hash, item);
		mlfs_debug("[DEBUG] hash item: lease inum %u state %u\n", item->inum, item->state);
		if(item->state == LEASE_WRITE && item->holders > 0) {
			if(item->hid == req_id) {
				//rpc_lease_change(item->mid, item->inum, LEASE_FREE, 0, 0, 0);
				modify_lease_state(req_id, item->inum, LEASE_FREE, 0, 0);
				mlfs_debug("lease discarded for inum %u\n", item->inum);
			}
		}

		//mlfs_free(item);
	}
	//HASH_CLEAR(hash_handle, lease_hash);

	//pthread_spin_unlock(&lcache_spinlock);
//#endif
	return 0;
}

int update_lease_manager(uint32_t inum, uint32_t new_kernfs_id)
{
	mlfs_lease_t* ls = lcache_find(inum);

	if (ls) {
		mlfs_printf("migrate - inum %u update lease manager from %u to %u\n",
				inum, ls->mid, new_kernfs_id);
		ls->mid = new_kernfs_id;
	}

	return 0;
}

#if 0
// modify lease state (this call is potentially blocking)
// FIXME: use locks!
int modify_lease_state(int libfs_id, int inum, int req_type, int log_version, addr_t log_block)
{
	struct inode* ip = icache_find(g_root_dev, inum);
	if (!ip) {
		struct dinode dip;
		ip = icache_alloc_add(g_root_dev, inum);

		read_ondisk_inode(g_root_dev, inum, &dip);

		// FIXME: possible synchronization issue
		// Sometimes LibFS requests lease for a dirty (undigested) inode
		// As a temporary workaround, just return if this happens

		if(dip.itype == 0)
			return 1;
		//mlfs_assert(dip.itype != 0);

		sync_inode_from_dinode(ip, &dip);

		ip->i_sb = sb;

		if(dip.dev == 0)
			return 1;
		//mlfs_assert(dip.dev != 0);
	}

	mlfs_info("modifying lease inum[%u] from %d to %d Req. ID = %d Holder ID= %d (num holders = %u)\n",
			inum, ip->lstate, req_type, libfs_id, ip->lhid, ip->lholders);

	// Cases with no conflicts:

	// (a) LibFS is giving up its lease
	if(req_type == LEASE_FREE) {
		if(ip->lstate == LEASE_WRITE) {
			mlfs_assert(ip->lhid == libfs_id);
			mlfs_assert(ip->lholders == 1);

			ip->lstate = LEASE_DIGEST_TO_ACQUIRE;
			//ip->lstate = req_type;
		}
		else if(ip->lstate == LEASE_READ || ip->lstate == LEASE_READ_REVOCABLE) {
			// FIXME: temporarily commenting assertion
			//mlfs_assert(ip->lholders > 0);

			//ip->lholders = 1;
			;
		}
		else {
			return 1;
			//panic("undefined codepath");
		}

		//ip->lhid = g_self_id; //set id to self
		if(ip->lholders > 0)
			ip->lholders--;
		//bitmap_clear(ip->libfs_ls_bitmap, libfs_id, 1);
		goto PERSIST;
	}

#ifdef LEASE_MIGRATION
	// We are trying to acquire the lease; update ltime and check if we need to migrate

	// update lease acquisition time if it's being accessed from a different node
	if(local_kernfs_id(ip->lhid) != local_kernfs_id(libfs_id))
		mlfs_get_time(&ip->ltime);


	// consecutive lease requests from same LibFS & requester's local kernfs isn't manager
	else if(local_kernfs_id(libfs_id) != g_self_id) {
		mlfs_time_t now;
		mlfs_time_t elaps;
		mlfs_get_time(&now);
		timersub(&now, &ip->ltime, &elaps);
		double sec = (double)(elaps.tv_sec * 1000000.0 + (double)elaps.tv_usec) / 1000000.0;

		mlfs_debug("Lease elapsed time: %f\n", sec);

		if(sec >= 1) {
			// migrate lease management to local kernfs
			ip->lmid = local_kernfs_id(ip->lhid);

			mlfs_debug("Migrating lease to KernFS ID = %d\n", ip->lmid);

			// flush all caches for inode
			int pos = 0;
			while(!bitmap_empty(ip->libfs_ls_bitmap, g_n_max_libfs)) {
				pos = find_next_bit(ip->libfs_ls_bitmap, g_n_max_libfs, pos);
				if(pos != libfs_id) {
					rpc_lease_flush(pos, ip->inum, false);
				}
				bitmap_clear(ip->libfs_ls_bitmap, pos, 1);
			}
		}
	}

#endif

	// (b) Multiple concurrent LibFS readers
	if((ip->lstate == LEASE_READ || ip->lstate == LEASE_READ_REVOCABLE)
			&& (req_type == LEASE_READ || req_type == LEASE_READ_REVOCABLE)) {
		ip->lhid = libfs_id;
		bitmap_set(ip->libfs_ls_bitmap, libfs_id, 1);
		if(req_type == LEASE_READ) {
			ip->lholders++;
			goto PERSIST;
		}
		else
			return 1;
	}

	// if I already hold an existing lease, remove myself and decrement lholders
	// NOTE: this shouldn't really happen since we surrender leases after closing a file/dir.
	// Should we panic?
	if(ip->lstate == LEASE_WRITE && ip->lholders > 0 && ip->lhid == libfs_id) {
		//mlfs_assert(ip->lholders > 0);
		//bitmap_clear(ip->libfs_ls_bitmap, libfs_id, 1);
		ip->lholders--;
	}

	// (c) remaining cases can involve conflicts if there are other lease holders

	// TODO: Add timeout mechanism to prevent prepetual stalling
	// if a LibFS holds a lease for too long (maliciously or otherwise)
	while(1) {	
		if(!cmpxchg(&ip->lholders, 0, 1)) {
			if(ip->lstate == LEASE_WRITE || ip->lstate == LEASE_DIGEST_TO_ACQUIRE
					|| ip->lstate == LEASE_DIGEST_TO_READ) {
				// check if we need to digest anything
				if(ip->lhid != libfs_id) {
					mlfs_printf("DEBUG ip->lhid %d req_id %d\n", ip->lhid, libfs_id);
					if(!rpc_lease_flush(ip->lhid, ip->inum, true))
						wait_till_digest_checkpoint(g_sync_ctx[ip->lhid]->peer, ip->lversion, ip->lblock);

					bitmap_clear(ip->libfs_ls_bitmap, ip->lhid, 1);
				}
				else {
					// if a LibFS process is downgrading its lease
					// make sure to digest before another acquires it
					mlfs_assert(req_type == LEASE_READ || req_type == LEASE_READ_REVOCABLE
							|| req_type == LEASE_WRITE);

					if(req_type == LEASE_READ)
						req_type = LEASE_DIGEST_TO_READ;
					else if(req_type == LEASE_READ_REVOCABLE)
						req_type = LEASE_DIGEST_TO_ACQUIRE;
				}	
			}

			int pos = 0;
			while(!bitmap_empty(ip->libfs_ls_bitmap, g_n_max_libfs)) {
				pos = find_next_bit(ip->libfs_ls_bitmap, g_n_max_libfs, pos);
				if(pos != libfs_id) {
					//FIXME: temporarily disabling icache flushes to avoid issue with persist_log_inode()
					//rpc_lease_flush(pos, ip->inum, false);
				}
				bitmap_clear(ip->libfs_ls_bitmap, pos, 1);
			}

			ip->lhid = libfs_id;
			ip->lstate = req_type;
			ip->lversion = log_version;

			// revocable leases don't increase lholder count
			if(ip->lstate == LEASE_READ_REVOCABLE)
				ip->lholders--;

			bitmap_set(ip->libfs_ls_bitmap, libfs_id, 1);
			goto PERSIST;
		}

		while(ip->lholders > 0)
			cpu_relax();
	}
//#if 0
PERSIST:

	write_ondisk_inode(ip);
	
	for(int i=0; i<g_n_nodes;i++) {
		if(i == g_self_id)
			continue;
		update_remote_ondisk_inode(i, ip);
	}
//#endif
	return 1;

}

#endif

#endif

#if 0
int resolve_lease_conflict(int sockfd, char *path, int type, uint32_t seqn)
{
	struct inode *ip;

	ip = namei(path);

	mlfs_info("trying to resolve conflicts [requested: %d, current: %d] for %s\n",
			type, ip->ilease, path);

	if(!ip) {
		mlfs_printf("%s", "trying to acquire lease for non-existing inode\n");
		assert(0);
	}

	int old_ilease = ip->ilease; 
	int new_ilease = lease_downgrade(type, ip->ilease);

	if(new_ilease) {
		//ensure that inode is not locked
		ilock(ip);
		iunlock(ip);

		//close fd (if already open)
		struct file *f;
		HASH_FIND_STR(g_fd_table.open_files_ht, path, f);
		if(f) {
			assert(f->ref > 0);
			mlfs_posix_close(f->fd);
		}

		if(new_ilease == LEASE_NONE) {
			purge_dir_caches(ip, path);
		}

		ip->ilease = new_ilease;
	}

	// only replicate data if we had a write lease that was revoked
	//FIXME: locking!
	int do_replicate = (old_ilease == LEASE_WRITE && new_ilease && atomic_load(&get_peer()->n_unsync_blk) > 0);
	mlfs_info("replicate:%s old_lease %d new_lease %d n_unsync_blk %lu\n",
			do_replicate?"yes":"no", old_ilease, new_ilease, atomic_load(&get_peer()->n_unsync_blk));
	rpc_lease_response(sockfd, seqn, do_replicate);
	if(do_replicate) {
		//triggering a local digest will automatically sync logs and digest remotely
		while(make_digest_request_async(100) != -EBUSY);
		//make_digest_request_async()
		//mlfs_do_rdigest();
	}

	mlfs_debug("lease downgraded path %s type %d\n", path, type);
}
#endif

#endif
