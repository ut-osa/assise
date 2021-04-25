/* mlfs file system interface
 *
 * Implement file-based operations with file descriptor and
 * struct file.
 */
#include "mlfs/mlfs_user.h"
#include "global/global.h"
#include "global/util.h"
#include "filesystem/file.h"
#include "log/log.h"
#include "concurrency/synchronization.h"

#if MLFS_LEASE
#include "experimental/leases.h"
#endif

#include <pthread.h>

struct open_file_table g_fd_table;

void mlfs_file_init(void)
{
	g_fd_table.open_files_ht = NULL;
	pthread_spin_init(&g_fd_table.lock, PTHREAD_PROCESS_SHARED); 
}

/*
static addr_t mlfs_update_get_fcache(struct inode *inode, 
		offset_t offset, addr_t log_addr)
{
	offset_t key;
	struct fcache_block *fc_block;

	key = (offset >> g_block_size_shift);

	fc_block = fcache_find(inode, key);

	if (fc_block) 
		return fc_block->log_addr;
	
	fc_block = fcache_alloc_add(inode, key, log_addr);

	if (!fc_block)
		return 0;

	return fc_block->log_addr;
}
*/

// Allocate a file structure.
/* FIXME: the ftable implementation is too naive. need to
 * improve way to allocate struct file */
struct file* mlfs_file_alloc(void)
{
	int i = 0;
	struct file *f;
	pthread_rwlockattr_t rwlattr;

	pthread_rwlockattr_setpshared(&rwlattr, PTHREAD_PROCESS_SHARED);

	pthread_spin_lock(&g_fd_table.lock);

	for(i = 0, f = &g_fd_table.open_files[0]; 
			i < g_max_open_files; i++, f++) {
		if(f->ref == 0) {
			memset(f, 0, sizeof(*f));
			f->ref = 1;
			f->fd = i;
			pthread_rwlock_init(&f->rwlock, &rwlattr);

			pthread_spin_unlock(&g_fd_table.lock);

			mlfs_debug("creating fd: %d | ref: %d\n", i, (&g_fd_table.open_files[i])->ref);

			return f;
		}
	}

	pthread_spin_unlock(&g_fd_table.lock);

	return NULL;
}

// Increment ref count for file f.
struct file* mlfs_file_dup(struct file *f)
{

	panic("not supported\n");

	pthread_rwlock_wrlock(&f->rwlock);
	
	if(f->ref < 1)
		panic("filedup");
	f->ref++;

	pthread_rwlock_unlock(&f->rwlock);
	
	return f;
}

int mlfs_file_close(struct file *f)
{
	struct file ff;

	mlfs_assert(f->ref > 0);

	pthread_rwlock_wrlock(&f->rwlock);
	
	ff = *f;

	mlfs_assert(ff.ip != NULL);

	if(--f->ref > 0) {
		pthread_rwlock_unlock(&f->rwlock);
		return 0;
	}

	//f->ref = 0;
	f->type = FD_NONE;

#ifdef DISTRIBUTED
	//remove file path from hashtable
	//relevant for remote reads
	//HASH_DEL(g_fd_table.open_files_ht, f);
#endif
	pthread_rwlock_unlock(&f->rwlock);

	iput(ff.ip);

	return 0;
}

// Get metadata about file f.
int mlfs_file_stat(struct file *f, struct stat *st)
{
	if(f->type == FD_INODE){
		ilock(f->ip);
		stati(f->ip, st);
		iunlock(f->ip);
		return 0;
	}
	return -1;
}

// Read from file f.
ssize_t mlfs_file_read(struct file *f, struct mlfs_reply *reply, size_t n)
{
	ssize_t r = 0;

	if (f->readable == 0) 
		return -EPERM;

	if (f->type == FD_INODE) {
		ilock(f->ip);

		if (f->off >= f->ip->size) {
			iunlock(f->ip);
			return 0;
		}

		r = readi(f->ip, reply, f->off, n, f->path);
		if (r < 0) 
			panic("read error\n");

		f->off += r;

		iunlock(f->ip);
		return r;
	}

	panic("mlfs_file_read\n");

	return -1;
}

int mlfs_file_read_offset(struct file *f, struct mlfs_reply *reply, size_t n, offset_t off)
{
	int r;

	if (f->readable == 0)
		return -EPERM;

	if (f->type == FD_INODE) {
		ilock(f->ip);

		if (off >= f->ip->size) {
			iunlock(f->ip);
			return 0;
		}

		r = readi(f->ip, reply, off, n, f->path);

		if (r < 0) 
			panic("read error\n");

		iunlock(f->ip);
		return r;
	}

	panic("mlfs_file_read_offset\n");

	return -1;
}

// Write `n' bytes from buffer `buf' start at `offset' to file `f'.
// return value: the bytes wrote to file or -1 if error occurs
// NOTE: This function will NOT update f->off
int mlfs_file_write(struct file *f, uint8_t *buf, size_t n, offset_t offset)
{
	int r;
	uint32_t max_io_size = (128 << 20);
	offset_t i = 0, file_size;
	uint32_t io_size = 0;
	offset_t size_aligned, size_prepended, size_appended;
	offset_t offset_aligned, offset_start, offset_end;
	char *data;

	if (f->writable == 0)
		return -EPERM;
	/*
	// PIPE is not supported 
	if(f->type == FD_PIPE)
		return pipewrite(f->pipe, buf, n);
	*/

	if (f->type == FD_INODE) {
		/*            a     b     c
		 *      (d) ----------------- (e)   (io data)
		 *       |      |      |      |     (4K alignment)
		 *           offset
		 *           aligned
		 *
		 *	a: size_prepended
		 *	b: size_aligned
		 *	c: size_appended
		 *	d: offset_start
		 *	e: offset_end
		 */

		mlfs_debug("%s\n", "+++ start transaction");	
		
		while (offset > f->ip->size) {
			mlfs_debug("sparse write to inum %u, offset %lu, len %lu, file size %lu, force fallocate\n",
					f->ip->inum, offset, n, f->ip->size);
			mlfs_file_fallocate(f, f->ip->size, offset - f->ip->size);
		}

		start_log_tx();

		offset_start = offset;
		offset_end = offset + n;

		offset_aligned = ALIGN(offset_start, g_block_size_bytes);

		/* when IO size is less than 4KB. */
		if (offset_end < offset_aligned) { 
			size_prepended = n;
			size_aligned = 0;
			size_appended = 0;
		} else {
			// compute portion of prepended unaligned write
			if (offset_start < offset_aligned) {
				size_prepended = offset_aligned - offset_start;
			} else
				size_prepended = 0;

			mlfs_assert(size_prepended < g_block_size_bytes);

			// compute portion of appended unaligned write
			size_appended = ALIGN(offset_end, g_block_size_bytes) - offset_end;
			if (size_appended > 0)
				size_appended = g_block_size_bytes - size_appended;

			size_aligned = n - size_prepended - size_appended; 
		}

		// add preprended portion to log
		if (size_prepended > 0) {
			ilock(f->ip);

			r = add_to_log(f->ip, buf, offset, size_prepended, L_TYPE_FILE);

			iunlock(f->ip);

			mlfs_assert(r > 0);

			offset += r;

			i += r;
		}

		// add aligned portion to log
		while(i < n - size_appended) {
			mlfs_assert((offset % g_block_size_bytes) == 0);
			
			io_size = n - size_appended - i;
			
			if(io_size > max_io_size)
				io_size = max_io_size;

			/* add_to_log updates inode block pointers */
			ilock(f->ip);

			/* do not copy user buffer to page cache */
			
			/* add buffer to log header */
			if ((r = add_to_log(f->ip, buf + i, offset, io_size, L_TYPE_FILE)) > 0)
				offset += r;

			iunlock(f->ip);

			if(r < 0)
				break;

			if(r != io_size)
				panic("short filewrite");

			i += r;
		}

		// add appended portion to log
		if (size_appended > 0) {
			ilock(f->ip);

			r = add_to_log(f->ip, buf + i, offset, size_appended, L_TYPE_FILE);

			iunlock(f->ip);

			mlfs_assert(r > 0);

			offset += r;

			i += r;
		}
	

		/* Optimization: writing inode to log does not require
		 * for write append or update. Kernfs can see the length in inode
		 * by looking up offset in a logheader and also mtime in a logheader */
		// iupdate(f->ip);	

		commit_log_tx();


		mlfs_debug("%s\n", "--- end transaction");	


		return i == n ? n : -1;
	}

	panic("filewrite");

	return -1;
}

/*!
 * Allocate zero to change file size
 */
#define ALLOC_IO_SIZE (64UL << 10)
#define _min(a, b) ({\
		__typeof__(a) _a = a;\
		__typeof__(b) _b = b;\
		_a < _b ? _a : _b; })

int mlfs_file_fallocate(struct file *f, offset_t offset, size_t len)
{
	struct inode *ip = f->ip;
	char falloc_buf[ALLOC_IO_SIZE];

	mlfs_assert(ip);
	memset(falloc_buf, 0, ALLOC_IO_SIZE);
	if (offset > ip->size) {
		panic("doesn't support sparse file\n");
	}
	if (offset + len > ip->size) {
		// only append 0 at the end of the file when
		// offset <= file size && offset + len > file_size
		// First, make sure offset and len start from the end of the file
		len -= ip->size - offset;
		offset = ip->size;

		for (size_t i = 0; i < len; i += ALLOC_IO_SIZE) {
			size_t io_size = _min(len - i, ALLOC_IO_SIZE);

			int ret = mlfs_file_write(f, (uint8_t *)falloc_buf, offset, io_size);
			// keep accumulating offset, here should hold `ret == io_size'
			offset += ret;
			if (ret < 0) {
				panic("fail to do fallocate\n");
				return ret;
			}
		}
	}
	return 0;
}

struct inode *mlfs_object_create(char *path, unsigned short type)
{
	offset_t offset;
	struct inode *inode = NULL;
	struct inode *parent_inode = NULL;
	struct mlfs_dirent *new_de = NULL;
	struct mlfs_dirent *log_entry = NULL;
	char name[DIRSIZ];
	uint64_t tsc_begin, tsc_end;
	
	start_log_tx();

	inode = dlookup_find(path);
	if (inode) {
		mlfs_debug("mlfs_object_create: already found in dlookup cache %s\n", path);
#if MLFS_LEASE
		acquire_lease(inode->inum, LEASE_WRITE, path);
#endif
		if (inode->itype != type) {
			inode->itype = type;
		}
		mlfs_get_time(&inode->ctime);
		inode->atime = inode->mtime = inode->ctime;
		iupdate(inode);

		commit_log_tx();
		return inode;
	}

	// Find parent_inode. when it returns, name will contain the last 
	// entry in the path, which is typically a file name.
	if ((parent_inode = nameiparent(path, name)) == 0) {
		abort_log_tx();
		return NULL;
	}

#if MLFS_LEASE
	char parent_path[MAX_PATH];
	get_parent_path(path, parent_path, name);
	acquire_lease(parent_inode->inum, LEASE_WRITE, parent_path);
#endif

	ilock(parent_inode);

#if 1
	inode = dir_lookup(parent_inode, name, &offset);
	if (inode) {
		mlfs_debug("mlfs_object_create: already found in dir %s\n", path);
		iunlock(parent_inode);

		if (inode->itype != type)
			inode->itype = type;

#if MLFS_LEASE
		// still need to acquire lease on file (due to ctime update)
		//acquire_lease(inode->inum, LEASE_WRITE, path);

		// release parent directory; no need to recreate file
		//mark_lease_revocable(parent_inode->inum);
#endif

		mlfs_get_time(&inode->ctime);
		inode->atime = inode->mtime = inode->ctime;
		iupdate(inode);

		commit_log_tx();

#if MLFS_LEASE
		m_barrier();

		mark_lease_revocable(parent_inode->inum);
#endif

		return inode;
	}
#endif

	if (enable_perf_stats) 
		tsc_begin = asm_rdtscp();

	// create new (locked) inode
	inode = icreate(type);

	if (enable_perf_stats) {
		tsc_end = asm_rdtscp();
		g_perf_stats.ialloc_tsc += (tsc_end - tsc_begin);
		g_perf_stats.ialloc_nr++;
	}

	inode->itype = type;
	inode->nlink = 1;

	iunlock(inode);

	add_to_loghdr(L_TYPE_INODE_CREATE, inode, 0, 
			sizeof(struct dinode), NULL, 0);

	mlfs_debug("create %s - inum %u\n", path, inode->inum);

	if (type == T_DIR) {
		log_entry = dir_add_links(inode, inode->inum, parent_inode->inum);
		parent_inode->nlink++;
		iupdate(parent_inode);
	}

	new_de = dir_add_entry(parent_inode, name, inode);
	iunlockput(parent_inode);

	if (!dlookup_find(path))
		dlookup_alloc_add(inode, path);

	commit_log_tx();

#if MLFS_LEASE
	//FIXME: temporarily disabling this
	//m_barrier();
	mark_lease_revocable(parent_inode->inum);
#endif

	mlfs_free(new_de);
	if (log_entry)
		mlfs_free(log_entry);

	return inode;
}
