//
// File-system system calls.
// Mostly argument checking, since we don't trust
// user code, and calls into file.c and fs.c.
//
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>

#include "global/global.h"
#include "global/types.h"
#include "filesystem/stat.h"
#include "filesystem/fs.h"
#include "filesystem/file.h"
#include "log/log.h"
#include "posix/posix_interface.h"

#if MLFS_LEASE
#include "experimental/leases.h"
#endif

#ifdef DISTRIBUTED
#include "mlfs/mlfs_interface.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define _min(a, b) ({\
		__typeof__(a) _a = a;\
		__typeof__(b) _b = b;\
		_a < _b ? _a : _b; })

#if 0
// Is the directory dp empty except for "." and ".." ?
static int isdirempty(struct inode *dp)
{
	int off;
	struct mlfs_dirent de;

	for(off=2*sizeof(de); off<dp->size; off+=sizeof(de)){
		if(readi(dp, (char*)&de, off, sizeof(de)) != sizeof(de))
			panic("isdirempty: readi");
		if(de.inum != 0)
			return 0;
	}
	return 1;
}
#endif

int posix_init = 0;

#define SHM_START_PATH "/shm_lease_test"
#define SHM_F_SIZE 128

void* create_shm() {
	void * addr;
	int fd = shm_open(SHM_START_PATH, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	if (fd < 0) {
		printf ("[LEASE_TEST] (%s) shm_open failed.\n", __func__);
		exit(-1);
	}

	int res = ftruncate(fd, SHM_F_SIZE);
	if (res < 0)
	{
		printf ("[LEASE_TEST] (%s) ftruncate error.\n", __func__);
		exit(-1);
	}

	addr = mmap(NULL, SHM_F_SIZE, PROT_WRITE, MAP_SHARED, fd, 0);
	if (addr == MAP_FAILED){
		printf ("[LEASE_TEST] (%s) mmap failed.\n", __func__);
		exit(-1);
	}

	return addr;
}

int mlfs_posix_open(char *path, int flags, uint16_t mode)
{
	struct file *f;
	struct inode *inode;
	int fd;

#if 0
	if(!posix_init) {
		int *shm_i;

		shm_i = (int*)create_shm();

		*shm_i += 1;

		// busy waiting.
		while (*shm_i > 0){
			usleep(100);
		}
		posix_init = 1;
	}
#endif



	//start_log_tx();

	mlfs_posix("[POSIX] open(%s) O_CREAT:%d\n", path, flags & O_CREAT);
#if 0
	char temp[MAX_PATH];
	strncpy(temp, path, MAX_PATH);
	temp[sizeof(temp) - 1] = '\0';
#endif

	if (flags & O_CREAT) {
		if (flags & O_DIRECTORY)
			panic("O_DIRECTORY cannot be set with O_CREAT\n");

		inode = mlfs_object_create(path, T_FILE);

		if (!inode) {
			return -ENOENT;
		}

		mlfs_debug("create file %s - inum %u\n", path, inode->inum);
	} else {
		// opendir API
		if (flags & O_DIRECTORY) {
			// Fall through..
			// it is OK to return fd for directory. glibc allocates 
			// DIR structure and fill it with fd and results from stats. 
			// check: sysdeps/posix/opendir.c
		}

		if ((inode = namei(path)) == NULL) {
			return -ENOENT;
		}

		if (inode->itype == T_DIR) {
			if (!(flags |= (O_RDONLY|O_DIRECTORY))) {
				return -EACCES;
			}
		}
	}

	f = mlfs_file_alloc();

	if (f == NULL) {
		iunlockput(inode);
		return -ENOMEM;
	}

	fd = f->fd;

	mlfs_debug("open file %s inum %u fd %d\n", path, inode->inum, fd);

	pthread_rwlock_wrlock(&f->rwlock);

	if (flags & O_DIRECTORY) {
		mlfs_debug("directory file inum %d\n", inode->inum);
		f->type = FD_DIR;
	} else {
		f->type = FD_INODE;
	}

	f->ip = inode;
	f->readable = !(flags & O_WRONLY);
	f->writable = (flags & O_WRONLY) || (flags & O_RDWR);
	f->off = 0;

#if MLFS_LEASE
	//if(f->writable && !O_CREAT) {
	//	acquire_parent_lease(f->ip, LEASE_WRITE, path);
	//}
#endif

#if 0
	strncpy(f->path, temp, MAX_PATH);
	f->path[sizeof(f->path) - 1] = '\0';
	HASH_ADD_STR(g_fd_table.open_files_ht, path, f);
	mlfs_debug("Adding file with path: %s | TEST: %s\n", f->path, temp);
#endif

	/* TODO: set inode permission based the mode 
	if (mode & S_IRUSR)
		// Set read permission
	if (mode & S_IWUSR)
		// Set write permission
	*/

	pthread_rwlock_unlock(&f->rwlock);

	return SET_MLFS_FD(fd);
}

int mlfs_posix_access(char *pathname, int mode)
{
	struct inode *inode;

	mlfs_posix("[POSIX] access(%s)\n", pathname);

	if (mode != F_OK)
		panic("does not support other than F_OK\n");

	inode = namei(pathname);

	if (!inode) {
		return -ENOENT;
	}

	iput(inode);

	return 0;
}

int mlfs_posix_creat(char *path, uint16_t mode)
{
	return mlfs_posix_open(path, O_CREAT|O_RDWR, mode);
}

int mlfs_posix_read(int fd, uint8_t *buf, int count)
{
	int ret = 0;
	struct file *f;

	mlfs_posix("[POSIX] read(fd=%d, size=%d)\n", fd, count);

	f = &g_fd_table.open_files[fd];

	pthread_rwlock_rdlock(&f->rwlock);

	mlfs_assert(f);

	if (f->ref == 0) {
		panic("file descriptor is wrong\n");
		return -EBADF;
	}

	struct mlfs_reply *reply = mlfs_zalloc(sizeof(struct mlfs_reply));
	reply->dst = buf;
	ret = mlfs_file_read(f, reply, count);

	pthread_rwlock_unlock(&f->rwlock);

	return ret;
}

int mlfs_posix_pread64(int fd, uint8_t *buf, int count, loff_t off)
{
	int ret = 0;
	struct file *f;

	mlfs_posix("[POSIX] pread64(fd=%d, size=%d, off=%lu)\n", fd, count, off);

	f = &g_fd_table.open_files[fd];

	pthread_rwlock_rdlock(&f->rwlock);

	mlfs_assert(f);

	if (f->ref == 0) {
		panic("file descriptor is wrong\n");
		return -EBADF;
	}

	struct mlfs_reply *reply = mlfs_zalloc(sizeof(struct mlfs_reply));
	reply->dst = buf;
	ret = mlfs_file_read_offset(f, reply, count, off);

	pthread_rwlock_unlock(&f->rwlock);

	return ret;
}

int mlfs_posix_write(int fd, uint8_t *buf, size_t count)
{
	int ret;
	struct file *f;

	uint64_t start_tsc_tmp;

	mlfs_posix("[POSIX] write(fd=%d, size=%lu)\n", fd, count);

#ifdef DISTRIBUTED
	//NOTE: we currently impose a 16 MB iosize limit (due to size of 'imm' for RDMA RPCs)
	assert(count <= 16384 * 1024);
#endif

	f = &g_fd_table.open_files[fd];

	pthread_rwlock_wrlock(&f->rwlock);

	mlfs_assert(f);

	if (f->ref == 0) {
		panic("file descriptor is wrong\n");
		return -EBADF;
	}

	//if (enable_perf_stats)
	//	start_tsc_tmp = asm_rdtscp();

	ret = mlfs_file_write(f, buf, count, f->off);

	// change offset here since mlfs_file_write doesn't touch f->off
	if (ret > 0) {
		f->off += ret;
	}

	pthread_rwlock_unlock(&f->rwlock);

	//if (enable_perf_stats)
	//	g_perf_stats.tmp_tsc += (asm_rdtscp() - start_tsc_tmp);

	return ret;
}

int mlfs_posix_pwrite64(int fd, uint8_t *buf, size_t count, loff_t off)
{
	int ret;
	struct file *f;

	uint64_t start_tsc_tmp;

	if (enable_perf_stats)
		start_tsc_tmp = asm_rdtscp();

	mlfs_posix("[POSIX] write(fd=%d, size=%lu)\n", fd, count);

#ifdef DISTRIBUTED
	//NOTE: we currently impose a 16 MB iosize limit (due to size of 'imm' for RDMA RPCs)
	assert(count <= 16384 * 1024);
#endif

	f = &g_fd_table.open_files[fd];

	pthread_rwlock_wrlock(&f->rwlock);

	mlfs_assert(f);

	if (f->ref == 0) {
		panic("file descriptor is wrong\n");
                errno = EBADF;
                return -1;
	}

	ret = mlfs_file_write(f, buf, count, off);

	// change offset here since mlfs_file_write doesn't touch f->off
	if (ret > 0) {
		f->off += ret;
	}

	pthread_rwlock_unlock(&f->rwlock);

	if (enable_perf_stats)
		g_perf_stats.tmp_tsc += (asm_rdtscp() - start_tsc_tmp);

	return ret;
}

int mlfs_posix_lseek(int fd, int64_t offset, int origin)
{
	struct file *f;
	int ret = 0;

	mlfs_posix("[POSIX] lseek(fd=%d, offset=%ld)\n", fd, offset);

	f = &g_fd_table.open_files[fd];

	if (f->ref == 0) {
		return -EBADF;
	}

	mlfs_assert(f);

	//lock file

	switch(origin) {
		case SEEK_SET:
			f->off = offset;
			break;
		case SEEK_CUR:
			f->off += offset;
			break;
		case SEEK_END:
			f->ip->size += offset;
			f->off = f->ip->size;
			break;
		default:
			ret = -EINVAL;
			break;
	}

	//unlock file
	return f->off;
}

int mlfs_posix_close(int fd)
{
	struct file *f;

	mlfs_posix("[POSIX] close(fd=%d)\n", fd);

	f = &g_fd_table.open_files[fd];

	if (!f) {
		return -EBADF;
	}

	mlfs_debug("close file inum %u fd %d\n", f->ip->inum, f->fd);

	return mlfs_file_close(f);
}

int mlfs_posix_mkdir(char *path, mode_t mode)
{
	struct inode *inode;

	mlfs_posix("[POSIX] mkdir(%s)\n", path);

	// return inode with holding ilock.
	inode = mlfs_object_create(path, T_DIR);

	if (!inode) {
		//abort_log_tx();
		return -ENOENT;
	}

	return 0;
}

int mlfs_posix_rmdir(char *path)
{
	return mlfs_posix_unlink(path);
}

int mlfs_posix_stat(const char *filename, struct stat *stat_buf)
{
	struct inode *inode;

	mlfs_posix("[POSIX] stat(%s)\n", filename);

	inode = namei((char *)filename);

	if (!inode) {
		return -ENOENT;
	}

	stati(inode, stat_buf);

	return 0;
}

int mlfs_posix_fstat(int fd, struct stat *stat_buf)
{
	struct file *f;

	mlfs_posix("[POSIX] fstat(%d)\n", fd);

	f = &g_fd_table.open_files[fd];

	if (f->ref == 0) 
		return -ENOENT;

	mlfs_assert(f->ip);

	stati(f->ip, stat_buf);

	return 0;
}

int mlfs_posix_fallocate(int fd, offset_t offset, offset_t len)
{
	struct file *f;
	uint32_t alloc_length;
	int ret = 0;

	mlfs_posix("[POSIX] fallocate(fd=%d, offset=%lu, len=%lu)\n", fd, offset, len);

	f = &g_fd_table.open_files[fd];

	if (f->ref == 0)
		return -EBADF;

	if (offset != 0) {
		mlfs_posix("[POSIX] fallocate: %s\n", "nonzero offset unsupported");
		return -EINVAL;
	}

	if (len < f->ip->size) {
		mlfs_posix("[POSIX] fallocate: length %lu < current inode size %lu\n", len, f->ip->size);
		return -EINVAL;
	}

	start_log_tx();

	add_to_loghdr(L_TYPE_ALLOC, f->ip, len, sizeof(offset_t), NULL, 0);

	commit_log_tx();
	
	return 0;
}

void *mlfs_posix_mmap(int fd)
{
	struct file *f;
	int ret;
	uint64_t blk_count;
	uint64_t blk_found;
	uint64_t blk_base;
	bmap_req_t bmap_req;

	mlfs_posix("[POSIX] mmap(fd=%d)", fd);

	f = &g_fd_table.open_files[fd];
	if (f->ref == 0) {
		mlfs_debug("mmap: bad fd %d\n", fd);
		return NULL;
	}
	blk_count = f->ip->size >> g_block_size_shift;
	mlfs_debug("blk_count = %lu, size = %lu\n", blk_count, f->ip->size);
	blk_found = 0;

	bmap_req.start_offset = 0;
	bmap_req.blk_count = blk_count;
	bmap_req.blk_count_found = 0;
	ret = bmap(f->ip, &bmap_req);
	if (ret == -EIO || bmap_req.dev != g_root_dev) {
		mlfs_posix("mmap: bad extent, error %d\n", ret);
		return NULL;
	}
	mlfs_debug("first extent: block %lu, length %u\n", bmap_req.block_no, bmap_req.blk_count_found);
	blk_base = bmap_req.block_no;
	blk_found = bmap_req.blk_count_found;
	while (blk_found < blk_count) {
		bmap_req.blk_count_found = 0;
		bmap_req.blk_count = blk_count - blk_found;
		bmap_req.start_offset = blk_found << g_block_size_shift;
		ret = bmap(f->ip, &bmap_req);
		if (ret == -EIO || bmap_req.dev != g_root_dev) {
                	mlfs_debug("mmap: bad extent, error %d\n", ret);
                	return NULL;
        	}
		mlfs_debug("next extent: block %lu, length %u\n", bmap_req.block_no, bmap_req.blk_count_found);
		if (blk_base + blk_found != bmap_req.block_no) {
			mlfs_debug("mmap: non-contiguous extent at block %lu, file block %lu\n", bmap_req.block_no, blk_found);
			return NULL;
		}
		blk_found += bmap_req.blk_count_found;
	}

	return (void *) ((blk_base << g_block_size_shift) + g_bdev[g_root_dev]->map_base_addr);
}

int mlfs_posix_unlink(const char *filename)
{
	int ret = 0;
	char name[DIRSIZ];
	struct inode *inode;
	struct inode *dir_inode;
	struct mlfs_dirent *log_entry;
	offset_t off;

	mlfs_posix("[POSIX] unlink(name=%s)\n", filename);

	/* TODO: handle struct file deletion
	 * e.g., unlink without calling close */
	start_log_tx();

	dir_inode = nameiparent((char *)filename, name);
	if (!dir_inode) {
		mlfs_debug("unlink: didn't find parent dir for file %s\n", filename);
		abort_log_tx();
		return -ENOENT;
	}

#if MLFS_LEASE
	//char parent_path[DIRSIZ];
	//get_parent_path((char *)filename, parent_path, name);
	//acquire_lease(dir_inode, LEASE_WRITE, parent_path);
#endif
	
	log_entry = dir_remove_entry(dir_inode, name, &inode);
	if (!inode) {
		abort_log_tx();
		return -ENOENT;
	}

	dlookup_del(filename);
	iput(dir_inode);
	iput(inode);
	ret = idealloc(inode);

	add_to_loghdr(L_TYPE_UNLINK, inode, 0, sizeof(struct dinode), NULL, 0);  
	commit_log_tx();

	if (log_entry)
		mlfs_free(log_entry);

	return ret;
}

int mlfs_posix_truncate(const char *filename, offset_t length)
{
	struct inode *inode;

	mlfs_posix("[POSIX] truncate(name=%s, size=%ld)\n", filename, length);

	start_log_tx();

	inode = namei((char *)filename);

	if (!inode) {
		abort_log_tx();
		return -ENOENT;
	}

#if MLFS_LEASE
	//acquire_parent_lease(inode, LEASE_WRITE, (char *)filename);
#endif

	itrunc(inode, length);

	commit_log_tx();

	iput(inode);

	return 0;
}

int mlfs_posix_ftruncate(int fd, offset_t length)
{
	struct file *f;
	int ret = 0;

	mlfs_posix("[POSIX] ftruncate(fd=%d, size=%ld)\n", fd, length);

	f = &g_fd_table.open_files[fd];

	if (f->ref == 0) {
		return -EBADF;
	}

	start_log_tx();

	ilock(f->ip);
	itrunc(f->ip, length);
	iunlock(f->ip);

	commit_log_tx();

	return 0;
}

int mlfs_posix_rename(char *oldpath, char *newpath)
{
	struct inode *old_dir_inode;
	struct inode *new_dir_inode;
	struct inode *ip;
	char old_file_name[DIRSIZ];
	char new_file_name[DIRSIZ];
	struct mlfs_dirent *log_old = NULL;
	struct mlfs_dirent *log_new = NULL;
	struct mlfs_dirent *log_replaced = NULL;
	offset_t off;
	
	uint64_t start_tsc_tmp;

	if (enable_perf_stats)
		start_tsc_tmp = asm_rdtscp();

	mlfs_posix("[POSIX] rename(old=%s, new=%s)\n", oldpath, newpath);
	start_log_tx();
	old_dir_inode = nameiparent((char *)oldpath, old_file_name);
	new_dir_inode = nameiparent((char *)newpath, new_file_name);

	log_replaced = dir_remove_entry(new_dir_inode, new_file_name, &ip);
	if (ip) {
		dlookup_del(newpath);
		iput(ip);
		idealloc(ip);
		add_to_loghdr(L_TYPE_UNLINK, ip, 0, sizeof(struct dinode), NULL, 0);
	}

	if (new_dir_inode == old_dir_inode) {
		// rename within directory
		dlookup_del(oldpath);

		log_new = dir_change_entry(new_dir_inode, old_file_name, new_file_name);
		iput(old_dir_inode);
		iput(new_dir_inode);
			
		if (!log_new) {
			abort_log_tx();
			if (log_replaced)
				mlfs_free(log_replaced);
			return -ENOENT;
		}

		commit_log_tx();

		if (log_replaced)
			mlfs_free(log_replaced);
		mlfs_free(log_new);

	} else {

#if MLFS_LEASE
		//char old_parent_path[DIRSIZ];
		//get_parent_path(oldpath, old_parent_path, old_file_name);
		// FIXME: temporarily disabling this
		//acquire_lease(old_dir_inode->inum, LEASE_WRITE, old_parent_path);
		// FIXME: temporarily replace with this
		//mark_lease_revocable(old_dir_inode->inum);

		char new_parent_path[DIRSIZ];
		get_parent_path(newpath, new_parent_path, new_file_name);
		acquire_lease(new_dir_inode->inum, LEASE_WRITE, new_parent_path);
#endif
		// rename across directories
		dlookup_del(oldpath);

		log_old = dir_remove_entry(old_dir_inode, old_file_name, &ip);
		if (!ip) {
			iput(old_dir_inode);
			iput(new_dir_inode);
			
			abort_log_tx();
			if (log_replaced)
				mlfs_free(log_replaced);

			return -ENOENT;
		}

		log_new = dir_add_entry(new_dir_inode, new_file_name, ip);
		dlookup_alloc_add(ip, newpath);
		iput(ip);
		iput(old_dir_inode);
		iput(new_dir_inode);

		commit_log_tx();
#if MLFS_LEASE
		m_barrier();
		//mark_lease_revocable(old_dir_inode->inum);
		mark_lease_revocable(new_dir_inode->inum);
#endif

		if (log_replaced)
			mlfs_free(log_replaced);
		mlfs_free(log_old);
		mlfs_free(log_new);

	}

	if (enable_perf_stats)
		g_perf_stats.tmp_tsc += (asm_rdtscp() - start_tsc_tmp);

	return 0;
}

int mlfs_posix_fsync(int fd)
{
	mlfs_posix("[POSIX] fsync(fd=%d)\n", fd);
#ifdef DISTRIBUTED
	mlfs_do_rsync();
#endif
	return 0;
}

size_t mlfs_posix_getdents(int fd, struct linux_dirent *buf, 
		size_t nbytes, offset_t off)
{
	struct file *f;
	int bytes;

	f = &g_fd_table.open_files[fd];

	if (f->ref == 0) {
		return -EBADF;
	}

	if (f->type != FD_DIR) 
		return -EBADF;

	/* glibc compute bytes with struct linux_dirent
	 * but ip->size is is computed by struct dirent, 
	 * which is much small size than struct linux_dirent
	if (nbytes < f->ip->size) 
		return -EINVAL;
	*/

	for(;;)	{
		if (f->off >= f->ip->size)
			return 0;

		bytes = dir_get_entry(f->ip, buf, f->off);
		f->off += bytes;

		if(buf->d_ino)
			break;
	}

	return sizeof(struct linux_dirent);
}

size_t mlfs_posix_getdents64(int fd, struct linux_dirent64 *buf, 
		size_t nbytes, offset_t off)
{
	struct file *f;
	int bytes;

	f = &g_fd_table.open_files[fd];

	if (f->ref == 0) {
		return -EBADF;
	}

	if (f->type != FD_DIR) 
		return -EBADF;

	/* glibc compute bytes with struct linux_dirent
	 * but ip->size is is computed by struct dirent, 
	 * which is much small size than struct linux_dirent
	if (nbytes < f->ip->size) 
		return -EINVAL;
	*/

	for(;;)	{
		if (f->off >= f->ip->size)
			return 0;

		bytes = dir_get_entry64(f->ip, buf, f->off);
		f->off += bytes;

		if(buf->d_ino)
			break;
	}

	return sizeof(struct linux_dirent64);
}

int mlfs_posix_fcntl(int fd, int cmd, void *arg)
{
	struct file *f;
	int ret = 0;

	mlfs_posix("[POSIX] fcntl(fd=%d, cmd=%d)", fd, cmd);

	f = &g_fd_table.open_files[fd];

	if (f->ref == 0) {
		return -EBADF;
	}

	if (cmd != F_SETLK) {
		mlfs_debug("%s: cmd %d\n", __func__, cmd);
		//panic("Only support F_SETLK\n");
	}

	return 0;
}

#ifdef __cplusplus
}
#endif
