#include "storage/storage.h"

struct block_device *g_bdev[g_n_devices + 1];

// storage device paths
char *g_dev_path[] = {
	(char *)"unused",
	(char *)"/dev/dax0.0",		// dev-dax
	(char *)"/backup/mlfs_ssd",	// SSD
	(char *)"/backup/mlfs_hdd",	// HDD
	(char *)"/dev/dax1.0",		// dev-dax [optional]
};

#ifdef __cplusplus
struct storage_operations storage_dax = {
	dax_init,
	dax_read,
	dax_read_unaligned,
	dax_write,
	dax_write_unaligned,
	dax_write_opt,
	dax_write_opt_unaligned,
	dax_erase,
	dax_commit,
	NULL,
	NULL,
	dax_exit,
};

struct storage_operations storage_hdd = {
	hdd_init,
	hdd_read,
	NULL,
	hdd_write,
	NULL,
	NULL,
	NULL,
	NULL,
	hdd_commit,
	NULL,
	NULL,
	hdd_exit,
};

struct storage_operations storage_aio = {
	mlfs_aio_init,
	mlfs_aio_read,
	NULL,
	mlfs_aio_write,
	NULL,
	NULL,
	NULL,
	mlfs_aio_commit,
	mlfs_aio_wait_io,
	mlfs_aio_erase,
	mlfs_aio_readahead,
	mlfs_aio_exit,
};

#else
struct storage_operations storage_dax = {
	.init = dax_init,
	.read = dax_read,
	.read_unaligned = dax_read_unaligned,
	.write = dax_write,
	.write_unaligned = dax_write_unaligned,
	.write_opt = dax_write_opt,
	.write_opt_unaligned = dax_write_opt_unaligned,
	.commit = dax_commit,
	.wait_io = NULL,
	.erase = dax_erase,
	.readahead = NULL,
	.exit = dax_exit,
};

struct storage_operations storage_hdd = {
	.init = hdd_init,
	.read = hdd_read,
	.read_unaligned = NULL,
	.write = hdd_write,
	.write_unaligned = NULL,
	.write_opt = NULL,
	.write_opt_unaligned = NULL,
	.commit = hdd_commit,
	.wait_io = NULL,
	.erase = NULL,
	.readahead = NULL,
	.exit = hdd_exit,
};

struct storage_operations storage_aio = {
	.init = mlfs_aio_init,
	.read = mlfs_aio_read,
	.read_unaligned = NULL,
	.write = mlfs_aio_write,
	.write_unaligned = NULL,
	.write_opt = NULL,
	.write_opt_unaligned = NULL,
	.commit = mlfs_aio_commit,
	.wait_io = mlfs_aio_wait_io,
	.erase = mlfs_aio_erase,
	.readahead = mlfs_aio_readahead,
	.exit = mlfs_aio_exit,
};


#endif
