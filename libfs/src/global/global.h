#ifndef __global_h__
#define __global_h__

#include "types.h"
#include "defs.h"

#ifdef __cplusplus
extern "C" {
#endif

extern uint8_t g_ssd_dev;
extern uint8_t g_log_dev;
extern uint8_t g_hdd_dev;

void mlfs_setup(void);

// Global parameters
#define g_block_size_bytes	4096UL
#define g_block_size_mask   (g_block_size_bytes - 1)
#define g_block_size_shift	12UL
// 2 works for microbenchmark
// 6 is generally too big. but Redis AOF mode requires at least 6.
#define g_max_blocks_per_operation 4
#define g_hdd_block_size_bytes 4194304UL
#define g_hdd_block_size_shift 22UL

// # of blocks allocated to private log (for each LibFS process)
//#define g_log_size 32768UL // (128 MB)
#define g_log_size 262144UL // (1 GB)
//#define g_log_size 524288UL // (2 GB)
//#define g_log_size 1310720UL // (5 GB)
//#define g_log_size 131072UL

#define g_directory_shift  16UL
#define g_directory_mask ((1 << ((sizeof(inum_t) * 8) - g_directory_shift)) - 1)
#define g_segsize_bytes    (1ULL << 30)  // 1 GB
#define g_max_read_cache_blocks  (1 << 18) // 2 GB

#define g_namespace_bits 6
#define g_namespace_mask (((1 << g_namespace_bits) - 1) << (32 - g_namespace_bits))

#define g_fd_start  1000000

/* note for this posix handlers' (syscall handler) return value and errno.
 * glibc checks syscall return value in INLINE_SYSCALL macro.
 * if the return value is negative, it sets the value as errno
 * (after turnning to positive one) and returns -1 to application.
 * Therefore, the return value must be correct -errno in posix semantic
 */

#define SET_MLFS_FD(fd) fd + g_fd_start
#define GET_MLFS_FD(fd) fd - g_fd_start

#define ROOTINO 1  // root i-number

#define NDIRECT 7
#define NINDIRECT (g_block_size_bytes / sizeof(addr_t))

#define N_FILE_PER_DIR (g_block_size_bytes / sizeof(struct mlfs_dirent))

#define NINODES 300000
#define g_max_open_files 1000000

// maximum size of full path
//#define MAX_PATH 4096
#define MAX_PATH 1024

//FIXME: Use inums instead of paths for remote reads
// Temporarily using a small path for read RPC
#define MAX_REMOTE_PATH 40

// Directory is a file containing a sequence of dirent structures.
#define DIRSIZ 28
//#define DIRSIZ 60

#define SHM_START_ADDR (void *)0x7ff000000000UL

#if MLFS_LEASE
#define SHM_SIZE (NINODES * sizeof(mlfs_lease_t))
#else
#define SHM_SIZE (200 << 20) //200 MB
#endif

#define SHM_NAME "/mlfs_shm"

#define MLFS_SEED defined(DISTRIBUTED) && defined(SEED)
#define MLFS_PASSIVE defined(DISTRIBUTED) && !defined(ACTIVE)
#define MLFS_MASTER defined(DISTRIBUTED) && defined(MASTER)
#define MLFS_LEASE defined(DISTRIBUTED) && (defined(MASTER) || defined(KERNFS)) && defined(USE_LEASE)
#define MLFS_REPLICA defined(DISTRIBUTED) && !defined(MASTER)
#define MLFS_HOT defined(DISTRIBUTED) && defined(HOT) && !defined(MASTER)
#define MLFS_COLD defined(DISTRIBUTED) && !defined(HOT) && !defined(MASTER)
#define MLFS_NAMESPACES defined(NAMESPACES) && defined(DISTRIBUTED)

// # of replicas in cluster
// Note: Only hot replicas are implemented (other replica types are not used)
#define g_n_hot_rep 1	// Hot replica
#define g_n_hot_bkp 0	// Hot backup (reserve)
#define g_n_cold_bkp 0	// Cold backup (reserve)
#define g_n_ext_rep 0	// External Replicas (not colocated)
#define g_n_nodes (g_n_hot_rep + g_n_hot_bkp + g_n_cold_bkp + g_n_ext_rep)

// # of LibFS processes (max)
#define g_n_max_libfs 30

/**
 *
 * All global variables here are default set by global.c
 * but might be changed the .ini reader.
 * Please keep the "g_" prefix so we know what is global and
 * what isn't.
 *
 */
// extern uint	g_n_devices; /* old NDEV */
// extern uint	g_root_dev; /* old ROOTDEV */
// extern uint	g_max_open_files; /*old NFILE*/
// extern uint	g_block_size_bytes; /* block size in bytes, old BSIZE*/
// extern uint	g_max_blocks_per_operation; /* old MAXOPBLOCKS*/
// extern uint	g_log_size_blks; /* log size in blocks, old LOGSIZE*/
// extern uint	g_fs_size_blks; /* filesystem size in blocks, old FSSIZE*/
// extern uint	g_max_extent_size_blks; /* filesystem size in blocks, old MAX_EXTENT_SIZE*/
// extern uint	g_block_reservation_size_blks; /* reservation size in blocks, old BRESRV_SIZE*/
// extern uint	g_fd_start; /* offset start of fds used by mlfs, old FD_START*/

#ifdef __cplusplus
}
#endif

#endif
