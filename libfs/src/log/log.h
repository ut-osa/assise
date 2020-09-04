#ifndef _LOG_H_
#define _LOG_H_

#include "concurrency/synchronization.h"
#ifdef DISTRIBUTED
#include "distributed/replication.h"
#include "distributed/rpc_interface.h"
#endif
#include "filesystem/shared.h"
//#include "filesystem/fs.h"
#include "io/block_io.h"
#include "global/global.h"
#include "global/types.h"
#include "global/util.h"
#include "ds/list.h"
#include "ds/stdatomic.h"

//#include <rdma/rdma_cma.h>
#include <pthread.h>

// In-memory metadata for log area.
// Log format
// log_sb(sb_blknr)|..garbages..|log data(start_blknr ~ next_avail - 1)|unused area...
// can garbage collect from sb_blknr to start_blknr
struct fs_log {
	uint8_t id;
	volatile struct log_superblock *log_sb;
	uint8_t dev;
	int volatile ready;
	// size of log as # of block.
	addr_t size;
	// superblock number of log area (the first block).
	addr_t log_sb_blk;
	addr_t start_blk;	
	// next available log header blockno 
	addr_t next_avail_header;
	// how many transactions are executing.
	uint8_t outstanding;

	// digesting, please wait.
	uint8_t digesting;

	uint32_t start_version;
	uint32_t avail_version;
	uint32_t n_digest_req;

	// pipe fd to make digest request.
	int digest_fd[2];

	// pipe fd for kernfs communication.
	int kernfs_fd;

	// libfs address
	struct sockaddr_un libfs_addr;

	// kernfs address
	struct sockaddr_un kernfs_addr;

	// # of logheaders in the lh_list.
	uint32_t nloghdr;

	// used for threads
	pthread_spinlock_t log_lock;
	// used for parent and child processes.
	pthread_mutex_t *shared_log_lock;
};

//forward declaration
struct inode;
extern volatile struct fs_log *g_fs_log;
extern volatile struct log_superblock *g_log_sb;

void init_log();
void add_to_loghdr(uint8_t type, struct inode *inode, offset_t data, 
		uint32_t length, void *extra, uint16_t extra_len);
void start_log_tx(void);
void abort_log_tx(void);
void commit_log_tx(void);
unsigned int make_digest_request_sync(int percent);
int make_digest_request_async(int percent);
void handle_digest_response(char *ack_cmd);
void wait_on_digesting();

#ifdef DISTRIBUTED
void signal_callback(struct app_context *msg);
int mlfs_do_rdigest();
#endif

static inline void set_digesting()
{
	while (1) {
		//if (!xchg_8(&g_fs_log->digesting, 1)) 
		if (!cmpxchg(&g_fs_log->digesting, 0, 1)) {
			mlfs_printf("set log digesting state%s", "\n");
			return;
		}

		while (g_fs_log->digesting) 
			cpu_relax();
	}
}

static inline void clear_digesting()
{
	while (1) {
		//if (!xchg_8(&g_fs_log->digesting, 1)) 
		if (cmpxchg(&g_fs_log->digesting, 1, 0)) {
			mlfs_printf("clear log digesting state%s", "\n");
			return;
		}

		while (g_fs_log->digesting) 
			cpu_relax();
	}
}

addr_t log_alloc(uint32_t nr_logblock);
void shutdown_log();

#endif
