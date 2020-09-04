#ifndef _PEER_MLFS_H_
#define _PEER_MLFS_H_

#ifdef DISTRIBUTED

#include "replication.h"

// peer type
enum peer_type {
	UNDEF_PEER = 0, //undefined peer - bootstrapping
	LIBFS_PEER,
	KERNFS_PEER
};

// role played by peer (only applicable to KERNFS_PEER)
enum peer_role {
	HOT_REPLICA = 1,	// Hot replicas can perform read/mutation fs operations
	HOT_BACKUP,		// Hot backups are passive
	COLD_BACKUP,		// Cold backups differ in that they can (optionally) cache cold data
	LOCAL_NODE, 		// Namespace for local replica group
	EXTERNAL_NODE		// Namespace for external replica group
};

//type or purpose of connection
enum sock_type {
	SOCK_IO = 0,	//IO operations
	SOCK_BG,	//Background Events
	SOCK_LS,	//Lease protocol
	SOCK_TYPE_COUNT
};

//registered memory region identifiers
enum mr_type {
	MR_NVM_LOG,	//NVM log
	MR_NVM_SHARED,  //NVM shared area
	MR_DRAM_CACHE,  //DRAM read cache
	MR_DRAM_BUFFER  //DRAM misc. buffer (unused)
};

enum rpc_pending_type {
	RPC_PENDING_WC = 0,	//Pending Work Completion
	RPC_PENDING_RESPONSE	//Pending RPC Response
};

struct peer_id {
	int id;
	char ip[INET_ADDRSTRLEN];
	int pid;
	int type;
	//char port[NI_MAXSERV];
	int role;
	int sockfd[SOCK_TYPE_COUNT]; //socket descriptors
	int sockcount;
	struct log_superblock *log_sb;
	char namespace_id[DIRSIZ];
	uint32_t inum_prefix;
};

struct peer_socket {
	int fd;
	int type;
	int islocal;
	uint32_t seqn;
	struct peer_id *peer;
};

struct rpc_pending_io {
	int sockfd;
	int type;
	uint32_t seq_n;
	struct list_head l;
};



#ifndef KERNFS
#define peer_bitmap_size (g_n_nodes)
#define sock_bitmap_size peer_bitmap_size * SOCK_TYPE_COUNT
#else
#define peer_bitmap_size (g_n_max_libfs + g_n_nodes)
#define sock_bitmap_size peer_bitmap_size * SOCK_TYPE_COUNT
#endif

extern int g_peer_count;
extern int g_sock_count;

extern struct peer_id *g_kernfs_peers[g_n_nodes];
extern struct peer_id *g_peers[peer_bitmap_size];
extern struct peer_socket *g_rpc_socks[sock_bitmap_size];

void peer_init();
void lock_peer_access();
void unlock_peer_access();
void add_peer_socket(int sockfd);
void remove_peer_socket(int sockfd);
struct peer_id * clone_peer(struct peer_id *input);
int local_kernfs_id(int id);
int get_next_peer_id(int id);
struct peer_id * find_peer(int sockfd);
struct peer_id * _find_peer(char* ip, uint32_t pid);
void register_peer_log(struct peer_id *peer, int find_id);
void unregister_peer_log(struct peer_id *peer);
void set_peer_id(struct peer_id *peer);
void clear_peer_id(struct peer_id *peer);
void print_peer_arrays();

#endif

#endif
