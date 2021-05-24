#ifdef DISTRIBUTED

#include "io/device.h"
#include "io/block_io.h"
#include "distributed/peer.h"
//#include "log/log.h"

#ifdef KERNFS
#include "fs.h"
#else
#include "filesystem/fs.h"
#endif

#ifdef KERNFS
struct peer_socket *g_rpc_socks[sock_bitmap_size];
#else
struct peer_socket *g_rpc_socks[sock_bitmap_size];
#endif

struct peer_id *g_kernfs_peers[g_n_nodes];
struct peer_id *g_peers[peer_bitmap_size];
int g_peer_count = 0;
int g_sock_count = 0;

DECLARE_BITMAP(peer_bitmap, peer_bitmap_size);
DECLARE_BITMAP(sock_bitmap, sock_bitmap_size);
pthread_mutex_t peer_bitmap_mutex = PTHREAD_MUTEX_INITIALIZER;

void peer_init()
{
	bitmap_zero(peer_bitmap, peer_bitmap_size);
	bitmap_zero(sock_bitmap, sock_bitmap_size);

	//pthread_mutexattr_t attr;
	//peer_bitmap_mutex = (pthread_mutex_t *)mlfs_zalloc(sizeof(pthread_mutex_t));
	//pthread_mutexattr_init(&attr);
	//pthread_mutex_init(peer_bitmap_mutex, NULL);
}

void lock_peer_access()
{
	pthread_mutex_lock(&peer_bitmap_mutex);
}

void unlock_peer_access()
{
	pthread_mutex_unlock(&peer_bitmap_mutex);
}

int local_kernfs_id(int id)
{
	char* ip = (char*)g_peers[id]->ip;

	return _find_peer(ip, 0)->id;
}

int get_next_peer_id(int id) {
	return find_next_bit(peer_bitmap, g_n_max_libfs, 0);
}

void add_peer_socket(int sockfd)
{
	mlfs_debug("found socket %d\n", sockfd);
	struct peer_socket *psock = mlfs_zalloc(sizeof(struct peer_socket));

	lock_peer_access();

	struct peer_id *peer = find_peer(sockfd);
	
	bitmap_set(sock_bitmap, sockfd, 1);
	//if peer is not found, then this is a LibFS process
	if(!peer) {
#ifdef LIBFS
		// only KernFS processes accept connections from LibFSes
		mlfs_assert(false);
#endif
		peer = mlfs_zalloc(sizeof(struct peer_id));
		strncpy(peer->ip, mp_channel_ip(sockfd), sizeof(char)*INET_ADDRSTRLEN);
		peer->pid = mp_channel_rpid(sockfd);
		mlfs_printf("Peer connected (ip: %s, pid: %u)\n", peer->ip, peer->pid);
	}

	int type = mp_channel_meta(sockfd);
	mlfs_assert(type < SOCK_TYPE_COUNT);

	peer->sockfd[type] = sockfd;
	peer->sockcount++;
	psock->fd = peer->sockfd[type];
	psock->seqn = 1;
	psock->type = type;
	psock->peer = peer;
	g_rpc_socks[peer->sockfd[type]] = psock;
	g_sock_count++;

	unlock_peer_access();

	mlfs_printf("Established connection with %s on sock:%d of type:%d and peer:%p\n",
			peer->ip, psock->fd, psock->type, g_rpc_socks[psock->fd]->peer);
	//print_peer_id(peer);
}

void remove_peer_socket(int sockfd)
{
	//struct peer_id *peer = find_peer(sockfd);
	struct peer_id *peer = g_rpc_socks[sockfd]->peer;
	mlfs_assert(peer);

	struct peer_socket *psock = g_rpc_socks[sockfd];
       	mlfs_assert(psock);	

#if MLFS_LEASE && defined(KERNFS)
	// if peer disconnects, cancel all related leases
	if(psock->type == SOCK_LS)
		discard_leases(peer->id);
#endif

	lock_peer_access();

	peer->sockfd[psock->type] = -1;
	peer->sockcount--;
	g_rpc_socks[sockfd] = NULL;
	g_sock_count--;
	bitmap_clear(sock_bitmap, sockfd, 1);

	mlfs_info("Disconnected with %s on sock:%d of type:%d\n",
			peer->ip, psock->fd, psock->type);
	free(psock);

	if(!peer->sockcount) {
		mlfs_printf("Peer-%d disconnected (ip:%s pid:%u)\n", peer->id, peer->ip, peer->pid);
		unregister_peer_log(peer);
		free(peer);
	}

	unlock_peer_access();
}



struct peer_id * clone_peer(struct peer_id *input)
{
	mlfs_assert(input != 0);
	struct peer_id *cloned = (struct peer_id *)
		mlfs_zalloc(sizeof(struct peer_id));

	cloned->id = input->id;
	strncpy(cloned->ip, input->ip, sizeof(char)*INET_ADDRSTRLEN);
	cloned->pid = input->pid;
	cloned->type = input->type;
	cloned->role = input->role;
	
	for(int i=0; i<SOCK_TYPE_COUNT; i++)
		cloned->sockfd[i] = input->sockfd[i];

	cloned->sockcount = input->sockcount;
	cloned->log_sb = input->log_sb;

	return cloned;
}
struct peer_id * find_peer(int sockfd)
{
	char* ip = mp_channel_ip(sockfd);

#ifdef LIBFS
	//KernFS processes have 0 pids
	uint32_t pid = 0;
#else
	uint32_t pid = mp_channel_rpid(sockfd);
#endif

	return _find_peer(ip, pid);
}

struct peer_id * _find_peer(char* ip, uint32_t pid)
{
	mlfs_debug("trying to find peer with ip %s and pid %u (peer count: %d | sock count: %d)\n",
			ip, pid, g_peer_count, g_sock_count);

	//lock_peer_access();

	// check sockets for yet-to-be-registered peers
	int idx = 0;
	for(int i=0; i<g_sock_count; i++) {
		idx = find_next_bit(sock_bitmap, sock_bitmap_size, idx);
		//mlfs_assert(g_rpc_socks[idx]);
		//mlfs_assert(g_rpc_socks[idx]->peer);
		if(g_rpc_socks[idx] && g_rpc_socks[idx]->peer == 0) {
			//idx++;
			continue;
		}
		mlfs_debug("sockfd[%d]: ip %s pid %u\n", i, g_rpc_socks[idx]->peer->ip, g_rpc_socks[idx]->peer->pid);
		if(!strcmp(g_rpc_socks[idx]->peer->ip, ip) && g_rpc_socks[idx]->peer->pid == pid) {
			//unlock_peer_access();
			return g_rpc_socks[idx]->peer;
		}
		idx++;
	}

	idx = 0;
	// next, check the g_peers array
	for(int i=0; i<g_peer_count; i++) {
		idx = find_next_bit(peer_bitmap, peer_bitmap_size, idx);
		//mlfs_assert(g_peers[idx]);
		//if(g_peers[i] == 0)
		//	continue;
		mlfs_debug("peer[%d]: ip %s pid %u\n", idx, g_peers[idx]->ip, g_peers[idx]->pid);
		if(!strcmp((char*)g_peers[idx]->ip, ip) && g_peers[idx]->pid == pid) {
			//unlock_peer_access();
			return (struct peer_id *)g_peers[idx];
		}
		idx++;
	}
	//unlock_peer_access();

	mlfs_debug("%s", "peer not found\n");
	return NULL;
}

void print_bits(unsigned int x)
{
    int i;
    for(i=8*sizeof(x)-1; i>=0; i--) {
        (x & (1 << i)) ? putchar('1') : putchar('0');
    }
    printf("\n");
}

void register_peer_log(struct peer_id *peer, int find_id)
{
	lock_peer_access();

	uint32_t idx;
	if(find_id) {
		idx = find_first_zero_bit(peer_bitmap, peer_bitmap_size);
		mlfs_debug("peer id: %u bitmap: %lu\n", idx, peer_bitmap[0]);
		//print_bits(peer_bitmap[0]);
	}
	else {
		idx = peer->id;
	}

	if(idx >= g_n_max_libfs + g_n_nodes)
		panic("Unable to register LibFS log. Try increasing g_n_max_libfs\n");

	mlfs_info("assigning peer (ip: %s pid: %u) to log id %d\n", peer->ip, peer->pid, idx);
	peer->id = idx;
        set_peer_id(peer);
	g_peers[peer->id] = peer;
	g_peer_count++;

	struct log_superblock *log_sb = (struct log_superblock *)
		mlfs_zalloc(sizeof(struct log_superblock));

	//read_log_superblock(peer->id, (struct log_superblock *)log_sb);

	addr_t start_blk = disk_sb[g_log_dev].log_start + idx * g_log_size + 1;
	log_sb->start_digest = start_blk;
	log_sb->start_persist = start_blk;

	//write_log_superblock(peer->id, (struct log_superblock *)log_sb);

	atomic_init(&log_sb->n_digest, 0);
	atomic_init(&log_sb->end, 0);

	peer->log_sb = log_sb;

	//NOTE: for now, don't register logs for kernfs peers
	if(peer->type != KERNFS_PEER) { 
		struct peer_id *next_peer = g_kernfs_peers[(g_self_id + 1) % g_n_nodes];
		uintptr_t base_addr =  0;
		if(g_n_nodes > 1)
			base_addr = mr_local_addr(next_peer->sockfd[SOCK_IO], MR_NVM_LOG);
		init_replication(idx, next_peer, start_blk, start_blk + g_log_size, base_addr, &log_sb->end);
	}
	unlock_peer_access();
}

void unregister_peer_log(struct peer_id *peer)
{
	mlfs_assert(peer->id >= 0);

	mlfs_printf("unregistering peer (ip: %s pid: %u) with log id %d\n", peer->ip, peer->pid, peer->id);
	//lock_peer_access();
	clear_peer_id(peer);
	g_peer_count--;
	g_peers[peer->id] = NULL;
	free(g_sync_ctx[peer->id]);
	g_sync_ctx[peer->id] = NULL;
	free(peer->log_sb);
	//unlock_peer_access();
}

void set_peer_id(struct peer_id *peer)
{
	bitmap_set(peer_bitmap, peer->id, 1);
}

void clear_peer_id(struct peer_id *peer)
{
	bitmap_clear(peer_bitmap, peer->id, 1);
}

// ---- USED FOR DEBUGGING ----
void print_peer_arrays()
{
	for(int i=0; i<g_peer_count; i++)
	{
		mlfs_printf("peer[%d]: pointer %p ip %s pid %u\n", i, g_peers[i], g_peers[i]->ip, g_peers[i]->pid);
	}
}

#endif
