#include "rpc_interface.h"
#include "distributed/peer.h"

#ifdef KERNFS
#include "fs.h"
#else
#include "filesystem/fs.h"
#include "mlfs/mlfs_interface.h"
#endif

#ifdef DISTRIBUTED

#if 0
int g_sockfd_io; // for io & rsync operations
int g_sockfd_bg; // for background operations
int g_sockfd_ls; // for communicating with other master
#endif

char g_self_ip[INET_ADDRSTRLEN];
int g_self_id = -1;
int g_kernfs_id = -1;
int rpc_shutdown = 0;

//struct timeval start_time, end_time;

//uint32_t msg_seq[g_n_nodes*sock_count] = {0};

int init_rpc(struct mr_context *regions, int n_regions, char *listen_port, signal_cb_fn signal_callback)
{
	assert(RPC_MSG_BYTES > MAX_REMOTE_PATH); //ensure that we can signal remote read requests (including file path)

	int chan_type = -1;

	peer_init();

	printf("%s\n", "fetching node's IP address..");

	printf("Process pid is %u\n", getpid());

	if(g_n_nodes > 1) {
		//Use RDMA IP when running Assise in distributed mode
		fetch_intf_ip(rdma_intf, g_self_ip);
		chan_type = CH_TYPE_REMOTE;
		printf("ip address on interface \'%s\' is %s\n", rdma_intf, g_self_ip);
	}
	else {
		//otherwise, default to localhost
		fetch_intf_ip(local_intf, g_self_ip);
		chan_type = CH_TYPE_LOCAL;
		printf("ip address on interface \'%s\' is %s\n", local_intf, g_self_ip);
	}


	printf("cluster settings:\n");
	//create array containing all peers
	for(int i=0; i<g_n_nodes; i++) {
		if(i<g_n_hot_rep) {
			g_peers[i] = clone_peer(&hot_replicas[i]);
			g_kernfs_peers[i] = g_peers[i];
		}
		else if(i>=g_n_hot_rep && i<(g_n_hot_rep + g_n_hot_bkp)) {
			g_peers[i] = clone_peer(&hot_backups[i - g_n_hot_rep]);
			g_kernfs_peers[i] = g_peers[i];
		}
		else if(i>=g_n_hot_rep + g_n_hot_bkp && i<(g_n_hot_rep + g_n_hot_bkp + g_n_cold_bkp)) {
			g_peers[i] = clone_peer(&cold_backups[i - g_n_hot_rep - g_n_hot_bkp]);
			g_kernfs_peers[i] = g_peers[i];
		} else {
#if MLFS_NAMESPACES
			g_peers[i] = clone_peer(&external_replicas[i - g_n_hot_rep - g_n_hot_bkp - g_n_cold_bkp]);
			g_kernfs_peers[i] = g_peers[i];
			g_kernfs_peers[i]->id = i;
			mlfs_printf("*** %s\n", "todo: register remote log");
			printf("--- EXTERNAL node %d - ip:%s\n", i, g_peers[i]->ip);
			continue;
#else
			panic("remote namespaces defined in configuration, but disabled in Makefile!\n");
#endif
		}

		g_kernfs_peers[i]->id = i;
		//g_peers[i] = g_kernfs_peers[i];
		register_peer_log(g_kernfs_peers[i], 0);

		if(!strcmp((char*)g_kernfs_peers[i]->ip, g_self_ip)) {
			g_kernfs_id = g_kernfs_peers[i]->id;
		}
			
		printf("--- node %d - ip:%s\n", i, g_peers[i]->ip);
	}

	//NOTE: disable this check if we want to allow external clients (i.e. no local shared area)
	if(g_kernfs_id == -1)
		panic("local KernFS IP is not defined in src/distributed/rpc_interface.h\n");

#ifdef KERNFS
	g_self_id = g_kernfs_id;
	init_rdma_agent(listen_port, regions, n_regions, RPC_MSG_BYTES, chan_type, add_peer_socket, remove_peer_socket,
			signal_callback);
#else
	init_rdma_agent(NULL, regions, n_regions, RPC_MSG_BYTES, chan_type, add_peer_socket, remove_peer_socket,
			signal_callback);
#endif

#ifdef KERNFS
	// create connections to KernFS instances with i > g_self_id
	int do_connect = 0;

	// KernFS uses a dedicated thread per connection to poll for inbound messages
	int always_poll = 1;

	// KernFS instances use a pid value of 0
	uint32_t pid = 0;
#else
	// connect to all KernFS instances
	int do_connect = 1;

	// LibFS is configured by default to use a dedicated polling thread for every connection
	// NOTE: In the absence of a polling thread, LibFS needs to manually poll inbound messages via RDMA_AWAIT_* primitives
	const char * poll_str = getenv("ALWAYSPOLL");
	int always_poll = 1;

	if(poll_str)
		always_poll = atoi(poll_str);

	uint32_t pid = getpid();
#endif

	int num_peers = 0;
	for(int i=0; i<g_n_nodes; i++) {
		if(!strcmp((char*)g_kernfs_peers[i]->ip, g_self_ip) && !do_connect) {
			do_connect = 1;
			continue;
		}

		num_peers++;

		if(do_connect) {
			printf("Connecting to KernFS instance %d [ip: %s]\n", i, g_kernfs_peers[i]->ip);

			add_connection((char*)g_kernfs_peers[i]->ip, listen_port, SOCK_IO, pid, chan_type, always_poll);
			add_connection((char*)g_kernfs_peers[i]->ip, listen_port, SOCK_BG, pid, chan_type, always_poll);
			add_connection((char*)g_kernfs_peers[i]->ip, listen_port, SOCK_LS, pid, chan_type, always_poll);

		}
	}

	mlfs_debug("%s", "awaiting remote KernFS connections\n");


	//gettimeofday(&start_time, NULL);
	
	while(g_sock_count < 3 * num_peers) {
		if(rpc_shutdown)
			return 1;
		//cpu_relax();
		sleep(1);
	}

	//gettimeofday(&end_time, NULL);
	//mlfs_printf("Bootstrap time: %lf\n", ((end_time.tv_sec  - start_time.tv_sec) * 1000000u + end_time.tv_usec - start_time.tv_usec) / 1.e6);
	
#ifdef LIBFS
	// contact the 1st KernFS (designated as a seed) to acquire a unique LibFS ID
	rpc_bootstrap(g_kernfs_peers[0]->sockfd[0]);

#endif
	
	//replication is currently only initiated by LibFS processes
#if 0
	for(int i=0; i<n_regions; i++) {
		if(regions[i].type == MR_NVM_LOG) {
			// pick the node with the next (circular) id as replication target
			struct peer_id *next_replica = g_kernfs_peers[(g_kernfs_id + 1) % g_n_nodes];

			printf("init chain replication: next replica [node %d ip - %s]\n",
					next_replica->id, next_replica->ip);
			init_replication(g_self_id, g_kernfs_peers[(g_kernfs_id + 1) % g_n_nodes],
					log_begin, g_log_size, regions[i].addr, log_end);
		}
	}
#endif

	//sleep(4);
	printf("%s\n", "MLFS cluster initialized");

		//gettimeofday(&start_time, NULL);
}

int shutdown_rpc()
{
	rpc_shutdown = 1;
	shutdown_rdma_agent();
	return 0;
}

int mlfs_process_id()
{
	return g_self_id;
}

int peer_sockfd(int node_id, int type)
{
	assert(node_id >= 0 && node_id < (g_n_max_libfs + g_n_nodes));
	return g_rpc_socks[g_kernfs_peers[node_id]->sockfd[type]]->fd;
}

#if 0
int rpc_connect(struct peer_id *peer, char *listen_port, int type, int poll_cq)
{
	time_t start = time(NULL);
	time_t current = start;
	time_t timeout = 5; // timeout interval in seconds

	int sockfd = add_connection(peer->ip, listen_port, type, poll_cq);

	//mlfs_info("Connecting to %s:%s on sock:%d\n", peer->ip, listen_port, peer->sockfd[type]);
	while(!rc_ready(sockfd)) {
		current = time(NULL);
		if(start + timeout < current)
			panic("failed to establish connection\n");
		//sleep(1);
	}
	//rpc_add_socket(peer, sockfd, type);
	return 0;
}

int rpc_listen(int sockfd, int count)
{
	int socktype = -1;
	struct peer_id *peer = NULL;

	/*
	time_t start = time(NULL);
	time_t current = start;
	time_t timeout = 10; //timeout interval in seconds
	*/

	mlfs_debug("%s", "listening for peer connections\n");

	for(int i=1; i<=count; i++) {
		while(rc_connection_count() < i || !rc_ready(rc_next_connection(sockfd))) {
			/*
			current = time(NULL);
			if(start + timeout < current)
				panic("failed to establish connection\n");
			*/

			if(rpc_shutdown)
				return 1;

			sleep(0.5);
		}

		sockfd = rc_next_connection(sockfd);
		socktype = rc_connection_type(sockfd);
		peer = find_peer(sockfd);

		if(!peer)
			panic("Unidentified peer tried to establish connection\n");

		rpc_add_socket(peer, sockfd, socktype);
	}

	mlfs_debug("%s", "found all peers\n");

	return 0;
}

int rpc_add_socket(struct peer_id *peer, int sockfd, int type)
{
	struct peer_socket *psock = mlfs_zalloc(sizeof(struct peer_socket));
	peer->sockfd[type] = sockfd;
	psock->fd = peer->sockfd[type];
	psock->seqn = 1;
	psock->type = type;
	psock->peer = peer;
	g_rpc_socks[peer->sockfd[type]] = psock;
	mlfs_info("Established connection with %s on sock:%d of type:%d\n",
			peer->ip, psock->fd, psock->type);
	//print_peer_id(peer);
}

#endif

struct rpc_pending_io * rpc_remote_read_async(char *path, loff_t offset, uint32_t io_size, uint8_t *dst, int rpc_wait)
{
	assert(g_n_cold_bkp > 0); //remote reads are only sent to reserve replicas

	//FIXME: search for cold_backup in g_peers array
	int sockfd = cold_backups[0].sockfd[SOCK_IO];
	//note: for signaling on the fast path, we write directly to the rdma buffer to avoid extra copying
	struct app_context *msg;
	int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);

#ifdef LIBFS
	if (enable_perf_stats)
		g_perf_stats.read_rpc_nr++;
#endif

	//setting msg->id allows us to insert a hook in the rdma driver to later wait until response received
	if(rpc_wait)
		msg->id = generate_rpc_seqn(g_rpc_socks[sockfd]); 

	// FIXME: Use inum instead of path for read RPCs
	// temporarily trimming paths to reduce overheads
	char trimmed_path[MAX_REMOTE_PATH];
	snprintf(trimmed_path, MAX_REMOTE_PATH, "%s", path);

	snprintf(msg->data, RPC_MSG_BYTES, "|read |%s |%lu|%u|%lu", trimmed_path, offset, io_size, (uintptr_t)dst);
	mlfs_info("trigger async remote read: path[%s] offset[%lu] io_size[%d]\n",
			path, offset, io_size);
	MP_SEND_MSG_ASYNC(sockfd, buffer_id, rpc_wait);
	struct rpc_pending_io *rpc = mlfs_zalloc(sizeof(struct rpc_pending_io));
	rpc->seq_n = msg->id;
	rpc->type = RPC_PENDING_RESPONSE;
	rpc->sockfd = sockfd;

	return rpc;
}

int rpc_remote_read_sync(char *path, loff_t offset, uint32_t io_size, uint8_t *dst)
{
	assert(g_n_cold_bkp > 0); //remote reads are only sent to reserve replicas

	int sockfd = cold_backups[0].sockfd[SOCK_IO]; 
	//note: for signaling on the fast path, we write directly to the rdma buffer to avoid extra copying
	struct app_context *msg;
	int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);
	uint64_t start_tsc;

#ifdef LIBFS
	if (enable_perf_stats)
		g_perf_stats.read_rpc_nr++;
#endif

	//setting msg->id allows us to insert a hook in the rdma driver to later wait until response received
	msg->id = generate_rpc_seqn(g_rpc_socks[sockfd]);

	// FIXME: Use inum instead of path for read RPCs
	// temporarily trimming paths to reduce overheads
	char trimmed_path[MAX_REMOTE_PATH];
	snprintf(trimmed_path, MAX_REMOTE_PATH, "%s", path);

	snprintf(msg->data, RPC_MSG_BYTES, "|read |%s |%lu|%u|%lu", trimmed_path, offset, io_size, (uintptr_t)dst);
	mlfs_info("trigger sync remote read: path[%s] offset[%lu] io_size[%d]\n",
			path, offset, io_size);

	//we still send an async msg, since we want to synchronously wait for the msg response and not
	//rdma-send completion
	MP_SEND_MSG_ASYNC(sockfd, buffer_id, 1);

#ifdef LIBFS
	if (enable_perf_stats)
		start_tsc = asm_rdtscp();
#endif
	//spin till we receive a response with the same sequence number
	//this is part of the messaging protocol that the remote peer should adhere to
	MP_AWAIT_RESPONSE(sockfd, msg->id);

#ifdef LIBFS
	if (enable_perf_stats)
		g_perf_stats.read_rpc_wait_tsc += (asm_rdtscp() - start_tsc);
#endif

	return 0;
}

//send back response for read rpc
//note: responses are always asynchronous
int rpc_remote_read_response(int sockfd, rdma_meta_t *meta, int mr_local, uint32_t seq_n)
{
	mlfs_info("trigger remote read response for mr: %d with seq_n: %u\n", mr_local, seq_n);
	//responses are sent from specified local memory region to requester's read cache
	if(seq_n) {
		meta->imm = seq_n; //set immediate to sequence number in order for requester to match it (in case of io wait)
		IBV_WRAPPER_WRITE_WITH_IMM_ASYNC(sockfd, meta, mr_local, MR_DRAM_CACHE);
	}
	else
		IBV_WRAPPER_WRITE_ASYNC(sockfd, meta, mr_local, MR_DRAM_CACHE);
}

//deprecated
#if 0
int rpc_remote_read_response(uintptr_t src, uintptr_t dst, uint16_t io_size, int mr_local, uint32_t seq_n)
{
	//responses are sent from specified local memory region to requester's read cache
	
	rdma_meta_t *meta = (rdma_meta_t*) mlfs_zalloc(sizeof(rdma_meta_t) + sizeof(struct ibv_sge));
	meta->addr = dst;
	meta->length = io_size;
	meta->sge_count = 1;
	meta->sge_entries[0].addr = (uintptr_t) data;
	meta->sge_entries[0].length = io_size;
	
	if(seq_n) {
		meta->imm = seq_n; //set immediate to sequence number in order for requester to match it (in case of io wait)
		IBV_WRAPPER_WRITE_WITH_IMM_ASYNC(g_sockfd, meta, mr_local, MR_DRAM_CACHE);
	}
	else
		IBV_WRAPPER_WRITE_ASYNC(g_sockfd, meta, mr_local, MR_DRAM_CACHE);
}
#endif

int rpc_lease_change(int mid, int rid, uint32_t inum, int type, uint32_t version, addr_t blknr, int sync)
{
	uint64_t start_tsc;

	int sockfd;
	if(!type)
		sockfd = g_peers[mid]->sockfd[SOCK_BG];
	else
		sockfd = g_peers[mid]->sockfd[SOCK_IO];

	//note: for signaling on the fast path, we write directly to the rdma buffer to avoid extra copying
	struct app_context *msg;
	int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);

#ifdef LIBFS
	if (enable_perf_stats) {
		start_tsc = asm_rdtscp();
		g_perf_stats.lease_rpc_nr++;
	}
#endif

	//setting msg->id allows us to insert a hook in the rdma driver to later wait until response received
	msg->id = generate_rpc_seqn(g_rpc_socks[sockfd]);

	// FIXME: Use inum instead of path for read RPCs
	// temporarily trimming paths to reduce overheads
	//char trimmed_path[MAX_REMOTE_PATH];
	//snprintf(trimmed_path, MAX_REMOTE_PATH, "%s", path);

	snprintf(msg->data, RPC_MSG_BYTES, "|lease |%u|%u|%d|%u|%lu", rid, inum, type, version, blknr);
	mlfs_printf("\x1b[33m [L] trigger lease acquire: inum[%u] type[%d] version[%u] blknr[%lu] (%s) \x1b[0m\n",
			inum, type, version, blknr, sync?"SYNC":"ASYNC");
	//mlfs_printf("msg->data %s buffer_id %d\n", msg->data, buffer_id);

	//we still send an async msg, since we want to synchronously wait for the msg response and not
	//rdma-send completion
	MP_SEND_MSG_ASYNC(sockfd, buffer_id, sync);

	//spin till we receive a response with the same sequence number
	//this is part of the messaging protocol that the remote peer should adhere to
	if(sync)
		MP_AWAIT_RESPONSE(sockfd, msg->id);

#ifdef LIBFS
	if (enable_perf_stats)
		g_perf_stats.lease_rpc_wait_tsc += (asm_rdtscp() - start_tsc);
#endif

	return 0;
}

int rpc_lease_response(int sockfd, uint32_t seq_n, int replicate)
{
	if(replicate) {
		mlfs_info("trigger lease response [+rsync] with seq_n: %u\n", seq_n);
		//note: for signaling on the fast path, we write directly to the rdma buffer to avoid extra copying
		struct app_context *msg;
		int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);

		msg->id = seq_n;

		snprintf(msg->data, RPC_MSG_BYTES, "|replicate |%d", replicate);

		MP_SEND_MSG_ASYNC(sockfd, buffer_id, 0);
	}
	else {
		mlfs_info("trigger lease response with seq_n: %u\n", seq_n);
		rdma_meta_t *meta = create_rdma_ack();
		//responses are sent from specified local memory region to requester's read cache
		if(seq_n) {
			meta->imm = seq_n; //set immediate to sequence number in order for requester to match it (in case of io wait)
			(sockfd, meta, 0, 0);
		}
		else
			panic("undefined codepath\n");
	}
}

int rpc_lease_flush_response(int sockfd, uint32_t seq_n, uint32_t inum, uint32_t version, addr_t blknr)
{
	struct app_context *msg;

	//int sockfd = g_peers[id]->sockfd[SOCK_BG];
	int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);

	// convert log segment (TODO: remove these conversions)
	// TODO: describe methodology for computing log segments are computed
	//int seg = abs(g_rpc_socks[sockfd]->peer->id - g_self_id) % g_n_nodes;
	//start_digest = start_digest - g_log_size * seg;

	snprintf(msg->data, RPC_MSG_BYTES, "|lease |%u|%d|%u|%lu", inum, LEASE_FREE, version, blknr);

	msg->id = seq_n; //set immediate to sequence number in order for requester to match it

	mlfs_info("trigger remote revoke response: inum[%u] version[%u] blknr[%lu]\n", inum, version, blknr);
	mlfs_printf("%s\n", msg->data);

	MP_SEND_MSG_ASYNC(sockfd, buffer_id, 0);
	return 0;
}



#ifdef KERNFS
// force a LibFS to flush dirty inodes/dirents (and digest if necessary)
int rpc_lease_flush(int peer_id, uint32_t inum, int force_digest)
{
	mlfs_info("trigger lease flush %speer %d inum %u\n", force_digest?"[+rsync/digest] ":"", peer_id, inum);

	// peer has already disconnected; exit
	if(!g_peers[peer_id])
		return -1;

	int sockfd = g_peers[peer_id]->sockfd[SOCK_LS];

	//note: for signaling on the fast path, we write directly to the rdma buffer to avoid extra copying
	struct app_context *msg;
	int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);
	uint64_t start_tsc;

	if (enable_perf_stats && force_digest) {
		g_perf_stats.lease_contention_nr++;
	}

	//setting msg->id allows us to insert a hook in the rdma driver to later wait until response received
	//msg->id = generate_rpc_seqn(g_rpc_socks[sockfd]);
	msg->id = 0;

	snprintf(msg->data, RPC_MSG_BYTES, "|flush |%u|%d", inum, force_digest);

	mlfs_printf("peer send: %s\n", msg->data);

	MP_SEND_MSG_ASYNC(sockfd, buffer_id, 0);

	//spin till we receive a response with the same sequence number
	//this is part of the messaging protocol that the remote peer should adhere to

	if(force_digest)
		MP_AWAIT_RESPONSE(sockfd, msg->id);

	return 0;
}

// lease request invalid (usually due to changing lease manangers)
// LibFS should flush its lease state and retry sending to new lease manager
int rpc_lease_invalid(int sockfd, int peer_id, uint32_t inum, uint32_t seqn)
{
	mlfs_info("mark lease outdated for peer %d inum %u\n", peer_id, inum);

	// peer has already disconnected; exit
	if(!g_peers[peer_id])
		return -1;

	//int sockfd = g_peers[peer_id]->sockfd[SOCK_LS];

	//note: for signaling on the fast path, we write directly to the rdma buffer to avoid extra copying
	struct app_context *msg;
	int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);
	uint64_t start_tsc;

	//setting msg->id allows us to insert a hook in the rdma driver to later wait until response received
	//msg->id = generate_rpc_seqn(g_rpc_socks[sockfd]);
	msg->id = seqn;

	snprintf(msg->data, RPC_MSG_BYTES, "|error |%u", inum);

	mlfs_printf("peer send: %s\n", msg->data);

	MP_SEND_MSG_ASYNC(sockfd, buffer_id, 0);

	return 0;
}

// force a LibFS to flush dirty inodes/dirents (and digest if necessary)
int rpc_lease_migrate(int peer_id, uint32_t inum, uint32_t kernfs_id)
{
	mlfs_info("trigger lease migration peer %d inum %u kernfs_id %u\n", peer_id, inum, kernfs_id);

	// peer has already disconnected; exit
	if(!g_peers[peer_id])
		return -1;

	int sockfd = g_peers[peer_id]->sockfd[SOCK_LS];

	//note: for signaling on the fast path, we write directly to the rdma buffer to avoid extra copying
	struct app_context *msg;
	int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);
	uint64_t start_tsc;

	//setting msg->id allows us to insert a hook in the rdma driver to later wait until response received
	msg->id = generate_rpc_seqn(g_rpc_socks[sockfd]);

	snprintf(msg->data, RPC_MSG_BYTES, "|migrate |%u|%u", inum, kernfs_id);

	MP_SEND_MSG_ASYNC(sockfd, buffer_id, 0);

	return 0;
}
#endif

// replicate log synchronously using provided rdma metadata
int rpc_replicate_log_sync(peer_meta_t *peer, struct list_head *rdma_entries, uint32_t imm)
{
	rpc_replicate_log(peer, rdma_entries, imm, 1);
	return 0;
}

// replicate log asynchronously
// Note: for async calls, await must be called to ensure that n_digest log block groups
// have been replicated.
struct rpc_pending_io * rpc_replicate_log_async(peer_meta_t *peer, struct list_head *rdma_entries, uint32_t imm)
{
	return rpc_replicate_log(peer, rdma_entries, imm, 0);
}

struct rpc_pending_io * rpc_replicate_log(peer_meta_t *peer, struct list_head *rdma_entries, uint32_t imm, int do_sync)
{
	struct rdma_meta_entry *rdma_entry, *rdma_tmp;
	struct rpc_pending_io *rpc = NULL;
	uint16_t seqn = 0;

	addr_t n_log_blk = 0;
	int steps = 0, requester_id = 0, rotated = 0, ack = 0;

	// last peer in the chain sends back an ack
	struct peer_id *ack_replica = g_kernfs_peers[(g_self_id + g_rsync_rf - 1) % g_rsync_rf];
	int send_sockfd = peer->info->sockfd[SOCK_IO];
	int rcv_sockfd = ack_replica->sockfd[SOCK_IO];

	uint64_t start_tsc_tmp;

	//if (enable_perf_stats)
	//	start_tsc_tmp = asm_rdtscp();

	if(imm)
		decode_rsync_metadata(imm, &seqn, &n_log_blk, &steps, &requester_id, &rotated, &ack);
	else {
		//FIXME: seqn is currently bound to 3 due to imm field size.
		// store these limits in a variable somewhere.
		seqn = (uint16_t) (generate_rpc_seqn(g_rpc_socks[ack_replica->sockfd[SOCK_IO]]) % N_RSYNC_THREADS) + 1;
	}


	list_for_each_entry_safe(rdma_entry, rdma_tmp, rdma_entries, head) {

		mlfs_rpc("rsync: dst_ip[%s] type:[%s] remote addr[0x%lx] len[%lu bytes] imm[%u] send_sockfd[%d] rcv_sockfd[%d]\n",
				peer->info->ip, do_sync?"sync":"async", rdma_entry->meta->addr, rdma_entry->meta->length, imm, send_sockfd,
				rcv_sockfd);


		if(imm)
			rdma_entry->meta->imm = imm;

		if(list_is_last(&rdma_entry->head, rdma_entries)) {
			assert(rdma_entry->meta->next == 0);
			if(do_sync) {
				set_peer_syncing(peer, seqn);
				ack = 1;
			}
		}

		if(g_n_nodes > 2 && !imm)
			rdma_entry->meta->imm = generate_rsync_metadata(g_self_id, seqn, rdma_entry->meta->length, rdma_entry->rotated, ack);

		if(rdma_entry->meta->imm) {
			IBV_WRAPPER_WRITE_WITH_IMM_ASYNC(send_sockfd, rdma_entry->meta, MR_NVM_LOG, MR_NVM_LOG);
			if(do_sync && list_is_last(&rdma_entry->head, rdma_entries))
				wait_on_peer_replicating(peer, seqn);
		}
		else {
			if(do_sync && list_is_last(&rdma_entry->head, rdma_entries)) {
				IBV_WRAPPER_WRITE_SYNC(send_sockfd, rdma_entry->meta, MR_NVM_LOG, MR_NVM_LOG);
				clear_peer_syncing(peer, seqn);
			}
			else
				IBV_WRAPPER_WRITE_ASYNC(send_sockfd, rdma_entry->meta, MR_NVM_LOG, MR_NVM_LOG);
		}

		//list_del(&rdma_entry->head);
		//mlfs_free(rdma_entry->meta);
		//mlfs_free(rdma_entry);
	}


	//if (enable_perf_stats)
	//	g_perf_stats.tmp_tsc += (asm_rdtscp() - start_tsc_tmp);

	//list_del(rdma_entries);
	return rpc;
}

int rpc_send_imm(int sockfd, uint32_t seqn)
{
	rdma_meta_t *meta = create_rdma_ack();
	if(seqn) {
		meta->imm = seqn; //set immediate to sequence number in order for requester to match it (in case of io wait)
		IBV_WRAPPER_WRITE_WITH_IMM_ASYNC(sockfd, meta, 0, 0);
	}

	mlfs_rpc("peer send: ack on sock:%d with seqn: %u\n", sockfd, seqn);
}

//acknowledge completion of an rpc
int rpc_send_ack(int sockfd, uint32_t seqn)
{
	struct app_context *msg;

	//int sockfd = g_peers[id]->sockfd[SOCK_BG];
	int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);

	// convert log segment (TODO: remove these conversions)
	// TODO: describe methodology for computing log segments are computed
	//int seg = abs(g_rpc_socks[sockfd]->peer->id - g_self_id) % g_n_nodes;
	//start_digest = start_digest - g_log_size * seg;

	snprintf(msg->data, RPC_MSG_BYTES, "|ack |%d|", 1);
	msg->id = seqn; //set immediate to sequence number in order for requester to match it

	mlfs_rpc("peer send: %s\n", msg->data);
	MP_SEND_MSG_ASYNC(sockfd, buffer_id, 0);

	return 0;
}

//send digest request asynchronously
struct rpc_pending_io * rpc_remote_digest_async(int peer_id, peer_meta_t *peer, uint32_t n_digest, int rpc_wait)
{
	//mlfs_assert(peer->n_used_nonatm > 0);

	if(!n_digest)
		n_digest = atomic_load(&peer->n_digest);

	if(!n_digest) {
		mlfs_debug("nothing to digest for peer %d, returning..\n", peer->info->id);
		return NULL;
	}

	//int sockfd = g_kernfs_peers[peer_id]->sockfd[SOCK_BG];
	int sockfd = peer_sockfd(peer_id, SOCK_BG);
	//int sockfd = peer->info->sockfd[SOCK_IO];
	
	if(peer->digesting) {
		//FIXME: optimize. blocking might be a bit extreme here
		wait_on_peer_digesting(peer);
		//mlfs_info("%s", "[L] remote digest failure: peer is busy\n");
	}

	set_peer_digesting(peer);
	peer->n_digest_req = n_digest;

	addr_t digest_blkno = peer->start_digest;
	addr_t start_blkno = g_sync_ctx[0]->begin;
	addr_t end_blkno = peer->remote_end;

	struct app_context *msg;
	int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);
	// TODO: describe how log devs are computed for remote nodes
	//int log_id = abs(peer->info->id - g_self_id) % g_n_nodes;
	int log_id = g_self_id;

	snprintf(msg->data, RPC_MSG_BYTES, "|digest |%d|%d|%u|%lu|%lu|%lu",
			log_id, g_log_dev, n_digest, digest_blkno, start_blkno, end_blkno);

	//setting msg->id allows us to insert a hook in the rdma agent to later wait until response received
	if(rpc_wait)
		msg->id = generate_rpc_seqn(g_rpc_socks[sockfd]);

	MP_SEND_MSG_ASYNC(sockfd, buffer_id, rpc_wait);

	mlfs_rpc("peer send: %s\n", msg->data);
	mlfs_info("trigger remote digest (async) on sock %d: dev[%d] digest_blkno[%lu] n_digest[%u] end_blkno[%lu] steps[%u]\n",
			sockfd, log_id, digest_blkno, n_digest, end_blkno, g_rsync_rf-1);

#if 0
	atomic_store(&peer->n_digest, 0);
	atomic_store(&peer->n_used, 0);
	atomic_store(&peer->n_used_blk, 0);
#endif

	/*
	else if(peer->digesting && (peer->n_used_blk_nonatm
		       	+ peer->n_unsync_blk_nonatm > g_sync_ctx->size*9/10)) {
		panic("[error] unable to rsync: remote log is full\n");
	}
	else
		mlfs_info("%s\n", "failed to trigger remote digest. remote node is busy");
	*/
	struct rpc_pending_io *rpc = mlfs_zalloc(sizeof(struct rpc_pending_io));
	rpc->seq_n = msg->id;
	rpc->sockfd = sockfd;

	return rpc;
}

//send digest request asynchronously
int rpc_remote_digest_sync(int peer_id, peer_meta_t *peer, uint32_t n_digest)
{
	if(!n_digest)
		n_digest = atomic_load(&peer->n_digest);

	if(!n_digest) {
		mlfs_debug("nothing to digest for peer %d, returning..\n", peer->info->id);
		return 0;
	}

	int sockfd = g_kernfs_peers[peer_id]->sockfd[SOCK_BG];
	//int sockfd = peer->info->sockfd[SOCK_IO];

	if(peer->digesting) {
		//FIXME: optimize. blocking might be a bit extreme here
 		wait_on_peer_digesting(peer);
		//mlfs_info("%s", "[L] remote digest failure: peer is busy\n");
	}

	set_peer_digesting(peer);
	peer->n_digest_req = n_digest;

	addr_t digest_blkno = peer->start_digest;
	addr_t start_blkno = g_sync_ctx[0]->begin;
	addr_t end_blkno = peer->remote_end;

	struct app_context *msg;
	int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);
	//int log_id = abs(peer->info->id - g_self_id) % g_n_nodes;
	int log_id = g_self_id;
	snprintf(msg->data, RPC_MSG_BYTES, "|digest |%d|%d|%u|%lu|%lu|%lu|%u",
			log_id, g_log_dev, n_digest, digest_blkno, start_blkno, end_blkno, g_rsync_rf-1);

	//setting msg->id allows us to insert a hook in the rdma driver to later wait until response received
	msg->id = generate_rpc_seqn(g_rpc_socks[sockfd]);

	MP_SEND_MSG_ASYNC(sockfd, buffer_id, 1);

	mlfs_rpc("peer send: %s\n", msg->data);
	mlfs_info("trigger remote digest (sync) on sock %d: dev[%d] digest_blkno[%u] n_digest[%lu] end_blkno[%lu] steps[%u]\n",
			sockfd, log_id, n_digest, digest_blkno, end_blkno, g_rsync_rf-1);

#if 0
	atomic_store(&peer->n_digest, 0);
	atomic_store(&peer->n_used, 0);
	atomic_store(&peer->n_used_blk, 0);
#endif

	//spin till we receive a response with the same sequence number
	//this is part of the messaging protocol that the remote peer should adhere to
	MP_AWAIT_RESPONSE(sockfd, msg->id);

	/*
	else if(peer->digesting && (peer->n_used_blk_nonatm
		       	+ peer->n_unsync_blk_nonatm > g_sync_ctx->size*9/10)) {
		panic("[error] unable to rsync: remote log is full\n");
	}
	else
		mlfs_info("%s\n", "failed to trigger remote digest. remote node is busy");
	*/
	return 0;
}

//send back digest response
int rpc_remote_digest_response(int sockfd, int id, int dev, addr_t start_digest, int n_digested, int rotated, uint32_t seq_n)
{
	struct app_context *msg;

	//int sockfd = g_peers[id]->sockfd[SOCK_BG];
	int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);

	// convert log segment (TODO: remove these conversions)
	// TODO: describe methodology for computing log segments are computed
	//int seg = abs(g_rpc_socks[sockfd]->peer->id - g_self_id) % g_n_nodes;
	//start_digest = start_digest - g_log_size * seg;

	snprintf(msg->data, RPC_MSG_BYTES, "|complete |%d|%d|%d|%lu|%d|%d",
			id, dev, n_digested, start_digest, rotated, 0);
	msg->id = seq_n; //set immediate to sequence number in order for requester to match it

	mlfs_rpc("peer send: %s\n", msg->data);
	mlfs_info("trigger remote digest response: n_digested[%d] rotated[%d]\n", n_digested, rotated);

	MP_SEND_MSG_ASYNC(sockfd, buffer_id, 0);
	return 0;
}

int rpc_forward_msg(int sockfd, char* data)
{
	struct app_context *msg;
	int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);

	msg->id = generate_rpc_seqn(g_rpc_socks[sockfd]);

	snprintf(msg->data, RPC_MSG_BYTES, "%s",data);

	mlfs_rpc("peer send: %s\n", msg->data);

	MP_SEND_MSG_ASYNC(sockfd, buffer_id, 1);
	return 0;
}

int rpc_bootstrap(int sockfd)
{

#ifdef LIBFS
	struct app_context *msg;
	int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);
	snprintf(msg->data, RPC_MSG_BYTES, "|bootstrap |%u", getpid());
	mlfs_rpc("peer send: %s\n", msg->data);
	msg->id = generate_rpc_seqn(g_rpc_socks[sockfd]);

	MP_SEND_MSG_ASYNC(sockfd, buffer_id, 1);

	//spin till we receive a response with the same sequence number
	MP_AWAIT_RESPONSE(sockfd, msg->id);
#else
	// undefined code path
	mlfs_assert(false);
#endif
}

int rpc_bootstrap_response(int sockfd, uint32_t seq_n)
{
	struct app_context *msg;
	int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);

	snprintf(msg->data, RPC_MSG_BYTES, "|bootstrap |%u", g_rpc_socks[sockfd]->peer->id);
	msg->id = seq_n; //set immediate to sequence number in order for requester to match it

	mlfs_rpc("peer send: %s\n", msg->data);
	MP_SEND_MSG_ASYNC(sockfd, buffer_id, 0);
}

struct rpc_pending_io* rpc_register_log(int sockfd, struct peer_id *peer)
{
	struct app_context *msg;
	int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);
	snprintf(msg->data, RPC_MSG_BYTES, "|peer |%d|%u|%s", peer->id, peer->pid, peer->ip);
	msg->id = generate_rpc_seqn(g_rpc_socks[sockfd]);

	MP_SEND_MSG_ASYNC(sockfd, buffer_id, 1);
	struct rpc_pending_io *rpc = mlfs_zalloc(sizeof(struct rpc_pending_io));
	rpc->seq_n = msg->id;
	rpc->type = RPC_PENDING_RESPONSE;
	rpc->sockfd = sockfd;
	return rpc;
}


int rpc_register_log_response(int sockfd, uint32_t seq_n)
{
	struct app_context *msg;
	int buffer_id = MP_ACQUIRE_BUFFER(sockfd, &msg);

	snprintf(msg->data, RPC_MSG_BYTES, "|peer |registered");
	msg->id = seq_n; //set immediate to sequence number in order for requester to match it

	MP_SEND_MSG_ASYNC(sockfd, buffer_id, 0);
}

struct mlfs_reply * rpc_get_reply_object(int sockfd, uint8_t *dst, uint32_t seqn)
{
	struct mlfs_reply *reply = mlfs_zalloc(sizeof(struct mlfs_reply));
	reply->remote = 1; //signifies remote destination buffer
	reply->sockfd = sockfd;
	reply->seqn = seqn;
	reply->dst = dst;

	return reply;
}

void rpc_await(struct rpc_pending_io *pending)
{
	if(pending->type == RPC_PENDING_WC)
		MP_AWAIT_WORK_COMPLETION(pending->sockfd, pending->seq_n);
	else
		MP_AWAIT_RESPONSE(pending->sockfd, pending->seq_n);
}

void print_peer_id(struct peer_id *peer)
{
	printf("---- Peer %d Info----\n", peer->id);
	printf("IP: %s\n", peer->ip);
	for(int j=0; j<SOCK_TYPE_COUNT; j++) {
		printf("SOCKFD[%d] = %d\n", j, peer->sockfd[j]);
	}
	printf("---------------------\n");
}

#endif
