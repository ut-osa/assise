#ifndef _RPC_INTERFACE_H_
#define _RPC_INTERFACE_H_

#ifdef DISTRIBUTED
#include <stdio.h>
#include <time.h>
#include <unistd.h>

#include "concurrency/synchronization.h"
#include "filesystem/shared.h"
#include "global/global.h"
#include "global/types.h"
#include "global/util.h"
#include "ds/list.h"
#include "ds/stdatomic.h"
#include "agent.h"
#include "replication.h"
#include "peer.h"

#define RPC_MSG_BYTES 1500
#define RPC_MSG_NBUFF 8

// RDMA interface name
static char* rdma_intf = "ib0";

// localhost interface name
static char* local_intf = "lo";

// Replica IP Mappings --------------------------------------------

static struct peer_id hot_replicas[g_n_hot_rep] = {
	{ .ip = "127.0.0.1", .role = HOT_REPLICA, .type = KERNFS_PEER},
//	{ .ip = "13.13.13.7", .role = HOT_REPLICA, .type = KERNFS_PEER},
};


static struct peer_id hot_backups[g_n_hot_bkp] = {
//	{ .ip = "172.17.15.4", .role = HOT_BACKUP, .type = KERNFS_PEER},
};

static struct peer_id cold_backups[g_n_cold_bkp] = {
//	{ .ip = "172.17.15.6", .role = COLD_BACKUP, .type = KERNFS_PEER},
};

static struct peer_id external_replicas[g_n_ext_rep + 1] = {
//	{ .ip = "172.17.15.10", .role = LOCAL_NODE, .type = KERNFS_PEER, .namespace_id = "sdp5", .inum_prefix = (1<<31) },
//	{ .ip = "172.17.15.8", .role = EXTERNAL_NODE, .type = KERNFS_PEER, .namespace_id = "sdp4", .inum_prefix = (1<<30) }
};

// ------------------------------------------------------------------

extern char g_self_ip[INET_ADDRSTRLEN];
extern int g_self_id;
extern int g_kernfs_id;

static inline uint32_t generate_rpc_seqn(struct peer_socket *sock)
{
	return ++sock->seqn;
}


static inline rdma_meta_t * create_rdma_meta(uintptr_t src,
		uintptr_t dst, uint16_t io_size)
{
	rdma_meta_t *meta = (rdma_meta_t*) mlfs_zalloc(sizeof(rdma_meta_t) + sizeof(struct ibv_sge));
	meta->addr = dst;
	meta->length = io_size;
	meta->sge_count = 1;
	meta->sge_entries[0].addr = src;
	meta->sge_entries[0].length = io_size;
	return meta;
}

static inline struct rdma_meta_entry * create_rdma_entry(uintptr_t src,
		uintptr_t dst, uint16_t io_size, int local_mr, int remote_mr)
{
	struct rdma_meta_entry *rdma_entry = (struct rdma_meta_entry *)
		mlfs_zalloc(sizeof(struct rdma_meta_entry));
	rdma_entry->meta = (rdma_meta_t*) mlfs_zalloc(sizeof(rdma_meta_t) + sizeof(struct ibv_sge));
	rdma_entry->meta->addr = dst;
	rdma_entry->meta->length = io_size;
	rdma_entry->meta->sge_count = 1;
	rdma_entry->meta->sge_entries[0].addr = src;
	rdma_entry->meta->sge_entries[0].length = io_size;
	rdma_entry->local_mr = local_mr;
	rdma_entry->remote_mr = remote_mr;
	INIT_LIST_HEAD(&rdma_entry->head);
	return rdma_entry;	
}

static inline rdma_meta_t * create_rdma_ack()
{
	rdma_meta_t *meta = (rdma_meta_t*) mlfs_zalloc(sizeof(rdma_meta_t) + sizeof(struct ibv_sge));
	meta->addr = (uintptr_t)0;
	meta->length = 0;
	meta->sge_count = 0;
	meta->next = 0;
	return meta;	

}


static inline uint32_t translate_namespace_inum(uint32_t inum, char *path_id)
{
	for(int i = 0; i < g_n_ext_rep + 1; i++) {
		if (!strncmp(external_replicas[i].namespace_id, path_id, DIRSIZ))
			return (inum | external_replicas[i].inum_prefix);
	}

	mlfs_printf("Error: didn't find matching namespace for id '%s'\n", path_id);
	panic("namespace parsing failed");
}

int init_rpc(struct mr_context *regions, int n_regions, char *port, signal_cb_fn callback);
int shutdown_rpc();

int rpc_connect(struct peer_id *peer, char *listen_port, int type, int poll_cq);
int rpc_listen(int sockfd, int count);
//int rpc_add_socket(struct peer_id *peer, int sockfd, int type);
void rpc_add_socket(int sockfd);
void rpc_remove_socket(int sockfd);
int rpc_bootstrap(int sockfd);
int rpc_bootstrap_response(int sockfd, uint32_t seq_n);
struct rpc_pending_io* rpc_register_log(int sockfd, struct peer_id *peer);
int rpc_register_log_response(int sockfd, uint32_t seq_n);
struct rpc_pending_io * rpc_remote_read_async(char *path, loff_t offset, uint32_t io_size, uint8_t *dst, int rpc_wait);
struct rpc_pending_io * rpc_remote_digest_async(int peer_id, peer_meta_t *peer, uint32_t n_digest, int rpc_wait);
int rpc_remote_read_sync(char *path, loff_t offset, uint32_t io_size, uint8_t *dst);
int rpc_remote_digest_sync(int peer_id, peer_meta_t *peer, uint32_t n_digest);
int rpc_remote_read_response(int sockfd, rdma_meta_t *meta, int mr_local, uint32_t seqn);
int rpc_remote_digest_response(int sockfd, int id, int dev, addr_t start_digest, int n_digested, int rotated, uint32_t seq_n);
int rpc_forward_msg(int sockfd, char* data);
int rpc_lease_change(int mid, int rid, uint32_t inum, int type, uint32_t version, addr_t blknr, int sync);
int rpc_lease_response(int sockfd, uint32_t seqn, int replicate);
int rpc_lease_invalid(int sockfd, int peer_id, uint32_t inum, uint32_t seqn);
int rpc_lease_flush_response(int sockfd, uint32_t seq_n, uint32_t inum, uint32_t version, addr_t blknr);
int rpc_lease_flush(int peer_id, uint32_t inum, int force_digest);
int rpc_lease_migrate(int peer_id, uint32_t inum, uint32_t kernfs_id);
int rpc_replicate_log_sync(peer_meta_t *peer, struct list_head *rdma_entries, uint32_t imm);
struct rpc_pending_io * rpc_replicate_log_async(peer_meta_t *peer, struct list_head *rdma_entries, uint32_t imm);
struct rpc_pending_io * rpc_replicate_log(peer_meta_t *peer, struct list_head *rdma_entries, uint32_t imm, int do_sync);
int rpc_send_imm(int sockfd, uint32_t seqn);
int rpc_send_ack(int sockfd, uint32_t seqn);
struct mlfs_reply * rpc_get_reply_object(int sockfd, uint8_t *dst, uint32_t seqn);
void rpc_await(struct rpc_pending_io *pending);

int peer_sockfd(int node_id, int type);
void print_peer_id(struct peer_id *peer);

#endif

#endif
