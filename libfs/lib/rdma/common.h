#ifndef RDMA_COMMON_H
#define RDMA_COMMON_H

#include <rdma/rdma_cma.h>
#include "globals.h"
#include "uthash.h"


//---------device metadata
struct context {
	int n_dev; //number of RDMA devices
	struct ibv_context *ctx[MAX_DEVICES];
	struct ibv_pd *pd[MAX_DEVICES];
	struct id_record *id_by_addr; //addr to id hashmap
	struct id_record *id_by_qp; //qp to id hashmap
};

//---------connection metadata
struct conn_context
{
	//unique id
	int sockfd;

	//device id
	int devid;

	//actual connection descriptor
	int realfd;

	//connection state
	int state;

	//channel type (local or remote)
	int ch_type;

	//app identifier
	int app_type;

	pid_t pid;

	//port number
	int portno;

	//completion queue
	struct ibv_cq *cq;

	//completion channel
	struct ibv_comp_channel *comp_channel;

	//background cq polling thread
	pthread_t cq_poller_thread;

	//polling mode: 1 means cq thread is always active, 0 means only during bootstrap
	int poll_always;

	//enables or disables background polling thread
	int poll_enable;

	//provides completion poll permission (in case of multiple threads)
	int poll_permission;

	//registered memory regions
	struct ibv_mr **local_mr;
	struct mr_context **remote_mr;

	//checks whether mr init message have been sent/recv'd
	int mr_init_sent;
	int mr_init_recv;

	//bootstrap flags (signifies whether access permissions are available for an mr)
	int *local_mr_ready;
	int *remote_mr_ready;
	int *local_mr_sent;

	//total number of remote MRs
	int remote_mr_total;

	//idx of local_mr to be sent next;
	int local_mr_to_sync;

	//send/rcv buffers
	struct ibv_mr **msg_send_mr;
	struct ibv_mr **msg_rcv_mr;
	struct message **msg_send;
	struct message **msg_rcv;

	//determines acquisitions of send buffers
	int *send_slots;
	uint8_t send_idx;
	uint8_t rcv_idx;

	//connection id
	struct rdma_cm_id *id;

	//locking and synchronization
	uint32_t last_send;
	uint32_t last_send_compl;
	uint32_t last_rcv;
	uint32_t last_rcv_compl;
	uint32_t last_msg; //used to ensure no conflicts when writing on msg send buffer

	pthread_mutex_t wr_lock;
	pthread_cond_t wr_completed;
	pthread_spinlock_t post_lock; //ensures that rdma ops on the same socket have monotonically increasing wr id

	struct app_response *pendings; //hashmap of pending application responses (used exclusively by application)
	struct buffer_record *buffer_bindings; //hashmap of send buffer ownership per wr_id
	pthread_spinlock_t buffer_lock; //concurrent access to buffer hashtable
	
	UT_hash_handle qp_hh;
};

//--------memory region metadta
struct mr_context
{
	//type enum
	int type;
	//start address
	addr_t addr;
	//length
	addr_t length;	
	//access keys
	uint32_t lkey;
	uint32_t rkey;
};

//---------rdma operation metadata
typedef struct rdma_metadata {
	int op;
	uint32_t wr_id;
	addr_t addr;
	addr_t length;
	uint32_t imm;
	int sge_count;
	struct rdma_metadata *next;
	struct ibv_sge sge_entries[];
} rdma_meta_t;

//---------user-level metadata
//msg payload
struct app_context {
	int sockfd;  //socket id on which the msg came on 
	uint32_t id; //can be used as an app-specific request-response matcher
	char* data;  //convenience pointer to data blocks
};

//msg response tracker
struct app_response { //used to keep track of pending responses
	uint32_t id;
	int ready;
	UT_hash_handle hh;
};

// user callbacks
typedef void(*app_conn_cb_fn)(int sockfd);
typedef void(*app_disc_cb_fn)(int sockfd);
typedef void(*app_recv_cb_fn)(struct app_context *msg);

extern app_conn_cb_fn app_conn_event;
extern app_disc_cb_fn app_disc_event;
extern app_recv_cb_fn app_recv_event;

#endif
