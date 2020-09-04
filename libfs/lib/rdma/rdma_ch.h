#ifndef RDMA_RC_H
#define RDMA_RC_H

#include <pthread.h>
#include <netdb.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <semaphore.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>

#include "uthash.h"
#include "utils.h"
#include "messaging.h"
#include "verbs.h"
#include "core_ch.h"

/*
enum mr_type
{
	MR_SYNC = 0, //memory regions of type 'MR_SYNC' are used by replication functions
	MR_CACHE,    //others can be used in any manner specified by the application
	MR_BUFFER,
};
*/

extern const int TIMEOUT_IN_MS;
extern const char *DEFAULT_PORT;

extern struct rdma_event_channel *ec;
extern int num_mrs;
extern struct mr_context *mrs;
extern int msg_size;
extern int cq_loop;
extern pthread_mutexattr_t attr;
extern pthread_mutex_t cq_lock;

//extern int archive_idx;
extern int exit_rc_loop;

//rdma_cm_id hashmap value (defined below in 'context')
struct id_record {
	struct sockaddr_in addr;
	uint32_t qp_num;
	struct rdma_cm_id *id;
	UT_hash_handle addr_hh;
	UT_hash_handle qp_hh;
};

struct buffer_record {
	uint32_t wr_id;
	int buff_id;
	UT_hash_handle hh;
};

static inline uint32_t last_compl_wr_id(struct conn_context *ctx, int send)
{
	//we maintain seperate wr_ids for send/rcv queues since
	//there is no ordering between their work requests
	if(send)
		return ctx->last_send_compl;
	else
		return ctx->last_rcv_compl;
}

//connection building
void build_connection(struct rdma_cm_id *id);
void build_shared_context(struct rdma_cm_id *id);
void build_conn_context(struct rdma_cm_id *id, int polling_loop);
void build_cq_channel(struct rdma_cm_id *id);
void build_qp_attr(struct rdma_cm_id *id, struct ibv_qp_init_attr *qp_attr);
void build_params(struct rdma_conn_param *params);

//event handling
void event_loop(struct rdma_event_channel *ec, int exit_on_connect, int exit_on_disconnect);

//request completions
void poll_cq(struct ibv_cq *cq, struct ibv_wc *wc);
void* poll_cq_spinning_loop();
void* poll_cq_blocking_loop();

//helper functions
struct rdma_cm_id* find_next_connection(struct rdma_cm_id* id);
struct rdma_cm_id* find_connection_by_addr(struct sockaddr_in *addr);
struct rdma_cm_id* find_connection_by_wc(struct ibv_wc *wc);
struct rdma_cm_id* get_connection(int sockfd);

struct conn_context* get_channel_ctx(int sockfd);


//channel state transitions
int rc_chan_add(struct rdma_cm_id *id, int app_type, pid_t pid, int polling_loop);
void rc_chan_connect(struct rdma_cm_id *id);
void rc_chan_disconnect(struct rdma_cm_id *id);
void rc_chan_clear(struct rdma_cm_id *id);

//channel work
void rc_process_completion(struct ibv_wc *wc);

//buffers
int rc_bind_buffer(struct rdma_cm_id *id, int buffer, uint32_t wr_id);
int rc_release_buffer(int sockfd, uint32_t wr_id);

//protection domain
struct ibv_pd * rc_get_pd(struct rdma_cm_id *id);

#endif
