#ifndef RDMA_AGENT_H
#define RDMA_AGENT_H

#include "utils.h"
#include "globals.h"
#include "common.h"
#include "core_ch.h"
#include "shmem_ch.h"
#include "verbs.h"
#include "messaging.h"
#include "mr.h"

static pthread_t comm_thread;

extern int rdma_initialized;
extern char port[10];

/*
void init_rdma_agent(char *listen_port, struct mr_context *regions,
		int region_count, uint16_t buffer_size,
		app_conn_cb_fn conn_callback,
		app_disc_cb_fn disc_callback,
		app_recv_cb_fn recv_callback);
*/

void init_rdma_agent(char *listen_port, struct mr_context *regions,
		int region_count, uint16_t buffer_size, int ch_type,
		app_conn_cb_fn app_connect,
		app_disc_cb_fn app_disconnect,
		app_recv_cb_fn app_receive);

void shutdown_rdma_agent();

int add_connection(char* ip, char *port, int app_type, pid_t pid, int ch_type, int polling_loop); 

static void on_pre_conn(struct rdma_cm_id *id);
static void on_connection(struct rdma_cm_id *id);
static void on_disconnect(struct rdma_cm_id *id);
static void on_completion(struct ibv_wc *wc);

static void* rdma_client_loop();
static void* rdma_server_loop();

#endif
