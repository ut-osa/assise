#include <sys/syscall.h>
#include <pthread.h>
#include <stdatomic.h>
#include "agent.h"

app_conn_cb_fn app_conn_event;
app_disc_cb_fn app_disc_event;
app_recv_cb_fn app_recv_event;

int rdma_initialized = 0;
char port[10];

//initialize memory region information
void init_rdma_agent(char *listen_port, struct mr_context *regions,
		int region_count, uint16_t buffer_size, int ch_type,
		app_conn_cb_fn app_connect,
		app_disc_cb_fn app_disconnect,
		app_recv_cb_fn app_receive)
{
	//pthread_mutex_lock(&global_mutex);
	if(rdma_initialized)
		return;

	if(region_count > MAX_MR)
		mp_die("region count is greater than MAX_MR");

	mrs = regions;
	num_mrs = region_count;
	msg_size = buffer_size;

	app_conn_event = app_connect;
	app_disc_event = app_disconnect;
	app_recv_event = app_receive;

	set_seed(5);

	if(listen_port)
		snprintf(port, sizeof(port), "%s", listen_port);

	s_conn_bitmap = calloc(MAX_CONNECTIONS, sizeof(int));
	s_conn_ctx = (struct conn_context **)calloc(MAX_CONNECTIONS, sizeof(struct conn_context*));

	ec = rdma_create_event_channel();

	if(!listen_port) {
		if(ch_type == CH_TYPE_REMOTE || ch_type == CH_TYPE_ALL)
			pthread_create(&comm_thread, NULL, rdma_client_loop, NULL);
		//if(ch_type == CH_TYPE_LOCAL || ch_type == CH_TYPE_ALL)
		//	pthread_create(&comm_thread, NULL, local_client_loop, NULL);
	}
	else {
		if(ch_type == CH_TYPE_REMOTE || ch_type == CH_TYPE_ALL)
			pthread_create(&comm_thread, NULL, rdma_server_loop, port);
		if(ch_type == CH_TYPE_LOCAL || ch_type == CH_TYPE_ALL)
			pthread_create(&comm_thread, NULL, local_server_loop, port);
	}

	rdma_initialized = 1;
}

void shutdown_rdma_agent()
{
#if 0
	void *ret;
	int sockfd = -1;

	int n = rc_connection_count();

	for(int i=0; i<n; i++) {
		sockfd = rc_next_connection(sockfd);
		if(rc_terminated(sockfd) != RC_CONNECTION_TERMINATED)
			rc_disconnect(get_connection(sockfd));
	}

   	//if(pthread_join(comm_thread, &ret) != 0)
	//	mp_die("pthread_join() error");
#endif
}

static void* rdma_client_loop()
{
	event_loop(ec, 0, 1); /* exit upon disconnect */
	rdma_destroy_event_channel(ec);
	debug_print("exiting rc_client_loop\n");
	return NULL;
}

static void* rdma_server_loop(void *port)
{
	struct sockaddr_in6 addr;
	struct rdma_cm_id *cm_id = NULL;

	memset(&addr, 0, sizeof(addr));
	addr.sin6_family = AF_INET6;
	addr.sin6_port = htons(atoi(port));

	rdma_create_id(ec, &cm_id, NULL, RDMA_PS_TCP);
	rdma_bind_addr(cm_id, (struct sockaddr *)&addr);
	rdma_listen(cm_id, 100); /* backlog=10 is arbitrary */

	printf("[RDMA-Server] Listening on port %d for connections. interrupt (^C) to exit.\n", atoi(port));

	event_loop(ec, 0, 0); /* do not exit upon disconnect */

	rdma_destroy_id(cm_id);
	rdma_destroy_event_channel(ec);

	debug_print("exiting rc_server_loop\n");

	return NULL;
}

//request connection to another RDMA agent (non-blocking)
//returns socket descriptor if successful, otherwise -1
int add_connection(char* ip, char *port, int app_type, pid_t pid, int ch_type, int polling_loop) 
{
	int sockfd = -1;

	debug_print("attempting to add connection to %s:%s\n", ip, port);

	if(!rdma_initialized)
		mp_die("can't add connection; client must be initialized first\n");

	if(ch_type == CH_TYPE_REMOTE) {
		struct addrinfo *addr;
		struct rdma_cm_id *cm_id = NULL;

		getaddrinfo(ip, port, NULL, &addr);

		rdma_create_id(ec, &cm_id, NULL, RDMA_PS_TCP);
		rdma_resolve_addr(cm_id, NULL, addr->ai_addr, TIMEOUT_IN_MS);

		freeaddrinfo(addr);

		sockfd = rc_chan_add(cm_id, app_type, pid, polling_loop);

		printf("[RDMA-Client] Creating connection (pid:%u, app_type:%d, status:pending) to %s:%s on sockfd %d\n",
				pid, app_type, ip, port, sockfd);
	}
	else if(ch_type == CH_TYPE_LOCAL) {

	        sockfd = shmem_chan_add(atoi(port), -1, app_type, pid, polling_loop);
		struct conn_context *ctx = s_conn_ctx[sockfd];
		int *arg = malloc(sizeof(int));
		*arg = sockfd;
		if(pthread_create(&ctx->cq_poller_thread, NULL, local_client_thread, arg) != 0)
			mp_die("Failed to create client_thread");
		printf("[Local-Client] Creating connection (pid:%u, app_type:%d, status:pending) to %s:%s on sockfd %d\n",
			pid, app_type, ip, port, sockfd);
	}
	else
		mp_die("Undefined channel type");

	return sockfd;
}

#if 0
struct sockaddr create_connection(char* ip, uint16_t port)
{
	struct addrinfo *addr;
	getaddrinfo(host, port, NULL, &addr);

	rdma_create_id(ec, &cm_id, NULL, RDMA_PS_TCP);
	rdma_resolve_addr(cm_id, NULL, addr->ai_addr, TIMEOUT_IN_MS);
	return addr;
}
#endif

