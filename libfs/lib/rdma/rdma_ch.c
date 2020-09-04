#include "rdma_ch.h"

//event channel. used to setup rdma RCs and communicate mr keys
struct rdma_event_channel *ec = NULL;

//local memory regions (for rdma reads/writes)
int num_mrs;
struct mr_context *mrs = NULL;

int msg_size; //msg buffer size

pthread_mutexattr_t attr;
pthread_mutex_t cq_lock;

const int TIMEOUT_IN_MS = 500;
const char* DEFAULT_PORT = "12345";

//static uint32_t cqe_archive[ARCHIVE_SIZE] = {0};
static struct context *s_ctx = NULL;

int exit_rc_loop = 0;

__attribute__((visibility ("hidden"))) 
void build_connection(struct rdma_cm_id *id)
{
	struct ibv_qp_init_attr qp_attr;

	struct conn_context *ctx = (struct conn_context *)id->context;

	if(!ctx)
		mp_die("connection context not built. make sure build_conn_context() is invoked.");

	build_shared_context(id);

	debug_print("building queues for socket #%d on device-%d\n", ctx->sockfd, ctx->devid);

	build_cq_channel(id);

	build_qp_attr(id, &qp_attr);

	if(rdma_create_qp(id, s_ctx->pd[ctx->devid], &qp_attr))
		mp_die("failed to create qp");

	// update connection hash tables
	struct id_record *entry = (struct id_record *)calloc(1, sizeof(struct id_record));
	struct sockaddr_in *addr_p = copy_ipv4_sockaddr(&id->route.addr.src_storage);
	
	if(addr_p == NULL)
		mp_die("compatibility issue: can't use a non-IPv4 address");

	entry->addr = *addr_p;
	entry->qp_num = id->qp->qp_num;
	entry->id = id;

	// register memory
	mr_register(ctx, mrs, num_mrs, msg_size);

	// post receive messages for all buffers
	for(int i=0; i<MAX_BUFFER; i++)
		receive_rdma_message(id, i);

	//add the structure to both hash tables
	HASH_ADD(qp_hh, s_ctx->id_by_qp, qp_num, sizeof(uint32_t), entry);
	HASH_ADD(addr_hh, s_ctx->id_by_addr, addr, sizeof(struct sockaddr_in), entry);
}

__attribute__((visibility ("hidden"))) 
void build_shared_context(struct rdma_cm_id *id)
{
#if 0
	if (s_ctx) {
		if (s_ctx->ctx != verbs)
			mp_die("cannot handle events in more device contexts.");
		return;
	}

	s_ctx = (struct context *)malloc(sizeof(struct context));

	s_ctx->ctx = verbs;
	s_ctx->pd = ibv_alloc_pd(s_ctx->ctx);
	s_ctx->id_by_addr = NULL;
	s_ctx->id_by_qp = NULL;
#else

	struct conn_context *ctx = (struct conn_context *)id->context;

	if (!s_ctx) {
		s_ctx = (struct context *)malloc(sizeof(struct context));
		s_ctx->id_by_addr = NULL;
		s_ctx->id_by_qp = NULL;
		s_ctx->n_dev = 0;

		for(int i=0; i<MAX_DEVICES; i++) {
			s_ctx->ctx[i] = NULL;
			s_ctx->pd[i] = NULL;
		}
	}


	for(int i=0; i<s_ctx->n_dev; i++) {
		if(s_ctx->ctx[i] == id->verbs) {
			ctx->devid = i;
			return;
		}
	}

	if(s_ctx->n_dev == MAX_DEVICES)
		mp_die("failed to allocate new rdma device. try increasing MAX_DEVICES.");

	// allocate new device
	debug_print("initializing rdma device-%d\n", s_ctx->n_dev);
	s_ctx->ctx[s_ctx->n_dev] = id->verbs;
	s_ctx->pd[s_ctx->n_dev] = ibv_alloc_pd(id->verbs);
	ctx->devid = s_ctx->n_dev;
	s_ctx->n_dev++;

#endif
}

__attribute__((visibility ("hidden"))) 
void build_conn_context(struct rdma_cm_id *id, int always_poll)
{
	struct conn_context *ctx = (struct conn_context *)calloc(1, sizeof(struct conn_context));
	ctx->local_mr = (struct ibv_mr **)calloc(MAX_MR, sizeof(struct ibv_mr*));
	ctx->remote_mr = (struct mr_context **)calloc(MAX_MR, sizeof(struct mr_context*));
	ctx->remote_mr_ready = (int *)calloc(MAX_MR, sizeof(int));
	ctx->local_mr_ready = (int *)calloc(MAX_MR, sizeof(int));
	ctx->local_mr_sent = (int *)calloc(MAX_MR, sizeof(int));
	ctx->msg_send_mr = (struct ibv_mr **)calloc(MAX_BUFFER, sizeof(struct ibv_mr*));
	ctx->msg_rcv_mr = (struct ibv_mr **)calloc(MAX_BUFFER, sizeof(struct ibv_mr*));
	ctx->msg_send = (struct message **)calloc(MAX_BUFFER, sizeof(struct message*));
	ctx->msg_rcv = (struct message **)calloc(MAX_BUFFER, sizeof(struct message*));
	ctx->send_slots = (int *)calloc(MAX_BUFFER, sizeof(int));
	ctx->pendings = NULL;
	ctx->buffer_bindings = NULL;
	ctx->poll_permission = 1;
	ctx->poll_always = always_poll;
	ctx->poll_enable = 1;

	id->context = ctx;
	ctx->id = id;

	pthread_mutex_init(&ctx->wr_lock, NULL);
	pthread_cond_init(&ctx->wr_completed, NULL);
	pthread_spin_init(&ctx->post_lock, PTHREAD_PROCESS_PRIVATE);
	pthread_spin_init(&ctx->buffer_lock, PTHREAD_PROCESS_PRIVATE);
}

__attribute__((visibility ("hidden"))) 
void build_cq_channel(struct rdma_cm_id *id)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	int idx = ctx->devid;

	// create completion queue channel
	ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx[idx]);

	if (!ctx->comp_channel)
		mp_die("ibv_create_comp_channel() failed\n");

	ctx->cq = ibv_create_cq(s_ctx->ctx[idx], 50, NULL, ctx->comp_channel, 0); /* cqe=10 is arbitrary */

	if (!ctx->cq)
		mp_die("ibv_create_cq() failed\n");

	ibv_req_notify_cq(ctx->cq, 0);

	// poll completion queues in the background (always or only during bootstrap)
#if 0
	printf("creating background thread to poll completions (spinning)\n");
	pthread_create(&ctx->cq_poller_thread, NULL, poll_cq_spinning_loop, ctx);
#else
	printf("creating background thread to poll completions (blocking)\n");
	pthread_create(&ctx->cq_poller_thread, NULL, poll_cq_blocking_loop, ctx);
#endif

#if 0
	//bind polling thread to a specific core to avoid contention
	cpu_set_t cpuset;
	int max_cpu_available = 0;
	int core_id = 1;

	CPU_ZERO(&cpuset);
	sched_getaffinity(0, sizeof(cpuset), &cpuset);

	// Search for the last / highest cpu being set
	for (int i = 0; i < 8 * sizeof(cpu_set_t); i++) {
		if (!CPU_ISSET(i, &cpuset)) {
			max_cpu_available = i;
			break;
		}
	}
	if(max_cpu_available <= 0)
		mp_die("unexpected config; # of available cpu cores must be > 0");

	core_id = (ctx->sockfd) % (max_cpu_available-1) + 2;

	printf("assigning poll_cq loop for sockfd %d to core %d\n", ctx->sockfd, core_id); 

	CPU_ZERO(&cpuset);
	CPU_SET(core_id, &cpuset); //try to bind pollers to different cores

	int ret = pthread_setaffinity_np(ctx->cq_poller_thread, sizeof(cpu_set_t), &cpuset);
	if(ret != 0)
		mp_die("failed to bind polling thread to a CPU core");
#endif
}

__attribute__((visibility ("hidden"))) 
void build_params(struct rdma_conn_param *params)
{
	memset(params, 0, sizeof(*params));

	params->initiator_depth = params->responder_resources = 1;
	params->rnr_retry_count = 7; /* infinite retry */
}

__attribute__((visibility ("hidden"))) 
void build_qp_attr(struct rdma_cm_id *id, struct ibv_qp_init_attr *qp_attr)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	memset(qp_attr, 0, sizeof(*qp_attr));

	qp_attr->send_cq = ctx->cq;
	qp_attr->recv_cq = ctx->cq;
	qp_attr->qp_type = IBV_QPT_RC;

	qp_attr->cap.max_send_wr = MAX_SEND_QUEUE_SIZE;
	qp_attr->cap.max_recv_wr = MAX_RECV_QUEUE_SIZE;
	qp_attr->cap.max_send_sge = 16;
	qp_attr->cap.max_recv_sge = 16;
}

__attribute__((visibility ("hidden"))) 
void event_loop(struct rdma_event_channel *ec, int exit_on_connect, int exit_on_disconnect)
{
	struct rdma_cm_event *event = NULL;
	struct rdma_conn_param cm_params;
	build_params(&cm_params);

	while (rdma_get_cm_event(ec, &event) == 0) {
		struct rdma_cm_event event_copy;

		memcpy(&event_copy, event, sizeof(*event));
		debug_print("received event[%d]: %s\n", event_copy.event, rdma_event_str(event_copy.event));

		rdma_ack_cm_event(event);

		if (event_copy.event == RDMA_CM_EVENT_ADDR_RESOLVED) {
			build_connection(event_copy.id);

			rdma_resolve_route(event_copy.id, TIMEOUT_IN_MS);

		}
		else if (event_copy.event == RDMA_CM_EVENT_ROUTE_RESOLVED) {
			rdma_connect(event_copy.id, &cm_params);

		}
		else if (event_copy.event == RDMA_CM_EVENT_CONNECT_REQUEST) {
			rc_chan_add(event_copy.id, -1, -1, 1);
			build_connection(event_copy.id);

			rdma_accept(event_copy.id, &cm_params);
		}
		else if (event_copy.event == RDMA_CM_EVENT_ESTABLISHED) {
			rc_chan_connect(event_copy.id);

			if (exit_on_connect)
				break;
		}
		else if (event_copy.event == RDMA_CM_EVENT_DISCONNECTED) {

			rc_chan_disconnect(event_copy.id);

			if (exit_on_disconnect) {
				rc_chan_clear(event_copy.id);
				break;
			}

		}
		else if (event_copy.event == RDMA_CM_EVENT_TIMEWAIT_EXIT) {
			//this event indicates that the recently destroyed queue pair is ready to be reused
			//at this point, clean up any allocated memory for connection
			rc_chan_clear(event_copy.id);
		}
		else {
			mp_die("unknown event");
		}
	}
}

//poll completions in a looping (blocking)
__attribute__((visibility ("hidden"))) 
void * poll_cq_blocking_loop(void *ctx)
{
	//if(stick_this_thread_to_core(POLL_THREAD_CORE_ID) == EINVAL)
	//	mp_die("failed to pin poll thread to core");

	struct ibv_cq *cq;
	struct ibv_wc wc;
	void *ev_ctx;

	//int ran_once = 0;
	while(((struct conn_context*)ctx)->poll_enable) {
		ibv_get_cq_event(((struct conn_context*)ctx)->comp_channel, &cq, &ev_ctx);
		ibv_ack_cq_events(cq, 1);
		ibv_req_notify_cq(cq, 0);
		poll_cq(((struct conn_context*)ctx)->cq, &wc);
	}

	printf("end poll_cq loop for sockfd %d\n", ((struct conn_context*)ctx)->sockfd);
	return NULL;
}

__attribute__((visibility ("hidden"))) 
void * poll_cq_spinning_loop(void *ctx)
{
	//if(stick_this_thread_to_core(POLL_THREAD_CORE_ID) == EINVAL)
	//	mp_die("failed to pin poll thread to core");

	struct ibv_wc wc;
	//int ran_once = 0;

	while(((struct conn_context*)ctx)->poll_enable) {
		if(((struct conn_context*)ctx)->poll_permission)
			poll_cq(((struct conn_context*)ctx)->cq, &wc);	
		ibw_cpu_relax();
	}

	printf("end poll_cq loop for sockfd %d\n", ((struct conn_context*)ctx)->sockfd);
	return NULL;
}

__attribute__((visibility ("hidden"))) 
inline void poll_cq(struct ibv_cq *cq, struct ibv_wc *wc)
{
	while(ibv_poll_cq(cq, 1, wc)) {
		if (wc->status == IBV_WC_SUCCESS) {

			//debug_print("trigger completion callback\n");
			rc_process_completion(wc);
		}
		else {
#ifdef DEBUG
			const char *descr;
			descr = ibv_wc_status_str(wc->status);
			debug_print("COMPLETION FAILURE (%s WR #%lu) status[%d] = %s\n",
					(wc->opcode & IBV_WC_RECV)?"RECV":"SEND",
					wc->wr_id, wc->status, descr);
#endif
			mp_die("poll_cq: status is not IBV_WC_SUCCESS");
		}
	}
}

__attribute__((visibility ("hidden"))) 
int rc_chan_add(struct rdma_cm_id *id, int app_type, pid_t pid, int poll_loop)
{
	int sockfd = find_first_empty_bit_and_set(s_conn_bitmap, MAX_CONNECTIONS);

	if(sockfd < 0)
		mp_die("can't open new connection; number of open sockets == MAX_CONNECTIONS");

	debug_print("adding connection on socket #%d\n", sockfd);

	//pre-emptively build ctx for connection; allows clients to poll state of connection
	build_conn_context(id, poll_loop);

	struct conn_context *ctx = (struct conn_context *) id->context;

	//FIXME: initialize port number (ctx->portno)
	s_conn_bitmap[sockfd] = 1;
	ctx->sockfd = sockfd;
	ctx->ch_type = CH_TYPE_REMOTE;
	ctx->app_type = app_type;
	ctx->pid = pid;
	s_conn_ctx[sockfd] = ctx;

	return sockfd;
}

__attribute__((visibility ("hidden"))) 
void rc_chan_connect(struct rdma_cm_id *id)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	//Deprecated: CH_CONNECTION_ACTIVE state is no longer used
	//set_channel_state(ctx, CH_CONNECTION_ACTIVE);
	//printf("Connection established [sockfd:%d]\n", ctx->sockfd);

	if(!ibw_cmpxchg(&ctx->mr_init_sent, 0, 1)) {
		int i = create_message(id, MSG_INIT, num_mrs);
		printf("SEND --> MSG_INIT [advertising %d memory regions]\n", num_mrs);
		send_rdma_message(id, i);
	}
}

__attribute__((visibility ("hidden"))) 
void rc_chan_disconnect(struct rdma_cm_id *id)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	app_disc_event(ctx->sockfd);

	set_channel_state(ctx, CH_CONNECTION_TERMINATED);

	printf("Connection terminated [sockfd:%d]\n", ctx->sockfd);


#if 1
	//delete from hashtables
	struct id_record *entry = NULL;
	HASH_FIND(qp_hh, s_ctx->id_by_qp, &id->qp->qp_num, sizeof(id->qp->qp_num), entry);
	if(!entry)
		mp_die("hash delete failed; id doesn't exist");
	HASH_DELETE(qp_hh, s_ctx->id_by_qp, entry);
	HASH_DELETE(addr_hh, s_ctx->id_by_addr, entry);
	free(entry);

	struct app_response *current_p, *tmp_p;
	struct buffer_record *current_b, *tmp_b;

	HASH_ITER(hh, ctx->pendings, current_p, tmp_p) {
		HASH_DEL(ctx->pendings,current_p);
		free(current_p);          
	}

	HASH_ITER(hh, ctx->buffer_bindings, current_b, tmp_b) {
		HASH_DEL(ctx->buffer_bindings,current_b);
		free(current_b);          
	}

	//destroy queue pair and disconnect
	rdma_destroy_qp(id);
	rdma_disconnect(id);
#endif
	//free(ctx);
}

__attribute__((visibility ("hidden"))) 
void rc_chan_clear(struct rdma_cm_id *id)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	if(!mp_is_channel_terminated(ctx->sockfd))
		mp_die("can't clear metadata for non-terminated connection");

	debug_print("clearing connection metadata for socket #%d\n", ctx->sockfd);
	s_conn_bitmap[ctx->sockfd] = 0;
	s_conn_ctx[ctx->sockfd] = NULL;

	for(int i=0; i<MAX_MR; i++) {
		if(ctx->local_mr_ready[i]) {
			debug_print("deregistering mr[addr:%lx, len:%lu]\n",
					(uintptr_t)ctx->local_mr[i]->addr, ctx->local_mr[i]->length);
			ibv_dereg_mr(ctx->local_mr[i]);
		}
		if(ctx->remote_mr_ready[i])
			free(ctx->remote_mr[i]);
	}

	free(ctx->local_mr);
	free(ctx->local_mr_ready);
	free(ctx->local_mr_sent);
	free(ctx->remote_mr);
	free(ctx->remote_mr_ready);

	for(int i=0; i<MAX_BUFFER; i++) {
		debug_print("deregistering msg_send_mr[addr:%lx, len:%lu]\n",
				(uintptr_t)ctx->msg_send_mr[i]->addr, ctx->msg_send_mr[i]->length);
		debug_print("deregistering msg_rcv_mr[addr:%lx, len:%lu]\n",
				(uintptr_t)ctx->msg_rcv_mr[i]->addr, ctx->msg_rcv_mr[i]->length);
		ibv_dereg_mr(ctx->msg_send_mr[i]);
		ibv_dereg_mr(ctx->msg_rcv_mr[i]);
		free(ctx->msg_send[i]);
		free(ctx->msg_rcv[i]);
	}

	free(ctx->msg_send_mr);
	free(ctx->msg_rcv_mr);
	free(ctx->msg_send);
	free(ctx->msg_rcv);
 
	free(ctx);
	free(id);
}


__attribute__((visibility ("hidden"))) 
void rc_process_completion(struct ibv_wc *wc)
{
	struct rdma_cm_id *id = find_connection_by_wc(wc);
	struct conn_context *ctx = (struct conn_context *)id->context;

	if (wc->opcode & IBV_WC_RECV) {
		debug_print("COMPLETION --> (RECV WR #%lu) [qp_num %u]\n", wc->wr_id, id->qp->qp_num);

		uint32_t rcv_i = wc->wr_id;
		//Immediate completions can serve as ACKs or messages
		if(wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM) { 
			uint32_t app_id = ntohl(wc->imm_data);
			debug_print("application callback: seqn = %u\n", app_id);
			if(app_id) {
				notify_response(ctx, app_id);
				struct app_context imm_msg;
				imm_msg.id = app_id;
				imm_msg.sockfd = ctx->sockfd;
				imm_msg.data = 0;
				app_recv_event(&imm_msg);
			}
		}
		else if (ctx->msg_rcv[rcv_i]->id == MSG_INIT) {
			if(ctx->app_type < 0) {
				ctx->app_type = ctx->msg_rcv[rcv_i]->meta.mr.type;
				ctx->pid = ctx->msg_rcv[rcv_i]->meta.mr.addr;
			}
			ctx->remote_mr_total = ctx->msg_rcv[rcv_i]->meta.mr.length;
			ctx->mr_init_recv = 1;
			printf("RECV <-- MSG_INIT [remote node advertises %d memory regions]\n",
						ctx->remote_mr_total);

			if(!ibw_cmpxchg(&ctx->mr_init_sent, 0, 1)) {
				int send_i = create_message(id, MSG_INIT, num_mrs);
				printf("SEND --> MSG_INIT [advertising %d memory regions]\n", num_mrs);
				send_rdma_message(id, send_i);
			}

		}
		else if (ctx->msg_rcv[rcv_i]->id == MSG_MR) {
			int idx = ctx->msg_rcv[rcv_i]->meta.mr.type;
			//printf("%s", "RECV <-- MSG_MR\n");
			if(idx > MAX_MR-1)
				mp_die("memory region number outside of MAX_MR");
			ctx->remote_mr[idx] = (struct mr_context *)calloc(1, sizeof(struct mr_context));
			ctx->remote_mr[idx]->type = idx;
			ctx->remote_mr[idx]->addr = ctx->msg_rcv[rcv_i]->meta.mr.addr;
			ctx->remote_mr[idx]->length = ctx->msg_rcv[rcv_i]->meta.mr.length;
			ctx->remote_mr[idx]->rkey = ctx->msg_rcv[rcv_i]->meta.mr.rkey;
			ctx->remote_mr_ready[idx] = 1;

			if(mr_all_synced(ctx)) {
				debug_print("[DEBUG] RECV COMPL - ALL SYNCED: buffer %d\n", rcv_i);
				set_channel_state(ctx, CH_CONNECTION_READY);
				app_conn_event(ctx->sockfd);
			}

			//send an ACK
			//create_message(id, MSG_MR_ACK);
			//send_rdma_message(id);
		}
		else if (ctx->msg_rcv[rcv_i]->id == MSG_READY) {
			//do nothing (deprecated)
		}
		else if (ctx->msg_rcv[rcv_i]->id == MSG_DONE) {
			//do nothing (deprecated)
		}
		else if (ctx->msg_rcv[rcv_i]->id == MSG_CUSTOM) {
			ctx->msg_rcv[rcv_i]->meta.app.sockfd = ctx->sockfd;

			//adding convenience pointers to data blocks
			ctx->msg_rcv[rcv_i]->meta.app.data = ctx->msg_rcv[rcv_i]->data;

			uint32_t app_id = ctx->msg_rcv[rcv_i]->meta.app.id; 

			debug_print("application callback: seqn = %u\n", app_id);
			
			//debug_print("trigger application callback\n");
			app_recv_event(&ctx->msg_rcv[rcv_i]->meta.app);

			//only trigger delivery notifications for app_ids greater than 0
			if(app_id) {
 				notify_response(ctx, app_id);
			}	
		}
		else {
			printf("invalid completion event with undefined id (%i) \n", ctx->msg_rcv[rcv_i]->id);
			mp_die("");
		}

		receive_rdma_message(id, rcv_i);
	
	}
	else if(wc->opcode == IBV_WC_RDMA_WRITE) {
		debug_print("COMPLETION --> (SEND WR #%lu) [qp_num %u]\n", wc->wr_id, id->qp->qp_num);
		ctx->last_send_compl = wc->wr_id;
	}
	else if(wc->opcode == IBV_WC_SEND) {
		debug_print("COMPLETION --> (SEND WR #%lu) [qp_num %u]\n", wc->wr_id, id->qp->qp_num);
		ctx->last_send_compl = wc->wr_id;

		int i = rc_release_buffer(ctx->sockfd, wc->wr_id);
		if (ctx->msg_send[i]->id == MSG_INIT || ctx->msg_send[i]->id == MSG_MR) {
			//printf("received MSG_MR_ACK\n");
			if(ctx->msg_send[i]->id == MSG_MR) {
				int idx = ctx->msg_send[i]->meta.mr.type;
				ctx->local_mr_sent[idx] = 1;
			}

			//if all local memory region keys haven't yet been synced, send the next
			if(!mr_all_sent(ctx)) {	
				int j = create_message(id, MSG_MR, 0);
				send_rdma_message(id, j);
				//printf("%s", "SEND --> MSG_MR\n");
			}
			else if(mr_all_synced(ctx)) {
					debug_print("[DEBUG] SEND COMPL - ALL SYNCED: wr_id %lu buffer %d\n", wc->wr_id, i);
					set_channel_state(ctx, CH_CONNECTION_READY);
					app_conn_event(ctx->sockfd);
			}
		}
	}
	else { 
		debug_print("skipping message with opcode:%i, wr_id:%lu \n", wc->opcode, wc->wr_id);
		return;
	}
}

__attribute__((visibility ("hidden"))) 
struct rdma_cm_id* find_connection_by_addr(struct sockaddr_in *addr)
{
	struct id_record *entry = NULL;
	//debug_print("[hash] looking up id with sockaddr: %s:%hu\n",
	//		inet_ntoa(addr->sin_addr), addr->sin_port);
	HASH_FIND(addr_hh, s_ctx->id_by_addr, addr, sizeof(*addr), entry);
	if(!entry)
		mp_die("hash lookup failed; id doesn't exist");
	return entry->id;
}

__attribute__((visibility ("hidden"))) 
struct rdma_cm_id* find_connection_by_wc(struct ibv_wc *wc)
{
	struct id_record *entry = NULL;
	//debug_print("[hash] looking up id with qp_num: %u\n", wc->qp_num);
	HASH_FIND(qp_hh, s_ctx->id_by_qp, &wc->qp_num, sizeof(wc->qp_num), entry);
	if(!entry)
		mp_die("hash lookup failed; id doesn't exist");
	return entry->id;
}

__attribute__((visibility ("hidden"))) 
struct rdma_cm_id* get_connection(int sockfd)
{
	if(sockfd > MAX_CONNECTIONS)
		mp_die("invalid sockfd; must be less than MAX_CONNECTIONS");

	if(s_conn_bitmap[sockfd])
		return s_conn_ctx[sockfd]->id;
	else
		return NULL;
}

//iterate over hashtable and execute anonymous function for each id
//note: function has to accept two arguments of type: rdma_cm_id* & void* (in that order)
__attribute__((visibility ("hidden"))) 
void execute_on_connections(void* custom_arg, void custom_func(struct rdma_cm_id*, void*))
{
	struct id_record *i = NULL;

	for(i=s_ctx->id_by_qp; i!=NULL; i=i->qp_hh.next) {
		custom_func(i->id, custom_arg);
	}
}

//bind acquired buffer to specific wr id
__attribute__((visibility ("hidden"))) 
int rc_bind_buffer(struct rdma_cm_id *id, int buffer, uint32_t wr_id)
{
#if 1
	debug_print("binding buffer[%d] --> (SEND WR #%u)\n", buffer, wr_id);
	struct conn_context *ctx = (struct conn_context *) id->context;

	struct buffer_record *rec = calloc(1, sizeof(struct buffer_record));
	rec->wr_id = wr_id;
	rec->buff_id = buffer;

	/*
	struct buffer_record *current_b, *tmp_b;
	HASH_ITER(hh, ctx->buffer_bindings, current_b, tmp_b) {
		debug_print("buffer_binding record: wr_id:%u buff_id:%u\n", current_b->wr_id, current_b->buff_id);
	}
	*/

	pthread_spin_lock(&ctx->buffer_lock);
	HASH_ADD(hh, ctx->buffer_bindings, wr_id, sizeof(rec->wr_id), rec);
	pthread_spin_unlock(&ctx->buffer_lock);
	return 1;
#endif
	//return 1;
}

__attribute__((visibility ("hidden"))) 
int rc_release_buffer(int sockfd, uint32_t wr_id)
{
#if 1
	struct conn_context *ctx = get_channel_ctx(sockfd);
	struct buffer_record *b;

	pthread_spin_lock(&ctx->buffer_lock);
	HASH_FIND(hh, ctx->buffer_bindings, &wr_id, sizeof(wr_id), b);
	if(b) {
		debug_print("released buffer[%d] --> (SEND WR #%u)\n", b->buff_id, wr_id);
		HASH_DEL(ctx->buffer_bindings, b);
		ctx->msg_send[b->buff_id]->header.control.used = 0;
		int ret = b->buff_id;
		free(b);

		pthread_spin_unlock(&ctx->buffer_lock);
		return ret;
	}
	else {
		pthread_spin_unlock(&ctx->buffer_lock);
		mp_die("failed to release buffer. possible race condition.\n");
	}

	return -1;
#endif
	//return 0;
}

__attribute__((visibility ("hidden"))) 
struct ibv_pd * rc_get_pd(struct rdma_cm_id *id)
{
	struct conn_context *ctx = (struct conn_context *) id->context;

	if(ctx->devid > MAX_DEVICES-1)
		mp_die("invalid rdma device index for connection.");

	return s_ctx->pd[ctx->devid];
}

