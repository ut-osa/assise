#include "verbs.h"
#include "messaging.h"

IBV_WRAPPER_FUNC(READ)
IBV_WRAPPER_FUNC(WRITE)
IBV_WRAPPER_FUNC(WRITE_WITH_IMM)

void IBV_WRAPPER_OP_SYNC(int sockfd, rdma_meta_t *meta, int local_id, int remote_id, int opcode)
{
	uint32_t wr_id = send_rdma_operation(get_connection(sockfd), meta, local_id, remote_id, opcode);
	MP_AWAIT_WORK_COMPLETION(sockfd, wr_id);
}

uint32_t IBV_WRAPPER_OP_ASYNC(int sockfd, rdma_meta_t *meta,
		int local_id, int remote_id, int opcode)
{
	uint32_t wr_id = send_rdma_operation(get_connection(sockfd), meta, local_id, remote_id, opcode);
	return wr_id;
}

__attribute__((visibility ("hidden"))) 
uint32_t send_rdma_operation(struct rdma_cm_id *id, rdma_meta_t *meta, int local_id, int remote_id, int opcode)
{
	int timeout = 5; //set bootstrap timeout to 5 sec
	struct mr_context *remote_mr = NULL;
	struct ibv_mr *local_mr = NULL;
	int one_sided = op_one_sided(opcode);
	struct conn_context *ctx = (struct conn_context *)id->context;
	rdma_meta_t *next_meta = NULL;
	struct ibv_send_wr *wr_head = NULL;
	struct ibv_send_wr *wr = NULL;
	struct ibv_send_wr *bad_wr = NULL;
	int opcount = 0;
	int ret;

	if(ctx->ch_type != CH_TYPE_REMOTE)
		mp_die("unsupported channel type");

	if(local_id > MAX_MR || remote_id > MAX_MR)
		mp_die("invalid memory regions specified");

	while(!mr_local_ready(ctx, local_id) || (one_sided && !mr_remote_ready(ctx, remote_id))) {
		if(timeout == 0)
			mp_die("failed to issue sync; no metadata available for remote mr\n");
		debug_print("keys haven't yet been received; sleeping for 1 sec...\n");
		timeout--;
		sleep(1);
	}

	local_mr = ctx->local_mr[local_id];
	if(one_sided)
		remote_mr = ctx->remote_mr[remote_id];

	pthread_mutex_lock(&ctx->wr_lock);
	//pthread_spin_lock(&ctx->post_lock);
	do {	
		opcount++;

		if(wr) {
			wr->next = (struct ibv_send_wr*) malloc(sizeof(struct ibv_send_wr));
			wr = wr->next;
		}
		else {
			wr = (struct ibv_send_wr*) malloc(sizeof(struct ibv_send_wr));
			wr_head = wr;
		}

		memset(wr, 0, sizeof(struct ibv_send_wr));

		for(int i=0; i<meta->sge_count; i++) {
			meta->sge_entries[i].lkey = local_mr->lkey;
		}

		if(one_sided) {
			wr->wr.rdma.remote_addr = meta->addr;
			wr->wr.rdma.rkey = remote_mr->rkey;
		}

#ifdef SANITY_CHECK
		uint64_t total_len = 0;
		for(int i=0; i<meta->sge_count; i++)
		{
			total_len += meta->sge_entries[i].length;
			if(!IBV_WITHIN_MR_RANGE((&meta->sge_entries[i]), local_mr)) {
				debug_print("failed to sync. given[addr:%lx, len:%u] - mr[addr:%lx, len:%lu]\n",
						meta->sge_entries[i].addr, meta->sge_entries[i].length,
						remote_mr->addr, remote_mr->length);
				mp_die("request outside bounds of registered mr");
			}
		}

		if(meta->length != total_len)
			exit(EXIT_FAILURE);

		if(one_sided && total_len && !IBV_WITHIN_MR_RANGE(meta, remote_mr)) {
			debug_print("failed to sync. given[addr:%lx, len:%lu] - mr[addr:%lx, len:%lu]\n",
					meta->addr, meta->length, remote_mr->addr, remote_mr->length);
			mp_die("request outside bounds of registered mr");
		}
#endif

		//struct ibv_send_wr wr, *bad_wr = NULL;
		//memset(&wr, 0, sizeof(wr));

		wr->wr_id = next_wr_id(ctx, 1);
		wr->opcode = opcode;

		wr->send_flags = IBV_SEND_SIGNALED;

#ifdef IBV_WRAPPER_INLINE
		//FIXME: find an appropriate cut-off point
		//hardcoded to a sane value for now
		if(meta->length < IBV_INLINE_THRESHOLD && opcode != IBV_WR_RDMA_READ) {
			wr->send_flags |= IBV_SEND_INLINE;
			debug_print("OPTIMIZE - inlining msg with size = %lu bytes\n", meta->length);
		}
#endif

		wr->num_sge = meta->sge_count;

		if(wr->num_sge)
			wr->sg_list = meta->sge_entries;
		else
			wr->sg_list = NULL;

		if(opcode == IBV_WR_RDMA_WRITE_WITH_IMM)
			wr->imm_data = htonl(meta->imm);

#ifdef IBV_RATE_LIMITER
		while(diff_counters(wr->wr_id, last_compl_wr_id(ctx, 1)) >= MAX_SEND_QUEUE_SIZE) {
			ibw_cpu_relax();
	       }
#endif

		debug_print("%s (SEND WR #%lu) [opcode %d, remote addr %lx, len %lu, qp_num %u]\n",
				stringify_verb(opcode), wr->wr_id, opcode, meta->addr, meta->length, id->qp->qp_num);

		for(int i=0; i<wr->num_sge; i++)
			debug_print("----------- sge%d [addr %lx, length %u]\n", i, wr->sg_list[i].addr, wr->sg_list[i].length);
		
		meta = meta->next;

	} while(meta); //loop to batch rdma operations

	debug_print("POST --> %s (SEND WR %lu) [batch size = %d]\n", stringify_verb(opcode), wr_head->wr_id, opcount);
	ret = ibv_post_send(id->qp, wr_head, &bad_wr);

	pthread_mutex_unlock(&ctx->wr_lock);
	//pthread_spin_unlock(&ctx->post_lock);

	if(ret) {
		printf("ibv_post_send: errno = %d\n", ret);
		mp_die("failed to post rdma operation");
	}

	uint32_t wr_id = wr_head->wr_id;
	
	while(wr_head) {
		wr = wr_head;
		wr_head = wr_head->next;
		free(wr);
	}

	return wr_id;
}


/* Perform a post_send for an RDMA message

   TODO MERGE with function 'send_rdma_operation'. 
*/

__attribute__((visibility ("hidden"))) 
uint32_t send_rdma_message(struct rdma_cm_id *id, int buffer)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;
	int ret;

	memset(&wr, 0, sizeof(wr));

	//wr.wr_id = (uintptr_t)id;
	wr.wr_id = next_wr_id(ctx, 2);
	wr.opcode = IBV_WR_SEND;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.send_flags = IBV_SEND_SIGNALED;

	sge.addr = (uintptr_t)ctx->msg_send[buffer];
	sge.length = sizeof(*ctx->msg_send[buffer]) + sizeof(char)*msg_size;
	sge.lkey = ctx->msg_send_mr[buffer]->lkey;

	if(rc_bind_buffer(id, buffer, wr.wr_id)) {
#ifdef IBV_RATE_LIMITER
		while(diff_counters(wr.wr_id, last_compl_wr_id(ctx, 1)) >= MAX_SEND_QUEUE_SIZE) {
			ibw_cpu_relax();
		}
#endif
		debug_print("POST --> (SEND WR #%lu) [addr %lx, len %u, qp_num %u]\n",
			wr.wr_id, sge.addr, sge.length, id->qp->qp_num);

		ret = ibv_post_send(id->qp, &wr, &bad_wr);
		if(ret) {
			printf("ibv_post_send: errno = %d\n", ret);
			mp_die("failed to post rdma operation");
		}
	}
	else
		mp_die("failed to bind send buffer");
	
	return wr.wr_id;
}

__attribute__((visibility ("hidden"))) 
void receive_rdma_message(struct rdma_cm_id *id, int buffer)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	struct ibv_recv_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	memset(&wr, 0, sizeof(wr));

	wr.wr_id = buffer;
	wr.sg_list = &sge;
	wr.num_sge = 1;

	sge.addr = (uintptr_t)ctx->msg_rcv[buffer];
	sge.length = sizeof(*ctx->msg_rcv[buffer]) + sizeof(char)*msg_size;
	sge.lkey = ctx->msg_rcv_mr[buffer]->lkey;

	debug_print("POST --> (RECV WR #%lu) [addr %lx, len %u, qp_num %u]\n",
			wr.wr_id, sge.addr, sge.length, id->qp->qp_num);

	ibv_post_recv(id->qp, &wr, &bad_wr);
}

__attribute__((visibility ("hidden"))) 
void receive_imm(struct rdma_cm_id *id, int buffer)
{
	struct conn_context *ctx = (struct conn_context *)id->context;

	struct ibv_recv_wr wr, *bad_wr = NULL;

	memset(&wr, 0, sizeof(wr));

	wr.wr_id = buffer;
	wr.sg_list = NULL;
	wr.num_sge = 0;

	debug_print("POST --> (RECV IMM WR #%lu)\n", wr.wr_id);

	ibv_post_recv(id->qp, &wr, &bad_wr);
}



