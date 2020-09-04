#include "rpc.h"


//------RPC Implementation------

//Send a message using an IBV_WR_SEND operation on a pre-allocated memory buffer [synchronous]
void MP_SEND_MSG_SYNC(int sockfd, int buffer_id, int solicit)
{
	uint32_t wr_id = MP_SEND_MSG_ASYNC(sockfd, buffer_id, solicit);
	MP_AWAIT_WORK_COMPLETION(sockfd, wr_id);
}

//Send a message using an IBV_WR_SEND operation on a pre-allocated memory buffer [asynchronous]
uint32_t MP_SEND_MSG_ASYNC(int sockfd, int buffer_id, int solicit)
{
	uint32_t wr_id = 0;
	struct conn_context *ctx = get_channel_ctx(sockfd);

	if(ctx->ch_type == CH_TYPE_LOCAL) {
		debug_print("sending local message on buffer[%d] - [RPC #%u] [Data: %s]\n",
				buffer_id, ctx->msg_send[buffer_id]->meta.app.id,
				ctx->msg_send[buffer_id]->meta.app.data);

		char ping_msg[1];
		ctx->msg_send[buffer_id]->header.control.ready = 1;

		// send signal (to notify server if it is not currently polling)
		if(send(ctx->realfd , ping_msg , 1 , 0) < 0)
			mp_die("send failed");
	}
	else if(ctx->ch_type == CH_TYPE_REMOTE) {
		debug_print("sending remote message on buffer[%d] - [RPC #%u] [Data: %s]\n",
				buffer_id, ctx->msg_send[buffer_id]->meta.app.id,
				ctx->msg_send[buffer_id]->meta.app.data);

		ctx->msg_send[buffer_id]->id = MSG_CUSTOM;

		wr_id = send_rdma_message(ctx->id, buffer_id);

		if(solicit) {
			//register_pending(id, ctx->msg_send[buffer_id]->meta.app.id);
		}
	}
	else
		mp_die("invalid channel type");

	return wr_id;
}

//------Request await functions------

/* Wait till response is received for an RPC
   
   Note: app_id is application-defined and both send & rcv messages
   must share the same app_id for this to work (which is set by user)
*/

void MP_AWAIT_RESPONSE(int sockfd, uint32_t app_id)
{
	struct conn_context *ctx = get_channel_ctx(sockfd);

	debug_print("spinning till response with seqn %u (last received seqn -> %u)\n",
			app_id, last_compl_wr_id(ctx, 0));

	//FIXME: set proper wraparound for when app_id exceeds uint32_t limit
	//while(ctx->last_rcv_compl < app_id || ((ctx->last_rcv_compl - app_id) > MAX_PENDING)) {
	while(ctx->last_rcv_compl < app_id) {
		ibw_cpu_relax();
	}
}

/* Wait till a send work request is completed.
   
   Useful for doing synchronous rdma operations.
   Note: wr_id of post sends must be monotonically increasing and share the same qp.
   Note: This function is a no-op for shared-memory channels (as they generate no work completions).
*/

void MP_AWAIT_WORK_COMPLETION(int sockfd, uint32_t wr_id)
{
	struct conn_context *ctx = get_channel_ctx(sockfd);

	if(ctx->ch_type == CH_TYPE_LOCAL) {
		// do nothing
	}
	else if(ctx->ch_type == CH_TYPE_REMOTE) {
		debug_print("spinning till WR %u completes (last completed WR -> %u)\n",
				wr_id, last_compl_wr_id(ctx, 1));

		while(cmp_counters(wr_id, last_compl_wr_id(ctx, 1)) > 0) {
			ibw_cpu_relax();
		}
	}
	else
		mp_die("invalid channel type");
}

/* Wait till all current pending send work requests are completed.
   
   Useful for amortizing the cost of spinning.
*/

void MP_AWAIT_PENDING_WORK_COMPLETIONS(int sockfd)
{
	struct conn_context *ctx = get_channel_ctx(sockfd);

	if(ctx->ch_type == CH_TYPE_LOCAL) {
		// do nothing
	}
	else if(ctx->ch_type == CH_TYPE_REMOTE) {
		uint32_t wr_id = ctx->last_send;

		debug_print("spinning till WR %u completes (last completed WR -> %u)\n",
				wr_id, last_compl_wr_id(ctx, 1));

		while(cmp_counters(wr_id, last_compl_wr_id(ctx, 1)) > 0) {
			ibw_cpu_relax();
		}
	}
	else
		mp_die("invalid channel type");
}


//------Buffer acquisition functions------

/* Acquire send buffer for message transmission

   This function should be called before sending a message to reserve a particular buffer
*/
int MP_ACQUIRE_BUFFER(int sockfd, struct app_context ** ptr)
{
	return _mp_acquire_buffer(sockfd, (void **) ptr, 1);
}

__attribute__((visibility ("hidden"))) 
int _mp_acquire_buffer(int sockfd, void ** ptr, int user)
{
	struct conn_context *ctx = get_channel_ctx(sockfd);

	debug_print("DEBUG before ID = %d\n", ctx->send_idx);

	int i = __sync_fetch_and_add(&ctx->send_idx, 1) % MAX_BUFFER;	

	volatile struct message *msg = (volatile struct message *) ctx->msg_send[i];
	if (msg->header.control.used)
		mp_die("Failed to acquire msg buffer. Buffer is locked.");

	debug_print("acquire buffer ID = %d on sockfd %d\n", i, sockfd);

	msg->header.control.used = 1;

	if(user) {
		ctx->msg_send[i]->meta.app.id = 0; //set app.id to zero
		ctx->msg_send[i]->meta.app.data = ctx->msg_send[i]->data; //set a convenience pointer for data
		*ptr = (void *) &ctx->msg_send[i]->meta.app;
	}

	return i;
}

/* Update pending entry to notify waiting threads that rpc response has been received

   This function can be called by the polling background thread, or the thread waiting
   waiting for the rpc completion
*/
__attribute__((visibility ("hidden"))) 
void notify_response(struct conn_context *ctx, uint32_t app_id)
{
	ctx->last_rcv_compl = app_id;

	debug_print("Received msg with seqn %u on sockfd %d\n",
			app_id, ctx->sockfd);
}

