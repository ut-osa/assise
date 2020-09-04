
#ifndef SHMEM_H
#define SHMEM_H

#include <assert.h>
#include <fcntl.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/cdefs.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/time.h>

#include "messaging.h"
#include "common.h"
//__BEGIN_DECLS

int shmem_chan_add(int portno, int realfd, int app_type, pid_t pid, int polling);
void shmem_chan_setup(int sockfd, volatile void *send_buf, volatile void *recv_buf);
void shmem_chan_disconnect(int sockfd);
//void shmem_chan_clear(int sockfd);

void shmem_poll_loop(int sockfd);

void * local_client_thread(void *arg);
void * local_server_thread(void *arg);
void * local_server_loop(void *port);

/**
 * \brief Initialize SHMEM channel state
 *
 * The channel-state structure and buffer must already be allocated.
 *
 * \param		ctx		Pointer to channel-state structure to initialize.
 * \param		send_buf	Pointer to send ring buffer for the channel. Must be aligned to a cacheline.
 * \param		recv_buf	Pointer to recv ring buffer for the channel. Must be aligned to a cacheline.
 * \param		size		Size (in bytes) of buffer.
 */
static inline int shmem_chan_state_init(struct conn_context *ctx, volatile void *send_buf,
		volatile void *recv_buf, size_t msg_size)
{

#if 1
	// check alignment and size of buffer.
	if (msg_size == 0) {
		return -1;
	}

	if (send_buf == NULL || (((uintptr_t)send_buf) % CACHELINE_BYTES) != 0) {
		return -1;
	}

	if (recv_buf == NULL || (((uintptr_t)recv_buf) % CACHELINE_BYTES) != 0) {
		return -1;
	}

	for(int i=0; i<MAX_BUFFER; i++) {
		ctx->msg_send[i] = (struct message *)((char *) send_buf + msg_size * i);
		ctx->msg_rcv[i] = (struct message *)((char *) recv_buf + msg_size * i);
		ctx->msg_rcv[i]->header.raw = 0;
	}
	
	ctx->poll_enable = 1;
	ctx->send_idx = 0;
	ctx->rcv_idx = 0;
#endif
	return 0;
}

/**
 * \brief Return pointer to a message if outstanding on 'ctx'.
 *
 * \param ctx		Pointer to SHMEM channel-state structure.
 *
 * \return Pointer to message if outstanding, or NULL.
 */

//FIXME: need a synchronization mechanism in case of simultaneous polling
static inline volatile struct message *shmem_poll(struct conn_context *ctx)
{
	msg_control_t ctrl_used =  ctx->msg_rcv[ctx->rcv_idx]->header.control.used;
	msg_control_t ctrl_ready = ctx->msg_rcv[ctx->rcv_idx]->header.control.ready;

	if (ctrl_used && ctrl_ready)
		return ctx->msg_rcv[ctx->rcv_idx];
	return NULL;
}

/**
 * \brief Return pointer to a message if outstanding on 'ctx' and
 * advance pointer.
 *
 * \param ctx		Pointer to SHMEM channel-state structure.
 *
 * \return Pointer to message if outstanding, or NULL.
 */
static inline volatile struct message *shmem_recv(struct conn_context *ctx)
{
	volatile struct message *msg = shmem_poll(ctx);

	if (msg != NULL) {
		if (++ctx->rcv_idx == MAX_BUFFER)
			ctx->rcv_idx = 0;
		return msg;
	}
	return NULL;
}

#if 1

static inline volatile bool shmem_can_send(struct conn_context *ctx)
{
	volatile struct message *msg = (volatile struct message *) ctx->msg_send[ctx->send_idx];
	return !msg->header.control.used;
}

static inline void shmem_release_buffer(volatile struct message *msg)
{
	msg->header.control.used = 0;
	msg->header.control.ready = 0;
}

#endif
//__END_DECLS

#endif // SHMEM_H
