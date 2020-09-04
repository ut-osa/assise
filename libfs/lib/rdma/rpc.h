#ifndef RPC_H
#define RPC_H

#include "rdma_ch.h"
#include "verbs.h"

// -- messaging wrappers
void MP_SEND_MSG_SYNC(int sockfd, int buffer_id, int solicit);
uint32_t MP_SEND_MSG_ASYNC(int sockfd, int buffer_id, int solicit);

// -- msg waiting
void MP_AWAIT_RESPONSE(int sockfd, uint32_t app_id);
void MP_AWAIT_WORK_COMPLETION(int sockfd, uint32_t wr_id);
void MP_AWAIT_PENDING_WORK_COMPLETIONS(int sockfd);

// -- buffer locks
//public
int MP_ACQUIRE_BUFFER(int sockfd, struct app_context ** ptr);
//private
int _mp_acquire_buffer(int sockfd, void ** ptr, int user);
void notify_response(struct conn_context *ctx, uint32_t app_id);

#endif
