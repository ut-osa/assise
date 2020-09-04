#ifndef CORE_CHAN_H
#define CORE_CHAN_H

#include <netinet/in.h>
#include <arpa/inet.h>

#include "globals.h"
#include "utils.h"
#include "common.h"


extern struct conn_context **s_conn_ctx;
extern int *s_conn_bitmap;

enum channel_type
{
	CH_TYPE_REMOTE = 0,
	CH_TYPE_LOCAL,
	CH_TYPE_ALL
};

enum channel_state
{
	CH_CONNECTION_TERMINATED = -1,
	CH_CONNECTION_PENDING,
	CH_CONNECTION_ACTIVE,
	CH_CONNECTION_READY,

};

// -- public --

// * channel attr
int mp_channel_count();
int mp_next_channel(int cur);
int mp_channel_meta(int sockfd);
int mp_channel_rpid(int sockfd);
char* mp_channel_ip(int sockfd);

// -- private --

// * channel state
int mp_is_channel_ready(int sockfd);
int mp_is_channel_active(int sockfd);
int mp_is_channel_terminated(int sockfd);
void set_channel_state(struct conn_context *ctx, int new_state);

// * channel context 
struct conn_context* get_channel_ctx(int sockfd);

#endif
