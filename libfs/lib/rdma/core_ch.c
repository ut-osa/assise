#include "core_ch.h"


int *s_conn_bitmap = NULL;
struct conn_context **s_conn_ctx = NULL;

int mp_channel_count()
{
	return find_bitmap_weight(s_conn_bitmap, MAX_CONNECTIONS); 
}


int mp_next_channel(int cur)
{
	int i = 0;

	if(cur < 0)
		i = find_first_set_bit(s_conn_bitmap, MAX_CONNECTIONS);
	else {
		i = find_next_set_bit(cur, s_conn_bitmap, MAX_CONNECTIONS);
	}

	if(i >= 0)
		return i;
	else
		return -1;	 
}

int mp_channel_meta(int sockfd)
{
	struct conn_context *ctx = get_channel_ctx(sockfd);

	if(ctx)
		return ctx->app_type;
	else
		return -1;
}

int mp_channel_rpid(int sockfd)
{
	struct conn_context *ctx = get_channel_ctx(sockfd);

	if(ctx)
		return ctx->pid;
	else
		return -1;
}


char* mp_channel_ip(int sockfd)
{
	struct conn_context *ctx = get_channel_ctx(sockfd);
	if(ctx) {
		char *s = malloc(sizeof(char)*INET_ADDRSTRLEN);
		if(ctx->ch_type == CH_TYPE_LOCAL) {
			struct sockaddr_in addr;
			socklen_t addr_size = sizeof(struct sockaddr_in);
     			int res = getpeername(ctx->realfd, (struct sockaddr *)&addr, &addr_size);
	     		strcpy(s, inet_ntoa(addr.sin_addr));
			return s;
		}
		else if(ctx->ch_type == CH_TYPE_REMOTE) {
			struct sockaddr_in *addr_in = copy_ipv4_sockaddr(&s_conn_ctx[sockfd]->id->route.addr.dst_storage);
			strcpy(s, inet_ntoa(addr_in->sin_addr));
			return s;
		}
		else
			mp_die("invalid channel type");
	}
	else
		mp_die("channel does not exist");
}

int mp_is_channel_ready(int sockfd)
{
	struct conn_context *ctx = get_channel_ctx(sockfd);
	
	if(ctx) {
		if(ctx->state == CH_CONNECTION_READY)
			return 1;
		else
			return 0;
	}
	else
		return 0;
}

int mp_is_channel_active(int sockfd)
{
#if 0
	struct conn_context *ctx = get_channel_ctx(sockfd);
	
	if(ctx) {
		if(ctx->state == CH_CONNECTION_ACTIVE)
			return 1;
		else
			return 0;
	}
	else
		return 0;
#else
	mp_die("codepath is deprecated");
#endif
}

int mp_is_channel_terminated(int sockfd)
{
	struct conn_context *ctx = get_channel_ctx(sockfd);
	
	if(ctx) {
		if(ctx->state == CH_CONNECTION_TERMINATED)
			return 1;
		else
			return 0;
	}
	else
		return 0;
}

__attribute__((visibility ("hidden"))) 
void set_channel_state(struct conn_context *ctx, int new_state)
{
	if(ctx->state == new_state)
		return;

	debug_print("modify state for socket #%d from %d to %d\n", ctx->sockfd, ctx->state, new_state);

	ctx->state = new_state;
	
	if((ctx->state == CH_CONNECTION_READY && !ctx->poll_always) || ctx->state == CH_CONNECTION_TERMINATED) {
		ctx->poll_enable = 0;	
  		//void *ret;
    		//if(pthread_join(ctx->cq_poller_thread, &ret) != 0)
		//	mp_die("pthread_join() error");

		//XXX: required?
        if(ctx->ch_type == CH_TYPE_REMOTE)
		    pthread_cancel(ctx->cq_poller_thread);
	}
}

__attribute__((visibility ("hidden"))) 
struct conn_context* get_channel_ctx(int sockfd)
{
	if(sockfd > MAX_CONNECTIONS)
		mp_die("invalid sockfd; must be less than MAX_CONNECTIONS");

	if(s_conn_bitmap[sockfd])
		return s_conn_ctx[sockfd];
	else {
		debug_print("[warning]: channel with uid %d does not exist\n", sockfd);
		return NULL;
	}
}

