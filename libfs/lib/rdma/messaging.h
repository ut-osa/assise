#ifndef RDMA_MESSAGING_H
#define RDMA_MESSAGING_H

#include <stddef.h>
#include <stdint.h>

#include "common.h"
#include "utils.h"
#include "mr.h"

enum message_id
{
	MSG_INVALID = 0,
	MSG_INIT,
	MSG_MR,
	MSG_READY,
	MSG_DONE,
	MSG_CUSTOM
};

// control word is 32-bit, because it must be possible to atomically write it
typedef uint32_t msg_control_t;
#define MSG_USED_BITS  1
#define MSG_READY_BITS 1
#define MSG_HEADER_BITS 30

struct msg_control {
	msg_control_t used:MSG_USED_BITS;
	msg_control_t ready:MSG_READY_BITS;
	msg_control_t header:MSG_HEADER_BITS;
	msg_control_t token:32;
};



struct message
{
	int id;

	union
	{
		struct mr_context mr;
		struct app_context app;
	} meta;

	union {
		struct msg_control control;
		uint64_t raw;
	} header;

	char data[];
};

//STATIC_ASSERT((sizeof(struct message)%CACHELINE_BYTES)==0,
//			   "Size of message is not a multiple of cache-line size");

int create_message(struct rdma_cm_id *id, int msg_type, int value);

#endif
