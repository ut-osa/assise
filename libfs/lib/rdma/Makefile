CC = gcc #-D_GNU_SOURCE # C compiler
#CFLAGS = -fPIC -Wall -Wextra -O2 -g # C flags
CFLAGS = -fPIC -O2 -g
CFLAGS += -I../rdma-core/build/include
LDFLAGS = -shared  # linking flags

RM = rm -f  # rm command
TARGET_LIB = librdma.so # target lib
SRCS = agent.c core_ch.c rdma_ch.c shmem_ch.c verbs.c mr.c messaging.c rpc.c utils.c # source files
OBJS = $(SRCS:.c=.o)

#CPPFLAGS += -DSANITY_CHECK
#CPPFLAGS += -DDEBUG
CPPFLAGS += -DIBV_RATE_LIMITER
CPPFLAGS += -DIBV_WRAPPER_INLINE
CPPFLAGS += -D_GNU_SOURCE
CPPFLAGS += -fvisibility=default

LDFLAGS +=  -Wl,-rpath=../rdma-core/build/lib -L../rdma-core/build/lib
LDLIBS = -lrdmacm -libverbs -lpthread -lrt

.PHONY: all
	all: ${TARGET_LIB}

$(TARGET_LIB): $(OBJS)
		$(CC) -g -o $@ $^ ${LDFLAGS} ${LDLIBS}

#$(SRCS:.c=.d):%.d:%.c
#		$(CC) $(CFLAGS) -MM $< >$@

#include $(SRCS:.c=.d)

.PHONY: clean
clean:
	${RM} ${TARGET_LIB} ${OBJS}

