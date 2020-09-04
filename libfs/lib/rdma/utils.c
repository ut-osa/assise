#include "utils.h"

__attribute__((visibility ("hidden"))) 
void mp_die(const char *reason)
{
	fprintf(stderr, "%s [error code: %d]\n", reason, errno);
	exit(EXIT_FAILURE);
}
