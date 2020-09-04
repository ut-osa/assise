#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <mlfs/mlfs_interface.h>	


int main(int argc, char ** argv)
{
	struct stat st = {0};

	//init_fs();

	//if (stat("/mlfs/zvon/Maildir/tmp/", &st) == -1) {
	  //  mkdir("/mlfs/zvon/Maildir/tmp/", 0700);
	//}

	//if (stat("/mlfs/zvon/Maildir/", &st) == -1) {
	    mkdir("/mlfs/", 0700);
	    mkdir("/mlfs/zvon/", 0700);
	    mkdir("/mlfs/zvon/Maildir/", 0700);
	    pause();
	    //mkdir("/mlfs/zvon/Maildir/cur/", 0700);
	    //mkdir("/mlfs/zvon/Maildir/tmp/", 0700);
	    //mkdir("/mlfs/zvon/Maildir/new/", 0700);
	//}

	//shutdown_fs();
	return 0;
}
