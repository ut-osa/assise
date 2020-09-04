#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <err.h>
#include <errno.h>
#include <string.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <ctype.h>
#include <math.h>
#include <time.h>
#include <assert.h>

#include <iostream>
#include <string>
#include <vector>
#include <list>
#include <random>
#include <memory>
#include <fstream>


void show_usage(const char *prog)
{
	std::cerr << "usage: " << prog
		<< "<num_of_files>"
        << std::endl;
}


int main(int argc, char *argv[])
{

    const char *test_dir_prefix = "/mlfs/";

    char *test_file_name = "testfile";

    std::string test_file;

    int fd;

    if(argc != 2)
        show_usage("create_files");

    int count = atoi(argv[1]);

    for(int i=1; i<count+1; i++) {

	    test_file.assign(test_dir_prefix);

	    test_file += std::string(test_file_name) + std::to_string(0) + "-" + std::to_string(i);

        printf("Creating file %s\n", test_file.c_str());
    	fd = open(test_file.c_str(), O_RDWR | O_CREAT,
				S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
		
        if (fd < 0) {
                err(1, "open");
        }

    }

    return 0;
}
