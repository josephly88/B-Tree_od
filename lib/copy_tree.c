#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>

#define FATAL do { fprintf(stderr, "Error at line %d, file %s (%d) [%s]\n", \
  __LINE__, __FILE__, errno, strerror(errno)); exit(1); } while(0)

#define BLK_SIZE 16384UL
#define CP 70000 // 65536 * 16KB = 1GB, support > 1GB

int main(int argc, char** argv){

    if(argc < 3){
        fprintf(stderr, "%s file {c, p}\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int fd, tfd;

    // Open files
    if((fd = open("/dev/nvme0n1", O_RDWR | O_SYNC)) == -1) FATAL;
    if((tfd = open(argv[1], O_RDWR|O_CREAT, 0644)) == -1) FATAL;
    char command[100] = " truncate -s 20GiB ";
    strcat(command, argv[1]);
    system(command);

    int dest, src;
    if(argv[2][0] == 'c'){
        dest = tfd;
        src = fd;
    }
    else if(argv[2][0] == 'p'){
        dest = fd;
        src = tfd;
    }
    else
        FATAL;

    unsigned char* buf;
    posix_memalign((void**) &buf, BLK_SIZE, BLK_SIZE);

    int i = 0;
    while(i < CP){
        pread(src, buf, BLK_SIZE, i * BLK_SIZE);
        pwrite(dest, buf, BLK_SIZE, i * BLK_SIZE);
        printf("Progress: %f%%\r", (double)i / CP * 100);
        i++;
    }

    free(buf);

    close(fd);
    close(tfd);

    return 0;
}
