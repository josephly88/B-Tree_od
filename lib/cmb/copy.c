#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#define CP_SIZE (1 << 29)
#define MAP_SIZE 16384UL

int main(int argc, char** argv){
    if(argc < 3){
        fprintf(stderr, "%s file {c, p}\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int fd = open("rand.txt", O_RDWR, 0644);
    int tfd = open(argv[1], O_RDWR|O_CREAT, 0644);
    ftruncate(tfd, CP_SIZE);

    u_int64_t* mem;
    u_int64_t* tmem;

    size_t len = CP_SIZE / MAP_SIZE;
    int i = 0;
    while(len--){
        mem = (u_int64_t*) mmap(NULL, MAP_SIZE, PROT_WRITE | PROT_READ, MAP_SHARED, fd, i * MAP_SIZE);
        if(mem == MAP_FAILED) exit(1);
        tmem = (u_int64_t*) mmap(NULL, MAP_SIZE, PROT_WRITE | PROT_READ, MAP_SHARED, tfd, i * MAP_SIZE);
        if(tmem == MAP_FAILED) exit(1);

        u_int64_t* dest = tmem;
        u_int64_t* src = mem;

        size_t page = MAP_SIZE / sizeof(u_int64_t);
        while(page--){
            *(dest++) = *(src++);
        }
        i++;
        printf("Progress: %f%%\r", i * MAP_SIZE / CP_SIZE * 100)

        munmap(mem, MAP_SIZE);
        munmap(tmem, MAP_SIZE);
    }

    close(fd);
    close(tfd);

    return 0;
}