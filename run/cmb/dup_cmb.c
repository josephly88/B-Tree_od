#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <ctype.h>
#include <termios.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <time.h>
  
#define FATAL do { fprintf(stderr, "Error at line %d, file %s (%d) [%s]\n", \
  __LINE__, __FILE__, errno, strerror(errno)); exit(1); } while(0)
 
#define MAP_SIZE 4096UL
#define MAP_MASK (MAP_SIZE - 1)
#define BAR_SIZE (1 << 29) // 0x2000 0000 (512MB)

int main(int argc, char **argv) {
	off_t addr = 0xc0000000;

    int cmb_fd;
    void *map_base, *virt_addr; 
	off_t target;

    off_t map_idx;

    int file_fd;
    void *file_map_base, *file_virt_addr;
    off_t file_target;

    off_t file_map_idx;
	
	if(argc < 3) {
		fprintf(stderr, "\nUsage:\t%s {file} c OR p\n"
            "\tfile: the target file\n"
			"\tc : copy cmb to file\n"
			"\tp : paste file to cmb\n"
			, argv[0]);
		exit(1);
	}

    // cmb
    if((cmb_fd = open("/dev/mem", O_RDWR | O_SYNC)) == -1) FATAL;
    printf("/dev/mem opened.\n"); 
    fflush(stdout);

	target = addr;

    /* Map one page */
    map_base = mmap(0, MAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, cmb_fd, target & ~MAP_MASK);
    if(map_base == (void *) -1) FATAL;
    fflush(stdout);

    map_idx = target & ~MAP_MASK;

    // file
    if((file_fd = open(argv[1], O_RDWR | O_SYNC)) == -1) FATAL;
    printf("%s opened.\n", argv[1]); 
    fflush(stdout);

	file_target = 0x0;

    /* Map one page */
    file_map_base = mmap(0, MAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, file_fd, file_target & ~MAP_MASK);
    if(file_map_base == (void *) -1) FATAL;
    fflush(stdout);

    file_map_idx = file_target & ~MAP_MASK;


	for(int i = 0; i < BAR_SIZE / sizeof(u_int64_t); i+=sizeof(u_int64_t)){
		target += i;
		file_target += i;
       
        if(target & ~MAP_MASK != map_idx){
            /* Map one page */
            map_base = mmap(0, MAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, cmb_fd, target & ~MAP_MASK);
            if(map_base == (void *) -1) FATAL;
            fflush(stdout);

            map_idx = target & ~MAP_MASK;
        }
        if(file_target & ~MAP_MASK != file_map_idx){
            /* Map one page */
            file_map_base = mmap(0, MAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, file_fd, file_target & ~MAP_MASK);
            if(file_map_base == (void *) -1) FATAL;
            fflush(stdout);

            file_map_idx = file_target & ~MAP_MASK;
        }

    	virt_addr = map_base + (target & MAP_MASK);
    	file_virt_addr = file_map_base + (file_target & MAP_MASK);

        if(argv[2][0] == 'c'){
           *((u_int64_t *) file_virt_addr) = *((u_int64_t *) virt_addr);  
        }
        
        if(argv[2][0] == 'p'){
           *((u_int64_t *) virt_addr) = *((u_int64_t *) file_virt_addr);  
        }


	}
	if(munmap(map_base, MAP_SIZE) == -1) FATAL;
	if(munmap(file_map_base, MAP_SIZE) == -1) FATAL;
	
    close(cmb_fd);
    close(file_fd);
    return 0;
}

