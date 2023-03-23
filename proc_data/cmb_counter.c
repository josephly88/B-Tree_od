/*
 * devmem2.c: Simple program to read/write from/to any location in memory.
 *
 *  Copyright (C) 2000, Jan-Derk Bakker (jdb@lartmaker.nl)
 *
 *
 * This software has been developed for the LART computing board
 * (http://www.lart.tudelft.nl/). The development has been sponsored by
 * the Mobile MultiMedia Communications (http://www.mmc.tudelft.nl/)
 * and Ubiquitous Communications (http://www.ubicom.tudelft.nl/)
 * projects.
 *
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

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
  
#define FATAL do { fprintf(stderr, "Error at line %d, file %s (%d) [%s]\n", \
  __LINE__, __FILE__, errno, strerror(errno)); exit(1); } while(0)
 
#define MAP_SIZE 4096UL
#define MAP_MASK (MAP_SIZE - 1)
#define ADDR 0xC0000000

int main(int argc, char **argv) {
    int fd;
    void *map_base;
    u_int64_t* virt_addr;
 
    if((fd = open("/dev/mem", O_RDWR | O_SYNC)) == -1) FATAL;
    printf("/dev/mem opened.\n"); 
    fflush(stdout);
    
    /* Map one page */
    map_base = mmap(0, MAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, ADDR & ~MAP_MASK);
    if(map_base == (void *) -1) FATAL;
    printf("Memory mapped at address %p.\n", map_base); 
    fflush(stdout);

    virt_addr = (u_int64_t*)map_base;

    if(argc > 1 && strcmp(argv[1],"-r") == 0){
        for(int i = 0; i < 4; i++){
            *virt_addr = 0;
            virt_addr++;
        }
    }

    virt_addr = (u_int64_t*)map_base;
    u_int64_t counter[4];

    for(int i = 0; i < 4; i++){
       counter[i] = *virt_addr;
       virt_addr++;
    }

    printf("Buffer Hit: %lu\n", counter[0]); 
    printf("Buffer Miss: %lu\n", counter[1]); 
    printf("Dirty Page Evict: %lu\n", counter[2]); 
    printf("Page Read from NAND: %lu\n", counter[3]); 
    fflush(stdout);

	if(munmap(map_base, MAP_SIZE) == -1) FATAL;
    close(fd);
    return 0;
}


