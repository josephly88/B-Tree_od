#ifndef MEMORY_MAP_H
#define MEMORY_MAP_H

#define PAGE_SIZE                       16384UL
#define MAP_SIZE                        16384UL //(1 << 29) // 512MB
#define MAP_MASK                        (MAP_SIZE - 1)

#define CMB_ADDR                        0xC0000000

#define START_ADDR                      0x00000000

#define BLOCK_MAPPING_START_ADDR        START_ADDR
#define BLOCK_MAPPING_END_ADDR          0x001FFFFF   // 2 MB

#define LEAF_CACHE_START_ADDR           0x00200000
#define LEAF_CACHE_BASE_ADDR            (LEAF_CACHE_START_ADDR + sizeof(ITV2Leaf))
#define LEAF_CACHE_END_ADDR             0x006FFFFF  // 5 MB

#define END_ADDR 0x20000000

#endif /* MEMORY_MAP_H */