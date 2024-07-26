#ifndef MEMORY_MAP_H
#define MEMORY_MAP_H

#define PAGE_SIZE                       16384UL
#define MAP_SIZE                        (1 << 29) // 512MB
#define MAP_MASK                        (MAP_SIZE - 1)

#define MAX_NUM_IU                      10000

#define CMB_ADDR                        0xC0000000

#define BLOCK_MAPPING_META_ADDR         0x00000000

#define BLOCK_MAPPING_START_ADDR        (BLOCK_MAPPING_META_ADDR + sizeof(meta_BKMap)) 
#define BLOCK_MAPPING_END_ADDR          0x004FFFFF   // 5 MB

#define APPEND_OPR_START_ADDR           (BLOCK_MAPPING_END_ADDR + 1)
#define APPEND_OPR_END_ADDR             (APPEND_OPR_START_ADDR + 0x063FFFFF)  // 100 MB

#define VALUE_POOL_START_ADDR           (APPEND_OPR_END_ADDR + 1)
#define VALUE_POOL_BASE_ADDR            (VALUE_POOL_START_ADDR + sizeof(VALUE_POOL<T>))
#define VALUE_POOL_END_ADDR             0x1FFFFFFF

#define END_ADDR 0x20000000 // 512MB

#endif /* MEMORY_MAP_H */
