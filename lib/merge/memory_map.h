#ifndef MEMORY_MAP_H
#define MEMORY_MAP_H

#define PAGE_SIZE                       16384UL
#define MAP_SIZE                        16384UL //(1 << 29) // 512MB
#define MAP_MASK                        (MAP_SIZE - 1)

#define CMB_ADDR                        0xC0000000
#define NUM_OF_CACHE                    100

#define START_ADDR                      0x00000000

#define BLOCK_MAPPING_START_ADDR        START_ADDR
#define BLOCK_MAPPING_END_ADDR          0x001FFFFF   // 2 MB

#define LEAF_CACHE_START_ADDR           0x00200000
#define LEAF_CACHE_BASE_ADDR            (LEAF_CACHE_START_ADDR + sizeof(ITV2Leaf))
#define LEAF_CACHE_END_ADDR             LEAF_CACHE_BASE_ADDR + (NUM_OF_CACHE + 1) * sizeof(ITV2LeafCache)
#define RED_BLACK_TREE_START_ADDR       LEAF_CACHE_END_ADDR
#define RED_BLACK_TREE_BASE_ADDR        (RED_BLACK_TREE_START_ADDR + sizeof(RBTree))
#define RED_BLACK_TREE_END_ADDR         RED_BLACK_TREE_BASE_ADDR + (NUM_OF_CACHE + 1) * sizeof(RBTreeNode)

#define END_ADDR 0x20000000 // 512MB

#endif /* MEMORY_MAP_H */