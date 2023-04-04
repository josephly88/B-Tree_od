#ifndef B_TREE_H //include guard
#define B_TREE_H

#include <iostream>
#include <fstream>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <time.h>
#include <chrono>
#include "memory_map.h"

using namespace std;

#define FATAL do { fprintf(stderr, "Error at line %d, file %s (%d) [%s]\n", \
  __LINE__, __FILE__, errno, strerror(errno)); exit(1); } while(0)

typedef enum {
    COPY_ON_WRITE, 
    REAL_CMB, 
    FAKE_CMB, 
    DRAM
} MODE;

typedef enum {
    BLACK,
    RED
} COLOR;

typedef enum {
    I,
    D,
    U
} OPR_CODE;

ofstream mylog;

bool needsplit;

// ## Verification
int HIT = 0;

// Evaluation Break Down
chrono::duration<double, micro> flash_diff;
chrono::duration<double, micro> cmb_diff;

int breakdown_state = 0;
chrono::duration<double, micro> tmp_diff;
chrono::duration<double, micro> trav_diff;
chrono::duration<double, micro> op_diff;
chrono::duration<double, micro> cow_diff;

// B-Tree
template <typename T> class BTree;
template <typename T> class BTreeNode;
class removeList;
// CMB
class CMB;
class BKMap;
// Leaf Cache
class LEAF_CACHE;
class LRU_QUEUE;
class LRU_QUEUE_ENTRY;
class RBTree;
class RBTreeNode;
// Append operation
template <typename T> class APPEND;
class APPEND_ENTRY;
template <typename T> class VALUE_POOL;

template <typename T>
class BTree{
	int m;				// degree
    int min_num; 		// minimun number of node
	u_int64_t root_id;		// Pointer to root node
	int fd;
	int block_size;
	int block_cap;
    MODE mode;
		
	public:
	    CMB* cmb;
        LEAF_CACHE* leafCache;
        APPEND<T>* append_map;

		BTree(char* filename, int degree, MODE _mode, bool lfcache, bool append);
		~BTree();

		void reopen(int _fd, MODE _mode, bool lfcache, bool append);

		void stat();
        void memory_map_addr();

		void tree_read(int fd, BTree* tree, int bkdown = 0);
		void tree_write(int fd, BTree* tree, int bkdown = 0);

		void node_read(u_int64_t block_id, BTreeNode<T>* node, int bkdown = 0);
		void node_write(u_int64_t block_id, BTreeNode<T>* node, int bkdown = 0);

		u_int64_t get_free_block_id();
		void set_block_id(u_int64_t block_id, bool bit);
		void print_used_block_id();

		void display_tree();
        void inorder_traversal(char* filename);

		void insertion(u_int64_t _k, T _v);
        void search(u_int64_t _k, T* buf);
        void update(u_int64_t _k, T _v);
		void deletion(u_int64_t _k);	
};

template <typename T>
class BTreeNode{

	public:	
		int m;				// degree
		int min_num; 		// minimun number of node
		int num_key;		// the number of keys
		u_int64_t *key;			// keys array
		T *value; 		// value array
		u_int64_t *child_id;	// array of child pointers
		bool is_leaf;		// Is leaf or not
		u_int64_t node_id;
        u_int64_t parent_id;
			
		BTreeNode(int _m, bool _is_leaf, u_int64_t _node_id);
		~BTreeNode();

		void stat();

		void display_tree(BTree<T>* t, MODE mode, int level);
        void inorder_traversal(BTree<T>* t, ofstream &outFile);

		void search(BTree<T>* t, u_int64_t _k, T* buf, u_int64_t rbtree_id = 0);
        u_int64_t update(BTree<T>* t, u_int64_t _k, T _v, removeList** list);

		u_int64_t traverse_insert(BTree<T>* t, u_int64_t _k, T _v, removeList** list);
		u_int64_t direct_insert(BTree<T>* t, u_int64_t _k, T _v, removeList** list, u_int64_t node_id1 = 0, u_int64_t node_id2 = 0);
		u_int64_t split(BTree<T>* t, u_int64_t node_id, u_int64_t parent_id, removeList** list);

		u_int64_t traverse_delete(BTree<T>* t, u_int64_t _k, removeList** _list);
		u_int64_t direct_delete(BTree<T>* t, u_int64_t _k, removeList** _list);
		u_int64_t rebalance(BTree<T>* t, int idx, removeList** _list);
		u_int64_t get_pred(BTree<T>* t);
		u_int64_t get_succ(BTree<T>* t);	
};

class removeList{
	public:
		int id;
		removeList* next;
		removeList(int _id, removeList* _next);
};

class CMB{
	int fd;
    off_t bar_addr;
	void* map_base;
    u_int64_t map_idx;
    void* cache_base;
    MODE mode;

	public:
		CMB(MODE _mode);
		~CMB();

        void* get_map_base();

        void cmb_memcpy(void* dest, void* src, size_t len);
        void remap(off_t offset);

		void read(void* buf, off_t offset, int size);
		void write(off_t offset, void* buf, int size);

        // BKMap
		u_int64_t get_new_node_id();
		u_int64_t get_block_id(u_int64_t node_id);
		void update_node_id(u_int64_t node_id, u_int64_t block_id);
        // Meta data
        u_int64_t get_num_key(u_int64_t node_id);
        void update_num_key(u_int64_t node_id, u_int64_t value);
        u_int64_t get_lower(u_int64_t node_id);
        void update_lower(u_int64_t node_id, u_int64_t value);
        u_int64_t get_upper(u_int64_t node_id);
        void update_upper(u_int64_t node_id, u_int64_t value);
};

class BKMap{
    public:
        u_int64_t block_id;
        u_int64_t num_key;
        u_int64_t lower;
        u_int64_t upper;
};

class LEAF_CACHE{
    public:
        LRU_QUEUE* LRU;
        RBTree* RBTREE;
        u_int64_t hit_lru_id;
        u_int64_t hit_rb_id;

        void rbtree_search_by_key(CMB* cmb, u_int64_t key);
        void rbtree_search_by_node_id(CMB* cmb, u_int64_t node_id);
        u_int64_t get_node_id(u_int64_t rb_id);
        u_int64_t get_lru_id(u_int64_t rb_id);

        void lru_enqueue(CMB* cmb, u_int64_t node_id);
        void lru_dequeue(CMB* cmb);
        void lru_remove(CMB* cmb, u_int64_t lru_id);
        void lru_reenqueue(CMB* cmb, u_int64_t lru_id);
};

class LRU_QUEUE{
    u_int64_t free_Stack_idx;
    u_int64_t free_idx;
    u_int64_t capacity;
    u_int64_t num_of_cache;

    public:
        u_int64_t Q_front_idx;
        u_int64_t Q_rear_idx;

        LRU_QUEUE(CMB* cmb);

        void stat(CMB* cmb);

        bool full();

        u_int64_t get_free_idx(CMB* cmb);
        void free_push(CMB* cmb, u_int64_t idx);
        u_int64_t free_pop(CMB* cmb);

        u_int64_t enqueue(CMB* cmb, u_int64_t rbtree_id, u_int64_t node_id);
        void dequeue(CMB* cmb);
        void remove(CMB* cmb, u_int64_t lru_id);
        void read(CMB* cmb, u_int64_t idx, LRU_QUEUE_ENTRY* buf);

        u_int64_t get_rb_tree_id(CMB* cmb, u_int64_t idx);
        void update_rb_tree_id(CMB* cmb, u_int64_t idx, u_int64_t rb_id);
    };

class LRU_QUEUE_ENTRY{
    public:
        u_int64_t RBTREE_idx;
        u_int64_t node_id;
        u_int64_t lastQ;
        u_int64_t nextQ;
};

class RBTree{
    u_int64_t root_idx;
    u_int64_t capacity;
    u_int64_t num_of_cache;

    public:
        RBTreeNode* pool;

        RBTree();
        ~RBTree();

        u_int64_t insert(CMB* cmb, u_int64_t node_id, u_int64_t LRU_id);
        void insertFix(u_int64_t idx, CMB* cmb);
        void remove(CMB* cmb, u_int64_t rbtree_idx);
        void removeFix(u_int64_t idx, u_int64_t parent_idx, CMB* cmb);
        u_int64_t search_by_key(CMB* cmb, u_int64_t key);
        u_int64_t search_by_node_id(CMB* cmb, u_int64_t node_id);

        void stat(CMB* cmb);
        void display(CMB* cmb);

        void free_push(u_int64_t idx);
        u_int64_t free_pop();

        void leafRotation(u_int64_t x_idx);
        void rightRotation(u_int64_t x_idx);

        u_int64_t succesor(u_int64_t idx);
};

class RBTreeNode{
    public:
        u_int64_t node_id;
        u_int64_t LRU_id;
        u_int64_t left_child_idx;
        u_int64_t right_child_idx;  
        u_int64_t color;    // Red - 1, Black - 0
        u_int64_t parent_idx;   

        RBTreeNode(u_int64_t _node_id, u_int64_t LRU_id, u_int64_t _color, u_int64_t _parent_idx);   

        void display(CMB* cmb, RBTreeNode* pool, u_int64_t idx, int level);
};

template <typename T>
class APPEND{
    public:
        VALUE_POOL<T>* value_pool;

        void append_entry(BTree<T>* t, u_int64_t node_id, OPR_CODE OPR, u_int64_t _k, T _v);
        bool full(CMB* cmb, u_int64_t node_id);
        void reduction(BTree<T>* t, u_int64_t node_id, BTreeNode<T>* node);
        u_int64_t search_entry(CMB* cmb, u_int64_t node_id, u_int64_t key);

        u_int64_t get_num(CMB* cmb, u_int64_t node_id);
        void write_num(CMB* cmb, u_int64_t node_id, u_int64_t num);

        OPR_CODE get_opr(CMB* cmb, u_int64_t node_id, u_int64_t idx);
        u_int64_t get_key(CMB* cmb, u_int64_t node_id, u_int64_t idx);
        T get_value(CMB* cmb, u_int64_t node_id, u_int64_t idx);
        void delete_entries(CMB* cmb, u_int64_t node_id);
};

class APPEND_ENTRY{
    public:
        u_int64_t opr;
        u_int64_t key;
        u_int64_t value_idx;
};

template <typename T>
class VALUE_POOL{
    public:
        u_int64_t free_Stack_idx;
        u_int64_t free_idx;
        u_int64_t capacity;
        u_int64_t num;

        VALUE_POOL(CMB* cmb);

        void stat(CMB* cmb);

        u_int64_t get_free_idx(CMB* cmb);
        void free_push(CMB* cmb, u_int64_t idx);
        u_int64_t free_pop(CMB* cmb);

        void get_value(CMB* cmb, u_int64_t idx, T* buf);
        void write_value(CMB* cmb, u_int64_t idx, T* buf);
};

template <typename T>
BTree<T>::BTree(char* filename, int degree, MODE _mode, bool lfcache, bool append){
    mylog << "BTree()" << endl;

    if(degree > (int)((PAGE_SIZE - sizeof(BTreeNode<T>) - sizeof(u_int64_t)) / (sizeof(u_int64_t) + sizeof(T))) ){
        cout << " Error: Degree exceed " << endl;
        return;
    }

    fd = open(filename, O_RDWR | O_DIRECT);
    block_size = PAGE_SIZE;
    m = degree;
    min_num = (m % 2 == 0) ? m / 2 - 1 : m / 2;
    block_cap = (block_size - sizeof(BTree)) * 8;
    root_id = 0;
    mode = _mode;
    leafCache = NULL;
    append_map = NULL;
    
    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    
    memcpy(buf, this, sizeof(BTree));
    pwrite(fd, buf, PAGE_SIZE, 1 * PAGE_SIZE);

    free(buf);

    set_block_id(0, true); // block 0 is reserved
    set_block_id(1, true);

    if(mode == COPY_ON_WRITE)
        cmb = NULL;
    else{
        // Map CMB
        cmb = new CMB(mode);    
        u_int64_t first_node_id = 1;
        cmb->write(0, &first_node_id, sizeof(u_int64_t));

        // Interval to Leaf Cache Optimization
        if(lfcache){
            leafCache = new LEAF_CACHE();
            leafCache->LRU = new LRU_QUEUE(cmb);
            leafCache->LRU->stat(cmb);
            leafCache->RBTREE = new RBTree();
            leafCache->RBTREE->stat(cmb);
        }     

        // Append entry Optimization
        if(append){
            append_map = new APPEND<T>();
            append_map->value_pool = new VALUE_POOL<T>(cmb);
        }       
    }

}

template <typename T>
BTree<T>::~BTree(){
    mylog << "~BTree()" << endl;
    close(fd);
    if(cmb) delete cmb;
    if(leafCache){
        delete leafCache->LRU;
        delete leafCache->RBTREE;
        delete leafCache;
    } 
    if(append_map){
        delete append_map->value_pool;
        delete append_map;
    }
}

template <typename T>
void BTree<T>::reopen(int _fd, MODE _mode, bool lfcache, bool append){
    mylog << "reopen()" << endl;
    fd = _fd;
    mode = _mode;
    tree_write(fd, this);
    if(mode == COPY_ON_WRITE)
        cmb = NULL;
    else{
        cmb = new CMB(mode);
        if(lfcache){
            leafCache = new LEAF_CACHE();

            off_t addr;

            leafCache->LRU = (LRU_QUEUE*) malloc(sizeof(LRU_QUEUE));
            addr = LRU_QUEUE_START_ADDR;
            cmb->read(leafCache->LRU, addr, sizeof(LRU_QUEUE));

            leafCache->RBTREE = new RBTree();

            u_int64_t itr_idx = leafCache->LRU->Q_front_idx;
            while(itr_idx){
                LRU_QUEUE_ENTRY* buf = new LRU_QUEUE_ENTRY;
                leafCache->LRU->read(cmb, itr_idx, buf);

                u_int64_t rb_idx = leafCache->RBTREE->insert(cmb, buf->node_id, itr_idx);
                addr = LRU_QUEUE_BASE_ADDR + itr_idx * sizeof(LRU_QUEUE_ENTRY) + ((char*)&buf->RBTREE_idx - (char*)buf);
                cmb->write(addr, &rb_idx, sizeof(u_int64_t));

                itr_idx = buf->nextQ;
                delete buf;
            }
        }

        if(append){
            append_map = new APPEND<T>();
            append_map->value_pool = (VALUE_POOL<T>*) malloc(sizeof(VALUE_POOL<T>));
            off_t addr = VALUE_POOL_START_ADDR;
            cmb->read(append_map->value_pool, addr, sizeof(VALUE_POOL<T>));
        }
    }

}

template <typename T>
void BTree<T>::stat(){
    cout << endl;
    cout << "Degree: " << m << endl;
    cout << "fd: " << fd << endl;
    cout << "Block size: " << block_size << endl;
    cout << "Block Capacity: " << block_cap << endl;
    cout << "Root Block ID: " << root_id << endl;
    cout << endl;

    mylog << "BTree.stat()" << endl;
    mylog << "\tDegree: " << m << endl;
    mylog << "\tfd: " << fd << endl;
    mylog << "\tBlock size: " << block_size << endl;
    mylog << "\tBlock Capacity: " << block_cap << endl;
    mylog << "\tRoot Block ID: " << root_id << endl;
    print_used_block_id();
}

template <typename T>
void BTree<T>::memory_map_addr(){
    cout << hex;
    cout << "START_ADDR:\t\t\t0x" << START_ADDR << endl;
    cout << "BLOCK_MAPPING_START_ADDR:\t0x" << BLOCK_MAPPING_START_ADDR << endl;
    cout << "BLOCK_MAPPING_END_ADDR:\t\t0x" << BLOCK_MAPPING_END_ADDR << endl;
    cout << "LRU_QUEUE_START_ADDR:\t\t0x" << LRU_QUEUE_START_ADDR << endl;
    cout << "LRU_QUEUE_BASE_ADDR:\t\t0x" << LRU_QUEUE_BASE_ADDR << endl;
    cout << "LRU_QUEUE_END_ADDR:\t\t0x" << LRU_QUEUE_END_ADDR << endl;
    cout << "APPEND_OPR_START_ADDR:\t\t0x" << APPEND_OPR_START_ADDR << endl;
    cout << "APPEND_OPR_END_ADDR:\t\t0x" << APPEND_OPR_END_ADDR << endl;
    cout << "VALUE_POOL_START_ADDR:\t\t0x" << VALUE_POOL_START_ADDR << endl;
    cout << "VALUE_POOL_BASE_ADDR:\t\t0x" << VALUE_POOL_BASE_ADDR << endl;
    cout << "VALUE_POOL_END_ADDR:\t\t0x" << VALUE_POOL_END_ADDR << endl; 
    cout << "END_ADDR:\t\t\t0x" << END_ADDR << endl;
    cout << dec;
}

template <typename T>
void BTree<T>::tree_read(int fd, BTree* tree, int bkdown){ 
    mylog << "tree_read()" << endl;

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);

    auto start = chrono::high_resolution_clock::now();
    pread(fd, buf, PAGE_SIZE, 1 * PAGE_SIZE);
    auto end = std::chrono::high_resolution_clock::now();
    flash_diff += end - start;

    switch (bkdown){
        case 1:
            trav_diff += end - start;
            break;
        case 2:
            op_diff += end - start;
            break;
        case 3:
            cow_diff += end - start;
    }
    
    memcpy((void*)tree, buf, sizeof(BTree));
    free(buf);
}

template <typename T>
void BTree<T>::tree_write(int fd, BTree* tree, int bkdown){
    mylog << "tree_write()" << endl;

    // Read before write to maintain the same block bitmap

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);

    auto start = chrono::high_resolution_clock::now();
    pread(fd, buf, PAGE_SIZE, 1 * PAGE_SIZE);
    auto end = std::chrono::high_resolution_clock::now();
    flash_diff += end - start;

    switch (bkdown){
        case 1:
            trav_diff += end - start;
            break;
        case 2:
            op_diff += end - start;
            break;
        case 3:
            cow_diff += end - start;
    }
    
    memcpy(buf, tree, sizeof(BTree));
    pwrite(fd, buf, PAGE_SIZE, 1 * PAGE_SIZE);
    free(buf);
}

template <typename T>
void BTree<T>::node_read(u_int64_t node_id, BTreeNode<T>* node, int bkdown){
    mylog << "node_read() - node_id:" << node_id << endl;

    if(node_id == 0){
        cout << " error : attempt to read node id = 0 " << endl;
        mylog << " error : attempt to read node id = 0 " << endl;
        return;
    }

    delete [] node->key;
    delete [] node->value;
    delete [] node->child_id;

    u_int64_t block_id;
    if(cmb)
        block_id = cmb->get_block_id(node_id);       
    else
        block_id = node_id;

    if(block_id < 2){
        cout << " error : attempt to read block id = " << block_id << endl;
        mylog << " error : attempt to read block id = " << block_id << endl;
        return;
    }    

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);

    auto start = chrono::high_resolution_clock::now();
    pread(fd, buf, PAGE_SIZE, block_id * PAGE_SIZE);
    auto end = std::chrono::high_resolution_clock::now();
    flash_diff += end - start;
    switch (bkdown){
        case 1:
            trav_diff += tmp_diff;
            tmp_diff = end - start;
            break;
        case 2:
            op_diff += tmp_diff;
            op_diff += end - start;
            tmp_diff = chrono::microseconds{0};
            break;
        case 3:
            cow_diff += end - start;
    }

    char* ptr = buf;
    memcpy((void*)node, ptr, sizeof(BTreeNode<T>));
    ptr += sizeof(BTreeNode<T>);

    node->key = new u_int64_t[node->m];
    memcpy(node->key, ptr, node->m * sizeof(u_int64_t));
    ptr += node->m * sizeof(u_int64_t);

    node->value = new T[node->m];
    memcpy(node->value, ptr, node->m * sizeof(T));
    ptr += node->m * sizeof(T);

    node->child_id = new u_int64_t[node->m + 1];
    memcpy(node->child_id, ptr, (node->m + 1) * sizeof(u_int64_t));
    ptr += (node->m + 1) * sizeof(u_int64_t);

    free(buf);
}

template <typename T>
void BTree<T>::node_write(u_int64_t node_id, BTreeNode<T>* node, int bkdown){
    mylog << "node_write() - node_id:" << node_id << endl;

    if(node_id == 0){
        cout << " Error : Attempt to write node id = 0 " << endl;
        mylog << " Error : Attempt to write node id = 0 " << endl;
        return;
    }

    u_int64_t block_id;
    if(cmb)
        block_id = cmb->get_block_id(node_id);
    else
        block_id = node_id;

    if(block_id < 2){
        cout << " error : attempt to write block id = " << block_id << endl;
        mylog << " error : attempt to write block id = " << block_id << endl;
        return;
    }

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);

    char* ptr = buf;
    memcpy(ptr, node, sizeof(BTreeNode<T>));
    ptr += sizeof(BTreeNode<T>);

    memcpy(ptr, node->key, node->m * sizeof(u_int64_t));
    ptr += node->m * sizeof(u_int64_t);

    memcpy(ptr, node->value, node->m * sizeof(T));
    ptr += node->m * sizeof(T);

    memcpy(ptr, node->child_id, (node->m + 1) * sizeof(u_int64_t));
    ptr += (node->m + 1) * sizeof(u_int64_t);

    auto start = chrono::high_resolution_clock::now();
    pwrite(fd, buf, PAGE_SIZE, block_id * PAGE_SIZE);
    auto end = std::chrono::high_resolution_clock::now();
    flash_diff += end - start;
    switch (bkdown){
        case 1:
            trav_diff += tmp_diff;
            tmp_diff = end - start;
            break;
        case 2:
            op_diff += tmp_diff;
            op_diff += end - start;
            tmp_diff = chrono::microseconds{0};
            break;
        case 3:
            cow_diff += end - start;
    }

    free(buf);
}

template <typename T>
u_int64_t BTree<T>::get_free_block_id(){
    mylog << "get_free_block_id()" << endl;

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    pread(fd, buf, PAGE_SIZE, 1 * PAGE_SIZE);

    char* byte_ptr =  buf + sizeof(BTree);
    u_int64_t id = 0;
    while(id < block_cap){
        char byte = *byte_ptr;
        for(int i = 0; i < 8; i++){
            if( !(byte & 1) ){
                free(buf);
                set_block_id(id, true);
                return id;
            }
            byte >>= 1;
            id++;
        }
        byte_ptr++;
    }
    free(buf);
    return 0;
}

template <typename T>
void BTree<T>::set_block_id(u_int64_t block_id, bool bit){
    mylog << "set_block_id() - set block id:" << block_id << " as " << bit << endl;

    if(block_id >= block_cap) return;

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    pread(fd, buf, PAGE_SIZE, 1 * PAGE_SIZE);

    char* byte_ptr =  buf + sizeof(BTree) + (block_id / 8);

    if(bit)
        *byte_ptr |= (1UL << (block_id % 8));
    else
        *byte_ptr &= ~(1UL << (block_id % 8));

    pwrite(fd, buf, PAGE_SIZE, 1 * PAGE_SIZE);

    free(buf);
}

template <typename T>
void BTree<T>::print_used_block_id(){
    mylog << "print_used_block_id()" << endl;
    mylog << "\tUsed Block : " << endl << "\t";

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    pread(fd, buf, PAGE_SIZE, 1 * PAGE_SIZE);

    int num_bk = 0;
    char* byte_ptr =  buf + sizeof(BTree);
    int id = 0;
    while(id < block_cap){
        char byte = *byte_ptr;
        for(int i = 0; i < 8; i++){
            if( (byte & 1) ){
                mylog << " " << id;
                num_bk++;
            }
            byte >>= 1;
            id++;
        }
        byte_ptr++;
    }
    mylog << endl << "#Number of Used Block = " << num_bk << endl;

    free(buf);
}

template <typename T>
void BTree<T>::display_tree(){
    cout << "display_tree()" << endl;

    if(root_id){
        BTreeNode<T>* root = new BTreeNode<T>(0, 0, 0);
        node_read(root_id, root);
        root->display_tree(this, mode, 0);
        delete root;
    }
    else
        cout << " The tree is empty! " << endl;
}

template <typename T>
void BTree<T>::inorder_traversal(char* filename){
    mylog << endl << "inorder_traversal()" << endl;

    if(root_id){
        ofstream outFile(filename);

        BTreeNode<T>* root = new BTreeNode<T>(0, 0, 0);
        node_read(root_id, root);
        root->inorder_traversal(this, outFile);
        delete root;
    }
    else
        mylog << " The tree is empty! " << endl;
}

template <typename T>
void BTree<T>::search(u_int64_t _k, T* buf){
    mylog << "search() - key:" << _k << endl;

    if(root_id){
        // Interval to Leaf Optimization
        if(leafCache){
            leafCache->rbtree_search_by_key(cmb, _k);

            if(leafCache->hit_rb_id != 0){
                u_int64_t hit_node_id = leafCache->get_node_id(leafCache->hit_rb_id);
                // Hit
                // Remove the cache from the queue, then Read the value
                // ##
                HIT++;
                BTreeNode<T>* leaf = new BTreeNode<T>(0, 0, 0);
                node_read(hit_node_id, leaf);
                leaf->search(this, _k, buf);
                delete leaf;

                return;
            }
        }

        BTreeNode<T>* root = new BTreeNode<T>(0, 0, 0);
        node_read(root_id, root, 1);
        root->search(this, _k, buf);
        delete root;
    }
    else
        buf = NULL;
}

template <typename T>
void BTree<T>::update(u_int64_t _k, T _v){
    mylog << "update() : update key:" << _k << endl;

    if(root_id){
        removeList* rmlist = NULL;

        if(leafCache){
            leafCache->rbtree_search_by_key(cmb, _k);

            if(leafCache->hit_rb_id != 0){
                u_int64_t hit_node_id = leafCache->get_node_id(leafCache->hit_rb_id);
                // Hit
                // Remove the cache from the queue, then Update the value 
                // ##
                HIT++;
                BTreeNode<T>* leaf = new BTreeNode<T>(0, 0, 0);
                node_read(hit_node_id, leaf);
                leaf->update(this, _k, _v, &rmlist);
                delete leaf;

                if(rmlist){
                    removeList* cur = rmlist;
                    removeList* next = NULL;
                    while(cur != NULL){
                        next = cur->next;
                        set_block_id(cur->id, false);
                        delete cur;
                        cur = next;
                    }
                }

                return;
            }
        }

        BTreeNode<T>* root = new BTreeNode<T>(0, 0, 0);
        node_read(root_id, root);
        int dup_node_id = root->update(this, _k, _v, &rmlist);

        if(dup_node_id == 0){
            delete root;
            return;
        }

        if(dup_node_id != root_id){
            root_id = dup_node_id;
            tree_write(fd, this);
        }

        if(rmlist){
            removeList* cur = rmlist;
            removeList* next = NULL;
            while(cur != NULL){
                next = cur->next;
                set_block_id(cur->id, false);
                delete cur;
                cur = next;
            }
        }
        delete root;
    }
}

template <typename T>
void BTree<T>::insertion(u_int64_t _k, T _v){
    mylog << "insertion() - key:" << _k << endl;

    needsplit = false;

    if(root_id){
        removeList* rmlist = NULL;

        if(leafCache){
            leafCache->rbtree_search_by_key(cmb, _k);

            if(leafCache->hit_rb_id != 0){
                u_int64_t hit_node_id = leafCache->get_node_id(leafCache->hit_rb_id);

                if(cmb->get_num_key(hit_node_id) < m - 1){
                    //Hit and no split
                    // ##
                    HIT++;
                    BTreeNode<T>* leaf = new BTreeNode<T>(0, 0, 0);
                    node_read(hit_node_id, leaf);
                    leaf->direct_insert(this, _k, _v, &rmlist, 0, 0);
                    delete leaf;

                    if(rmlist){
                        removeList* cur = rmlist;
                        removeList* next = NULL;
                        while(cur != NULL){
                            next = cur->next;
                            set_block_id(cur->id, false);
                            delete cur;
                            cur = next;
                        }
                    }

                    return;
                }
            }
        }

        BTreeNode<T>* root = new BTreeNode<T>(0, 0, 0);
        node_read(root_id, root, 1);
        int dup_node_id = root->traverse_insert(this, _k, _v, &rmlist);

        if(dup_node_id == 0){
            delete root;
            return;
        }

        if(dup_node_id != root_id){
            root_id = dup_node_id;
            tree_write(fd, this, 3);
        }       

        if(needsplit){
            int new_root_id;     

            if(cmb){
                new_root_id = cmb->get_new_node_id();
                cmb->update_node_id(new_root_id, get_free_block_id());
                cmb->update_num_key(new_root_id, 0);
            }
            else
                new_root_id = get_free_block_id();

            BTreeNode<T>* new_root = new BTreeNode<T>(m, false, new_root_id);
            node_write(new_root_id, new_root, 2);

            root_id = root->split(this, root_id, new_root_id, &rmlist);

            tree_write(fd, this, 2);

            delete new_root;    
        }
        
        if(rmlist){
            removeList* cur = rmlist;
            removeList* next = NULL;
            while(cur != NULL){
                next = cur->next;
                set_block_id(cur->id, false);
                delete cur;
                cur = next;
            }
        }
                
        delete root;
    }
    else{
        if(cmb){
            root_id = cmb->get_new_node_id();
            cmb->update_node_id(root_id, get_free_block_id());
            cmb->update_num_key(root_id, 0);
            if(append_map){
                append_map->write_num(cmb, root_id, 0);
            }
        }
        else
            root_id = get_free_block_id();

        tree_write(fd, this, 2);

        BTreeNode<T>* root = new BTreeNode<T>(m, true, root_id);
        node_write(root_id, root, 2);
        delete root;
        insertion(_k, _v);
    }
}

template <typename T>
void BTree<T>::deletion(u_int64_t _k){
    mylog << "deletion() - key:" << _k << endl;

    if(root_id){
        removeList* rmlist = NULL;

        if(leafCache){
            leafCache->rbtree_search_by_key(cmb, _k);

            if(leafCache->hit_rb_id != 0){
                u_int64_t hit_node_id = leafCache->get_node_id(leafCache->hit_rb_id);

                if(cmb->get_num_key(hit_node_id) > min_num){
                    //Hit and no merge
                    // ##
                    HIT++;
                    BTreeNode<T>* leaf = new BTreeNode<T>(0, 0, 0);
                    node_read(hit_node_id, leaf);
                    leaf->direct_delete(this, _k, &rmlist);
                    delete leaf;

                    if(rmlist){
                        removeList* cur = rmlist;
                        removeList* next = NULL;
                        while(cur != NULL){
                            next = cur->next;
                            set_block_id(cur->id, false);
                            delete cur;
                            cur = next;
                        }
                    }

                    return;
                }
            }
        }

        BTreeNode<T>* root = new BTreeNode<T>(0, 0, 0);
        node_read(root_id, root);
        int dup_node_id = root->traverse_delete(this, _k, &rmlist);

        if(dup_node_id == 0){
            delete root;
            return;
        }

        if(root_id != dup_node_id){
            root_id = dup_node_id;
            tree_write(fd, this);
        }            

        node_read(root_id, root);
        if(root->num_key == 0){
            if(cmb)
                rmlist = new removeList(cmb->get_block_id(root_id), rmlist);
            else
                rmlist = new removeList(root_id, rmlist);
            if(root->is_leaf){
                root_id = 0;             
            }
            else{
                root_id = root->child_id[0];
            }
            tree_write(fd, this);
        } 
        
        if(rmlist){
            removeList* cur = rmlist;
            removeList* next = NULL;
            while(cur != NULL){
                next = cur->next;
                set_block_id(cur->id, false);
                delete cur;
                cur = next;
            }
        }
        delete root;          
    }
    else
        return;
}

template <typename T>
BTreeNode<T>::BTreeNode(int _m, bool _is_leaf, u_int64_t _node_id){
    mylog << "BTreeNode()" << endl;

    m = _m;
    min_num = (_m % 2 == 0) ? _m / 2 - 1 : _m / 2;
    num_key = 0;
    key = new u_int64_t[m];
    value = new T[m];
    child_id = new u_int64_t[m+1];
    is_leaf = _is_leaf;
    node_id = _node_id;
    parent_id = 0;
}

template <typename T>
BTreeNode<T>::~BTreeNode(){
    mylog << "~BTreeNode()" << endl;

    delete [] key;
    delete [] value;
    delete [] child_id;
}

template <typename T>
void BTreeNode<T>::stat(){
    mylog << "BTreeNode.stat()" << endl;
    mylog << "\tDegree: " << m << endl;
    mylog << "\tMinimun Key: " << min_num << endl;
    mylog << "\t# keys: " << num_key << endl;
    mylog << "\tIs leaf? : " << is_leaf << endl;
    mylog << "\tNode ID: " << node_id << endl;
}

template <typename T>
void BTreeNode<T>::display_tree(BTree<T>* t, MODE mode, int level){
    
    for(int j = 0; j < level; j++) cout << '\t';
    if(mode == COPY_ON_WRITE)
        cout << '[' << node_id << ']' << endl;
    else
        cout << '[' << node_id << "=>" << t->cmb->get_block_id(node_id) << ']' << endl;

    BTreeNode<T>* node = new BTreeNode<T>(0,0,0);
    t->node_read(node_id, node);
    if(t->append_map && is_leaf)
        t->append_map->reduction(t, node_id, node);

    int i = 0;
    for(i = 0; i < node->num_key; i++){
        if(!is_leaf){
            BTreeNode<T>* child = new BTreeNode<T>(0, 0, 0);
            t->node_read(node->child_id[i], child);
            child->display_tree(t, mode, level + 1);
            delete child;
        }
        for(int j = 0; j < level; j++) cout << '\t';
        cout << node->key[i] << endl;;
    }
    if(!is_leaf){
        BTreeNode<T>* child = new BTreeNode<T>(0, 0, 0);
        t->node_read(node->child_id[i], child);
        child->display_tree(t, mode, level + 1);
        delete child;
    }
}

template <typename T>
void BTreeNode<T>::inorder_traversal(BTree<T>* t, ofstream &outFile){
    mylog << "inorder_traversal()" << endl;

    BTreeNode<T>* node = new BTreeNode<T>(0,0,0);
    t->node_read(node_id, node);

    if(t->append_map && is_leaf){
        t->append_map->reduction(t, node_id, node);
    }

    int i = 0;
    for(i = 0; i < node->num_key; i++){
        if(!is_leaf){
            BTreeNode<T>* child = new BTreeNode<T>(0, 0, 0);
            t->node_read(node->child_id[i], child);
            child->inorder_traversal(t, outFile);
            delete child;
        }
        outFile << node->key[i] << '\t' << node->value[i].str << endl;;
    }
    if(!is_leaf){
        BTreeNode<T>* child = new BTreeNode<T>(0, 0, 0);
        t->node_read(node->child_id[i], child);
        child->inorder_traversal(t, outFile);
        delete child;
    }

    delete node;
}

template <typename T>
void BTreeNode<T>::search(BTree<T>* t, u_int64_t _k, T* buf, u_int64_t rbtree_id){
    mylog << "search() - key:" << _k << endl;

    if(t->append_map && is_leaf){
        u_int64_t entry_idx = t->append_map->search_entry(t->cmb, node_id, _k);
        if(entry_idx != 0){
            T ret_val = t->append_map->get_value(t->cmb, node_id, entry_idx);
            memcpy(buf, &ret_val, sizeof(T));

            if(t->leafCache && is_leaf){
                if(t->leafCache->hit_rb_id != 0 && t->leafCache->get_node_id(t->leafCache->hit_rb_id) == node_id){
                    t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
                }
                else{
                    t->leafCache->lru_enqueue(t->cmb, node_id);
                }
            } 

            return;
        }
    }

    int i;
    for(i = 0; i < num_key; i++){
        if(_k == key[i]){
            // Key match
            memcpy(buf, &value[i], sizeof(T));

            op_diff += tmp_diff;
            tmp_diff = chrono::microseconds{0};

            // Add to the leaf interval cache
            if(t->leafCache && is_leaf){
                if(t->leafCache->hit_rb_id != 0 && t->leafCache->get_node_id(t->leafCache->hit_rb_id) == node_id){
                    t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
                }
                else{
                    t->leafCache->lru_enqueue(t->cmb, node_id);
                }
            }                

            return;
        }
        if(_k < key[i]){
            if(!is_leaf){
                BTreeNode<T>* child = new BTreeNode<T>(0, 0, 0);
                t->node_read(child_id[i], child, 1);
                child->search(t, _k, buf);
                delete child;
            }
            else
                buf = NULL;

            return;
        }
    }

    if(!is_leaf){
        BTreeNode<T>* child = new BTreeNode<T>(0, 0, 0);
        t->node_read(child_id[i], child, 1);
        child->search(t, _k, buf);
        delete child;
    }
    else
        buf = NULL;
}

template <typename T>
u_int64_t BTreeNode<T>::update(BTree<T>* t, u_int64_t _k, T _v, removeList** list){
    mylog << "update() - key:" << _k << endl;

    if(t->append_map && is_leaf){
        u_int64_t entry_idx = t->append_map->search_entry(t->cmb, node_id, _k);
        if(entry_idx != 0){
            OPR_CODE last_opr = t->append_map->get_opr(t->cmb, node_id, entry_idx);
            if(last_opr != D){
                t->append_map->append_entry(t, node_id, U, _k, _v);
            }

            // Add to the leaf interval cache
            if(t->leafCache && is_leaf){
                if(t->leafCache->hit_rb_id != 0 && t->leafCache->get_node_id(t->leafCache->hit_rb_id) == node_id){
                    t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
                }
                else{
                    t->leafCache->lru_enqueue(t->cmb, node_id);
                }
            }    

            return node_id;                
        } 
    }

    int i;
    for(i = 0; i < num_key; i++){
        // Key match
        if(_k == key[i]){
            if(t->append_map && is_leaf)
                t->append_map->append_entry(t, node_id, U, _k, _v);
            else{
                value[i] = _v;

                if(t->cmb){
                    *list = new removeList(t->cmb->get_block_id(node_id), *list);
                    t->cmb->update_node_id(node_id, t->get_free_block_id());
                }
                else{
                    *list = new removeList(node_id, *list);
                    node_id = t->get_free_block_id();
                }                

                t->node_write(node_id, this);
            }

            // Add to the leaf interval cache
            if(t->leafCache && is_leaf){
                if(t->leafCache->hit_rb_id != 0 && t->leafCache->get_node_id(t->leafCache->hit_rb_id) == node_id){
                    t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
                }
                else{
                    t->leafCache->lru_enqueue(t->cmb, node_id);
                }
            }                

            return node_id;
        }
        if(_k < key[i])
            break;        
    }

    if(is_leaf)
        return 0; // Not found
    else{
        BTreeNode<T>* child = new BTreeNode<T>(0, 0, 0);
        t->node_read(child_id[i], child);
        int dup_child_id = child->update(t, _k, _v, list);

        if(dup_child_id == 0){
            delete child;
            return 0;
        }

        // Only copy-on-write would propagate
        if(dup_child_id != child_id[i]){
            child_id[i] = dup_child_id;
            *list = new removeList(node_id, *list);
            node_id = t->get_free_block_id();
            t->node_write(node_id, this);
        }

        delete child;

        return node_id;
    }
}

template <typename T>
u_int64_t BTreeNode<T>::traverse_insert(BTree<T>* t, u_int64_t _k, T _v, removeList** list){
    mylog << "traverse_insert() - key:" << _k << endl;
    
    if(is_leaf){
        return direct_insert(t, _k, _v, list);
    }        
    else{
        int i;
        for(i = 0; i < num_key; i++){
            if(_k == key[i])
                return 0;     
            if(_k < key[i])
                break;
        }
        
        BTreeNode<T>* child = new BTreeNode<T>(0, 0, 0);
        t->node_read(child_id[i], child, 1);
        int dup_child_id = child->traverse_insert(t, _k, _v, list);

        if(dup_child_id == 0){
            delete child;
            return 0;
        }

        // Only copy-on-write would propagate
        if(dup_child_id != child_id[i]){
            child_id[i] = dup_child_id;
            *list = new removeList(node_id, *list);
            node_id = t->get_free_block_id();
            t->node_write(node_id, this, 3);
        }

        if(needsplit)
            node_id = split(t, child_id[i], node_id, list);
        else if(t->append_map && child->is_leaf){           
            if(t->cmb->get_num_key(child_id[i]) >= m)
                node_id = split(t, child_id[i], node_id, list);
        }
            

        delete child;

        return node_id;
    }
}

template <typename T>
u_int64_t BTreeNode<T>::direct_insert(BTree<T>* t, u_int64_t _k, T _v, removeList** list, u_int64_t node_id1, u_int64_t node_id2){
    /* Assume the list is not full */
    if(num_key >= m) return node_id;

    mylog << "direct_insert() - key:" << _k << endl;

    if(t->append_map && is_leaf){
        t->append_map->append_entry(t, node_id, I, _k, _v);

        if(t->cmb->get_num_key(node_id) == 0){
            t->cmb->update_lower(node_id, _k);
            t->cmb->update_upper(node_id, _k);
        }
        else{
            if(_k < t->cmb->get_lower(node_id)) t->cmb->update_lower(node_id, _k);
            if(_k > t->cmb->get_upper(node_id)) t->cmb->update_upper(node_id, _k);
        }
        u_int64_t num_k = t->cmb->get_num_key(node_id);
        t->cmb->update_num_key(node_id, num_k+1);
        if(num_k >= m) needsplit = true;
    }
    else{
        int idx;
        for(idx = 0; idx < num_key; idx++){
            if(_k == key[idx])
                return node_id;     // No two keys are the same
            else{
                if(_k < key[idx])
                    break;
            }
        }

        int i;
        for(i = num_key; i > idx; i--){
            key[i] = key[i-1];
            value[i] = value[i-1];
            if(!is_leaf) child_id[i+1] = child_id[i];
        }
        if(!is_leaf) child_id[i+1] = child_id[i];

        key[idx] = _k;
        value[idx] = _v;
        if(node_id1 != 0) child_id[idx] = node_id1;
        if(node_id2 != 0) child_id[idx+1] = node_id2;
        num_key++;
        if(num_key >= m) needsplit = true;

        if(t->cmb){
            *list = new removeList(t->cmb->get_block_id(node_id), *list);
            t->cmb->update_node_id(node_id, t->get_free_block_id());
            t->cmb->update_num_key(node_id, num_key);
            t->cmb->update_lower(node_id, key[0]);
            t->cmb->update_upper(node_id, key[num_key - 1]);
        }
        else{
            *list = new removeList(node_id, *list);
            node_id = t->get_free_block_id();
        }

        t->node_write(node_id, this, 2);
    }

    if(t->leafCache && is_leaf && num_key <= m - 1 && t->cmb->get_num_key(node_id) <= m - 1){
        if(t->leafCache->hit_rb_id != 0 && t->leafCache->get_node_id(t->leafCache->hit_rb_id) == node_id){
            t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
        }
        else{
            t->leafCache->rbtree_search_by_node_id(t->cmb, node_id);
            if(t->leafCache->hit_lru_id != 0){
                t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
            }
            else{
                t->leafCache->lru_enqueue(t->cmb, node_id);
            }
        }
    }

    return node_id;
}

template <typename T>
u_int64_t BTreeNode<T>::split(BTree<T>*t, u_int64_t spt_node_id, u_int64_t parent_id, removeList** list){
    mylog << "split() - node id:" << spt_node_id << " parent node id:" << parent_id << endl;

    needsplit = false;
    
    BTreeNode<T>* node = new BTreeNode<T>(0, 0, 0);
    t->node_read(spt_node_id, node, 2);
    if(t->append_map && node->is_leaf){
        t->append_map->reduction(t, spt_node_id, node);
    }

    BTreeNode<T>* parent = new BTreeNode<T>(0, 0, 0);
    t->node_read(parent_id, parent, 2);  

    int new_node_id;
    if(t->cmb){
        new_node_id = t->cmb->get_new_node_id();
        t->cmb->update_node_id(new_node_id, t->get_free_block_id());
    }
    else
        new_node_id = t->get_free_block_id();
    BTreeNode<T>* new_node = new BTreeNode<T>(m, node->is_leaf, new_node_id);

    int i, j;
    for( i = min_num+1, j = 0; i < node->num_key; i++, j++){
        new_node->key[j] = node->key[i];
        new_node->value[j] = node->value[i];
        if(!node->is_leaf)
            new_node->child_id[j] = node->child_id[i];
        new_node->num_key++;
    }
    if(!node->is_leaf)
        new_node->child_id[j] = node->child_id[i];

    if(t->cmb){
        *list = new removeList(t->cmb->get_block_id(node->node_id), *list);
        
        t->cmb->update_num_key(new_node_id, new_node->num_key);
        t->cmb->update_lower(new_node_id, new_node->key[0]);
        t->cmb->update_upper(new_node_id, new_node->key[new_node->num_key-1]);

        t->cmb->update_node_id(node->node_id, t->get_free_block_id());
        t->cmb->update_num_key(node->node_id, min_num);
        t->cmb->update_upper(node->node_id, node->key[min_num-1]);
        
        if(t->append_map && node->is_leaf){
            t->append_map->delete_entries(t->cmb, node->node_id);
            t->append_map->write_num(t->cmb, new_node_id, 0);
        }
    }
    else{
        *list = new removeList(node->node_id, *list);
        node->node_id = t->get_free_block_id();    
    }
        
    int dup_par_id = parent->direct_insert(t, node->key[min_num], node->value[min_num], list, node->node_id, new_node_id);
    node->num_key = min_num;

    node->parent_id = dup_par_id;
    new_node->parent_id = dup_par_id;

    t->node_write(node->node_id, node, 2);
    t->node_write(new_node_id, new_node, 2);

    // Update or Create a new cache for node
    if(t->leafCache && node->is_leaf && node->num_key <= m - 1){
        if(t->leafCache->hit_rb_id != 0 && t->leafCache->get_node_id(t->leafCache->hit_rb_id) == node_id){
            t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
        }
        else{
            t->leafCache->rbtree_search_by_node_id(t->cmb, node->node_id);
            if(t->leafCache->hit_lru_id != 0){
                t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
            }
            else{
                t->leafCache->lru_enqueue(t->cmb, node->node_id);
            }
        }
    }

    // Update or Create a new cache for new node
    if(t->leafCache && new_node->is_leaf && new_node->num_key <= m - 1){
        t->leafCache->lru_enqueue(t->cmb, new_node->node_id);
    }

    delete node;
    delete parent;
    delete new_node;

    return dup_par_id;
}

template <typename T>
u_int64_t BTreeNode<T>::traverse_delete(BTree<T> *t, u_int64_t _k, removeList** list){
    mylog << "traverse_delete() - key:" << _k << endl;
    
    int i;
    bool found = false;

    if(t->append_map && is_leaf){
        t->append_map->reduction(t, node_id, this);
    }

    for(i = 0; i < num_key; i++){
        if(_k <= key[i]){
            if(_k == key[i]) found = true;
            break;
        }
    }

    if(is_leaf){
        if(found)
            return direct_delete(t, _k, list);
        else
            return 0;
    } 
    else{
        if(found){
            BTreeNode<T>* node = new BTreeNode<T>(0, 0, 0);
            t->node_read(child_id[i+1], node);

            int succ_id = node->get_succ(t);
            BTreeNode<T>* succ = new BTreeNode<T>(0, 0, 0);
            t->node_read(succ_id, succ);

            bool borrow_from_succ = false;
            if(t->append_map){
                if(t->cmb->get_num_key(succ_id) > min_num)
                    borrow_from_succ = true;
            }                
            else{
                if(succ->num_key > min_num)
                    borrow_from_succ = true;
            }

            if(borrow_from_succ){
                mylog << "borrow_from_succesor() - node id:" << node->node_id << endl;
                if(t->append_map)
                    t->append_map->reduction(t, succ_id, succ);

                // Borrow from succ
                key[i] = succ->key[0];
                value[i] = succ->value[0];
                // Delete the kv from succ
                child_id[i+1] = node->traverse_delete(t, key[i], list);

                if(t->cmb){
                    *list = new removeList(t->cmb->get_block_id(node_id), *list);
                    t->cmb->update_node_id(node_id, t->get_free_block_id());
                    if(i == 0) t->cmb->update_lower(node_id, key[0]);
                    if(i == num_key-1) t->cmb->update_upper(node_id, key[num_key-1]);

                    t->cmb->update_lower(succ_id, succ->key[1]);
                }
                else{
                    *list = new removeList(node_id, *list);
                    node_id = t->get_free_block_id();
                }
                t->node_write(node_id, this);
                i = i + 1;
            }
            else{
                t->node_read(child_id[i], node);

                int pred_id = node->get_pred(t);
                BTreeNode<T>* pred = new BTreeNode<T>(0, 0, 0);  
                t->node_read(pred_id, pred);
                if(t->append_map)
                    t->append_map->reduction(t, pred_id, pred);

                mylog << "borrow_from_predecesor() - node id:" << node->node_id << endl;
                // Borrow from pred
                key[i] = pred->key[pred->num_key - 1];
                value[i] = pred->value[pred->num_key - 1];
                // Delete the kv form pred
                child_id[i] = node->traverse_delete(t, key[i], list);

                if(t->cmb){
                    *list = new removeList(t->cmb->get_block_id(node_id), *list);
                    t->cmb->update_node_id(node_id, t->get_free_block_id());
                    if(i == 0) t->cmb->update_lower(node_id, key[0]);
                    if(i == num_key-1) t->cmb->update_upper(node_id, key[num_key-1]);

                    t->cmb->update_upper(pred_id, pred->key[pred->num_key - 2]);
                }
                else{
                    *list = new removeList(node_id, *list);
                    node_id = t->get_free_block_id();
                }

                t->node_write(node_id, this);
                
                delete pred;
            }

            delete node;
            delete succ;

            return rebalance(t, i, list);
        }
        else{
            BTreeNode<T>* child = new BTreeNode<T>(0, 0, 0);
            t->node_read(child_id[i], child);

            int dup_child_id = child->traverse_delete(t, _k, list);

            if(dup_child_id == 0){
                delete child;
                return 0;
            }

            // Only copy-on-write would propagate
            if(dup_child_id != child_id[i]){
                child_id[i] = dup_child_id;
                *list = new removeList(node_id, *list);
                node_id = t->get_free_block_id();
                t->node_write(node_id, this);
            }

            delete child;

            return rebalance(t, i, list);
        }
    }
}

template <typename T>
u_int64_t BTreeNode<T>::direct_delete(BTree<T>* t, u_int64_t _k, removeList** list){
    mylog << "direct_delete() - key:" << _k << endl;
    
    if(t->append_map && is_leaf){
        u_int64_t entry_idx = t->append_map->search_entry(t->cmb, node_id, _k);
        if(entry_idx != 0){
            OPR_CODE last_opr = t->append_map->get_opr(t->cmb, node_id, entry_idx);
            if(last_opr != D){
                T empty;
                t->append_map->append_entry(t, node_id, D, _k, empty);
                t->cmb->update_num_key(node_id, t->cmb->get_num_key(node_id) - 1);                
            }

            if(t->leafCache && is_leaf && t->cmb->get_num_key(node_id) >= min_num){
                if(t->leafCache->hit_rb_id != 0 && t->leafCache->get_node_id(t->leafCache->hit_rb_id) == node_id){
                    t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
                }
                else{
                    t->leafCache->rbtree_search_by_node_id(t->cmb, node_id);
                    if(t->leafCache->hit_lru_id != 0){
                        t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
                    }
                    else{
                        t->leafCache->lru_enqueue(t->cmb, node_id);
                    }
                }
            }
            
            return node_id;
        }        
    }

    int i, idx;
    for(i = 0; i < num_key; i++){
        if(key[i] == _k) break;
    }
    idx = i;
    
    if(i == num_key){
        mylog << "~~Key not found~~" << endl;

        if(t->leafCache && is_leaf && num_key >= min_num){
            if(t->leafCache->hit_rb_id != 0 && t->leafCache->get_node_id(t->leafCache->hit_rb_id) == node_id){
                t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
            }
            else{
                t->leafCache->rbtree_search_by_node_id(t->cmb, node_id);
                if(t->leafCache->hit_lru_id != 0){
                    t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
                }
                else{
                    t->leafCache->lru_enqueue(t->cmb, node_id);
                }
            }
        }

        return 0;
    } 

    if(i < num_key && t->append_map && is_leaf){
        T empty;
        t->append_map->append_entry(t, node_id, D, _k, empty);
        t->cmb->update_num_key(node_id, t->cmb->get_num_key(node_id) - 1);
    }
    else{
        if(i < num_key-1){
            for(; i < num_key-1; i++){
                key[i] = key[i+1];
                value[i] = value[i+1];
                if(!is_leaf) child_id[i] = child_id[i+1];
            }
            if(!is_leaf) child_id[i] = child_id[i+1];
        }
        num_key--;

        if(t->cmb){
            *list = new removeList(t->cmb->get_block_id(node_id), *list);
            t->cmb->update_node_id(node_id, t->get_free_block_id());
            t->cmb->update_num_key(node_id, num_key);
            if(idx == 0) t->cmb->update_lower(node_id, key[0]);
            if(idx == num_key) t->cmb->update_upper(node_id, key[num_key - 1]);
        }
        else{
            *list = new removeList(node_id, *list);
            node_id = t->get_free_block_id();
        }

        t->node_write(node_id, this);
    }

    if(t->leafCache && is_leaf && num_key >= min_num){
        if(t->leafCache->hit_rb_id != 0 && t->leafCache->get_node_id(t->leafCache->hit_rb_id) == node_id){
            t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
        }
        else{
            t->leafCache->rbtree_search_by_node_id(t->cmb, node_id);
            if(t->leafCache->hit_lru_id != 0){
                t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
            }
            else{
                t->leafCache->lru_enqueue(t->cmb, node_id);
            }
        }
    }

    return node_id;
}

template <typename T>
u_int64_t BTreeNode<T>::rebalance(BTree<T>* t, int idx, removeList** list){

    BTreeNode<T>* node = new BTreeNode<T>(0, 0, 0);
    t->node_read(child_id[idx], node);

    if(t->append_map && node->is_leaf){
        if(t->cmb->get_num_key(child_id[idx]) >= min_num){
            delete node;
            return node_id;
        }
    }
    else if(node->num_key >= min_num){
        delete node;
        return node_id;
    }

    mylog << "rebalance() - node id:" << child_id[idx] << endl;

    BTreeNode<T>* left = new BTreeNode<T>(0, 0, 0);
    if(idx - 1 >= 0)
        t->node_read(child_id[idx - 1], left);
    BTreeNode<T>* right = new BTreeNode<T>(0, 0, 0);
    if(idx + 1 <= num_key)
        t->node_read(child_id[idx + 1], right);
    int trans_node_id;

    bool child_is_leaf = (idx - 1 >= 0) ? left->is_leaf : right->is_leaf;
    if(t->append_map && child_is_leaf){
        if(idx - 1 >= 0 && t->cmb->get_num_key(left->node_id) > min_num){
            mylog << "borrow_from_left_sibling() - node id:" << child_id[idx] << endl;
            t->node_read(child_id[idx-1], left);   
            t->append_map->reduction(t, left->node_id, left);

            // Append insert to right
            t->append_map->append_entry(t, child_id[idx], I, key[idx-1], value[idx-1]);
            t->cmb->update_num_key(child_id[idx], t->cmb->get_num_key(child_id[idx]) + 1);
            t->cmb->update_lower(child_id[idx], key[idx-1]);

            // Borrow from left to parent
            key[idx-1] = left->key[left->num_key - 1];
            value[idx-1] = left->value[left->num_key - 1];
            
            *list = new removeList(t->cmb->get_block_id(node_id), *list);
            t->cmb->update_node_id(node_id, t->get_free_block_id());
            if(idx - 1 == 0) t->cmb->update_lower(node_id, key[0]);
            if(idx == num_key) t->cmb->update_upper(node_id, key[num_key-1]);

            t->node_write(node_id, this);   

            // Append delete to left
            T empty;
            t->append_map->append_entry(t, left->node_id, D, left->key[left->num_key-1], empty);            

            left->num_key -= 1;
            t->cmb->update_num_key(left->node_id, left->num_key);
            t->cmb->update_upper(left->node_id, left->key[left->num_key-1]);             
        }
        else if(idx + 1 <= num_key && t->cmb->get_num_key(right->node_id) > min_num){
            mylog << "borrow_from_right_sibling() - node id:" << child_id[idx] << endl;
            t->node_read(child_id[idx+1], right);
            t->append_map->reduction(t, right->node_id, right);

            // Append insert to left
            t->append_map->append_entry(t, child_id[idx], I, key[idx], value[idx]);
            t->cmb->update_num_key(child_id[idx], t->cmb->get_num_key(child_id[idx]) + 1);
            t->cmb->update_upper(child_id[idx], key[idx]);

            // Borrow from right to parent
            key[idx] = right->key[0];
            value[idx] = right->value[0];
            
            *list = new removeList(t->cmb->get_block_id(node_id), *list);
            t->cmb->update_node_id(node_id, t->get_free_block_id());
            if(idx == 0) t->cmb->update_lower(node_id, key[0]);
            if(idx == num_key-1) t->cmb->update_upper(node_id, key[num_key-1]);

            t->node_write(node_id, this);  

            // Append delete to right
            T empty;
            t->append_map->append_entry(t, right->node_id, D, right->key[0], empty);            

            right->num_key -= 1;
            t->cmb->update_num_key(right->node_id, right->num_key);
            t->cmb->update_lower(right->node_id, right->key[1]);          
        }
        else{
            mylog << "merge() - node id:" << child_id[idx] << endl;
            if(idx == t->cmb->get_num_key(node_id)) idx -= 1;
            t->node_read(child_id[idx], left);
            t->append_map->reduction(t, left->node_id, left);
            t->node_read(child_id[idx+1], right);
            t->append_map->reduction(t, right->node_id, right);
                // Insert parent's kv to left
            left->key[left->num_key] = key[idx];
            left->value[left->num_key] = value[idx];
            left->num_key++;
                // Insert right to left
            for(int i = 0; i < right->num_key; i++){
                left->key[left->num_key] = right->key[i];
                left->value[left->num_key] = right->value[i];
                if(!right->is_leaf) 
                    left->child_id[left->num_key+1] = right->child_id[i+1];
                left->num_key++;  
            }

            *list = new removeList(t->cmb->get_block_id(left->node_id), *list);
            t->cmb->update_node_id(left->node_id, t->get_free_block_id());
            t->cmb->update_num_key(left->node_id, left->num_key);
            t->cmb->update_lower(left->node_id, left->key[0]);
            t->cmb->update_upper(left->node_id, left->key[left->num_key-1]);
            t->append_map->delete_entries(t->cmb, left->node_id);
            t->node_write(left->node_id, left);

            node_id = direct_delete(t, key[idx], list);
            child_id[idx] = left->node_id;
            *list = new removeList(t->cmb->get_block_id(node_id), *list);
            t->cmb->update_node_id(node_id, t->get_free_block_id());
            t->node_write(node_id, this);
            
            *list = new removeList(t->cmb->get_block_id(right->node_id), *list);
            t->append_map->delete_entries(t->cmb, right->node_id);     

            // Remove the leaf cache of right if it is in the queue
            if(t->leafCache && right->is_leaf){
                if(t->leafCache->hit_rb_id != 0 && t->leafCache->get_node_id(t->leafCache->hit_rb_id) == right->node_id){
                    t->leafCache->lru_remove(t->cmb, t->leafCache->hit_lru_id);
                }
                else{
                    t->leafCache->rbtree_search_by_node_id(t->cmb, right->node_id);
                    if(t->leafCache->hit_lru_id != 0){
                        t->leafCache->lru_remove(t->cmb, t->leafCache->hit_lru_id);
                    }
                }
            }

            // Update or Create a new cache for left
            if(t->leafCache && left->is_leaf && left->num_key >= min_num){
                if(t->leafCache->hit_rb_id != 0 && t->leafCache->get_node_id(t->leafCache->hit_rb_id) == left->node_id){
                    t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
                }
                else{
                    t->leafCache->rbtree_search_by_node_id(t->cmb, left->node_id);
                    if(t->leafCache->hit_lru_id != 0){
                        t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
                    }
                    else{
                        t->leafCache->lru_enqueue(t->cmb, left->node_id);
                    }
                }
            }
        }
    }
    else{
        if(idx - 1 >= 0 && left->num_key > min_num){
            mylog << "borrow_from_left_sibling() - node id:" << child_id[idx] << endl;
            //Borrowing from left
            t->node_read(child_id[idx-1], left);        
            t->node_read(child_id[idx], right);
            trans_node_id = (left->is_leaf) ? 0 : left->child_id[left->num_key];
                // Insert to right
            child_id[idx] = right->direct_insert(t, key[idx-1], value[idx-1], list, trans_node_id);
                // Borrow from left
            key[idx-1] = left->key[left->num_key - 1];
            value[idx-1] = left->value[left->num_key - 1];
                // Delete from left
            child_id[idx-1] = left->direct_delete(t, key[idx-1], list);

            if(t->cmb){
                *list = new removeList(t->cmb->get_block_id(node_id), *list);
                t->cmb->update_node_id(node_id, t->get_free_block_id());
                if(idx - 1 == 0) t->cmb->update_lower(node_id, key[0]);
                if(idx == num_key) t->cmb->update_upper(node_id, key[num_key-1]);
            }
            else{
                *list = new removeList(node_id, *list);
                node_id = t->get_free_block_id();
            }

            t->node_write(node_id, this);
        }
        else if(idx + 1 <= num_key && right->num_key > min_num){
            mylog << "borrow_from_right_sibling() - node id:" << child_id[idx] << endl;
            //Borrowing from right
            t->node_read(child_id[idx], left);
            t->node_read(child_id[idx+1], right);
            trans_node_id = (right->is_leaf) ? 0 : right->child_id[0];
                // Insert to left
            child_id[idx] = left->direct_insert(t, key[idx], value[idx], list, 0, trans_node_id);
                // Borrow from right
            key[idx] = right->key[0];
            value[idx] = right->value[0];
                // Delete from left
            child_id[idx+1] = right->direct_delete(t, key[idx], list);
            
            if(t->cmb){
                *list = new removeList(t->cmb->get_block_id(node_id), *list);
                t->cmb->update_node_id(node_id, t->get_free_block_id());
                if(idx == 0) t->cmb->update_lower(node_id, key[0]);
                if(idx == num_key-1) t->cmb->update_upper(node_id, key[num_key-1]);
            }
            else{
                *list = new removeList(node_id, *list);
                node_id = t->get_free_block_id();
            }

            t->node_write(node_id, this);       
        }   
        else{
            mylog << "merge() - node id:" << child_id[idx] << endl;

            //Merge with right unless idx = num_key
            if(idx == num_key) idx -= 1;
            t->node_read(child_id[idx], left);
            t->node_read(child_id[idx+1], right);
            trans_node_id = (right->is_leaf) ? 0 : right->child_id[0];
                // Insert parent's kv to left
            left->key[left->num_key] = key[idx];
            left->value[left->num_key] = value[idx];
            if(!left->is_leaf)
                left->child_id[left->num_key+1] = right->child_id[0];
            left->num_key++;
                // Insert right to left
            for(int i = 0; i < right->num_key; i++){
                left->key[left->num_key] = right->key[i];
                left->value[left->num_key] = right->value[i];
                if(!right->is_leaf) 
                    left->child_id[left->num_key+1] = right->child_id[i+1];
                left->num_key++;  
            }
                // Store left node
            if(t->cmb){
                *list = new removeList(t->cmb->get_block_id(left->node_id), *list);
                t->cmb->update_node_id(left->node_id, t->get_free_block_id());
                t->cmb->update_num_key(left->node_id, left->num_key);
                t->cmb->update_lower(left->node_id, left->key[0]);
                t->cmb->update_upper(left->node_id, left->key[left->num_key-1]);
            }
            else{
                *list = new removeList(left->node_id, *list);
                left->node_id = t->get_free_block_id();
            }
            t->node_write(left->node_id, left);
                // Delete the parent's kv
            node_id = direct_delete(t, key[idx], list);
            child_id[idx] = left->node_id;

            if(t->cmb){
                *list = new removeList(t->cmb->get_block_id(node_id), *list);
                t->cmb->update_node_id(node_id, t->get_free_block_id());
            }
            else{
                *list = new removeList(node_id, *list);
                node_id = t->get_free_block_id();
            }

            t->node_write(node_id, this);

            if(t->cmb)
                *list = new removeList(t->cmb->get_block_id(right->node_id), *list);
            else
                *list = new removeList(right->node_id, *list);

            // Remove the leaf cache of right if it is in the queue
            if(t->leafCache && right->is_leaf){
                if(t->leafCache->hit_rb_id != 0 && t->leafCache->get_node_id(t->leafCache->hit_rb_id) == right->node_id){
                    t->leafCache->lru_remove(t->cmb, t->leafCache->hit_lru_id);
                }
                else{
                    t->leafCache->rbtree_search_by_node_id(t->cmb, right->node_id);
                    if(t->leafCache->hit_lru_id != 0){
                        t->leafCache->lru_remove(t->cmb, t->leafCache->hit_lru_id);
                    }
                }
            }

            // Update or Create a new cache for left
            if(t->leafCache && left->is_leaf && left->num_key >= min_num){
                if(t->leafCache->hit_rb_id != 0 && t->leafCache->get_node_id(t->leafCache->hit_rb_id) == left->node_id){
                    t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
                }
                else{
                    t->leafCache->rbtree_search_by_node_id(t->cmb, left->node_id);
                    if(t->leafCache->hit_lru_id != 0){
                        t->leafCache->lru_reenqueue(t->cmb, t->leafCache->hit_lru_id);
                    }
                    else{
                        t->leafCache->lru_enqueue(t->cmb, left->node_id);
                    }
                }
            }
        }
    }

    delete node;
    delete left,
    delete right;

    return node_id;
}

template <typename T>
u_int64_t BTreeNode<T>::get_pred(BTree<T>* t){
    mylog << "get_pred() - node id:" << node_id << endl;

    if(is_leaf)
        return node_id;
    else{
        BTreeNode<T>* node = new BTreeNode<T>(0, 0, 0);
        t->node_read(child_id[num_key], node);
        int ret = node->get_pred(t);
        delete node;
        return ret;
    }        
}

template <typename T>
u_int64_t BTreeNode<T>::get_succ(BTree<T>* t){
    mylog << "get_succ() - node id:" << node_id << endl; 

    if(is_leaf)
        return node_id;
    else{
        BTreeNode<T>* node = new BTreeNode<T>(0, 0, 0);
        t->node_read(child_id[0], node);
        int ret = node->get_succ(t);
        delete node;
        return ret;
    }
}

removeList::removeList(int _id, removeList* _next){
    id = _id;
    next = _next;
}

CMB::CMB(MODE _mode){
    mylog << "CMB()" << endl;

    mode = _mode;

    if (mode == REAL_CMB){
        if ((fd = open("/dev/mem", O_RDWR | O_SYNC)) == -1) FATAL;
        printf("\n/dev/mem opened.\n");
        bar_addr = CMB_ADDR;
    }
    else if (mode == FAKE_CMB || mode == DRAM){
        if ((fd = open("dup_cmb", O_RDWR | O_SYNC)) == -1) FATAL; 
        printf("\ndup_cmb opened.\n");
        bar_addr = 0;
    }
    
    map_idx = bar_addr & ~MAP_SIZE;

        /* Map one page */  
    map_base = mmap(0, MAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, map_idx);
    if (map_base == (void*)-1) FATAL;
    printf("Memory mapped at address %p.\n", map_base);

    if(mode == DRAM){
        cache_base = malloc(MAP_SIZE);

        cout << " Cache base = " << cache_base << "\n" << endl;
        mylog << " Cache base = " << cache_base << "\n" << endl;
        u_int64_t* dest = (u_int64_t*) cache_base;
        u_int64_t* src = (u_int64_t*) map_base;
        cout << " Cache CMB " << endl;
        int i = MAP_SIZE / sizeof(u_int64_t);
        while(i--){
            *dest++ = *src++;
            cout << "--" << (1 - ((double)i / (MAP_SIZE / sizeof(u_int64_t)))) * 100 << "%--\r";
        }
    }
    else
        cache_base = NULL;
}

CMB::~CMB(){
    mylog << "~CMB()" << endl;

    if(mode == DRAM){
        memcpy(map_base, cache_base, MAP_SIZE);    
        free(cache_base);
    }

    if (munmap(map_base, MAP_SIZE) == -1) FATAL;
	close(fd);
}

void* CMB::get_map_base(){
    return map_base;
}

void CMB::cmb_memcpy(void* dest, void* src, size_t len){

    u_int64_t* d = (u_int64_t*) dest;
    u_int64_t* s = (u_int64_t*) src;

    auto start = chrono::high_resolution_clock::now();
    for(int i = 0; i < len / sizeof(u_int64_t); i++)
        *d++ = *s++;
    auto end = std::chrono::high_resolution_clock::now();
    cmb_diff += end - start;    
}

void CMB::remap(off_t offset){    
    if( ((bar_addr + offset) & ~MAP_MASK) != map_idx ){
        // mylog << "remap() - offset:" << offset << " map_idx : " << map_idx << "->" << ((bar_addr + offset) & ~MAP_MASK) << endl;
        if(cache_base)
            memcpy(map_base, cache_base, MAP_SIZE);  

        /* Unmap the previous */
        if (munmap(map_base, MAP_SIZE) == -1) FATAL;        

        map_idx = (bar_addr + offset) & ~MAP_MASK;

        /* Remap a new page */
        map_base = mmap(0, MAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, map_idx);
	    if (map_base == (void*)-1) FATAL;

        if(cache_base)
            cmb_memcpy(cache_base, map_base, MAP_SIZE);
    }
}

void CMB::read(void* buf, off_t offset, int size){
    //mylog << "CMB.read() - offset:" << offset << ", size = " << size << endl;

    int remain_size = size;
    off_t cur_offset = offset;

    while(remain_size){
        int cp_size = remain_size;

        if(((bar_addr + cur_offset) & ~MAP_MASK) < ((bar_addr + cur_offset + remain_size - 1) & ~MAP_MASK)){
            cp_size = (((bar_addr + cur_offset) & ~MAP_MASK) + MAP_SIZE) - (bar_addr + cur_offset); 
        }

        remap(cur_offset);

        void* virt_addr;
        if(cache_base)
            virt_addr = (char*)cache_base + ((cur_offset) & MAP_MASK);	
        else
            virt_addr = (char*)map_base + ((cur_offset) & MAP_MASK);

        cmb_memcpy(((char*) buf + cur_offset - offset), virt_addr, cp_size);

        remain_size -= cp_size;
        cur_offset += cp_size;
    }
}

void CMB::write(off_t offset, void* buf, int size){
    //mylog << "CMB.write() - offset:" << offset << ", size = " << size << endl;

    int remain_size = size;
    off_t cur_offset = offset;

    while(remain_size){
        int cp_size = remain_size;

        if(((bar_addr + cur_offset) & ~MAP_MASK) < ((bar_addr + cur_offset + remain_size - 1) & ~MAP_MASK)){
            cp_size = (((bar_addr + cur_offset) & ~MAP_MASK) + MAP_SIZE) - (bar_addr + cur_offset); 
        }

        remap(cur_offset);

        void* virt_addr;
        if(cache_base)
            virt_addr = (char*)cache_base + ((cur_offset) & MAP_MASK);	
        else
            virt_addr = (char*)map_base + ((cur_offset) & MAP_MASK);

        cmb_memcpy(virt_addr, ((char*) buf + cur_offset - offset), cp_size);

        remain_size -= cp_size;
        cur_offset += cp_size;
    }
}

u_int64_t CMB::get_new_node_id(){
    mylog << "get_new_node_id()" << endl;

    u_int64_t new_node_id;
    read(&new_node_id, BLOCK_MAPPING_START_ADDR, sizeof(u_int64_t));

    u_int64_t next_node_id = new_node_id + 1;
    if(next_node_id > (BLOCK_MAPPING_END_ADDR - BLOCK_MAPPING_START_ADDR) / sizeof(u_int64_t)){
        cout << "CMB Block Mapping Limit Exceeded" << endl;
        mylog << "CMB Block Mapping Limit Exceeded" << endl;
        exit(1);
    }
    write(BLOCK_MAPPING_START_ADDR, &next_node_id, sizeof(u_int64_t));

    return new_node_id;
}

u_int64_t CMB::get_block_id(u_int64_t node_id){
    mylog << "get_block_id() - node id:" << node_id << endl;
    BKMap ref;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(BKMap) + ((char*)&ref.block_id - (char*)&ref);
    if(addr > BLOCK_MAPPING_END_ADDR){
        cout << "CMB Block Mapping Read Out of Range" << endl;
        mylog << "CMB Block Mapping Read Out of Range" << endl;
        exit(1);
    }

    u_int64_t readval;
	read(&readval, addr, sizeof(u_int64_t));
    return readval;
}

void CMB::update_node_id(u_int64_t node_id, u_int64_t block_id){
    mylog << "update_node_id() - node id:" << node_id << " block id:" << block_id << endl;
    BKMap ref;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(BKMap) + ((char*)&ref.block_id - (char*)&ref);
    if(addr > BLOCK_MAPPING_END_ADDR){
        cout << "CMB Block Mapping Write Out of Range" << endl;
        mylog << "CMB Block Mapping Write Out of Range" << endl;
        exit(1);
    }

    u_int64_t writeval = block_id;
	write(addr, &writeval, sizeof(u_int64_t));
}

u_int64_t CMB::get_num_key(u_int64_t node_id){
    mylog << "get_num_key() - node id:" << node_id << endl;
    BKMap ref;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(BKMap) + ((char*)&ref.num_key - (char*)&ref);
    if(addr > BLOCK_MAPPING_END_ADDR){
        cout << "CMB Block Mapping Read Out of Range" << endl;
        mylog << "CMB Block Mapping Read Out of Range" << endl;
        exit(1);
    }

    u_int64_t readval;
	read(&readval, addr, sizeof(u_int64_t));
    return readval;
}

void CMB::update_num_key(u_int64_t node_id, u_int64_t value){
    mylog << "update_num_key() - node id:" << node_id << " value:" << value << endl;
    BKMap ref;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(BKMap) + ((char*)&ref.num_key - (char*)&ref);
    if(addr > BLOCK_MAPPING_END_ADDR){
        cout << "CMB Block Mapping Write Out of Range" << endl;
        mylog << "CMB Block Mapping Write Out of Range" << endl;
        exit(1);
    }

	write(addr, &value, sizeof(u_int64_t));
}

u_int64_t CMB::get_lower(u_int64_t node_id){
    mylog << "get_lower() - node id:" << node_id << endl;
    BKMap ref;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(BKMap) + ((char*)&ref.lower - (char*)&ref);
    if(addr > BLOCK_MAPPING_END_ADDR){
        cout << "CMB Block Mapping Read Out of Range" << endl;
        mylog << "CMB Block Mapping Read Out of Range" << endl;
        exit(1);
    }

    u_int64_t readval;
	read(&readval, addr, sizeof(u_int64_t));
    return readval;
}

void CMB::update_lower(u_int64_t node_id, u_int64_t value){
    mylog << "update_lower() - node id:" << node_id << " value:" << value << endl;
    BKMap ref;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(BKMap) + ((char*)&ref.lower - (char*)&ref);
    if(addr > BLOCK_MAPPING_END_ADDR){
        cout << "CMB Block Mapping Write Out of Range" << endl;
        mylog << "CMB Block Mapping Write Out of Range" << endl;
        exit(1);
    }

	write(addr, &value, sizeof(u_int64_t));
}

u_int64_t CMB::get_upper(u_int64_t node_id){
    mylog << "get_upper() - node id:" << node_id << endl;
    BKMap ref;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(BKMap) + ((char*)&ref.upper - (char*)&ref);
    if(addr > BLOCK_MAPPING_END_ADDR){
        cout << "CMB Block Mapping Read Out of Range" << endl;
        mylog << "CMB Block Mapping Read Out of Range" << endl;
        exit(1);
    }

    u_int64_t readval;
	read(&readval, addr, sizeof(u_int64_t));
    return readval;
}

void CMB::update_upper(u_int64_t node_id, u_int64_t value){
    mylog << "update_upper() - node id:" << node_id << " value:" << value << endl;
    BKMap ref;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(BKMap) + ((char*)&ref.upper - (char*)&ref);
    if(addr > BLOCK_MAPPING_END_ADDR){
        cout << "CMB Block Mapping Write Out of Range" << endl;
        mylog << "CMB Block Mapping Write Out of Range" << endl;
        exit(1);
    }

	write(addr, &value, sizeof(u_int64_t));
}

void LEAF_CACHE::rbtree_search_by_key(CMB* cmb, u_int64_t key){
    hit_lru_id = 0;
    hit_rb_id = 0;
    
    hit_rb_id = RBTREE->search_by_key(cmb, key);
    if(hit_rb_id) hit_lru_id = get_lru_id(hit_rb_id);
        
}

void LEAF_CACHE::rbtree_search_by_node_id(CMB* cmb, u_int64_t node_id){
    hit_lru_id = 0;
    hit_rb_id = 0;

    hit_rb_id = RBTREE->search_by_node_id(cmb, node_id);
    if(hit_rb_id) hit_lru_id = get_lru_id(hit_rb_id);
}

u_int64_t LEAF_CACHE::get_node_id(u_int64_t rb_id){
    return RBTREE->pool[rb_id].node_id;
}

u_int64_t LEAF_CACHE::get_lru_id(u_int64_t rb_id){
    return RBTREE->pool[rb_id].LRU_id;
}

void LEAF_CACHE::lru_enqueue(CMB* cmb, u_int64_t node_id){
    mylog << "LEAF_CACHE::lru_enqueue() - node_id = " << node_id << endl;
    if(LRU->full()){
        lru_dequeue(cmb);
    }
    u_int64_t lru_id = LRU->enqueue(cmb, 0, node_id);
    u_int64_t rb_id = RBTREE->insert(cmb, node_id, lru_id);
    LRU->update_rb_tree_id(cmb, lru_id, rb_id);
}

void LEAF_CACHE::lru_dequeue(CMB* cmb){
    mylog << "LEAF_CACHE::lru_dequeue()" << endl;
    u_int64_t rb_id = LRU->get_rb_tree_id(cmb, LRU->Q_front_idx);
    LRU->dequeue(cmb);
    RBTREE->remove(cmb, rb_id);
}

void LEAF_CACHE::lru_remove(CMB* cmb, u_int64_t lru_id){
    mylog << "LEAF_CACHE::lru_remove() - lru_id = " << lru_id << endl;
    u_int64_t rb_id = LRU->get_rb_tree_id(cmb, lru_id);
    LRU->remove(cmb, lru_id);
    RBTREE->remove(cmb, rb_id);
}

void LEAF_CACHE::lru_reenqueue(CMB* cmb, u_int64_t lru_id){
    mylog << "LEAF_CACHE::lru_reenqueue() - lru_id = " << lru_id << endl;
    LRU_QUEUE_ENTRY* buf = new LRU_QUEUE_ENTRY;

    LRU->read(cmb, lru_id, buf);
    LRU->remove(cmb, lru_id);
    u_int64_t new_id = LRU->enqueue(cmb, buf->RBTREE_idx, buf->node_id);
    if(new_id != lru_id)
        RBTREE->pool[buf->RBTREE_idx].LRU_id = new_id;

    delete buf;
}

LRU_QUEUE::LRU_QUEUE(CMB* cmb){
    mylog << "LRU_QUEUE()" << endl;
    Q_front_idx = 0;
    Q_rear_idx = 0;
    free_Stack_idx = 0;
    free_idx = 1;
    capacity = ((LRU_QUEUE_END_ADDR - LRU_QUEUE_BASE_ADDR) / sizeof(LRU_QUEUE_ENTRY)) - 1;
    num_of_cache = 0;

    off_t addr = LRU_QUEUE_START_ADDR;

    cmb->write(addr, this, sizeof(LRU_QUEUE));
}

void LRU_QUEUE::stat(CMB* cmb){
    LRU_QUEUE* meta = (LRU_QUEUE*) malloc(sizeof(LRU_QUEUE));

    off_t addr = LRU_QUEUE_START_ADDR;
    cmb->read(meta, addr, sizeof(LRU_QUEUE));

    cout << "LRU_QUEUE: stat()" << endl;
    cout << "Q_front_idx = " << meta->Q_front_idx << endl;
    cout << "Q_rear_idx = " << meta->Q_rear_idx << endl;
    cout << "free_Stack_idx = " << meta->free_Stack_idx << endl;
    cout << "free_idx = " << meta->free_idx << endl;
    cout << "capacity = " << meta->capacity << endl;
    cout << "num_of_cache = " << meta->num_of_cache << endl;

    int num = 0;
    u_int64_t cur_idx = meta->Q_front_idx;
    cout << "< [LRU_ID] RBTree_ID -> Node_ID : Lower-Upper >" << endl;
    while(cur_idx){
        num++;
        off_t addr = LRU_QUEUE_BASE_ADDR + cur_idx * sizeof(LRU_QUEUE_ENTRY);
        LRU_QUEUE_ENTRY* entry = new LRU_QUEUE_ENTRY();
        read(cmb, cur_idx, entry);

        cout << "<" << "(" << num << ".)"<< "[" << cur_idx << "] " << entry->RBTREE_idx << "->" << entry->node_id << ": " << cmb->get_lower(entry->node_id) << "-" << cmb->get_upper(entry->node_id) << " > - ";

        cur_idx = entry->nextQ;
        delete entry;
    }
    cout << endl;

    free(meta);
}

bool LRU_QUEUE::full(){
    mylog << "LRU_QUEUE::full(): " << num_of_cache << "/" << capacity << endl;
    if(num_of_cache >= capacity)
        return true;
    return false;
}

u_int64_t LRU_QUEUE::get_free_idx(CMB* cmb){
    mylog << "LRU_QUEUE::get_free_idx()" << endl;
    // If the stack is empty
    if(free_Stack_idx == 0){
        off_t ret_idx = free_idx;
        free_idx++;

        // Write the new free_idx back
        off_t addr = LRU_QUEUE_START_ADDR + ((char*) &free_idx - (char*) this);
        cmb->write(addr, &free_idx, sizeof(u_int64_t));

        return ret_idx;
    }
    else
        return free_pop(cmb);
}

void LRU_QUEUE::free_push(CMB* cmb, u_int64_t idx){
    mylog << "LRU_QUEUE::free_push(): idx = " << idx << endl;
    // Set the new head->next as the current head idx
    off_t addr = LRU_QUEUE_BASE_ADDR + idx * sizeof(LRU_QUEUE_ENTRY);
    if(addr >= LRU_QUEUE_END_ADDR){
        cout << "Leaf Cache Limit Exceed" << endl;
        mylog << "Leaf Cache Limit Exceed" << endl;
        exit(1);
    }
    cmb->write(addr, &free_Stack_idx, sizeof(u_int64_t));

    // Set the stack head to the new head
    addr = LRU_QUEUE_START_ADDR + ((char*)&free_Stack_idx - (char*)this);
    cmb->write(addr, &idx, sizeof(u_int64_t)); 

    free_Stack_idx = idx;
}

u_int64_t LRU_QUEUE::free_pop(CMB* cmb){
    mylog << "LRU_QUEUE::free_pop()" << endl;
    u_int64_t ret, next;

    if(free_Stack_idx == 0)
        return 0;

    // Return value would be the stack head
    ret = free_Stack_idx;
    
    // Read the new stack head from the head idx
    off_t addr = LRU_QUEUE_BASE_ADDR + free_Stack_idx * sizeof(LRU_QUEUE_ENTRY);
    if(addr >= LRU_QUEUE_END_ADDR){
        cout << "Leaf Cache Limit Excced" << endl;
        mylog << "Leaf Cache Limit Excced" << endl;
        exit(1);
    }
    cmb->read(&next, addr, sizeof(u_int64_t));

    // Write the new stack head back
    free_Stack_idx = next;

    addr = LRU_QUEUE_START_ADDR + ((char*)&free_Stack_idx - (char*)this);
    cmb->write(addr, &free_Stack_idx, sizeof(u_int64_t));

    return ret;
}

u_int64_t LRU_QUEUE::enqueue(CMB* cmb, u_int64_t rbtree_id, u_int64_t node_id){
    mylog << "LRU::enqueue() : " << "rbtree_id = " << rbtree_id << " node_id = " << node_id << endl;
    if(full()){
        cout << "LRU::enqueue() FAIL: Queue full" << endl;
        mylog << "LRU::enqueue() FAIL: Queue full" << endl;
        exit(1);
    }
    
    // Get the new idx
    u_int64_t new_idx = get_free_idx(cmb);

    // Write the cache to the CMB
    LRU_QUEUE_ENTRY* new_cache = new LRU_QUEUE_ENTRY();
    
    new_cache->RBTREE_idx = rbtree_id;
    new_cache->node_id = node_id;
    new_cache->lastQ = Q_rear_idx;
    new_cache->nextQ = 0;

    off_t addr = LRU_QUEUE_BASE_ADDR + new_idx * sizeof(LRU_QUEUE_ENTRY);
    if(addr >= LRU_QUEUE_END_ADDR){
        cout << "Leaf Cache Limit Excced" << endl;
        mylog << "Leaf Cache Limit Excced" << endl;
        exit(1);
    }
    cmb->write(addr, new_cache, sizeof(LRU_QUEUE_ENTRY));

    // Set the old rear->next to new cache
    if(Q_rear_idx != 0){
        addr = LRU_QUEUE_BASE_ADDR + Q_rear_idx * sizeof(LRU_QUEUE_ENTRY) + ((char*)&(new_cache->nextQ) - (char*)new_cache);
        if(addr >= LRU_QUEUE_END_ADDR){
            cout << "Leaf Cache Limit Excced" << endl;
            mylog << "Leaf Cache Limit Excced" << endl;
            exit(1);
        }
        cmb->write(addr, &new_idx, sizeof(u_int64_t));
    }

    // Set the rear idx to the new cache idx
    // If front == 0 which means the queue is empty, set the front as well
    addr = LRU_QUEUE_START_ADDR + ((char*)&Q_rear_idx - (char*)this);
    if(addr >= LRU_QUEUE_END_ADDR){
        cout << "Leaf Cache Limit Excced" << endl;
        mylog << "Leaf Cache Limit Excced" << endl;
        exit(1);
    }
    cmb->write(addr, &new_idx, sizeof(u_int64_t));
    Q_rear_idx = new_idx;

    if(Q_front_idx == 0){
        addr = LRU_QUEUE_START_ADDR + ((char*)&Q_front_idx - (char*)this);
        if(addr >= LRU_QUEUE_END_ADDR){
            cout << "Leaf Cache Limit Excced" << endl;
            mylog << "Leaf Cache Limit Excced" << endl;
            exit(1);
        }
        cmb->write(addr, &new_idx, sizeof(u_int64_t));
        Q_front_idx = new_idx;
    }

    num_of_cache++;
    addr = LRU_QUEUE_START_ADDR + ((char*) &num_of_cache - (char*) this);
    cmb->write(addr, &num_of_cache, sizeof(u_int64_t));

    delete new_cache;

    return new_idx;
}

void LRU_QUEUE::dequeue(CMB* cmb){
    mylog << "LRU::dequeue()" << endl;
    if(Q_front_idx == 0)
        return;

    u_int64_t del_idx = Q_front_idx;

    // Read the second idx
    LRU_QUEUE_ENTRY* buf = new LRU_QUEUE_ENTRY();
    off_t addr = LRU_QUEUE_BASE_ADDR + Q_front_idx * sizeof(LRU_QUEUE_ENTRY);
    cmb->read(buf, addr, sizeof(LRU_QUEUE_ENTRY));

    // Set the front to the second idx
    addr = LRU_QUEUE_START_ADDR + ((char*) &Q_front_idx - (char*)this);
    cmb->write(addr, &(buf->nextQ), sizeof(u_int64_t));
    Q_front_idx = buf->nextQ;

    // If the Q_front_idx == 0, which means the queue is empty, set the rear to 0 as well
    if(Q_front_idx == 0){
        addr = LRU_QUEUE_START_ADDR + ((char*) &Q_rear_idx - (char*)this);
        cmb->write(addr, &Q_front_idx, sizeof(u_int64_t));
        Q_rear_idx = 0;
    }
    else{
        // Set front->lastQ to 0
        u_int64_t zero = 0;
        addr = LRU_QUEUE_BASE_ADDR + Q_front_idx * sizeof(LRU_QUEUE_ENTRY) + ((char*)&(buf->lastQ) - (char*)buf);
        cmb->write(addr, &zero, sizeof(u_int64_t));
    }

    // return the free cache slot to free stack
    free_push(cmb, del_idx);

    num_of_cache--;
    addr = LRU_QUEUE_START_ADDR + ((char*) &num_of_cache - (char*) this);
    cmb->write(addr, &num_of_cache, sizeof(u_int64_t));

    delete buf;
}

void LRU_QUEUE::remove(CMB* cmb, u_int64_t lru_id){
    mylog << "LRU::remove(): lru_id = " << lru_id << endl;
    if(lru_id == 0) return;

    off_t addr;
    u_int64_t zero = 0;

    LRU_QUEUE_ENTRY* buf = new LRU_QUEUE_ENTRY();
    read(cmb, lru_id, buf);

    if(Q_front_idx != lru_id){
        // Set the last->next = cur->next
        addr = LRU_QUEUE_BASE_ADDR + buf->lastQ * sizeof(LRU_QUEUE_ENTRY) + ((char*)&(buf->nextQ) - (char*)buf);
        cmb->write(addr, &(buf->nextQ), sizeof(u_int64_t));
    }
    else{
        // Set the front = cur->next
        addr = LRU_QUEUE_START_ADDR + ((char*)&Q_front_idx - (char*)this);
        cmb->write(addr, &(buf->nextQ), sizeof(u_int64_t));
        Q_front_idx = buf->nextQ;
    }

    if(Q_rear_idx != lru_id){
        // Set the next->last = cur->last
        addr = LRU_QUEUE_BASE_ADDR + buf->nextQ * sizeof(LRU_QUEUE_ENTRY) + ((char*)&(buf->lastQ) - (char*)buf);
        cmb->write(addr, &(buf->lastQ), sizeof(u_int64_t));
    }
    else{
        // Set the rear = cur->last
        addr = LRU_QUEUE_START_ADDR + ((char*)&Q_rear_idx - (char*)this);
        cmb->write(addr, &(buf->lastQ), sizeof(u_int64_t));
        Q_rear_idx = buf->lastQ;
    }

    free_push(cmb, lru_id);

    num_of_cache--;
    addr = LRU_QUEUE_START_ADDR + ((char*) &num_of_cache - (char*) this);
    cmb->write(addr, &num_of_cache, sizeof(u_int64_t));

    delete buf;
}

void LRU_QUEUE::read(CMB* cmb, u_int64_t idx, LRU_QUEUE_ENTRY* buf){
    mylog << "LRU_QUEUE::read(): idx = " << idx << endl;
    off_t addr = LRU_QUEUE_BASE_ADDR + idx * sizeof(LRU_QUEUE_ENTRY);
    if(addr >= LRU_QUEUE_END_ADDR){
        cout << "Leaf Cache Limit Excced" << endl;
        mylog << "Leaf Cache Limit Excced" << endl;
        exit(1);
    }
    cmb->read(buf, addr, sizeof(LRU_QUEUE_ENTRY));
}

u_int64_t LRU_QUEUE::get_rb_tree_id(CMB* cmb, u_int64_t idx){
    mylog << "LRU_QUEUE::get_rb_tree_id(): idx = " << idx << endl;
    LRU_QUEUE_ENTRY* ref = new LRU_QUEUE_ENTRY();
    u_int64_t rb_id;

    off_t addr = LRU_QUEUE_BASE_ADDR + idx * sizeof(LRU_QUEUE_ENTRY) + ((char*)&ref->RBTREE_idx - (char*)ref);
    if(addr >= LRU_QUEUE_END_ADDR){
        cout << "Leaf Cache Limit Excced" << endl;
        mylog << "Leaf Cache Limit Excced" << endl;
        exit(1);
    }
    cmb->read(&rb_id, addr, sizeof(u_int64_t));

    delete ref;

    return rb_id;
}

void LRU_QUEUE::update_rb_tree_id(CMB* cmb, u_int64_t idx, u_int64_t rb_id){
    mylog << "LRU_QUEUE::update_rb_tree_id(): idx = " << idx << " rb_id = " << rb_id << endl;
    LRU_QUEUE_ENTRY* ref = new LRU_QUEUE_ENTRY();

    off_t addr = LRU_QUEUE_BASE_ADDR + idx * sizeof(LRU_QUEUE_ENTRY) + ((char*)&ref->RBTREE_idx - (char*)ref);
    if(addr >= LRU_QUEUE_END_ADDR){
        cout << "Leaf Cache Limit Excced" << endl;
        mylog << "Leaf Cache Limit Excced" << endl;
        exit(1);
    }
    cmb->write(addr, &rb_id, sizeof(u_int64_t));

    delete ref;
}

RBTree::RBTree(){
    root_idx = 0;
    capacity = NUM_OF_CACHE - 1;
    num_of_cache = 0;
    pool = (RBTreeNode*) calloc(NUM_OF_CACHE, sizeof(RBTreeNode));

    u_int64_t first_free = 1;
    memcpy(pool, &first_free, sizeof(u_int64_t));
}

RBTree::~RBTree(){
    free(pool);
}

RBTreeNode::RBTreeNode(u_int64_t _node_idx, u_int64_t _LRU_id, u_int64_t _color, u_int64_t _parent_idx){
    node_id = _node_idx;
    LRU_id = _LRU_id;
    color = _color;
    parent_idx = _parent_idx;
    left_child_idx = 0;
    right_child_idx = 0;
}

u_int64_t RBTree::insert(CMB* cmb, u_int64_t node_id, u_int64_t LRU_id){

    mylog << "RBTree()::insert() - node_id : " << node_id << " LRU_id = " << LRU_id<< endl;
    u_int64_t ret_idx;
    if(root_idx == 0){
        root_idx = free_pop();

        RBTreeNode* new_node = new RBTreeNode(node_id, LRU_id, BLACK, 0);
        memcpy(&pool[root_idx], new_node, sizeof(RBTreeNode));

        ret_idx = root_idx;

        delete new_node;
    }
    else{
        u_int64_t cur_idx = root_idx;
        u_int64_t cur_lower, cur_upper;
        u_int64_t child_idx, new_idx;
        RBTreeNode* cur_node;

        u_int64_t lower = cmb->get_lower(node_id);
        u_int64_t upper = cmb->get_upper(node_id);

        while(cur_idx){
            cur_node = &pool[cur_idx];

            cur_lower = cmb->get_lower(cur_node->node_id);
            if(upper < cur_lower){
                child_idx = cur_node->left_child_idx;
            }
            else{
                cur_upper = cmb->get_upper(cur_node->node_id);
                if(lower > cur_upper){
                    child_idx = cur_node->right_child_idx;
                }
                else{
                    cout << "Overlapping intervals" << endl;
                    mylog << "Overlapping intervals" << endl;
                    exit(1);
                }
            }

            if(child_idx != 0){
                cur_idx = child_idx;
            }
            else{
                // Write the new node
                new_idx = free_pop();

                RBTreeNode* new_node = new RBTreeNode(node_id, LRU_id, RED, cur_idx);
                memcpy(&pool[new_idx], new_node, sizeof(RBTreeNode));

                // Update the cur->child = new_idx
                if(upper < cur_lower){
                    cur_node->left_child_idx = new_idx;
                }
                else{
                    cur_node->right_child_idx = new_idx;
                }

                delete new_node;
                break;
            }
        }
        insertFix(new_idx, cmb);

        ret_idx = new_idx;
    }

    num_of_cache++;

    return ret_idx;
}

void RBTree::insertFix(u_int64_t idx, CMB* cmb){
    
    u_int64_t cur_idx = idx;
    RBTreeNode* cur;
    while(cur_idx){
        cur = &pool[cur_idx];    

        RBTreeNode* parent = &pool[cur->parent_idx];

        if(parent->color == RED){
            u_int64_t grand_left_child = pool[parent->parent_idx].left_child_idx;
            u_int64_t grand_right_child = pool[parent->parent_idx].right_child_idx;

            if(grand_left_child == cur->parent_idx){
                // Parent is on the left
                RBTreeNode* uncle = &pool[grand_right_child];

                if(uncle->color == RED){
                    // Case 1
                    parent->color = BLACK;
                    uncle->color = BLACK;
                    if(parent->parent_idx != root_idx){
                        pool[parent->parent_idx].color = RED;
                    }                   

                    cur_idx = parent->parent_idx;
                }
                else{
                    // Case 2 & 3
                    u_int64_t cur_parent_idx = cur->parent_idx;
                    if(cur_idx == parent->right_child_idx){
                        leafRotation(cur_parent_idx);
                        cur_idx = cur_parent_idx;
                        cur = &pool[cur_idx];
                        parent = &pool[cur->parent_idx];
                    }

                    parent->color = BLACK;
                    pool[parent->parent_idx].color = RED;
                    rightRotation(parent->parent_idx);

                    cur_idx = 0;
                }
            }
            else{
                // Parent is on the right
                RBTreeNode* uncle = &pool[grand_left_child];

                if(uncle->color == RED){
                    // Case 1
                    parent->color = BLACK;
                    uncle->color = BLACK;
                    // Grandparent->color = RED;
                    if(parent->parent_idx != root_idx){
                        pool[parent->parent_idx].color = RED;
                    }

                    cur_idx = parent->parent_idx;
                }
                else{
                    // Case 2 & 3
                    u_int64_t cur_parent_idx = cur->parent_idx;
                    if(cur_idx == parent->left_child_idx){
                        rightRotation(cur_parent_idx);
                        cur_idx = cur_parent_idx;
                        cur = &pool[cur_idx];
                        parent = &pool[cur->parent_idx];
                    }

                    parent->color = BLACK;
                    pool[parent->parent_idx].color = RED;
                    leafRotation(parent->parent_idx);

                    cur_idx = 0;
                }
            }
        }
        else{
            return;
        }
    }
}

void RBTree::remove(CMB* cmb, u_int64_t rbtree_idx){
    mylog << "RBTree::remove() - rbtree_idx = " << rbtree_idx << endl;
    u_int64_t delete_idx = rbtree_idx;
    if(delete_idx == 0){
        cout << "Data not found" << endl;
        return;
    }
    RBTreeNode* delete_node = &pool[delete_idx];

    u_int64_t target_idx = 0;
    RBTreeNode* target_node;
    u_int64_t replace_idx = 0;

    if(delete_node->left_child_idx == 0 || delete_node->right_child_idx == 0){
        // Has <= 1 child
        target_idx = delete_idx;
    }
    else{
        // Has two children
        target_idx = succesor(delete_node->right_child_idx);
    }
    target_node = &pool[target_idx];

    // Set the replace_idx as the target's child
    if(target_node->left_child_idx != 0)
        replace_idx = target_node->left_child_idx;
    else
        replace_idx = target_node->right_child_idx;

    if(replace_idx != 0){
        // If the replace_idx is not NIL, set it's parent to target->parent
        pool[replace_idx].parent_idx = target_node->parent_idx;
    }

    if(target_node->parent_idx == 0){
        // If the target is the root, set the root as the replacement
        root_idx = replace_idx;
    }
    else{
        RBTreeNode* parent = &pool[target_node->parent_idx];
        if(parent->left_child_idx == target_idx){
            // If target is the left child, set the replace to the left child of target->parent
            parent->left_child_idx = replace_idx;
        }
        else{
            parent->right_child_idx = replace_idx;
        }
    }

    if(target_idx != delete_idx){
        // Replace the succesor node id to the internal node
        delete_node->node_id = target_node->node_id;
        delete_node->LRU_id = target_node->LRU_id;
        // Update the LRU -> RBTree relationship
        LRU_QUEUE_ENTRY* LRU_ref = new LRU_QUEUE_ENTRY();
        
        off_t addr = LRU_QUEUE_BASE_ADDR + target_node->LRU_id * sizeof(LRU_QUEUE_ENTRY) + ((char*)&LRU_ref->RBTREE_idx - (char*)LRU_ref);
        cmb->write(addr, &delete_idx, sizeof(u_int64_t));

        delete LRU_ref;        
    }
    if(target_node->color == BLACK){
        removeFix(replace_idx, target_node->parent_idx, cmb);
    }

    free_push(target_idx);

    num_of_cache--;
}

void RBTree::removeFix(u_int64_t idx, u_int64_t parent_idx, CMB* cmb){
    u_int64_t cur_idx = idx;
    u_int64_t cur_parent_idx = parent_idx;
    RBTreeNode* cur;
    while(true){
        cur = &pool[cur_idx];

        if(cur_idx != root_idx && cur->color == BLACK){ 
            RBTreeNode* cur_parent = &pool[cur_parent_idx];

            if(cur_idx == cur_parent->left_child_idx){
                // Current is the left child
                u_int64_t sibling_idx = cur_parent->right_child_idx;
                RBTreeNode* sibling = &pool[sibling_idx];

                if(sibling->color == RED){
                    //Case 1
                    sibling->color = BLACK;
                    cur_parent->color = RED;
                    leafRotation(cur_parent_idx);
                    sibling_idx = cur_parent->right_child_idx;
                    sibling = &pool[sibling_idx];
                }

                RBTreeNode* sibling_left_child = &pool[sibling->left_child_idx];
                RBTreeNode* sibling_right_child = &pool[sibling->right_child_idx];
                
                if(sibling_left_child->color == BLACK && sibling_right_child->color == BLACK){
                    // Case 2
                    sibling->color = RED;
                    cur_idx = cur_parent_idx;
                    cur_parent_idx = pool[cur_idx].parent_idx;
                }
                else{
                    if(sibling_right_child->color == BLACK){
                        // Case 3
                        sibling_left_child->color = BLACK;
                        sibling->color = RED;
                        rightRotation(sibling_idx);

                        sibling_idx = cur_parent->right_child_idx;
                        sibling = &pool[sibling_idx];
                        sibling_right_child = &pool[sibling->right_child_idx];
                    }

                    // Case 4
                    sibling->color = cur_parent->color;
                    cur_parent->color = BLACK;
                    sibling_right_child->color = BLACK;
                    leafRotation(cur_parent_idx);
                    cur_idx = root_idx;
                }    
            }
            else{
                // Current is the right child
                u_int64_t sibling_idx = cur_parent->left_child_idx;
                RBTreeNode* sibling = &pool[sibling_idx];

                if(sibling->color == RED){
                    //Case 1
                    sibling->color = BLACK;
                    cur_parent->color = RED;
                    rightRotation(cur_parent_idx);
                    sibling_idx = cur_parent->left_child_idx;
                    sibling = &pool[sibling_idx];
                }

                RBTreeNode* sibling_left_child = &pool[sibling->left_child_idx];
                RBTreeNode* sibling_right_child = &pool[sibling->right_child_idx];

                if(sibling_left_child->color == BLACK && sibling_right_child->color == BLACK){
                    // Case 2
                    sibling->color = RED;
                    cur_idx = cur_parent_idx;
                    cur_parent_idx = pool[cur_idx].parent_idx;
                }
                else{
                    if(sibling_left_child->color == BLACK){
                        // Case 3
                        sibling_right_child->color = BLACK;
                        sibling->color = RED;
                        leafRotation(sibling_idx);

                        sibling_idx = cur_parent->left_child_idx;
                        sibling = &pool[sibling_idx];
                        sibling_left_child = &pool[sibling->left_child_idx];
                    }

                    // Case 4
                    sibling->color = cur_parent->color;
                    cur_parent->color = BLACK;
                    sibling_left_child->color = BLACK;
                    rightRotation(cur_parent_idx);
                    cur_idx = root_idx;
                }   
            }
        }
        else{
            cur->color = BLACK;
            break;
        }  
    }
}

u_int64_t RBTree::search_by_key(CMB* cmb, u_int64_t key){
    mylog << "RBTree::search() - key = " << key << endl;
    u_int64_t cur_idx = root_idx;
    RBTreeNode* cur_node;
    u_int64_t cur_lower, cur_upper;

    while(cur_idx){
        cur_node = &pool[cur_idx];

        cur_lower = cmb->get_lower(cur_node->node_id);
        if(key < cur_lower){
            cur_idx = cur_node->left_child_idx;
        }
        else{
            cur_upper = cmb->get_upper(cur_node->node_id);
            if(key > cur_upper){
                cur_idx = cur_node->right_child_idx;
            }
            else{
                return cur_idx;
            }
        }
    }

    return 0;
}

u_int64_t RBTree::search_by_node_id(CMB* cmb, u_int64_t node_id){
    mylog << "RBTree::search() - node_id = " << node_id << endl;
    u_int64_t cur_idx = root_idx;
    RBTreeNode* cur_node;
    u_int64_t upper, cur_lower, cur_upper;

    upper = cmb->get_upper(node_id);

    while(cur_idx){
        cur_node = &pool[cur_idx];

        if(cur_node->node_id == node_id){
            return cur_idx;
        }
        else{
            cur_lower = cmb->get_lower(cur_node->node_id);
            if(upper < cur_lower){
                cur_idx = cur_node->left_child_idx;
            }
            else{
                cur_idx = cur_node->right_child_idx;
            }
        }
    }

    return 0;
}

void RBTree::stat(CMB* cmb){
    
    cout << "RBTree: stat()" << endl;
    cout << "root_idx = " << root_idx << endl;
    cout << "capacity = " << capacity << endl;
    cout << "num_of_cache = " << num_of_cache << endl;
    
    display(cmb);
}

void RBTree::display(CMB* cmb){
    if(root_idx != 0){
        RBTreeNode* node = &pool[root_idx];
        cout << "( [LRU_ID] RBTree_ID -> Node_ID : Lower-Upper : Color)" << endl;
        node->display(cmb, pool, root_idx, 0);
    }
}

void RBTreeNode::display(CMB* cmb, RBTreeNode* pool, u_int64_t idx, int level){
    if(right_child_idx != 0){
        RBTreeNode* node = &pool[right_child_idx];        
        node->display(cmb, pool, right_child_idx, level + 1);
    }

    for(int i = 0; i < level; i++)
        cout << "\t";
    string color_code = (color == RED) ? "Red" : "Black";
    cout << " [" << LRU_id << "] " << "(" << idx << "->" << node_id << ":"<< cmb->get_lower(node_id) << "-" << cmb->get_upper(node_id) << ":" << color_code << ")" << endl;
    

    if(left_child_idx != 0){
        RBTreeNode* node = &pool[left_child_idx];
        node->display(cmb, pool, left_child_idx, level + 1);
    }
}

void RBTree::free_push(u_int64_t idx){
    memcpy(&pool[idx], pool, sizeof(u_int64_t));
    memcpy(pool, &idx, sizeof(u_int64_t));
}

u_int64_t RBTree::free_pop(){
    u_int64_t free_idx;
    memcpy(&free_idx, pool, sizeof(u_int64_t));

    u_int64_t new_free_idx;
    memcpy(&new_free_idx, &pool[free_idx], sizeof(u_int64_t));
    if(new_free_idx == 0)
        new_free_idx = free_idx + 1;
    memcpy(pool, &new_free_idx, sizeof(u_int64_t));

    return free_idx;
}

void RBTree::leafRotation(u_int64_t x_idx){
    off_t addr;
    bool root = false;
    RBTreeNode* x = &pool[x_idx];
    u_int64_t y_idx = x->right_child_idx;
    RBTreeNode* y = &pool[y_idx];
    RBTreeNode* parent;
    if(x_idx == root_idx)
        root = true;
    else
        parent = &pool[x->parent_idx];

    x->right_child_idx = y->left_child_idx;
    pool[y->left_child_idx].parent_idx = x_idx;

    // x->parent->child = y
    if(root){
        root_idx = y_idx;
    }
    else{
        if(parent->left_child_idx == x_idx)
            parent->left_child_idx = y_idx;
        else
            parent->right_child_idx = y_idx;
    }      
    y->parent_idx = x->parent_idx;

    y->left_child_idx = x_idx;
    x->parent_idx = y_idx;
}

void RBTree::rightRotation(u_int64_t x_idx){
    off_t addr;
    bool root = false;
    RBTreeNode* x = &pool[x_idx];
    u_int64_t y_idx = x->left_child_idx;
    RBTreeNode* y = &pool[y_idx];
    RBTreeNode* parent;
    if(x_idx == root_idx)
        root = true;
    else
        parent = &pool[x->parent_idx];

    x->left_child_idx = y->right_child_idx;
    pool[y->right_child_idx].parent_idx = x_idx;

    // x->parent->child = y
    if(root){
        root_idx = y_idx;
    }
    else{
        if(parent->left_child_idx == x_idx)
            parent->left_child_idx = y_idx;
        else
            parent->right_child_idx = y_idx;
    }    
    y->parent_idx = x->parent_idx;    

    y->right_child_idx = x_idx;
    x->parent_idx = y_idx;
}

u_int64_t RBTree::succesor(u_int64_t idx){
    u_int64_t cur_idx = idx;
    RBTreeNode* cur_node;
    
    while(cur_idx){
        cur_node = &pool[cur_idx];
        if(cur_node->left_child_idx){
            cur_idx = cur_node->left_child_idx;
        }
        else{
            return cur_idx;
        }
    }

    return 0;
}

template <typename T>
void APPEND<T>::append_entry(BTree<T>* t, u_int64_t node_id, OPR_CODE OPR, u_int64_t _k, T _v){
    mylog << "APPEND::append_entry()" << endl;

    if(full(t->cmb, node_id)){
        // Reduction, write to the node
        BTreeNode<T>* node = new BTreeNode<T>(0,0,0);
        t->node_read(node_id, node);
        reduction(t, node_id, node);
        t->node_write(node_id, node);
        t->append_map->delete_entries(t->cmb, node_id);
        delete node;
    }

    APPEND_ENTRY* new_entry = new APPEND_ENTRY();
    new_entry->opr = (u_int64_t) OPR;
    new_entry->key = _k;
    if(OPR == I || OPR == U){
        new_entry->value_idx = value_pool->get_free_idx(t->cmb);
        value_pool->write_value(t->cmb, new_entry->value_idx, &_v);
    }

    u_int64_t num = get_num(t->cmb, node_id);
    off_t addr = APPEND_OPR_START_ADDR + node_id * (sizeof(u_int64_t) + NUM_OF_APPEND * sizeof(APPEND_ENTRY));
    addr += sizeof(u_int64_t) + num * sizeof(APPEND_ENTRY);
    t->cmb->write(addr, new_entry, sizeof(APPEND_ENTRY));

    write_num(t->cmb, node_id, num+1);

    delete new_entry;
}

template <typename T>
bool APPEND<T>::full(CMB* cmb, u_int64_t node_id){
    u_int64_t num = get_num(cmb, node_id);
    if(num >= NUM_OF_APPEND){
        mylog << "APPEND::full() - node_id = " << node_id << endl;
        return true;
    }        
    else
        return false;
}

template <typename T>
void APPEND<T>::reduction(BTree<T>* t, u_int64_t node_id, BTreeNode<T>* node){
    mylog << "APPEND::reduction() - node_id = " << node_id << endl;

    u_int64_t entry_num = get_num(t->cmb, node_id);
    for(u_int64_t idx = 1; idx <= entry_num; idx++){
        OPR_CODE opr = get_opr(t->cmb, node_id, idx);

        if(opr == I){
            u_int64_t _k = get_key(t->cmb, node_id, idx);
            T _v = get_value(t->cmb, node_id, idx);

            int i;
            bool ins = true ;
            for(i = 0; i < node->num_key; i++){
                if(_k == node->key[i]){
                    ins = false;
                    break;
                }
                else{
                    if(_k < node->key[i])
                        break;
                }
            }
            if(ins){
                for(int j = node->num_key; j > i; j--){
                    node->key[j] = node->key[j-1];
                    node->value[j] = node->value[j-1];
                }
                node->key[i] = _k;
                node->value[i] = _v;
                node->num_key++;
            }
        }
        else if(opr == U){
            u_int64_t _k = get_key(t->cmb, node_id, idx);
            T _v = get_value(t->cmb, node_id, idx);

            for(int i = 0; i < node->num_key; i++){
                if(_k == node->key[i])
                    node->value[i] = _v;
                if(_k < node->key[i])
                    break;
            }
        }
        else if(opr == D){
            u_int64_t _k = get_key(t->cmb, node_id, idx);

            int i;
            for(i = 0; i < node->num_key; i++)
                if(node->key[i] == _k) break;
            if(i == node->num_key)
                continue;
            else if(i < node->num_key - 1){
                for(; i < node->num_key-1; i++){
                    node->key[i] = node->key[i+1];
                    node->value[i] = node->value[i+1];
                }
            }
            node->num_key--;            
        }
        else{
            cout << "Incorrect OPR: " << opr << endl;
            mylog << "Incorrect OPR: " << opr << endl;
            exit(1);
        }
    }
}

template <typename T>
u_int64_t APPEND<T>::search_entry(CMB* cmb, u_int64_t node_id, u_int64_t key){
    mylog << "APPEND::search_entry() - node_id = " << node_id << endl;

    u_int64_t num = get_num(cmb, node_id);
    while(num){
        u_int64_t ret = get_key(cmb, node_id, num);
        if(key == ret)
            return num;
        num--;
    }
    return 0;
}

template <typename T>
u_int64_t APPEND<T>::get_num(CMB* cmb, u_int64_t node_id){
    mylog << "APPEND::get_num() - node_id = " << node_id << endl;
    
    off_t addr = APPEND_OPR_START_ADDR + node_id * (sizeof(u_int64_t) + NUM_OF_APPEND * sizeof(APPEND_ENTRY));
    u_int64_t ret;
    cmb->read(&ret, addr, sizeof(u_int64_t));
    return ret;
}

template <typename T>
void APPEND<T>::write_num(CMB* cmb, u_int64_t node_id, u_int64_t num){
    mylog << "APPEND::write_num() - node_id = " << node_id << ", num = " << num << endl;

    off_t addr = APPEND_OPR_START_ADDR + node_id * (sizeof(u_int64_t) + NUM_OF_APPEND * sizeof(APPEND_ENTRY));
    cmb->write(addr, &num, sizeof(u_int64_t));
}

template <typename T>
OPR_CODE APPEND<T>::get_opr(CMB* cmb, u_int64_t node_id, u_int64_t idx){
    mylog << "APPEND::get_opr() - node_id = " << node_id << ", idx = " << idx << endl;

    APPEND_ENTRY ref;
    u_int64_t ret;
    off_t addr = APPEND_OPR_START_ADDR + node_id * (sizeof(u_int64_t) + NUM_OF_APPEND * sizeof(APPEND_ENTRY));
    addr += sizeof(u_int64_t) + (idx-1) * sizeof(APPEND_ENTRY) + ((char*)&ref.opr - (char*)&ref);
    cmb->read(&ret, addr, sizeof(u_int64_t));

    return (OPR_CODE) ret;
}

template <typename T>
u_int64_t APPEND<T>::get_key(CMB* cmb, u_int64_t node_id, u_int64_t idx){
    mylog << "APPEND::get_key() - node_id = " << node_id << ", idx = " << idx << endl;

    APPEND_ENTRY ref;
    u_int64_t ret;
    off_t addr = APPEND_OPR_START_ADDR + node_id * (sizeof(u_int64_t) + NUM_OF_APPEND * sizeof(APPEND_ENTRY));
    addr += sizeof(u_int64_t) + (idx-1) * sizeof(APPEND_ENTRY) + ((char*)&ref.key - (char*)&ref);
    cmb->read(&ret, addr, sizeof(u_int64_t));

    return ret;
}

template <typename T>
T APPEND<T>::get_value(CMB* cmb, u_int64_t node_id, u_int64_t idx){
    mylog << "APPEND::get_value() - node_id = " << node_id << ", idx = " << idx << endl;

    APPEND_ENTRY ref;
    u_int64_t value_idx;
    T ret;
    off_t addr = APPEND_OPR_START_ADDR + node_id * (sizeof(u_int64_t) + NUM_OF_APPEND * sizeof(APPEND_ENTRY));
    addr += sizeof(u_int64_t) + (idx-1) * sizeof(APPEND_ENTRY) + ((char*)&ref.value_idx - (char*)&ref);
    cmb->read(&value_idx, addr, sizeof(u_int64_t));

    value_pool->get_value(cmb, value_idx, &ret);

    return ret;
}

template <typename T>
void APPEND<T>::delete_entries(CMB* cmb, u_int64_t node_id){
    mylog << "APPEND::delete_entries() - node_id = " << node_id << endl;

    APPEND_ENTRY ref;
    u_int64_t num = get_num(cmb, node_id);
    while(num){
        OPR_CODE opr = get_opr(cmb, node_id, num);
        if(opr == I or opr == U){
            u_int64_t value_idx;
            off_t addr = APPEND_OPR_START_ADDR + node_id * (sizeof(u_int64_t) + NUM_OF_APPEND * sizeof(APPEND_ENTRY));
            addr += sizeof(u_int64_t) + (num-1) * sizeof(APPEND_ENTRY) + ((char*)&ref.value_idx - (char*)&ref);
            cmb->read(&value_idx, addr, sizeof(u_int64_t));

            value_pool->free_push(cmb, value_idx);
        }
        num--;
    }
    write_num(cmb, node_id, 0);
}

template <typename T>
VALUE_POOL<T>::VALUE_POOL(CMB* cmb){
    free_Stack_idx = 0;
    free_idx = 0;
    num = 0;
    capacity = (VALUE_POOL_END_ADDR - VALUE_POOL_BASE_ADDR) / sizeof(T) - 1;

    off_t addr = VALUE_POOL_START_ADDR;
    cmb->write(addr, this, sizeof(VALUE_POOL<T>));
}

template <typename T>
void VALUE_POOL<T>::stat(CMB* cmb){
    VALUE_POOL<T>* meta = (VALUE_POOL<T>*) malloc(sizeof(VALUE_POOL<T>));

    off_t addr = VALUE_POOL_START_ADDR;
    cmb->read(meta, addr, sizeof(VALUE_POOL<T>));

    cout << "Free_Stack_idx = " << free_Stack_idx << endl;
    cout << "free_idx = " << free_idx << endl;
    cout << "capacity = " << capacity << endl;
    cout << "num = " << num << endl;

    free(meta);
}

template <typename T>
u_int64_t VALUE_POOL<T>::get_free_idx(CMB* cmb){
    mylog << "VALUE_POOL::get_free_idx()" << endl;
    // If the stack is empty
    if(free_Stack_idx == 0){
        off_t ret_idx = free_idx;
        free_idx++;

        // Write the new free_idx back
        off_t addr = VALUE_POOL_START_ADDR + ((char*) &free_idx - (char*) this);
        cmb->write(addr, &free_idx, sizeof(u_int64_t));

        return ret_idx;
    }
    else
        return free_pop(cmb);
}

template <typename T>
void VALUE_POOL<T>::free_push(CMB* cmb, u_int64_t idx){
    mylog << "VALUE_POOL::free_push(): idx = " << idx << endl;
    // Set the new head->next as the current head idx
    off_t addr = VALUE_POOL_BASE_ADDR + idx * sizeof(T);
    if(addr >= VALUE_POOL_END_ADDR){
        cout << "Value Pool Limit Exceed" << endl;
        mylog << "Value Pool Limit Exceed" << endl;
        exit(1);
    }
    cmb->write(addr, &free_Stack_idx, sizeof(u_int64_t));

    // Set the stack head to the new head
    addr = VALUE_POOL_START_ADDR + ((char*)&free_Stack_idx - (char*)this);
    cmb->write(addr, &idx, sizeof(u_int64_t)); 

    free_Stack_idx = idx;
}

template <typename T>
u_int64_t VALUE_POOL<T>::free_pop(CMB* cmb){
    mylog << "VALUE_POOL::free_pop()" << endl;
    u_int64_t ret, next;

    if(free_Stack_idx == 0)
        return 0;

    // Return value would be the stack head
    ret = free_Stack_idx;
    
    // Read the new stack head from the head idx
    off_t addr = VALUE_POOL_BASE_ADDR + free_Stack_idx * sizeof(T);
    if(addr >= VALUE_POOL_END_ADDR){
        cout << "Value Pool Limit Excced" << endl;
        mylog << "Value Pool Limit Excced" << endl;
        exit(1);
    }
    cmb->read(&next, addr, sizeof(u_int64_t));

    // Write the new stack head back
    free_Stack_idx = next;

    addr = VALUE_POOL_START_ADDR + ((char*)&free_Stack_idx - (char*)this);
    cmb->write(addr, &free_Stack_idx, sizeof(u_int64_t));

    return ret;
}

template <typename T>
void VALUE_POOL<T>::get_value(CMB* cmb, u_int64_t idx, T* buf){
    off_t addr = VALUE_POOL_BASE_ADDR + idx * sizeof(T);
    if(addr >= VALUE_POOL_END_ADDR){
        cout << "Value Pool Limit Excced" << endl;
        mylog << "Value Pool Limit Excced" << endl;
        exit(1);
    }
    cmb->read(buf, addr, sizeof(T));
}

template <typename T>
void VALUE_POOL<T>::write_value(CMB* cmb, u_int64_t idx, T* buf){
    off_t addr = VALUE_POOL_BASE_ADDR + idx * sizeof(T);
    if(addr >= VALUE_POOL_END_ADDR){
        cout << "Value Pool Limit Excced" << endl;
        mylog << "Value Pool Limit Excced" << endl;
        exit(1);
    }
    cmb->write(addr, buf, sizeof(T));
}


#endif /* B_TREE_H */
