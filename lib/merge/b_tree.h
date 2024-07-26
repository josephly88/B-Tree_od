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

// Evaluation Break Down
chrono::duration<double, micro> flash_diff;
chrono::duration<double, micro> cmb_diff;

size_t tmp_diff;
size_t kv_size;
size_t op_size_flash;
size_t op_size_cmb;
size_t cow_size;
char structural_change;

// B-Tree
template <typename T> class BTree;
template <typename T> class BTreeNode;
class removeList;
// CMB
class CMB;
class BKMap;
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
    int height;
    MODE mode;
		
	public:
	    CMB* cmb;
        APPEND<T>* append_map;

		BTree(char* filename, int degree, MODE _mode, bool append);
		~BTree();

		void reopen(int _fd, MODE _mode, bool append);

		void stat();
        void memory_map_addr();

		void tree_read(int fd, BTree* tree);
		void tree_write(int fd, BTree* tree);

		void node_read(u_int64_t block_id, BTreeNode<T>* node);
		void node_write(u_int64_t block_id, BTreeNode<T>* node);

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

template <typename T>
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
        void addr_bkmap_check(off_t addr);
        void addr_bkmap_meta_check(off_t addr);
		u_int64_t get_new_node_id();
		u_int64_t get_block_id(u_int64_t node_id);
		void update_node_id(u_int64_t node_id, u_int64_t block_id);
        // Meta data
        u_int64_t get_num_kv(u_int64_t node_id);
        void update_num_kv(u_int64_t node_id, u_int64_t value);
        u_int64_t get_is_leaf(u_int64_t node_id);
        void update_is_leaf(u_int64_t node_id, u_int64_t value);

        u_int64_t get_iu_ptr(u_int64_t node_id);
        void update_iu_ptr(u_int64_t node_id, u_int64_t value);

        // Append Meta
        u_int64_t get_num_iu();
        void update_num_iu(u_int64_t num);
        u_int64_t get_clear_ptr();
        void set_clear_ptr(u_int64_t node_id);

        u_int64_t get_next_iu_id();
        void update_next_iu_id(u_int64_t value);
        u_int64_t get_free_iu_id();
        void update_free_iu_id(u_int64_t value);

        u_int64_t get_next_val_id();
        void update_next_val_id(u_int64_t value);
        u_int64_t get_free_val_id();
        void update_free_val_id(u_int64_t value);

        u_int64_t pop_iu_id(); 
        void push_iu_id(u_int64_t iu_id); 
        u_int64_t pop_val_id(); 
        void push_val_id(u_int64_t val_id); 

        void append_entry(u_int64_t node_id, OPR_CODE OPR, u_int64_t _k, T _v);
        bool full(); 
        void reduction(u_int64_t node_id, BTreeNode<T>* node);
        u_int64_t clear_iu(u_int64_t node_id);

        u_int64_t search_entry(u_int64_t node_id, u_int64_t key);

        OPR_CODE get_opr(u_int64_t iu_id);
        u_int64_t get_key(u_int64_t iu_id);
        u_int64_t get_value_id(u_int64_t iu_id);
        u_int64_t get_next_iu_id(u_int64_t iu_id);
        T get_value(u_int64_t val_id);
        void write_value(u_int64_t val_id, T* buf);
};

class meta_BKMap{
    public:
        u_int64_t new_node_id;
        u_int64_t clear_ptr;
        u_int64_t num_iu;
        u_int64_t next_iu_id;
        u_int64_t free_iu_id;
        u_int64_t next_val_id;
        u_int64_t free_val_id;
};

class BKMap{
    public:
        u_int64_t block_id;
        u_int64_t num_kv;
        u_int64_t is_leaf;
        u_int64_t iu_ptr; 
};

class LRU_list_entry{
    public:
        u_int64_t last;
        u_int64_t next;
}

class node_LRU{
   public:
        LRU_list_entry list_pool[MAX_NUM_IU];
        u_int64_t head; 
}

class APPEND_ENTRY{
    public:
        u_int64_t opr;
        u_int64_t key;
        u_int64_t next;
        u_int64_t value_ptr;
};

template <typename T>
BTree<T>::BTree(char* filename, int degree, MODE _mode, bool append){
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
    append_map = NULL;
    height = 0;
    
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
        cmb->write(NEXT_FREE_NODE_ID_ADDR, &first_node_id, sizeof(u_int64_t));

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
    if(append_map){
        delete append_map->value_pool;
        delete append_map;
    }
}

template <typename T>
void BTree<T>::reopen(int _fd, MODE _mode, bool append){
    mylog << "reopen()" << endl;
    fd = _fd;
    mode = _mode;
    tree_write(fd, this);
    if(mode == COPY_ON_WRITE)
        cmb = NULL;
    else{
        cmb = new CMB(mode);

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
    if(NUM_OF_APPEND){
        cout << "NUM_OF_APPEND: " << NUM_OF_APPEND << endl;
    }
    cout << endl;

    mylog << "BTree.stat()" << endl;
    mylog << "\tDegree: " << m << endl;
    mylog << "\tfd: " << fd << endl;
    mylog << "\tBlock size: " << block_size << endl;
    mylog << "\tBlock Capacity: " << block_cap << endl;
    mylog << "\tRoot Block ID: " << root_id << endl;
    if(NUM_OF_APPEND){
        mylog << "NUM_OF_APPEND: " << NUM_OF_APPEND << endl;
    }
    print_used_block_id();
}

template <typename T>
void BTree<T>::memory_map_addr(){
    cout << hex;
    cout << "BLOCK_MAPPING_START_ADDR:\t0x" << BLOCK_MAPPING_START_ADDR << endl;
    cout << "BLOCK_MAPPING_END_ADDR:\t\t0x" << BLOCK_MAPPING_END_ADDR << endl;
    cout << "APPEND_OPR_START_ADDR:\t\t0x" << APPEND_OPR_START_ADDR << endl;
    cout << "APPEND_OPR_END_ADDR:\t\t0x" << APPEND_OPR_END_ADDR << endl;
    cout << "VALUE_POOL_START_ADDR:\t\t0x" << VALUE_POOL_START_ADDR << endl;
    cout << "VALUE_POOL_BASE_ADDR:\t\t0x" << VALUE_POOL_BASE_ADDR << endl;
    cout << "VALUE_POOL_END_ADDR:\t\t0x" << VALUE_POOL_END_ADDR << endl; 
    cout << "END_ADDR:\t\t\t0x" << END_ADDR << endl;
    cout << dec;
}

template <typename T>
void BTree<T>::tree_read(int fd, BTree* tree){ 
    mylog << "tree_read()" << endl;

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);

    auto start = chrono::high_resolution_clock::now();
    pread(fd, buf, PAGE_SIZE, 1 * PAGE_SIZE);
    auto end = std::chrono::high_resolution_clock::now();
    flash_diff += end - start;

    tmp_diff = PAGE_SIZE;
    
    memcpy((void*)tree, buf, sizeof(BTree));
    free(buf);
}

template <typename T>
void BTree<T>::tree_write(int fd, BTree* tree){
    mylog << "tree_write()" << endl;

    // Read before write to maintain the same block bitmap

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);

    auto start = chrono::high_resolution_clock::now();
    pread(fd, buf, PAGE_SIZE, 1 * PAGE_SIZE);
    auto end = std::chrono::high_resolution_clock::now();
    flash_diff += end - start;

    tmp_diff = PAGE_SIZE;
    
    memcpy(buf, tree, sizeof(BTree));
    pwrite(fd, buf, PAGE_SIZE, 1 * PAGE_SIZE);
    free(buf);
}

template <typename T>
void BTree<T>::node_read(u_int64_t node_id, BTreeNode<T>* node){
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

    tmp_diff = PAGE_SIZE;

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
void BTree<T>::node_write(u_int64_t node_id, BTreeNode<T>* node){
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

    tmp_diff = PAGE_SIZE;

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
    else{
        ofstream outFile(filename);
        mylog << " The tree is empty! " << endl;
    }
}

template <typename T>
void BTree<T>::search(u_int64_t _k, T* buf){
    mylog << "search() - key:" << _k << endl;

    if(root_id){
        if(append_map){
            u_int64_t getIsLeaf = cmb->get_is_leaf(root_id);
            if(getIsLeaf == 1){
                u_int64_t entry_idx = append_map->search_entry(cmb, root_id, _k);
                if(entry_idx != 0){
                    T ret_val = append_map->get_value(cmb, root_id, entry_idx);
                    memcpy(buf, &ret_val, sizeof(T));
                    return;
                }
            }
        }

        BTreeNode<T>* root = new BTreeNode<T>(0, 0, 0);
        node_read(root_id, root);
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
            cow_size += tmp_diff;
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

        BTreeNode<T>* root = new BTreeNode<T>(0, 0, 0);
        node_read(root_id, root);
        int dup_node_id = root->traverse_insert(this, _k, _v, &rmlist);

        if(dup_node_id == 0){
            delete root;
            return;
        }

        if(dup_node_id != root_id){
            root_id = dup_node_id;
            tree_write(fd, this);
            cow_size += tmp_diff;
        }       

        if(needsplit){
            height++;
            cout << endl << "Tree height => " << height << endl;

            int new_root_id;     

            if(cmb){
                new_root_id = cmb->get_new_node_id();
                cmb->update_node_id(new_root_id, get_free_block_id());
                op_size_cmb += tmp_diff;
                cmb->update_num_kv(new_root_id, 0);
                op_size_cmb += tmp_diff;
                if(append_map){
                    cmb->update_is_leaf(new_root_id, 0);
                    op_size_cmb += tmp_diff;
                }
            }
            else
                new_root_id = get_free_block_id();

            BTreeNode<T>* new_root = new BTreeNode<T>(m, false, new_root_id);
            node_write(new_root_id, new_root);
            op_size_flash += tmp_diff;

            root_id = root->split(this, root_id, new_root_id, &rmlist);

            tree_write(fd, this);
            op_size_flash += tmp_diff;

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
            cmb->update_num_kv(root_id, 0);
            if(append_map){
                append_map->write_num(cmb, root_id, 0);
                cmb->update_is_leaf(root_id, 1);
            }
        }
        else
            root_id = get_free_block_id();

        tree_write(fd, this);
        op_size_flash += tmp_diff;

        BTreeNode<T>* root = new BTreeNode<T>(m, true, root_id);
        node_write(root_id, root);
        op_size_flash += tmp_diff;
        delete root;
        
        height++;
        cout << endl << "Tree height => " << height << endl;

        insertion(_k, _v);
    }
}

template <typename T>
void BTree<T>::deletion(u_int64_t _k){
    mylog << "deletion() - key:" << _k << endl;

    if(root_id){
        removeList* rmlist = NULL;

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
            cow_size += tmp_diff;
        }            

        node_read(root_id, root);
        if(root->num_key == 0){
            height--;
            cout << endl << "Tree height => " << height << endl;

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
            op_size_flash += tmp_diff; 
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

    int i;
    for(i = 0; i < num_key; i++){
        if(_k == key[i]){
            op_size_flash += tmp_diff;

            // Key match
            memcpy(buf, &value[i], sizeof(T));

            return;
        }
        if(_k < key[i]){
            if(!is_leaf){
                BTreeNode<T>* child = new BTreeNode<T>(0, 0, 0);
                t->node_read(child_id[i], child);
                child->search(t, _k, buf);
                delete child;
            }
            else
                buf = NULL;

            return;
        }
    }

    if(!is_leaf){
        
        if(t->append_map){
            u_int64_t getIsLeaf = t->cmb->get_is_leaf(child_id[i]);
            if(getIsLeaf == 1){
                u_int64_t entry_idx = t->append_map->search_entry(t->cmb, child_id[i], _k);
                if(entry_idx != 0){
                    T ret_val = t->append_map->get_value(t->cmb, child_id[i], entry_idx);
                    memcpy(buf, &ret_val, sizeof(T));
                    return;
                }
            }
        }

        BTreeNode<T>* child = new BTreeNode<T>(0, 0, 0);
        t->node_read(child_id[i], child);
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
                op_size_cmb += tmp_diff;
            }

            return node_id;                
        } 
    }

    int i;
    for(i = 0; i < num_key; i++){
        // Key match
        if(_k == key[i]){
            if(t->append_map && is_leaf){
                t->append_map->append_entry(t, node_id, U, _k, _v);
                op_size_cmb += tmp_diff;
            }
            else{
                value[i] = _v;

                if(t->cmb){
                    *list = new removeList(t->cmb->get_block_id(node_id), *list);
                    t->cmb->update_node_id(node_id, t->get_free_block_id());
                    op_size_cmb += tmp_diff;
                }
                else{
                    *list = new removeList(node_id, *list);
                    node_id = t->get_free_block_id();
                }                

                t->node_write(node_id, this);
                op_size_flash += tmp_diff;
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
            cow_size += tmp_diff;
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
        t->node_read(child_id[i], child);
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
            t->node_write(node_id, this);
            cow_size += tmp_diff;
        }

        if(needsplit){
            node_id = split(t, child_id[i], node_id, list);
        }
        else if(t->append_map && child->is_leaf){           
            if(t->cmb->get_num_kv(child_id[i]) >= m)
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
        op_size_cmb += tmp_diff;

        u_int64_t num_k = t->cmb->get_num_kv(node_id) + 1;
        t->cmb->update_num_kv(node_id, num_k);
        op_size_cmb += tmp_diff;
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
            op_size_cmb += tmp_diff;
            t->cmb->update_num_kv(node_id, num_key);
            op_size_cmb += tmp_diff;
        }
        else{
            *list = new removeList(node_id, *list);
            node_id = t->get_free_block_id();
        }

        t->node_write(node_id, this);
        op_size_flash += tmp_diff;
    }

    return node_id;
}

template <typename T>
u_int64_t BTreeNode<T>::split(BTree<T>*t, u_int64_t spt_node_id, u_int64_t parent_id, removeList** list){
    mylog << "split() - node id:" << spt_node_id << " parent node id:" << parent_id << endl;

    needsplit = false;
    structural_change = 'T';
    
    BTreeNode<T>* node = new BTreeNode<T>(0, 0, 0);
    t->node_read(spt_node_id, node);
    if(t->append_map && node->is_leaf){
        t->append_map->reduction(t, spt_node_id, node);
    }

    BTreeNode<T>* parent = new BTreeNode<T>(0, 0, 0);
    t->node_read(parent_id, parent);  

    int new_node_id;
    if(t->cmb){
        new_node_id = t->cmb->get_new_node_id();
        t->cmb->update_node_id(new_node_id, t->get_free_block_id());
        op_size_cmb += tmp_diff;
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
        
        t->cmb->update_num_kv(new_node_id, new_node->num_key);
        op_size_cmb += tmp_diff;

        t->cmb->update_node_id(node->node_id, t->get_free_block_id());
        op_size_cmb += tmp_diff;
        t->cmb->update_num_kv(node->node_id, min_num);
        op_size_cmb += tmp_diff;
        
        if(t->append_map && node->is_leaf){
            t->append_map->delete_entries(t->cmb, node->node_id);
            op_size_cmb += tmp_diff;
            t->append_map->write_num(t->cmb, new_node_id, 0);
            op_size_cmb += tmp_diff;
            if(node->is_leaf)
                t->cmb->update_is_leaf(new_node_id, 1);
            else
                t->cmb->update_is_leaf(new_node_id, 0);
            op_size_cmb += tmp_diff;
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

    t->node_write(node->node_id, node);
    op_size_flash += tmp_diff;
    t->node_write(new_node_id, new_node);
    op_size_flash += tmp_diff;

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
                if(t->cmb->get_num_kv(succ_id) > min_num)
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
                    op_size_cmb += tmp_diff;
                }
                else{
                    *list = new removeList(node_id, *list);
                    node_id = t->get_free_block_id();
                }
                t->node_write(node_id, this);
                op_size_flash += tmp_diff;
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
                    op_size_cmb += tmp_diff;
                }
                else{
                    *list = new removeList(node_id, *list);
                    node_id = t->get_free_block_id();
                }

                t->node_write(node_id, this);
                op_size_flash += tmp_diff;
                
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
                cow_size += tmp_diff;
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
                op_size_cmb += tmp_diff;
                t->cmb->update_num_kv(node_id, t->cmb->get_num_kv(node_id) - 1);                
                op_size_cmb += tmp_diff;
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

        return 0;
    } 

    if(i < num_key && t->append_map && is_leaf){
        T empty;
        t->append_map->append_entry(t, node_id, D, _k, empty);
        op_size_cmb += tmp_diff;
        t->cmb->update_num_kv(node_id, t->cmb->get_num_kv(node_id) - 1);
        op_size_cmb += tmp_diff;
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
            op_size_cmb += tmp_diff;
            t->cmb->update_num_kv(node_id, num_key);
            op_size_cmb += tmp_diff;
        }
        else{
            *list = new removeList(node_id, *list);
            node_id = t->get_free_block_id();
        }

        t->node_write(node_id, this);
        op_size_flash += tmp_diff;
    }

    return node_id;
}

template <typename T>
u_int64_t BTreeNode<T>::rebalance(BTree<T>* t, int idx, removeList** list){

    BTreeNode<T>* node = new BTreeNode<T>(0, 0, 0);
    t->node_read(child_id[idx], node);

    if(t->append_map && node->is_leaf){
        if(t->cmb->get_num_kv(child_id[idx]) >= min_num){
            delete node;
            return node_id;
        }
    }
    else if(node->num_key >= min_num){
        delete node;
        return node_id;
    }

    mylog << "rebalance() - node id:" << child_id[idx] << endl;
    structural_change = 'T';

    BTreeNode<T>* left = new BTreeNode<T>(0, 0, 0);
    if(idx - 1 >= 0)
        t->node_read(child_id[idx - 1], left);
    BTreeNode<T>* right = new BTreeNode<T>(0, 0, 0);
    if(idx + 1 <= num_key)
        t->node_read(child_id[idx + 1], right);
    int trans_node_id;

    bool child_is_leaf = (idx - 1 >= 0) ? left->is_leaf : right->is_leaf;
    if(t->append_map && child_is_leaf){
        if(idx - 1 >= 0 && t->cmb->get_num_kv(left->node_id) > min_num){
            mylog << "borrow_from_left_sibling() - node id:" << child_id[idx] << endl;
            t->node_read(child_id[idx-1], left);   
            t->append_map->reduction(t, left->node_id, left);

            // Append insert to right
            t->append_map->append_entry(t, child_id[idx], I, key[idx-1], value[idx-1]);
            op_size_cmb += tmp_diff;
            t->cmb->update_num_kv(child_id[idx], t->cmb->get_num_kv(child_id[idx]) + 1);
            op_size_cmb += tmp_diff;

            // Borrow from left to parent
            key[idx-1] = left->key[left->num_key - 1];
            value[idx-1] = left->value[left->num_key - 1];
            
            *list = new removeList(t->cmb->get_block_id(node_id), *list);
            t->cmb->update_node_id(node_id, t->get_free_block_id());
             op_size_cmb += tmp_diff;

            t->node_write(node_id, this);   
            op_size_flash += tmp_diff;

            // Append delete to left
            T empty;
            t->append_map->append_entry(t, left->node_id, D, left->key[left->num_key-1], empty);            
            op_size_cmb += tmp_diff;

            left->num_key -= 1;
            t->cmb->update_num_kv(left->node_id, left->num_key);
            op_size_cmb += tmp_diff;
        }
        else if(idx + 1 <= num_key && t->cmb->get_num_kv(right->node_id) > min_num){
            mylog << "borrow_from_right_sibling() - node id:" << child_id[idx] << endl;
            t->node_read(child_id[idx+1], right);
            t->append_map->reduction(t, right->node_id, right);

            // Append insert to left
            t->append_map->append_entry(t, child_id[idx], I, key[idx], value[idx]);
            op_size_cmb += tmp_diff;
            t->cmb->update_num_kv(child_id[idx], t->cmb->get_num_kv(child_id[idx]) + 1);
            op_size_cmb += tmp_diff;

            // Borrow from right to parent
            key[idx] = right->key[0];
            value[idx] = right->value[0];
            
            *list = new removeList(t->cmb->get_block_id(node_id), *list);
            t->cmb->update_node_id(node_id, t->get_free_block_id());
            op_size_cmb += tmp_diff;

            t->node_write(node_id, this);  
            op_size_flash += tmp_diff;

            // Append delete to right
            T empty;
            t->append_map->append_entry(t, right->node_id, D, right->key[0], empty);            
            op_size_cmb += tmp_diff;

            right->num_key -= 1;
            t->cmb->update_num_kv(right->node_id, right->num_key);
            op_size_cmb += tmp_diff;
        }
        else{
            mylog << "merge() - node id:" << child_id[idx] << endl;
            if(idx == t->cmb->get_num_kv(node_id)) idx -= 1;
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
            t->cmb->update_num_kv(left->node_id, left->num_key);
            t->append_map->delete_entries(t->cmb, left->node_id);
            op_size_cmb += tmp_diff;
            t->node_write(left->node_id, left);
            op_size_flash += tmp_diff;

            node_id = direct_delete(t, key[idx], list);
            child_id[idx] = left->node_id;
            *list = new removeList(t->cmb->get_block_id(node_id), *list);
            t->cmb->update_node_id(node_id, t->get_free_block_id());
            op_size_cmb += tmp_diff;
            t->node_write(node_id, this);
            op_size_flash += tmp_diff;
            
            *list = new removeList(t->cmb->get_block_id(right->node_id), *list);
            t->append_map->delete_entries(t->cmb, right->node_id);     
            op_size_cmb += tmp_diff;

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
            }
            else{
                *list = new removeList(node_id, *list);
                node_id = t->get_free_block_id();
            }

            t->node_write(node_id, this);
            op_size_flash += tmp_diff;
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
            }
            else{
                *list = new removeList(node_id, *list);
                node_id = t->get_free_block_id();
            }

            t->node_write(node_id, this);       
            op_size_flash += tmp_diff;
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
                op_size_cmb += tmp_diff;
                t->cmb->update_num_kv(left->node_id, left->num_key);
                op_size_cmb += tmp_diff;
            }
            else{
                *list = new removeList(left->node_id, *list);
                left->node_id = t->get_free_block_id();
            }
            t->node_write(left->node_id, left);
            op_size_flash += tmp_diff;
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
            op_size_flash += tmp_diff;

            if(t->cmb)
                *list = new removeList(t->cmb->get_block_id(right->node_id), *list);
            else
                *list = new removeList(right->node_id, *list);
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

template <typename T>
CMB<T>::CMB(MODE _mode){
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

template <typename T>
CMB<T>::~CMB(){
    mylog << "~CMB()" << endl;

    if(mode == DRAM){
        memcpy(map_base, cache_base, MAP_SIZE);    
        free(cache_base);
    }

    if (munmap(map_base, MAP_SIZE) == -1) FATAL;
	close(fd);
}

template <typename T>
void* CMB<T>::get_map_base(){
    return map_base;
}

template <typename T>
void CMB<T>::cmb_memcpy(void* dest, void* src, size_t len){

    u_int64_t* d = (u_int64_t*) dest;
    u_int64_t* s = (u_int64_t*) src;

    auto start = chrono::high_resolution_clock::now();
    for(int i = 0; i < len / sizeof(u_int64_t); i++)
        *d++ = *s++;
    auto end = std::chrono::high_resolution_clock::now();
    cmb_diff += end - start;    
}

template <typename T>
void CMB<T>::remap(off_t offset){    
    if( ((bar_addr + offset) & ~MAP_MASK) != map_idx ){
         mylog << "remap() - offset:" << offset << " map_idx : " << map_idx << "->" << ((bar_addr + offset) & ~MAP_MASK) << endl;
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

template <typename T>
void CMB<T>::read(void* buf, off_t offset, int size){
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

template <typename T>
void CMB<T>::write(off_t offset, void* buf, int size){
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

template <typename T>
void CMB<T>::addr_bkmap_check(off_t addr){
    if(addr > BLOCK_MAPPING_END_ADDR){
        cout << "CMB Block Mapping Access Out of Range" << endl;
        mylog << "CMB Block Mapping Access Out of Range" << endl;
        exit(1);
    }
}

template <typename T>
void CMB<T>::addr_bkmap_meta_check(off_t addr){
    if(addr > BLOCK_MAPPING_START_ADDR){
        cout << "CMB Block Mapping Meta Access Out of Range" << endl;
        mylog << "CMB Block Mapping Meta Access Out of Range" << endl;
        exit(1);
    }
}

template <typename T>
u_int64_t CMB<T>::get_new_node_id(){
    mylog << "get_new_node_id()" << endl;

    meta_BKMap ref;
    off_t addr = BLOCK_MAPPING_META_ADDR + ((char*)&ref.new_node_id - (char*)&ref);
    u_int64_t new_node_id;
    read(&new_node_id, addr, sizeof(u_int64_t));

    if(new_node_id > (BLOCK_MAPPING_END_ADDR - BLOCK_MAPPING_START_ADDR) / sizeof(u_int64_t)){
        cout << "CMB Block Mapping Limit Exceeded" << endl;
        mylog << "CMB Block Mapping Limit Exceeded" << endl;
        exit(1);
    }
    u_int64_t next_node_id = new_node_id + 1;
    write(addr, &next_node_id, sizeof(u_int64_t));

    return new_node_id;
}

template <typename T>
u_int64_t CMB<T>::get_block_id(u_int64_t node_id){
    mylog << "get_block_id() - node id:" << node_id << endl;
    BKMap ref;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(BKMap) + ((char*)&ref.block_id - (char*)&ref);
    addr_bkmap_check(addr);

    u_int64_t readval;
	read(&readval, addr, sizeof(u_int64_t));
    return readval;
}

template <typename T>
void CMB<T>::update_node_id(u_int64_t node_id, u_int64_t block_id){
    mylog << "update_node_id() - node id:" << node_id << " block id:" << block_id << endl;
    BKMap ref;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(BKMap) + ((char*)&ref.block_id - (char*)&ref);
    addr_bkmap_check(addr);

    u_int64_t writeval = block_id;
	write(addr, &writeval, sizeof(u_int64_t));

    tmp_diff = sizeof(u_int64_t);
}

template <typename T>
u_int64_t CMB<T>::get_num_kv(u_int64_t node_id){
    mylog << "get_num_kv() - node id:" << node_id << endl;
    BKMap ref;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(BKMap) + ((char*)&ref.num_kv - (char*)&ref);
    addr_bkmap_check(addr);

    u_int64_t readval;
	read(&readval, addr, sizeof(u_int64_t));
    return readval;
}

template <typename T>
void CMB<T>::update_num_kv(u_int64_t node_id, u_int64_t value){
    mylog << "update_num_kv() - node id:" << node_id << " value:" << value << endl;
    BKMap ref;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(BKMap) + ((char*)&ref.num_kv - (char*)&ref);
    addr_bkmap_check(addr);

	write(addr, &value, sizeof(u_int64_t));
    tmp_diff = sizeof(u_int64_t);
}

template <typename T>
u_int64_t CMB<T>::get_is_leaf(u_int64_t node_id){
    mylog << "get_is_leaf() - node id:" << node_id << endl;
    BKMap ref;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(BKMap) + ((char*)&ref.is_leaf - (char*)&ref);
    addr_bkmap_check(addr);

    u_int64_t readval;
	read(&readval, addr, sizeof(u_int64_t));
    return readval;
}

template <typename T>
void CMB<T>::update_is_leaf(u_int64_t node_id, u_int64_t value){
    mylog << "update_is_leaf() - node id:" << node_id << " value:" << value << endl;
    BKMap ref;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(BKMap) + ((char*)&ref.is_leaf - (char*)&ref);
    addr_bkmap_check(addr);

	write(addr, &value, sizeof(u_int64_t));
    tmp_diff = sizeof(u_int64_t);
}

template <typename T>
u_int64_t CMB<T>::get_iu_ptr(u_int64_t node_id){
    mylog << "get_iu_ptr() - node id:" << node_id << endl;
    BKMap ref;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(BKMap) + ((char*)&ref.iu_ptr - (char*)&ref);
    addr_bkmap_check(addr);

    u_int64_t readval;
	read(&readval, addr, sizeof(u_int64_t));
    return readval;
}

template <typename T>
void CMB<T>::update_iu_ptr(u_int64_t node_id, u_int64_t value){
    mylog << "update_iu_ptr() - node id:" << node_id << " value:" << value << endl;
    BKMap ref;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(BKMap) + ((char*)&ref.iu_ptr - (char*)&ref);
    addr_bkmap_check(addr);

	write(addr, &value, sizeof(u_int64_t));
    tmp_diff = sizeof(u_int64_t);
}

template <typename T>
u_int64_t CMB<T>::get_num_iu(){
    meta_BKMap ref;
    off_t addr = BLOCK_MAPPING_META_ADDR + ((char*)&ref.num_iu - (char*)&ref);
    addr_bkmap_meta_check(addr);
    
    u_int64_t readval;
    read(&readval, addr, sizeof(u_int64_t);

    mylog << "get_num_iu() - value: " << readval << endl;

    return readval;
}

template <typename T>
void CMB<T>::update_num_iu(u_int64_t num){
    mylog << "update_num_iu() - value: " << num << endl;

    meta_BKMap ref;
    off_t addr = BLOCK_MAPPING_META_ADDR + ((char*)&ref.num_iu - (char*)&ref);
    addr_bkmap_meta_check(addr);
    
	write(addr, &value, sizeof(u_int64_t));
}

template <typename T>
u_int64_t CMB<T>::get_clear_ptr(){
    meta_BKMap ref;
    off_t addr = BLOCK_MAPPING_META_ADDR + ((char*)&ref.clear_ptr - (char*)&ref);
    addr_bkmap_meta_check(addr);
    
    u_int64_t readval;
    read(&readval, addr, sizeof(u_int64_t);

    mylog << "get_clear_ptr() - node_id: " << readval << endl;

    return readval;
}

template <typename T>
void CMB<T>::update_clear_ptr(u_int64_t node_id){
    mylog << "update_clear_ptr() - value: " << node_id << endl;

    meta_BKMap ref;
    off_t addr = BLOCK_MAPPING_META_ADDR + ((char*)&ref.clear_ptr - (char*)&ref);
    addr_bkmap_meta_check(addr);
    
	write(addr, &value, sizeof(u_int64_t));
}

template <typename T>
u_int64_t CMB<T>::get_next_iu_id(){
    meta_BKMap ref;
    off_t addr = BLOCK_MAPPING_META_ADDR + ((char*)&ref.next_iu_id - (char*)&ref);
    addr_bkmap_meta_check(addr);
    
    u_int64_t readval;
    read(&readval, addr, sizeof(u_int64_t);

    mylog << "get_next_iu() - node_id: " << readval << endl;

    return readval;
}

template <typename T>
void CMB<T>::update_next_iu_id(u_int64_t value){
    mylog << "update_next_iu_id() - value: " << value << endl;

    meta_BKMap ref;
    off_t addr = BLOCK_MAPPING_META_ADDR + ((char*)&ref.next_iu_id - (char*)&ref);
    addr_bkmap_meta_check(addr);
    
	write(addr, &value, sizeof(u_int64_t));
}

template <typename T>
u_int64_t CMB<T>::get_free_iu_id(){
    meta_BKMap ref;
    off_t addr = BLOCK_MAPPING_META_ADDR + ((char*)&ref.free_iu_id - (char*)&ref);
    addr_bkmap_meta_check(addr);
    
    u_int64_t readval;
    read(&readval, addr, sizeof(u_int64_t);

    mylog << "get_free_iu() - node_id: " << readval << endl;

    return readval;
}

template <typename T>
void CMB<T>::update_free_iu_id(u_int64_t value){
    mylog << "update_free_iu_id() - value: " << value << endl;

    meta_BKMap ref;
    off_t addr = BLOCK_MAPPING_META_ADDR + ((char*)&ref.free_iu_id - (char*)&ref);
    addr_bkmap_meta_check(addr);
    
	write(addr, &value, sizeof(u_int64_t));
}

template <typename T>
u_int64_t CMB<T>::get_next_val_id(){
    meta_BKMap ref;
    off_t addr = BLOCK_MAPPING_META_ADDR + ((char*)&ref.next_val_id - (char*)&ref);
    addr_bkmap_meta_check(addr);
    
    u_int64_t readval;
    read(&readval, addr, sizeof(u_int64_t);

    mylog << "get_next_val() - node_id: " << readval << endl;

    return readval;
}

template <typename T>
void CMB<T>::update_next_val_id(u_int64_t value){
    mylog << "update_next_val_id() - value: " << value << endl;

    meta_BKMap ref;
    off_t addr = BLOCK_MAPPING_META_ADDR + ((char*)&ref.next_val_id - (char*)&ref);
    addr_bkmap_meta_check(addr);
    
	write(addr, &value, sizeof(u_int64_t));
}

template <typename T>
u_int64_t CMB<T>::get_free_val_id(){
    meta_BKMap ref;
    off_t addr = BLOCK_MAPPING_META_ADDR + ((char*)&ref.free_val_id - (char*)&ref);
    addr_bkmap_meta_check(addr);
    
    u_int64_t readval;
    read(&readval, addr, sizeof(u_int64_t);

    mylog << "get_free_val() - node_id: " << readval << endl;

    return readval;
}

template <typename T>
void CMB<T>::update_free_val_id(u_int64_t value){
    mylog << "update_free_val_id() - value: " << value << endl;

    meta_BKMap ref;
    off_t addr = BLOCK_MAPPING_META_ADDR + ((char*)&ref.free_val_id - (char*)&ref);
    addr_bkmap_meta_check(addr);
    
	write(addr, &value, sizeof(u_int64_t));
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
        op_size_cmb += tmp_diff;
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

    tmp_diff = sizeof(APPEND_ENTRY) + sizeof(u_int64_t) + sizeof(T);

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

    tmp_diff = sizeof(u_int64_t);
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
u_int64_t APPEND<T>::get_value_ptr(CMB* cmb, u_int64_t node_id, u_int64_t idx){
    mylog << "APPEND::get_value_ptr() - node_id = " << node_id << ", idx = " << idx << endl;

    APPEND_ENTRY ref;
    u_int64_t value_idx;
    off_t addr = APPEND_OPR_START_ADDR + node_id * (sizeof(u_int64_t) + NUM_OF_APPEND * sizeof(APPEND_ENTRY));
    addr += sizeof(u_int64_t) + (idx-1) * sizeof(APPEND_ENTRY) + ((char*)&ref.value_idx - (char*)&ref);
    cmb->read(&value_idx, addr, sizeof(u_int64_t));

    return value_idx;
}

template <typename T>
T APPEND<T>::get_value(CMB* cmb, u_int64_t node_id, u_int64_t idx){
    mylog << "APPEND::get_value() - node_id = " << node_id << ", idx = " << idx << endl;

    u_int64_t value_idx = get_value_ptr(cmb, node_id, idx);

    T ret;
    value_pool->get_value(cmb, value_idx, &ret);

    return ret;
}

template <typename T>
void APPEND<T>::delete_entries(CMB* cmb, u_int64_t node_id){
    mylog << "APPEND::delete_entries() - node_id = " << node_id << endl;

    APPEND_ENTRY ref;
    u_int64_t num = get_num(cmb, node_id);
    u_int64_t idx = num;
    while(idx){
        OPR_CODE opr = get_opr(cmb, node_id, idx);
        if(opr == I or opr == U){
            u_int64_t value_idx = get_value_ptr(cmb, node_id, idx);

            value_pool->free_push(cmb, value_idx);
        }
        idx--;
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
void VALUE_POOL<T>::update_num(CMB* cmb, u_int64_t value){
    
    mylog << "VALUE_POOL::update_num(): " << num << "/" << capacity << endl;

    off_t addr;
    addr = VALUE_POOL_START_ADDR + ((char*) &num - (char*) this);
    cmb->write(addr, &value, sizeof(u_int64_t));
}

template <typename T>
u_int64_t VALUE_POOL<T>::get_free_idx(CMB* cmb){
    mylog << "VALUE_POOL::get_free_idx()" << endl;
    if(num >= capacity){
        cout << "Value Pool Limit Exceeded, No Available value" << endl;
        mylog << "Value Pool Limit Exceeded, No Available value" << endl;
    }
    // If the stack is empty
    if(free_Stack_idx == 0){
        off_t ret_idx = free_idx;
        free_idx++;

        // Write the new free_idx back
        off_t addr;
        addr = VALUE_POOL_START_ADDR + ((char*) &free_idx - (char*) this);
        cmb->write(addr, &free_idx, sizeof(u_int64_t));

        mylog << "VALUE_POOL::get_free_idx() - " << ret_idx << endl;

        num++;
        update_num(cmb, num);

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
        cout << "Value Pool Addr out of range: 0x" << hex << addr << " - idx = " << dec << idx << endl;
        mylog << "Value Pool Addr out of range: 0x" << hex << addr << " - idx = " << dec << idx << endl;
        exit(1);
    }
    cmb->write(addr, &free_Stack_idx, sizeof(u_int64_t));

    // Set the stack head to the new head
    addr = VALUE_POOL_START_ADDR + ((char*)&free_Stack_idx - (char*)this);
    cmb->write(addr, &idx, sizeof(u_int64_t)); 
    num--;
    update_num(cmb, num);

    free_Stack_idx = idx;
}

template <typename T>
u_int64_t VALUE_POOL<T>::free_pop(CMB* cmb){
    mylog << "VALUE_POOL::free_pop() - free_Stack_idx = " << free_Stack_idx << endl;
    u_int64_t ret, next;

    if(free_Stack_idx == 0)
        return 0;

    // Return value would be the stack head
    ret = free_Stack_idx;
    
    // Read the new stack head from the head idx
    off_t addr = VALUE_POOL_BASE_ADDR + free_Stack_idx * sizeof(T);
    if(addr >= VALUE_POOL_END_ADDR){
        cout << "Value Pool Addr out of range: 0x" << hex << addr << " - idx = " << dec << free_Stack_idx << endl;
        mylog << "Value Pool Addr out of range: 0x" << hex << addr << " - idx = " << dec << free_Stack_idx << endl;
        exit(1);
    }
    cmb->read(&next, addr, sizeof(u_int64_t));

    // Write the new stack head back
    free_Stack_idx = next;

    addr = VALUE_POOL_START_ADDR + ((char*)&free_Stack_idx - (char*)this);
    cmb->write(addr, &free_Stack_idx, sizeof(u_int64_t));

    num++;
    update_num(cmb, num);

    return ret;
}

template <typename T>
void VALUE_POOL<T>::get_value(CMB* cmb, u_int64_t idx, T* buf){
    off_t addr = VALUE_POOL_BASE_ADDR + idx * sizeof(T);
    if(addr >= VALUE_POOL_END_ADDR){
        cout << "Value Pool Addr out of range: 0x" << hex << addr << " - idx = " << dec << idx << endl;
        mylog << "Value Pool Addr out of range: 0x" << hex << addr << " - idx = " << dec << idx << endl;
        exit(1);
    }
    cmb->read(buf, addr, sizeof(T));
}

template <typename T>
void VALUE_POOL<T>::write_value(CMB* cmb, u_int64_t idx, T* buf){
    off_t addr = VALUE_POOL_BASE_ADDR + idx * sizeof(T);
    if(addr >= VALUE_POOL_END_ADDR){
        cout << "Value Pool Addr out of range: 0x" << hex << addr << " - idx = " << dec << idx << endl;
        mylog << "Value Pool Addr out of range: 0x" << hex << addr << " - idx = " << dec << idx << endl;
        exit(1);
    }
    cmb->write(addr, buf, sizeof(T));
}


#endif /* B_TREE_H */
