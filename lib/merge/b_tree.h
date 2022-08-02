#ifndef B_TREE_H //include guard
#define B_TREE_H

#include <iostream>
#include <fstream>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
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

ofstream mylog;

template <typename T> class BTree;
template <typename T> class BTreeNode;
class removeList;
class CMB;
class ITV2Leaf;
class ITV2LeafCache;

template <typename T>
class BTree{
	int m;				// degree
	u_int64_t root_id;		// Pointer to root node
	int fd;
	int block_size;
	int block_cap;
    MODE mode;
		
	public:
	    CMB* cmb;

		BTree(char* filename, int degree, MODE _mode);
		~BTree();

		void reopen(int _fd, MODE _mode);

		void stat();

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

		u_int64_t cmb_get_new_node_id();

		u_int64_t cmb_get_block_id(u_int64_t node_id);
		void cmb_update_node_id(u_int64_t node_id, u_int64_t block_id);
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

		void search(BTree<T>* t, u_int64_t _k, T* buf);
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
    off_t map_idx;
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

};

class ITV2Leaf{
    u_int64_t Q_front_idx;
    u_int64_t Q_rear_idx;
    u_int64_t free_Stack_idx;
    u_int64_t free_idx;

    public:
        ITV2Leaf(CMB* cmb);

        void stat(CMB* cmb);

        u_int64_t get_free_idx(CMB* cmb);
        void free_push(CMB* cmb, u_int64_t idx);
        u_int64_t free_pop(CMB* cmb);

        void enqueue(CMB* cmb, u_int64_t node_id, u_int64_t lower, u_int64_t upper);
        void dequeue(CMB* cmb, ITV2LeafCache* buf);

        u_int64_t search(CMB*, u_int64_t);    
    };

class ITV2LeafCache{
    public:
        u_int64_t node_id;
        u_int64_t lower;
        u_int64_t upper;
        u_int64_t lastQ;
        u_int64_t nextQ;
};

template <typename T>
BTree<T>::BTree(char* filename, int degree, MODE _mode){
    mylog << "BTree()" << endl;

    if(degree > (int)((PAGE_SIZE - sizeof(BTreeNode<T>) - sizeof(u_int64_t)) / (sizeof(u_int64_t) + sizeof(T))) ){
        cout << " Error: Degree exceed " << endl;
        return;
    }

    fd = open(filename, O_RDWR | O_DIRECT);
    block_size = PAGE_SIZE;
    m = degree;
    block_cap = (block_size - sizeof(BTree)) * 8;
    root_id = 0;
    mode = _mode;
    
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
    }

}

template <typename T>
BTree<T>::~BTree(){
    mylog << "~BTree()" << endl;
    close(fd);
    if(cmb) delete cmb;
}

template <typename T>
void BTree<T>::reopen(int _fd, MODE _mode){
    mylog << "reopen()" << endl;
    fd = _fd;
    mode = _mode;
    tree_write(fd, this);
    if(mode == COPY_ON_WRITE)
        cmb = NULL;
    else
        cmb = new CMB(mode);
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
void BTree<T>::tree_read(int fd, BTree* tree){ 
    mylog << "tree_read()" << endl;

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    pread(fd, buf, PAGE_SIZE, 1 * PAGE_SIZE);
    
    memcpy((void*)tree, buf, sizeof(BTree));
    free(buf);
}

template <typename T>
void BTree<T>::tree_write(int fd, BTree* tree){
    mylog << "tree_write()" << endl;

    // Read before write to maintain the same block bitmap

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    pread(fd, buf, PAGE_SIZE, 1 * PAGE_SIZE);
    
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
        cmb->read(&block_id, node_id * sizeof(u_int64_t), sizeof(u_int64_t));  
    else
        block_id = node_id;

    if(block_id < 2){
        cout << " error : attempt to read block id = " << block_id << endl;
        mylog << " error : attempt to read block id = " << block_id << endl;
        return;
    }

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    pread(fd, buf, PAGE_SIZE, block_id * PAGE_SIZE);

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
        cmb->read(&block_id, node_id * sizeof(u_int64_t), sizeof(u_int64_t));  
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

    pwrite(fd, buf, PAGE_SIZE, block_id * PAGE_SIZE);

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
        }       

        node_read(root_id, root);
        if(root->num_key >= m){  
            int new_root_id;     

            if(cmb){
                new_root_id = cmb_get_new_node_id();
                cmb_update_node_id(new_root_id, get_free_block_id());
            }
            else
                new_root_id = get_free_block_id();

            BTreeNode<T>* new_root = new BTreeNode<T>(m, false, new_root_id);
            node_write(new_root_id, new_root);

            root_id = root->split(this, root_id, new_root_id, &rmlist);

            tree_write(fd, this);

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
            root_id = cmb_get_new_node_id();
            cmb_update_node_id(root_id, get_free_block_id());
        }
        else
            root_id = get_free_block_id();

        tree_write(fd, this);

        BTreeNode<T>* root = new BTreeNode<T>(m, true, root_id);
        node_write(root_id, root);
        delete root;
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
        }            

        node_read(root_id, root);
        if(root->num_key == 0){
            if(cmb)
                rmlist = new removeList(cmb_get_block_id(root_id), rmlist);
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
u_int64_t BTree<T>::cmb_get_new_node_id(){
    mylog << "cmb_get_new_node_id()" << endl;

    u_int64_t new_node_id;
    cmb->read(&new_node_id, BLOCK_MAPPING_START_ADDR, sizeof(u_int64_t));

    u_int64_t next_node_id = new_node_id + 1;
    if(next_node_id > (BLOCK_MAPPING_END_ADDR - BLOCK_MAPPING_START_ADDR) / sizeof(u_int64_t)){
        cout << "CMB Block Mapping Limit Exceeded" << endl;
        mylog << "CMB Block Mapping Limit Exceeded" << endl;
        exit(1);
    }
    cmb->write(BLOCK_MAPPING_START_ADDR, &next_node_id, sizeof(u_int64_t));

    return new_node_id;
}

template <typename T>
u_int64_t BTree<T>::cmb_get_block_id(u_int64_t node_id){
    mylog << "cmb_get_block_id() - node id:" << node_id << endl;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(u_int64_t);
    if(addr > BLOCK_MAPPING_END_ADDR){
        cout << "CMB Block Mapping Read Out of Range" << endl;
        mylog << "CMB Block Mapping Read Out of Range" << endl;
        exit(1);
    }

    u_int64_t readval;
	cmb->read(&readval, addr, sizeof(u_int64_t));
    return readval;
}

template <typename T>
void BTree<T>::cmb_update_node_id(u_int64_t node_id, u_int64_t block_id){
    mylog << "cmb_update_node_id() - node id:" << node_id << " block id:" << block_id << endl;
    off_t addr = BLOCK_MAPPING_START_ADDR + node_id * sizeof(u_int64_t);
    if(addr > BLOCK_MAPPING_END_ADDR){
        cout << "CMB Block Mapping Write Out of Range" << endl;
        mylog << "CMB Block Mapping Write Out of Range" << endl;
        exit(1);
    }

    u_int64_t writeval = block_id;
	cmb->write(addr, &writeval, sizeof(u_int64_t));
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
        cout << '[' << node_id << "=>" << t->cmb_get_block_id(node_id) << ']' << endl;

    int i = 0;
    for(i = 0; i < num_key; i++){
        if(!is_leaf){
            BTreeNode<T>* node = new BTreeNode<T>(0, 0, 0);
            t->node_read(child_id[i], node);
            node->display_tree(t, mode, level + 1);
            delete node;
        }
        for(int j = 0; j < level; j++) cout << '\t';
        cout << key[i] << endl;;
    }
    if(!is_leaf){
        BTreeNode<T>* node = new BTreeNode<T>(0, 0, 0);
        t->node_read(child_id[i], node);
        node->display_tree(t, mode, level + 1);
        delete node;
    }
}

template <typename T>
void BTreeNode<T>::inorder_traversal(BTree<T>* t, ofstream &outFile){
    mylog << "inorder_traversal()" << endl;

    int i = 0;
    for(i = 0; i < num_key; i++){
        if(!is_leaf){
            BTreeNode<T>* node = new BTreeNode<T>(0, 0, 0);
            t->node_read(child_id[i], node);
            node->inorder_traversal(t, outFile);
            delete node;
        }
        outFile << key[i] << '\t' << value[i].str << endl;;
    }
    if(!is_leaf){
        BTreeNode<T>* node = new BTreeNode<T>(0, 0, 0);
        t->node_read(child_id[i], node);
            node->inorder_traversal(t, outFile);
            delete node;
    }
}

template <typename T>
void BTreeNode<T>::search(BTree<T>* t, u_int64_t _k, T* buf){
    mylog << "serach() - key:" << _k << endl;

    int i;
    for(i = 0; i < num_key; i++){
        if(_k == key[i]){
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
    
    int i;
    for(i = 0; i < num_key; i++){
        // Key match
        if(_k == key[i]){
            value[i] = _v;
            if(t->cmb){
                *list = new removeList(t->cmb_get_block_id(node_id), *list);
                t->cmb_update_node_id(node_id, t->get_free_block_id());
            }
            else{
                *list = new removeList(node_id, *list);
                node_id = t->get_free_block_id();
            }                

            t->node_write(node_id, this);
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
        }
                
        t->node_read(child_id[i], child);
        if(child->num_key >= m)
            node_id = split(t, child_id[i], node_id, list);

        delete child;

        return node_id;
    }
}

template <typename T>
u_int64_t BTreeNode<T>::direct_insert(BTree<T>* t, u_int64_t _k, T _v, removeList** list, u_int64_t node_id1, u_int64_t node_id2){
    /* Assume the list is not full */
    if(num_key >= m) return node_id;

    mylog << "direct_insert() - key:" << _k << endl;

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

    if(t->cmb){
        *list = new removeList(t->cmb_get_block_id(node_id), *list);
        t->cmb_update_node_id(node_id, t->get_free_block_id());
    }
    else{
        *list = new removeList(node_id, *list);
        node_id = t->get_free_block_id();
    }

    t->node_write(node_id, this);

    return node_id;
}

template <typename T>
u_int64_t BTreeNode<T>::split(BTree<T>*t, u_int64_t spt_node_id, u_int64_t parent_id, removeList** list){
    mylog << "split() - node id:" << spt_node_id << " parent node id:" << parent_id << endl;
    
    BTreeNode<T>* node = new BTreeNode<T>(0, 0, 0);
    t->node_read(spt_node_id, node);

    BTreeNode<T>* parent = new BTreeNode<T>(0, 0, 0);
    t->node_read(parent_id, parent);

    int new_node_id;
    if(t->cmb){
        new_node_id = t->cmb_get_new_node_id();
        t->cmb_update_node_id(new_node_id, t->get_free_block_id());
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
        *list = new removeList(t->cmb_get_block_id(node->node_id), *list);
        t->cmb_update_node_id(node->node_id, t->get_free_block_id());
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
    t->node_write(new_node_id, new_node);

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

            if(succ->num_key > min_num){
                mylog << "borrow_from_succesor() - node id:" << node->node_id << endl;
                // Borrow from succ
                key[i] = succ->key[0];
                value[i] = succ->value[0];
                // Delete the kv from succ
                child_id[i+1] = node->traverse_delete(t, key[i], list);

                if(t->cmb){
                    *list = new removeList(t->cmb_get_block_id(node_id), *list);
                    t->cmb_update_node_id(node_id, t->get_free_block_id());
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

                mylog << "borrow_from_predecesor() - node id:" << node->node_id << endl;
                // Borrow from pred
                key[i] = pred->key[pred->num_key - 1];
                value[i] = pred->value[pred->num_key - 1];
                // Delete the kv form pred
                child_id[i] = node->traverse_delete(t, key[i], list);

                if(t->cmb){
                    *list = new removeList(t->cmb_get_block_id(node_id), *list);
                    t->cmb_update_node_id(node_id, t->get_free_block_id());
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
    
    int i;
    for(i = 0; i < num_key; i++){
        if(key[i] == _k) break;
    }
    
    if(i == num_key) return 0;

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
        *list = new removeList(t->cmb_get_block_id(node_id), *list);
        t->cmb_update_node_id(node_id, t->get_free_block_id());
    }
    else{
        *list = new removeList(node_id, *list);
        node_id = t->get_free_block_id();
    }

    t->node_write(node_id, this);

    return node_id;
}

template <typename T>
u_int64_t BTreeNode<T>::rebalance(BTree<T>* t, int idx, removeList** list){

    BTreeNode<T>* node = new BTreeNode<T>(0, 0, 0);
    t->node_read(child_id[idx], node);

    if(node->num_key >= min_num){
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

    if(idx - 1 >= 0 && left->num_key > min_num){
        mylog << "borrow_from_sibling() - node id:" << child_id[idx] << endl;
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
            *list = new removeList(t->cmb_get_block_id(node_id), *list);
            t->cmb_update_node_id(node_id, t->get_free_block_id());
        }
        else{
            *list = new removeList(node_id, *list);
            node_id = t->get_free_block_id();
        }

        t->node_write(node_id, this);                
    }
    else if(idx + 1 <= num_key && right->num_key > min_num){
        mylog << "borrow_from_sibling() - node id:" << child_id[idx] << endl;
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
            *list = new removeList(t->cmb_get_block_id(node_id), *list);
            t->cmb_update_node_id(node_id, t->get_free_block_id());
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
        child_id[idx] = left->direct_insert(t, key[idx], value[idx], list, 0, trans_node_id);
        left->key[left->num_key] = key[idx];
        left->value[left->num_key] = value[idx];
        if(!right->is_leaf)
            left->child_id[left->num_key+1] = right->child_id[0];
        left->num_key++;
            // Insert right to left
        t->node_read(child_id[idx], left);
        for(int i = 0; i < right->num_key; i++){
            left->key[left->num_key] = right->key[i];
            left->value[left->num_key] = right->value[i];
            if(!right->is_leaf) 
                left->child_id[left->num_key+1] = right->child_id[i+1];
            left->num_key++;  
        }
            // Store left node
        if(t->cmb){
            *list = new removeList(t->cmb_get_block_id(node_id), *list);
            t->cmb_update_node_id(left->node_id, t->get_free_block_id());
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
            *list = new removeList(t->cmb_get_block_id(node_id), *list);
            t->cmb_update_node_id(node_id, t->get_free_block_id());
        }
        else{
            *list = new removeList(node_id, *list);
            node_id = t->get_free_block_id();
        }

        t->node_write(node_id, this);

        if(t->cmb)
            *list = new removeList(t->cmb_get_block_id(right->node_id), *list);
        else
            *list = new removeList(right->node_id, *list);
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
    for(int i = 0; i < len / sizeof(u_int64_t); i++)
        *d++ = *s++;
}

void CMB::remap(off_t offset){
    if( ((bar_addr + offset) & ~MAP_MASK) != map_idx ){
        mylog << "remap() - offset:" << offset << endl;
        if(cache_base)
            memcpy(map_base, cache_base, MAP_SIZE);  

        map_idx = (bar_addr + offset) & ~MAP_MASK;

        /* Unmap the previous */
        if (munmap(map_base, MAP_SIZE) == -1) FATAL;

        /* Remap a new page */
        map_base = mmap(0, MAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, map_idx);
	    if (map_base == (void*)-1) FATAL;

        if(cache_base)
            cmb_memcpy(cache_base, map_base, MAP_SIZE);
    }
}

void CMB::read(void* buf, off_t offset, int size){
    mylog << "CMB.read() - offset:" << offset << endl;
    remap(offset);

    void* virt_addr;
    if(cache_base)
	    virt_addr = (char*)cache_base + ((offset) & MAP_MASK);	
    else
        virt_addr = (char*)map_base + ((offset) & MAP_MASK);
	cmb_memcpy(buf, virt_addr, size);
}

void CMB::write(off_t offset, void* buf, int size){
    mylog << "CMB.write() - offset:" << offset << endl;
    remap(offset);

    void* virt_addr;
    if(cache_base)
	    virt_addr = (char*)cache_base + ((offset) & MAP_MASK);	
    else
        virt_addr = (char*)map_base + ((offset) & MAP_MASK);
    cmb_memcpy(virt_addr, buf, size);
}

ITV2Leaf::ITV2Leaf(CMB* cmb){
    Q_front_idx = 0;
    Q_rear_idx = 0;
    free_Stack_idx = 0;
    free_idx = 1;

    off_t offset = LEAF_CACHE_START_ADDR;

    cmb->write(offset, this, sizeof(ITV2Leaf));
}

void ITV2Leaf::stat(CMB* cmb){
    ITV2Leaf* meta = (ITV2Leaf*) malloc(sizeof(ITV2Leaf));

    off_t offset = LEAF_CACHE_START_ADDR;
    cmb->read(meta, offset, sizeof(ITV2Leaf));

    cout << "ITV2Leaf: stat()" << endl;
    cout << "Q_front_idx = " << meta->Q_front_idx << endl;
    cout << "Q_rear_idx = " << meta->Q_rear_idx << endl;
    cout << "free_Stack_idx = " << meta->free_Stack_idx << endl;
    cout << "free_idx = " << meta->free_idx << endl;

    free(meta);
}

u_int64_t ITV2Leaf::get_free_idx(CMB* cmb){
    // If the stack is empty
    if(free_Stack_idx == 0){
        off_t ret_idx = free_idx;
        free_idx++;

        // Write the new free_idx back
        off_t offset = LEAF_CACHE_START_ADDR + ((char*) &free_idx - (char*) this);
        cmb->write(offset, &free_idx, sizeof(u_int64_t));

        return ret_idx;
    }
    else
        return free_pop(cmb);
}

void ITV2Leaf::free_push(CMB* cmb, u_int64_t idx){
    // Set the new head->next as the current head idx
    off_t offset = LEAF_CACHE_BASE_ADDR + idx * sizeof(ITV2LeafCache);
    if(offset >= LEAF_CACHE_END_ADDR){
        cout << "Leaf Cache Limit Excced" << endl;
        mylog << "Leaf Cache Limit Excced" << endl;
        exit(1);
    }
    cmb->write(offset, &free_Stack_idx, sizeof(u_int64_t));

    // Set the stack head to the new head
    offset = LEAF_CACHE_START_ADDR + ((char*)&free_Stack_idx - (char*)this);
    cmb->write(offset, &idx, sizeof(u_int64_t)); 

    free_Stack_idx = idx;
}

u_int64_t ITV2Leaf::free_pop(CMB* cmb){
    u_int64_t ret, next;

    if(free_Stack_idx == 0)
        return 0;

    // Return value would be the stack head
    ret = free_Stack_idx;
    
    // Read the new stack head from the head idx
    off_t offset = LEAF_CACHE_BASE_ADDR + free_Stack_idx * sizeof(ITV2LeafCache);
    if(offset >= LEAF_CACHE_END_ADDR){
        cout << "Leaf Cache Limit Excced" << endl;
        mylog << "Leaf Cache Limit Excced" << endl;
        exit(1);
    }
    cmb->read(&next, offset, sizeof(u_int64_t));

    // Write the new stack head back
    free_Stack_idx = next;

    offset = LEAF_CACHE_START_ADDR + ((char*)&free_Stack_idx - (char*)this);
    if(offset >= LEAF_CACHE_END_ADDR){
        cout << "Leaf Cache Limit Excced" << endl;
        mylog << "Leaf Cache Limit Excced" << endl;
        exit(1);
    }
    cmb->write(offset, &free_Stack_idx, sizeof(u_int64_t));

    return ret;
}

void ITV2Leaf::enqueue(CMB* cmb, u_int64_t node_id, u_int64_t lower, u_int64_t upper){
    // Get the new idx
    u_int64_t new_idx = get_free_idx(cmb);

    // Write the cache to the CMB
    ITV2LeafCache* new_cache = new ITV2LeafCache();
    new_cache->node_id = node_id;
    new_cache->lower = lower;
    new_cache->upper = upper;
    new_cache->lastQ = Q_rear_idx;
    new_cache->nextQ = 0;

    off_t offset = LEAF_CACHE_BASE_ADDR + new_idx * sizeof(ITV2LeafCache);
    if(offset >= LEAF_CACHE_END_ADDR){
        cout << "Leaf Cache Limit Excced" << endl;
        mylog << "Leaf Cache Limit Excced" << endl;
        exit(1);
    }
    cmb->write(offset, new_cache, sizeof(ITV2LeafCache));

    // Set the old rear->next to new cache
    if(Q_rear_idx != 0){
        offset = LEAF_CACHE_BASE_ADDR + Q_rear_idx * sizeof(ITV2LeafCache) + ((char*)&(new_cache->nextQ) - (char*)new_cache);
        if(offset >= LEAF_CACHE_END_ADDR){
            cout << "Leaf Cache Limit Excced" << endl;
            mylog << "Leaf Cache Limit Excced" << endl;
            exit(1);
        }
        cmb->write(offset, &new_idx, sizeof(u_int64_t));
    }

    // Set the rear idx to the new cache idx
    // If front == 0 which means the queue is empty, set the front as well
    offset = LEAF_CACHE_START_ADDR + ((char*)&Q_rear_idx - (char*)this);
    if(offset >= LEAF_CACHE_END_ADDR){
        cout << "Leaf Cache Limit Excced" << endl;
        mylog << "Leaf Cache Limit Excced" << endl;
        exit(1);
    }
    cmb->write(offset, &new_idx, sizeof(u_int64_t));
    Q_rear_idx = new_idx;

    if(Q_front_idx == 0){
        offset = LEAF_CACHE_START_ADDR + ((char*)&Q_front_idx - (char*)this);
        if(offset >= LEAF_CACHE_END_ADDR){
            cout << "Leaf Cache Limit Excced" << endl;
            mylog << "Leaf Cache Limit Excced" << endl;
            exit(1);
        }
        cmb->write(offset, &new_idx, sizeof(u_int64_t));
        Q_front_idx = new_idx;
    }

    delete new_cache;
}

void ITV2Leaf::dequeue(CMB* cmb, ITV2LeafCache* buf){
    if(Q_front_idx == 0){
        memset(buf, 0, sizeof(ITV2LeafCache));
        return;
    }

    u_int64_t del_idx = Q_front_idx;

    // Read the second idx
    off_t offset = LEAF_CACHE_BASE_ADDR + Q_front_idx * sizeof(ITV2LeafCache);
    cmb->read(buf, offset, sizeof(ITV2LeafCache));

    // Set the front to the second idx
    offset = LEAF_CACHE_START_ADDR + ((char*) &Q_front_idx - (char*)this);
    cmb->write(offset, &(buf->nextQ), sizeof(u_int64_t));
    Q_front_idx = buf->nextQ;

    // If the Q_front_idx == 0, which means the queue is empty, set the rear to 0 as well
    if(Q_front_idx == 0){
        offset = LEAF_CACHE_START_ADDR + ((char*) &Q_rear_idx - (char*)this);
        cmb->write(offset, &Q_front_idx, sizeof(u_int64_t));
        Q_rear_idx = 0;
    }
    else{
        // Set front->lastQ to 0
        u_int64_t zero = 0;
        offset = LEAF_CACHE_BASE_ADDR + Q_front_idx * sizeof(ITV2LeafCache) + ((char*)&(buf->lastQ) - (char*)buf);
        cmb->write(offset, &zero, sizeof(u_int64_t));
    }

    // return the free cache slot to free stack
    free_push(cmb, del_idx);
}

u_int64_t ITV2Leaf::search(CMB* cmb, u_int64_t key){
    return 0;
}

#endif /* B_TREE_H */
