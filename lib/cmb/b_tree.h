#ifndef B_TREE_H //include guard
#define B_TREE_H

#include <iostream>
#include <fstream>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

using namespace std;

#define FATAL do { fprintf(stderr, "Error at line %d, file %s (%d) [%s]\n", \
  __LINE__, __FILE__, errno, strerror(errno)); exit(1); } while(0)

#define PAGE_SIZE 16384UL
#define MAP_SIZE 16384UL
#define MAP_MASK (MAP_SIZE - 1)

#define CMB_ADDR 0xC0000000

enum mode {REAL_CMB, FAKE_CMB, DRAM};
#define CUR_MODE REAL_CMB

template <typename T> class BTree;
template <typename T> class BTreeNode;
class CMB;
class removeList;
ofstream mylog;

template <typename T>
class BTree{
	int m;				// degree
	u_int64_t root_id;		// Pointer to root node
	int fd;
	int block_size;
	int block_cap;
	CMB* cmb;
		
	public:	
		BTree(char* filename, int degree);
		~BTree();

		void reopen(int _fd);

		void stat();

		void tree_read(int fd, BTree* tree);
		void tree_write(int fd, BTree* tree);

		void node_read(u_int64_t node_id, BTreeNode<T>* node);
		void node_write(u_int64_t node_id, BTreeNode<T>* node);

		u_int64_t get_free_node_id();

		u_int64_t get_block_id(u_int64_t node_id);
		void update_node_id(u_int64_t node_id, u_int64_t block_id);

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
		
		BTreeNode(int _m, bool _is_leaf, u_int64_t _node_id);
		~BTreeNode();

        void stat();

		void display_tree(BTree<T>* t, int level);
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

class CMB{
	int fd;
    off_t bar_addr;
	void* map_base;
    off_t map_idx;
    void* cache_base;

	public:
		CMB();
		~CMB();

        void cmb_memcpy(void* dest, void* src, size_t len);
        void remap(off_t offset);

		void read(void* buf, off_t offset, int size);
		void write(off_t offset, void* buf, int size);

};

class removeList{
	public:
		int id;
		removeList* next;

		removeList(int _id, removeList* _next);
};

template <typename T>
BTree<T>::BTree(char* filename, int degree){
    mylog << "BTree()" << endl;

    if(degree > (int)((PAGE_SIZE - sizeof(BTreeNode<T>) - sizeof(u_int64_t)) / (sizeof(u_int64_t) + sizeof(T))) ){
        cout << " Error: Degree exceed " << endl;
        return;
    }

	// Create new file and initialize the tree
    fd = open(filename, O_CREAT | O_RDWR | O_DIRECT, S_IRUSR | S_IWUSR);
    block_size = PAGE_SIZE;
    m = degree;
    block_cap = (block_size - sizeof(BTree)) * 8;
    root_id = 0;

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    
    memcpy(buf, this, sizeof(BTree));
    write(fd, buf, PAGE_SIZE);

    free(buf);

    set_block_id(0, true);
    
	// Map CMB
	cmb = new CMB();    
    u_int64_t first_node_id = 1;
    cmb->write(0, &first_node_id, sizeof(u_int64_t));    
}

template <typename T>
BTree<T>::~BTree(){
    mylog << "~BTree()" << endl;
    close(fd);
	delete cmb;	
}

template <typename T>
void BTree<T>::reopen(int _fd){
    mylog << "reopen()" << endl;
	fd = _fd;
	cmb = new CMB();
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

    lseek(fd, 0, SEEK_SET);

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    read(fd, buf, PAGE_SIZE);
    
    memcpy((void*)tree, buf, sizeof(BTree));
    free(buf);
}

template <typename T>
void BTree<T>::tree_write(int fd, BTree* tree){
    mylog << "tree_write()" << endl;

    lseek(fd, 0, SEEK_SET);

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    read(fd, buf, PAGE_SIZE);
    
    lseek(fd, 0, SEEK_SET);
    memcpy(buf, tree, sizeof(BTree));
    write(fd, buf, PAGE_SIZE);
    free(buf);
}

template <typename T>
void BTree<T>::node_read(u_int64_t node_id, BTreeNode<T>* node){
    mylog << "node_read() - node_id:" << node_id << endl;

    if(node_id == 0){
        cout << " Error : Attempt to read node id = 0 " << endl;
        mylog << " Error : Attempt to read node id = 0 " << endl;
        return;
    }

    delete [] node->key;
    delete [] node->value;
    delete [] node->child_id;

	u_int64_t block_id;
    cmb->read(&block_id, node_id * sizeof(u_int64_t), sizeof(u_int64_t));

    lseek(fd, block_id * PAGE_SIZE, SEEK_SET);

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    read(fd, buf, PAGE_SIZE);

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
    cmb->read(&block_id, node_id * sizeof(u_int64_t), sizeof(u_int64_t));

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

    lseek(fd, block_id * PAGE_SIZE, SEEK_SET);
    write(fd, buf, PAGE_SIZE);

    free(buf);
}

template <typename T>
u_int64_t BTree<T>::get_free_node_id(){
    mylog << "get_free_node_id()" << endl;

    u_int64_t free_node_id;
    cmb->read(&free_node_id, 0, sizeof(u_int64_t));
    u_int64_t next_node_id = free_node_id + 1;
    cmb->write(0, &next_node_id, sizeof(u_int64_t));

    return free_node_id;
}

template <typename T>
u_int64_t BTree<T>::get_block_id(u_int64_t node_id){
    mylog << "get_block_id() - node id:" << node_id << endl;
    u_int64_t readval;
	cmb->read(&readval, node_id * sizeof(u_int64_t), sizeof(u_int64_t));
    return readval;
}

template <typename T>
void BTree<T>::update_node_id(u_int64_t node_id, u_int64_t block_id){
    mylog << "update_node_id() - node id:" << node_id << " block id:" << block_id << endl;
    u_int64_t writeval = block_id;
	cmb->write(node_id * sizeof(u_int64_t), &writeval, sizeof(u_int64_t));
}

template <typename T>
u_int64_t BTree<T>::get_free_block_id(){
    mylog << "get_free_block_id()" << endl;

    lseek(fd, 0, SEEK_SET);

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    read(fd, buf, PAGE_SIZE);

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

    lseek(fd, 0, SEEK_SET);

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    read(fd, buf, PAGE_SIZE);

    char* byte_ptr =  buf + sizeof(BTree) + (block_id / 8);

    if(bit)
        *byte_ptr |= (1UL << (block_id % 8));
    else
        *byte_ptr &= ~(1UL << (block_id % 8));

    lseek(fd, 0, SEEK_SET);
    write(fd, buf, PAGE_SIZE);

    free(buf);
}

template <typename T>
void BTree<T>::print_used_block_id(){
    mylog << "print_used_block_id()" << endl;
    mylog << "\tUsed Block : " << endl << "\t";

    lseek(fd, 0, SEEK_SET);

    char* buf;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    read(fd, buf, PAGE_SIZE);

    char* byte_ptr =  buf + sizeof(BTree);
    int id = 0;
    while(id < block_cap){
        char byte = *byte_ptr;
        for(int i = 0; i < 8; i++){
            if( (byte & 1) ){
                mylog << " " << id;
            }
            byte >>= 1;
            id++;
        }
        byte_ptr++;
    }
    mylog << endl;

    free(buf);
}

template <typename T>
void BTree<T>::display_tree(){
    mylog << "display_tree()" << endl;

    if(root_id){
        BTreeNode<T>* root = new BTreeNode<T>(0, 0, 0);
        node_read(root_id, root);
        root->display_tree(this, 0);
        delete root;
    }
    else
        mylog << " The tree is empty! " << endl;
}

template <typename T>
void BTree<T>::inorder_traversal(char* filename){
    mylog << "inorder_traversal()" << endl;

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

        node_read(root_id, root);
        if(root->num_key >= m){  
            int new_root_id = get_free_node_id();
            update_node_id(new_root_id, get_free_block_id());
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
        root_id = get_free_node_id();
        update_node_id(root_id, get_free_block_id());
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

        node_read(root_id, root);
        if(root->num_key == 0){
            rmlist = new removeList(get_block_id(root_id), rmlist);
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
void BTreeNode<T>::display_tree(BTree<T>* t, int level){
    mylog << "display_tree()" << endl;

    for(int j = 0; j < level; j++) mylog << '\t';
        mylog << '[' << node_id << "=>" << t->get_block_id(node_id) << ']' << endl;;

    int i = 0;
    for(i = 0; i < num_key; i++){
        if(!is_leaf){
            BTreeNode<T>* node = new BTreeNode<T>(0, 0, 0);
            t->node_read(child_id[i], node);
            node->display_tree(t, level + 1);
            delete node;
        }
        for(int j = 0; j < level; j++) mylog << '\t';
        mylog << key[i] << endl;;
    }
    if(!is_leaf){
        BTreeNode<T>* node = new BTreeNode<T>(0, 0, 0);
        t->node_read(child_id[i], node);
        node->display_tree(t, level + 1);
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
            *list = new removeList(t->get_block_id(node_id), *list);
            t->update_node_id(node_id, t->get_free_block_id());

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
                
        t->node_read(child_id[i], child);
        if(child->num_key >= m){
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

    *list = new removeList(t->get_block_id(node_id), *list);
    t->update_node_id(node_id, t->get_free_block_id());

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

    int new_node_id = t->get_free_node_id();
    t->update_node_id(new_node_id, t->get_free_block_id());
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

    *list = new removeList(t->get_block_id(node->node_id), *list);

    t->update_node_id(node->node_id, t->get_free_block_id());
    int dup_par_id = parent->direct_insert(t, node->key[min_num], node->value[min_num], list, node->node_id, new_node_id);
    node->num_key = min_num;    

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
                // Borrow from succ
                key[i] = succ->key[0];
                value[i] = succ->value[0];
                // Delete the kv from succ
                child_id[i+1] = node->traverse_delete(t, key[i], list);

                *list = new removeList(t->get_block_id(node_id), *list);
                t->update_node_id(node_id, t->get_free_block_id());
                t->node_write(node_id, this);
                i = i + 1;
            }
            else{
                t->node_read(child_id[i], node);

                int pred_id = node->get_pred(t);
                BTreeNode<T>* pred = new BTreeNode<T>(0, 0, 0);    
                t->node_read(pred_id, pred);

                // Borrow from pred
                key[i] = pred->key[pred->num_key - 1];
                value[i] = pred->value[pred->num_key - 1];
                // Delete the kv form pred
                child_id[i] = node->traverse_delete(t, key[i], list);

                *list = new removeList(t->get_block_id(node_id), *list);
                t->update_node_id(node_id, t->get_free_block_id());
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

    *list = new removeList(t->get_block_id(node_id), *list);
    t->update_node_id(node_id, t->get_free_block_id());

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

        *list = new removeList(t->get_block_id(node_id), *list);
        t->update_node_id(node_id, t->get_free_block_id());
        t->node_write(node_id, this);                
    }
    else if(idx + 1 <= num_key && right->num_key > min_num){
        //Borrowing from right
        t->node_read(child_id[idx], left);
        t->node_read(child_id[idx+1], right);
        trans_node_id = (right->is_leaf) ? 0 : right->child_id[0];
            // Insert to left
        child_id[idx] = left->direct_insert(t, key[idx], value[idx], list, 0, trans_node_id);
            // Borrow from right
        key[idx] = right->key[0];
        value[idx] = right->value[0];
            // Delete form left
        child_id[idx+1] = right->direct_delete(t, key[idx], list);

        *list = new removeList(t->get_block_id(node_id), *list);
        t->update_node_id(node_id, t->get_free_block_id());
        t->node_write(node_id, this);         
    }   
    else{
        //Merge with right unless idx = num_key
        if(idx == num_key) idx -= 1;
        t->node_read(child_id[idx], left);
        t->node_read(child_id[idx+1], right);
        trans_node_id = (right->is_leaf) ? 0 : right->child_id[0];
            // Insert parent's kv to left
        child_id[idx] = left->direct_insert(t, key[idx], value[idx], list, 0, trans_node_id);
            // Insert right to left
        for(int i = 0; i < right->num_key; i++){
            t->node_read(child_id[idx], left);
            trans_node_id = (right->is_leaf) ? 0 : right->child_id[i+1];
            child_id[idx] = left->direct_insert(t, right->key[i], right->value[i], list, 0, trans_node_id);
        }
        t->node_read(child_id[idx], left);
            // Delete the parent's kv
        node_id = direct_delete(t, key[idx], list);
        child_id[idx] = left->node_id;

        *list = new removeList(t->get_block_id(node_id), *list);
        t->update_node_id(node_id, t->get_free_block_id());
        t->node_write(node_id, this);

        *list = new removeList(t->get_block_id(right->node_id), *list);
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

CMB::CMB(){
    mylog << "CMB()" << endl;

    if (CUR_MODE == REAL_CMB){
        if ((fd = open("/dev/mem", O_RDWR | O_SYNC)) == -1) FATAL;
        printf("\n/dev/mem opened.\n");
        bar_addr = CMB_ADDR;
    }
    else if (CUR_MODE == FAKE_CMB || CUR_MODE == DRAM){
        if ((fd = open("fake_cmb", O_RDWR | O_SYNC)) == -1) FATAL; 
        printf("\nfake_cmb opened.\n");
        bar_addr = 0;
    }
    
    map_idx = bar_addr & ~MAP_SIZE;

        /* Map one page */  
    map_base = mmap(0, MAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, map_idx);
    if (map_base == (void*)-1) FATAL;
    printf("Memory mapped at address %p.\n", map_base);

    cache_base = malloc(MAP_SIZE);

    u_int64_t* dest = (u_int64_t*) cache_base;
    u_int64_t* src = (u_int64_t*) map_base;
    for(int i = 0; i < MAP_SIZE / sizeof(u_int64_t); i++)
        *dest++ = *src++;
}

CMB::~CMB(){
    mylog << "~CMB()" << endl;

    if(CUR_MODE == DRAM)
        memcpy(map_base, cache_base, MAP_SIZE);    

    if (munmap(map_base, MAP_SIZE) == -1) FATAL;
    free(cache_base);
	close(fd);
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
        if(CUR_MODE == DRAM)
            memcpy(map_base, cache_base, MAP_SIZE);  

        map_idx = (bar_addr + offset) & ~MAP_MASK;

        /* Unmap the previous */
        if (munmap(map_base, MAP_SIZE) == -1) FATAL;

        /* Rempa a new page */
        map_base = mmap(0, MAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, map_idx);
	    if (map_base == (void*)-1) FATAL;

        cmb_memcpy(cache_base, map_base, MAP_SIZE);
    }
}

void CMB::read(void* buf, off_t offset, int size){
    mylog << "CMB.read() - offset:" << offset << endl;
    remap(offset);

	void* virt_addr = (char*)cache_base + ((offset) & MAP_MASK);	
	cmb_memcpy(buf, virt_addr, size);
}

void CMB::write(off_t offset, void* buf, int size){
    mylog << "CMB.write() - offset:" << offset << endl;
    remap(offset);

    void* virt_addr;
    if(CUR_MODE == REAL_CMB || CUR_MODE == FAKE_CMB){
        virt_addr = (char*)map_base + ((bar_addr + offset) & MAP_MASK);	
        cmb_memcpy(virt_addr, buf, size);
    }

	virt_addr = (char*)cache_base + ((offset) & MAP_MASK);	
	cmb_memcpy(virt_addr, buf, size);
}

#endif /* B_TREE_H */
