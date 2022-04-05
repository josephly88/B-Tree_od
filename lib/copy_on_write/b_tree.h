#ifndef B_TREE_H //include guard
#define B_TREE_H

#include <iostream>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;

#define PAGE_SIZE 4096UL

template <typename T> class BTree;
template <typename T> class BTreeNode;
class removeList;

template <typename T>
class BTree{
	int m;				// degree
	u_int64_t root_id;		// Pointer to root node
	int fd;
	int block_size;
	int block_cap;
		
	public:
		BTree(char* filename, int degree);
		~BTree();

		void reopen(int _fd);

		void stat();

		void tree_read(int fd, BTree* tree);
		void tree_write(int fd, BTree* tree);

		void node_read(u_int64_t block_id, BTreeNode<T>* node);
		void node_write(u_int64_t block_id, BTreeNode<T>* node);

		int get_free_block_id();
		void set_block_id(u_int64_t block_id, bool bit);
		void print_used_block_id();

		void traverse();
		T* search(u_int64_t _k);

		void insertion(u_int64_t _k, T _v);
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

		void traverse(BTree<T>* t, int level);
		T* search(BTree<T>* t, u_int64_t _k);

		int traverse_insert(BTree<T>* t, u_int64_t _k, T _v, removeList** list);
		int direct_insert(BTree<T>* t, u_int64_t _k, T _v, removeList** list, u_int64_t node_id1 = 0, u_int64_t node_id2 = 0);
		int split(BTree<T>* t, u_int64_t node_id, u_int64_t parent_id, removeList** list);

		int traverse_delete(BTree<T>* t, u_int64_t _k, removeList** _list);
		int direct_delete(BTree<T>* t, u_int64_t _k, removeList** _list);
		int rebalance(BTree<T>* t, u_int64_t idx, removeList** _list);
		int get_pred(BTree<T>* t);
		int get_succ(BTree<T>* t);	
};

class removeList{
	public:
		int id;
		removeList* next;
		removeList(int _id, removeList* _next);
		~removeList();
};

template <typename T>
BTree<T>::BTree(char* filename, int degree){
    if(degree > (int)((PAGE_SIZE - sizeof(BTreeNode<T>) - sizeof(int)) / (sizeof(int) + sizeof(char))) ){
        cout << " Error: Degree exceed " << endl;
        return;
    }

    fd = open(filename, O_CREAT | O_RDWR | O_DIRECT, S_IRUSR | S_IWUSR);
    block_size = PAGE_SIZE;
    m = degree;
    block_cap = (block_size - sizeof(BTree)) * 8;
    root_id = 0;
    
    u_int8_t* buf = NULL;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    memset(buf, 0, PAGE_SIZE);
    
    memcpy(buf, this, sizeof(BTree));
    write(fd, buf, PAGE_SIZE);

    free(buf);

    set_block_id(0, true);
}

template <typename T>
BTree<T>::~BTree(){
    close(fd);
}

template <typename T>
void BTree<T>::reopen(int _fd){
    fd = _fd;
    tree_write(fd, this);
}

template <typename T>
void BTree<T>::stat(){
    cout << endl;
    cout << "Degree: " << m << endl;
    cout << "fd: " << fd << endl;
    cout << "Block size: " << block_size << endl;
    cout << "Block Capacity: " << block_cap << endl;
    cout << "Root Block ID: " << root_id << endl;
    print_used_block_id();
    cout << endl;
}

template <typename T>
void BTree<T>::tree_read(int fd, BTree* tree){ 
    uint8_t* buf = NULL;
    lseek(fd, 0, SEEK_SET);
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    read(fd, buf, PAGE_SIZE);
    
    memcpy((void*)tree, buf, sizeof(BTree));
    free(buf);
}

template <typename T>
void BTree<T>::tree_write(int fd, BTree* tree){
    uint8_t* buf = NULL;
    lseek(fd, 0, SEEK_SET);
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    read(fd, buf, PAGE_SIZE);
    
    memcpy(buf, tree, sizeof(BTree));
    lseek(fd, 0, SEEK_SET);
    write(fd, buf, PAGE_SIZE);
    free(buf);
}

template <typename T>
void BTree<T>::node_read(u_int64_t block_id, BTreeNode<T>* node){
    if(block_id == 0){
        cout << " Error : Attempt to read block id = 0 " << endl;
        return;
    }

    uint8_t* buf = NULL;
    lseek(fd, block_id * PAGE_SIZE, SEEK_SET);
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    read(fd, buf, PAGE_SIZE);

    uint8_t* ptr = buf;
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
void BTree<T>::node_write(u_int64_t block_id, BTreeNode<T>* node){
    if(block_id == 0){
        cout << " Error : Attempt to write block id = 0 " << endl;
        return;
    }

    uint8_t* buf = NULL;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);

    uint8_t* ptr = buf;
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
int BTree<T>::get_free_block_id(){
    uint8_t* buf = NULL;
    lseek(fd, 0, SEEK_SET);
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    read(fd, buf, PAGE_SIZE);

    uint8_t* byte_ptr =  buf + sizeof(BTree);
    int id = 0;
    while(id < block_cap){
        uint8_t byte = *byte_ptr;
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
    if(block_id >= block_cap) return;

    uint8_t* buf = NULL;
    lseek(fd, 0, SEEK_SET);
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    read(fd, buf, PAGE_SIZE);

    uint8_t* byte_ptr =  buf + sizeof(BTree) + (block_id / 8);

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
    cout << endl;
    cout << "Used Block : " << endl;

    uint8_t* buf = NULL;
    lseek(fd, 0, SEEK_SET);
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    read(fd, buf, PAGE_SIZE);

    uint8_t* byte_ptr =  buf + sizeof(BTree);
    int id = 0;
    while(id < block_cap){
        uint8_t byte = *byte_ptr;
        for(int i = 0; i < 8; i++){
            if( (byte & 1) ){
                cout << " " << id;
            }
            byte >>= 1;
            id++;
        }
        byte_ptr++;
    }
    cout << endl << endl;

    free(buf);
}

template <typename T>
void BTree<T>::traverse(){
    cout << endl << "Tree Traversal: " << endl;

    if(root_id){
        BTreeNode<T>* root = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
        node_read(root_id, root);
        root->traverse(this, 0);
        delete root;
    }
    else
        cout << " The tree is empty! " << endl;

    cout << endl;
}

template <typename T>
T* BTree<T>::search(u_int64_t _k){
    if(root_id){
        BTreeNode<T>* root = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
        node_read(root_id, root);
        return root->search(this, _k);
        delete root;
    }
    else
        return NULL;
}

template <typename T>
void BTree<T>::insertion(u_int64_t _k, T _v){

    if(root_id){
        removeList* rmlist = NULL;

        BTreeNode<T>* root = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
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
            int new_root_id = get_free_block_id();
            BTreeNode<T>* new_root = new BTreeNode<T>(m, false, new_root_id);
            node_write(new_root_id, new_root);

            root_id = root->split(this, root_id, new_root_id, &rmlist);

            tree_write(fd, this);

            delete new_root;
        }
        if(rmlist){
            removeList* rmlist_itr = rmlist;
            while(rmlist_itr != NULL){
                set_block_id(rmlist_itr->id, false);
                rmlist_itr = rmlist_itr->next;
            }
            delete rmlist;
        }
        delete root;

    }
    else{
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
    if(root_id){
        removeList* rmlist = NULL;

        BTreeNode<T>* root = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
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
            removeList* rmlist_itr = rmlist;
            while(rmlist_itr != NULL){
                set_block_id(rmlist_itr->id, false);
                rmlist_itr = rmlist_itr->next;
            }
            delete rmlist;
        }
        delete root;          
    }
    else
        return;
}

template <typename T>
BTreeNode<T>::BTreeNode(int _m, bool _is_leaf, u_int64_t _node_id){
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
    delete key;
    delete value;
    delete child_id;
}

template <typename T>
void BTreeNode<T>::stat(){
    cout << endl;
    cout << "Degree: " << m << endl;
    cout << "Minimun Key: " << min_num << endl;
    cout << "# keys: " << num_key << endl;
    cout << "Is leaf? : " << is_leaf << endl;
    cout << "Node ID: " << node_id << endl;
    cout << endl;
}

template <typename T>
void BTreeNode<T>::traverse(BTree<T>* t, int level){
    for(int j = 0; j < level; j++) cout << '\t';
        cout << '[' << node_id << ']' << endl;;

    int i = 0;
    for(i = 0; i < num_key; i++){
        if(!is_leaf){
            BTreeNode<T>* node = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
            t->node_read(child_id[i], node);
            node->traverse(t, level + 1);
            delete node;
        }
        for(int j = 0; j < level; j++) cout << '\t';
        cout << key[i] << '(' << value[i] << ')' << endl;;
    }
    if(!is_leaf){
        BTreeNode<T>* node = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
        t->node_read(child_id[i], node);
        node->traverse(t, level + 1);
        delete node;
    }
}

template <typename T>
T* BTreeNode<T>::search(BTree<T>* t, u_int64_t _k){
    int i;
    for(i = 0; i < num_key; i++){
        if(_k == key[i]) return &value[i];
        if(_k < key[i]){
            if(!is_leaf){
                BTreeNode<T>* child = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
                t->node_read(child_id[i], child);
                T* ret = child->search(t, _k);
                delete child;
                return ret;
            }
            else
                return NULL;
        }
    }
    if(!is_leaf){
        BTreeNode<T>* child = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
        t->node_read(child_id[i], child);
        T* ret = child->search(t, _k);
        delete child;
        return ret;
    }
    else
        return NULL;
}

template <typename T>
int BTreeNode<T>::traverse_insert(BTree<T>* t, u_int64_t _k, T _v, removeList** list){
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
        
        BTreeNode<T>* child = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
        t->node_read(child_id[i], child);
        int dup_child_id = child->traverse_insert(t, _k, _v, list);

        if(dup_child_id == 0){
            delete child;
            return 0;
        }

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
int BTreeNode<T>::direct_insert(BTree<T>* t, u_int64_t _k, T _v, removeList** list, u_int64_t node_id1, u_int64_t node_id2){
    /* Assume the list is not full */
    if(num_key >= m) return node_id;

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

    *list = new removeList(node_id, *list);
    node_id = t->get_free_block_id();

    t->node_write(node_id, this);

    return node_id;
}

template <typename T>
int BTreeNode<T>::split(BTree<T>*t, u_int64_t spt_node_id, u_int64_t parent_id, removeList** list){
    BTreeNode<T>* node = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
    t->node_read(spt_node_id, node);

    BTreeNode<T>* parent = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
    t->node_read(parent_id, parent);

    int new_node_id = t->get_free_block_id();
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

    node->node_id = t->get_free_block_id();            
    int dup_par_id = parent->direct_insert(t, node->key[min_num], node->value[min_num], list, node->node_id, new_node_id);
    node->num_key = min_num;    
    
    *list = new removeList(spt_node_id, *list);
    *list = new removeList(parent_id, *list);

    t->node_write(node->node_id, node);
    t->node_write(new_node_id, new_node);

    delete node;
    delete parent;
    delete new_node;

    return dup_par_id;
}

template <typename T>
int BTreeNode<T>::traverse_delete(BTree<T> *t, u_int64_t _k, removeList** list){
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
            BTreeNode<T>* node = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
            t->node_read(child_id[i+1], node);

            int succ_id = node->get_succ(t);
            BTreeNode<T>* succ = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));            
            t->node_read(succ_id, succ);

            if(succ->num_key > min_num){
                // Borrow from succ
                key[i] = succ->key[0];
                value[i] = succ->value[0];
                child_id[i+1] = node->traverse_delete(t, key[i], list);
                *list = new removeList(node_id, *list);
                node_id = t->get_free_block_id();
                t->node_write(node_id, this);
                i = i+1;
            }
            else{
                t->node_read(child_id[i], node);

                int pred_id = node->get_pred(t);
                BTreeNode<T>* pred = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));            
                t->node_read(pred_id, pred);

                // Borrow from pred
                key[i] = pred->key[pred->num_key - 1];
                value[i] = pred->value[pred->num_key - 1];
                child_id[i] = node->traverse_delete(t, key[i], list);
                *list = new removeList(node_id, *list);
                node_id = t->get_free_block_id();
                t->node_write(node_id, this);
                
                delete pred;
            }

            delete node;
            delete succ;

            return rebalance(t, i, list);
        }
        else{
            BTreeNode<T>* child = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
            t->node_read(child_id[i], child);

            int dup_child_id = child->traverse_delete(t, _k, list);

            if(dup_child_id == 0){
                delete child;
                return 0;
            }

            if(dup_child_id != child_id[i]){
                child_id[i] = dup_child_id;
                *list = new removeList(node_id, *list);
                node_id = t->get_free_block_id();
                t->node_write(node_id, this);
            }

            return rebalance(t, i, list);
        }
    }
}

template <typename T>
int BTreeNode<T>::direct_delete(BTree<T>* t, u_int64_t _k, removeList** list){
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

    *list = new removeList(node_id, *list);
    node_id = t->get_free_block_id();

    t->node_write(node_id, this);

    return node_id;
}

template <typename T>
int BTreeNode<T>::rebalance(BTree<T>* t, u_int64_t idx, removeList** list){

    BTreeNode<T>* node = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
    t->node_read(child_id[idx], node);

    if(node->num_key >= min_num){
        delete node;
        return node_id;
    }

    BTreeNode<T> *left = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
    if(idx - 1 >= 0)
        t->node_read(child_id[idx - 1], left);
    BTreeNode<T> *right = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
    if(idx + 1 <= num_key)
        t->node_read(child_id[idx + 1], right);
    int trans_node_id;

    if(idx - 1 >= 0 && left->num_key > min_num){
        //Borrowing from left
        t->node_read(child_id[idx-1], left);
        t->node_read(child_id[idx], right);
        trans_node_id = (left->is_leaf) ? 0 : left->child_id[left->num_key];
        child_id[idx] = right->direct_insert(t, key[idx-1], value[idx-1], list, trans_node_id);
        key[idx-1] = left->key[left->num_key - 1];
        value[idx-1] = left->value[left->num_key - 1];
        child_id[idx-1] = left->direct_delete(t, key[idx-1], list);
        *list = new removeList(node_id, *list);
        node_id = t->get_free_block_id();
        t->node_write(node_id, this);                
    }
    else if(idx + 1 <= num_key && right->num_key > min_num){
        //Borrowing from right
        t->node_read(child_id[idx], left);
        t->node_read(child_id[idx+1], right);
        trans_node_id = (right->is_leaf) ? 0 : right->child_id[0];
        child_id[idx] = left->direct_insert(t, key[idx], value[idx], list, 0, trans_node_id);
        key[idx] = right->key[0];
        value[idx] = right->value[0];
        child_id[idx+1] = right->direct_delete(t, key[idx], list);
        *list = new removeList(node_id, *list);
        node_id = t->get_free_block_id();
        t->node_write(node_id, this);         
    }   
    else{
        //Merge with right unless idx = num_key
        if(idx == num_key) idx -= 1;
        t->node_read(child_id[idx], left);
        t->node_read(child_id[idx+1], right);
        trans_node_id = (right->is_leaf) ? 0 : right->child_id[0];
        child_id[idx] = left->direct_insert(t, key[idx], value[idx], list, 0, trans_node_id);
        for(int i = 0; i < right->num_key; i++){
            t->node_read(child_id[idx], left);
            trans_node_id = (right->is_leaf) ? 0 : right->child_id[i+1];
            child_id[idx] = left->direct_insert(t, right->key[i], right->value[i], list, 0, trans_node_id);
        }
        t->node_read(child_id[idx], left);
        node_id = direct_delete(t, key[idx], list);
        child_id[idx] = left->node_id;
        *list = new removeList(node_id, *list);
        node_id = t->get_free_block_id();
        t->node_write(node_id, this);

        *list = new removeList(right->node_id, *list);
    }

    delete node;
    delete left,
    delete right;

    return node_id;
}

template <typename T>
int BTreeNode<T>::get_pred(BTree<T>* t){
    if(is_leaf)
        return node_id;
    else{
        BTreeNode<T>* node = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
        t->node_read(child_id[num_key], node);
        int ret = node->get_pred(t);
        delete node;
        return ret;
    }        
}

template <typename T>
int BTreeNode<T>::get_succ(BTree<T>* t){
    if(is_leaf)
        return node_id;
    else{
        BTreeNode<T>* node = (BTreeNode<T>*) calloc(1, sizeof(BTreeNode<T>));
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

removeList::~removeList(){
    if(next){
        delete next;
    }
}


#endif /* B_TREE_H */
