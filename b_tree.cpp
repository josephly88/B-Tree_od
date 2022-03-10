#include "b_tree.h"

void tree_read(fstream* file, BTree* tree){ file->seekg(0, ios::beg); file->read((char*) tree, sizeof(BTree));
}

void tree_write(fstream* file, BTree* tree){
    file->seekg(0, ios::beg);
    file->write((char*) tree, sizeof(BTree));
}

BTree::BTree(string filename, int _block_size, fstream* _file){
    //file.open(filename, ios::in | ios::out | ios::binary);
    _file->open(filename, ios::in | ios::out | ios::trunc | ios::binary);
    file_ptr = _file;
    block_size = _block_size;
    m = (_block_size - sizeof(BTreeNode) - sizeof(int)) / (sizeof(int) + sizeof(char) + sizeof(int));
    node_cap = (_block_size - sizeof(BTree)) * 8;
    root_id = 0;

    tree_write(file_ptr, this);

    // Set the bit map to all 0
    void* empty_bytes = calloc(1, node_cap / 8);
    file_ptr->seekp(sizeof(BTree), ios::beg);
    file_ptr->write((char*)empty_bytes, node_cap / 8);
    free(empty_bytes);

    set_block_id(0, true);
}

void BTree::stat(){
    cout << endl;
    cout << "Block size : " << block_size << endl;
    cout << "Size of BTreeNode : " << sizeof(BTreeNode) << endl;        
    cout << "Size of K-V : " << sizeof(int) + sizeof(char) + sizeof(streamoff) << "(" << sizeof(streamoff) << ")" << endl;
    cout << "Degree : " << m << endl;
    cout << "Node capacity : " << node_cap << endl;
    cout << "Root id : " << root_id << endl;
    cout << endl;
}

void BTree::node_read(int id, BTreeNode* node){
    streamoff offset = id * block_size;
    
    file_ptr->seekg(offset, ios::beg);
    file_ptr->read((char*)node, sizeof(BTreeNode));

    node->key = new int[node->m];
    file_ptr->read((char*)node->key, node->m * sizeof(int));
    node->value = new char[node->m];
    file_ptr->read((char*)node->value, node->m * sizeof(char));
    node->child_id = new int[node->m + 1];
    file_ptr->read((char*)node->child_id, (node->m + 1) * sizeof(int)); 
}

void BTree::node_write(int id, BTreeNode* node){
    if(id == 0)
        cout << " Error : Attempt to write node id = 0 " << endl;

    streamoff offset = id * block_size;

    file_ptr->seekp(offset, ios::beg);
    file_ptr->write((char*)node, sizeof(BTreeNode));

    file_ptr->write((char*)node->key, node->m * sizeof(int));
    file_ptr->write((char*)node->value, node->m * sizeof(char));
    file_ptr->write((char*)node->child_id, (node->m + 1) * sizeof(int));
}

int BTree::get_free_block_id(){
    file_ptr->seekg(sizeof(BTree), ios::beg);
    int id = 0;
    while(id < node_cap){
        uint8_t byte;
        file_ptr->read((char*) &byte, sizeof(uint8_t));
        for(int i = 0; i < 8; i++){
            if( !(byte & 1) ){
                set_block_id(id, true);
                return id;
            }
            byte >>= 1;
            id++;
        }
    }
    return 0;
}

void BTree::set_block_id(int block_id, bool bit){
    if(block_id >= node_cap) return;

    int offset = sizeof(BTree) + (block_id / 8);

    file_ptr->seekg(offset, ios::beg);
    uint8_t byte;
    file_ptr->read((char*)(&byte), sizeof(uint8_t));

    if(bit)
        byte |= (1UL << (block_id % 8));
    else
        byte &= ~(1UL << (block_id % 8));

    file_ptr->seekp(offset, ios::beg);
    file_ptr->write((char*)(&byte), sizeof(uint8_t));

    file_ptr->seekg(offset, ios::beg);
    uint8_t byte1;
    file_ptr->read((char*)(&byte1), sizeof(uint8_t));
}

void BTree::print_used_node_id(){
    cout << endl;
    cout << "Used node : " << endl;

    file_ptr->seekg(sizeof(BTree), ios::beg);
    int id = 0;
    while(id < node_cap){
        uint8_t byte;
        file_ptr->read((char*) &byte, sizeof(uint8_t));
        for(int i = 0; i < 8; i++){
            if( (byte & 1) ){
                cout << " " << id;
            }
            byte >>= 1;
            id++;
        }
    }

    cout << endl << endl;
}

void BTree::traverse(){
    cout << endl << "Tree Traversal: " << endl;

    if(root_id){
        BTreeNode* root = (BTreeNode*) calloc(1, sizeof(BTreeNode));
        node_read(root_id, root);
        root->traverse(this, 0);
        delete root;
    }
    else
        cout << " The tree is empty! " << endl;

    cout << endl;
}

char* BTree::search(int _k){
    if(root_id){
        BTreeNode* root = (BTreeNode*) calloc(1, sizeof(BTreeNode));
        node_read(root_id, root);
        return root->search(this, _k);
        delete root;
    }
    else
        return NULL;
}

void BTree::insertion(int _k, char _v){

    if(root_id){
        removeList* rmlist = NULL;

        BTreeNode* root = (BTreeNode*) calloc(1, sizeof(BTreeNode));
        node_read(root_id, root);
        int dup_node_id = root->traverse_insert(this, _k, _v, &rmlist);

        if(dup_node_id == 0){
            delete root;
            return;
        }

        if(dup_node_id != root_id){
            root_id = dup_node_id;
            tree_write(file_ptr, this);
        }            

        node_read(root_id, root);
        if(root->num_key >= m){            
            int new_root_id = get_free_block_id();
            BTreeNode* new_root = new BTreeNode(m, false, new_root_id);
            node_write(new_root_id, new_root);

            root_id = root->split(this, root_id, new_root_id, &rmlist);

            tree_write(file_ptr, this);

            delete new_root;
        }
        if(rmlist){
            rmlist->removeNode(this);
            delete rmlist;
        }
        delete root;

    }
    else{
        root_id = get_free_block_id();
        tree_write(file_ptr, this);            

        BTreeNode* root = new BTreeNode(m, true, root_id);
        node_write(root_id, root);
        delete root;
        insertion(_k, _v);     
    }
}

void BTree::deletion(int _k){
    if(root_id){
        removeList* rmlist = NULL;

        BTreeNode* root = (BTreeNode*) calloc(1, sizeof(BTreeNode));
        node_read(root_id, root);
        int dup_node_id = root->traverse_delete(this, _k, &rmlist);

        if(dup_node_id == 0){
            delete root;
            return;
        }

        if(root_id != dup_node_id){
            root_id = dup_node_id;
            tree_write(file_ptr, this);
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
            tree_write(file_ptr, this);
        } 
        if(rmlist){
            rmlist->removeNode(this);
            delete rmlist;
        }
        delete root;          
    }
    else
        return;
}

BTreeNode::BTreeNode(int _m, bool _is_leaf, int _node_id){
    m = _m;
    min_num = (_m % 2 == 0) ? _m / 2 - 1 : _m / 2;
    num_key = 0;
    key = new int[m];
    value = new char[m];
    child_id = new int[m+1];
    is_leaf = _is_leaf;
    node_id = _node_id;
}

BTreeNode::~BTreeNode(){
    delete key;
    delete value;
    delete child_id;
}

void BTreeNode::traverse(BTree* t, int level){
    for(int j = 0; j < level; j++) cout << '\t';
        cout << '[' << node_id << ']' << endl;;

    int i = 0;
    for(i = 0; i < num_key; i++){
        if(!is_leaf){
            BTreeNode* node = (BTreeNode*) calloc(1, sizeof(BTreeNode));
            t->node_read(child_id[i], node);
            node->traverse(t, level + 1);
            delete node;
        }
        for(int j = 0; j < level; j++) cout << '\t';
        cout << key[i] << '(' << value[i] << ')' << endl;;
    }
    if(!is_leaf){
        BTreeNode* node = (BTreeNode*) calloc(1, sizeof(BTreeNode));
        t->node_read(child_id[i], node);
        node->traverse(t, level + 1);
        delete node;
    }
}

char* BTreeNode::search(BTree* t, int _k){
    int i;
    for(i = 0; i < num_key; i++){
        if(_k == key[i]) return &value[i];
        if(_k < key[i]){
            if(!is_leaf){
                BTreeNode* child = (BTreeNode*) calloc(1, sizeof(BTreeNode));
                t->node_read(child_id[i], child);
                char* ret = child->search(t, _k);
                delete child;
                return ret;
            }
            else
                return NULL;
        }
    }
    if(!is_leaf){
        BTreeNode* child = (BTreeNode*) calloc(1, sizeof(BTreeNode));
        t->node_read(child_id[i], child);
        char* ret = child->search(t, _k);
        delete child;
        return ret;
    }
    else
        return NULL;
}

int BTreeNode::traverse_insert(BTree* t, int _k, char _v, removeList** list){
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
        
        BTreeNode* child = (BTreeNode*) calloc(1, sizeof(BTreeNode));
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

int BTreeNode::direct_insert(BTree* t, int _k, char _v, removeList** list, int node_id1, int node_id2){
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

int BTreeNode::split(BTree*t, int spt_node_id, int parent_id, removeList** list){
    BTreeNode* node = (BTreeNode*) calloc(1, sizeof(BTreeNode));
    t->node_read(spt_node_id, node);

    BTreeNode* parent = (BTreeNode*) calloc(1, sizeof(BTreeNode));
    t->node_read(parent_id, parent);

    int new_node_id = t->get_free_block_id();
    BTreeNode* new_node = new BTreeNode(m, node->is_leaf, new_node_id);

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

int BTreeNode::traverse_delete(BTree *t, int _k, removeList** list){
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
            BTreeNode* node = (BTreeNode*) calloc(1, sizeof(BTreeNode));
            t->node_read(child_id[i+1], node);

            int succ_id = node->get_succ(t);
            BTreeNode* succ = (BTreeNode*) calloc(1, sizeof(BTreeNode));            
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
                BTreeNode* pred = (BTreeNode*) calloc(1, sizeof(BTreeNode));            
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
            BTreeNode* child = (BTreeNode*) calloc(1, sizeof(BTreeNode));
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

int BTreeNode::direct_delete(BTree* t, int _k, removeList** list){
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

int BTreeNode::rebalance(BTree* t, int idx, removeList** list){

    BTreeNode* node = (BTreeNode*) calloc(1, sizeof(BTreeNode));
    t->node_read(child_id[idx], node);

    if(node->num_key >= min_num){
        delete node;
        return node_id;
    }

    BTreeNode *left = (BTreeNode*) calloc(1, sizeof(BTreeNode));
    if(idx - 1 >= 0)
        t->node_read(child_id[idx - 1], left);
    BTreeNode *right = (BTreeNode*) calloc(1, sizeof(BTreeNode));
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

int BTreeNode::get_pred(BTree* t){
    if(is_leaf)
        return node_id;
    else{
        BTreeNode* node = (BTreeNode*) calloc(1, sizeof(BTreeNode));
        t->node_read(child_id[num_key], node);
        int ret = node->get_pred(t);
        delete node;
        return ret;
    }        
}

int BTreeNode::get_succ(BTree* t){
    if(is_leaf)
        return node_id;
    else{
        BTreeNode* node = (BTreeNode*) calloc(1, sizeof(BTreeNode));
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

void removeList::removeNode(BTree* t){
    if(next){
        next->removeNode(t);
        delete next;
    }
    t->set_block_id(id, false);  
}

CMB::CMB(off_t bar_addr){
	if ((fd = open("/dev/mem", O_RDWR | O_SYNC)) == -1) FATAL;
	printf("/dev/mem opened.\n");

	/* Map one page */
	map_base = mmap(0, MAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, bar_addr & ~MAP_MASK);
	if (map_base == (void*)-1) FATAL;
	printf("Memory mapped at address %p.\n\n", map_base);
}

CMB::~CMB(){
	if (munmap(map_base, MAP_SIZE) == -1) FATAL;
	close(fd);
}

void CMB::cmb_read(void* buf, off_t offset, int size){
	void* virt_addr = (char*)map_base + offset;	
	memcpy(buf, virt_addr, size);
}

void CMB::cmb_write(off_t offset, void* buf, int size){
	void* virt_addr = (char*)map_base + offset;	
	memcpy(virt_addr, buf, size);
}

