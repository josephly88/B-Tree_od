#include "b_tree.h"

void BTree::tree_read(int fd, BTree* tree){ 
    uint8_t* buf = NULL;
    lseek(fd, 0, SEEK_SET);
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    read(fd, buf, PAGE_SIZE);
    
    memcpy((void*)tree, buf, sizeof(BTree));
    free(buf);
}

void BTree::tree_write(int fd, BTree* tree){
    uint8_t* buf = NULL;
    lseek(fd, 0, SEEK_SET);
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    read(fd, buf, PAGE_SIZE);
    
    memcpy(buf, tree, sizeof(BTree));
    lseek(fd, 0, SEEK_SET);
    write(fd, buf, PAGE_SIZE);
    free(buf);
}

BTree::BTree(char* filename, int degree){
    if(degree > (int)((PAGE_SIZE - sizeof(BTreeNode) - sizeof(int)) / (sizeof(int) + sizeof(char))) ){
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

BTree::~BTree(){
    close(fd);
}

void BTree::reopen(int _fd){
    fd = _fd;
    tree_write(fd, this);
}

void BTree::stat(){
    cout << endl;
    cout << "Degree: " << m << endl;
    cout << "fd: " << fd << endl;
    cout << "Block size: " << block_size << endl;
    cout << "Block Capacity: " << block_cap << endl;
    cout << "Root Block ID: " << root_id << endl;
    print_used_block_id();
    cout << endl;
}

void BTree::node_read(int block_id, BTreeNode* node){
    if(block_id == 0){
        cout << " Error : Attempt to read block id = 0 " << endl;
        return;
    }

    uint8_t* buf = NULL;
    lseek(fd, block_id * PAGE_SIZE, SEEK_SET);
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);
    read(fd, buf, PAGE_SIZE);

    uint8_t* ptr = buf;
    memcpy((void*)node, ptr, sizeof(BTreeNode));
    ptr += sizeof(BTreeNode);

    node->key = new int[node->m];
    memcpy(node->key, ptr, node->m * sizeof(int));
    ptr += node->m * sizeof(int);

    node->value = new char[node->m];
    memcpy(node->value, ptr, node->m * sizeof(char));
    ptr += node->m * sizeof(char);

    node->child_id = new int[node->m + 1];
    memcpy(node->child_id, ptr, (node->m + 1) * sizeof(int));
    ptr += (node->m + 1) * sizeof(int);

    free(buf);
}

void BTree::node_write(int block_id, BTreeNode* node){
    if(block_id == 0){
        cout << " Error : Attempt to write block id = 0 " << endl;
        return;
    }

    uint8_t* buf = NULL;
    posix_memalign((void**)&buf, PAGE_SIZE, PAGE_SIZE);

    uint8_t* ptr = buf;
    memcpy(ptr, node, sizeof(BTreeNode));
    ptr += sizeof(BTreeNode);

    memcpy(ptr, node->key, node->m * sizeof(int));
    ptr += node->m * sizeof(int);

    memcpy(ptr, node->value, node->m * sizeof(char));
    ptr += node->m * sizeof(char);

    memcpy(ptr, node->child_id, (node->m + 1) * sizeof(int));
    ptr += (node->m + 1) * sizeof(int);

    lseek(fd, block_id * PAGE_SIZE, SEEK_SET);
    write(fd, buf, PAGE_SIZE);

    free(buf);
}

int BTree::get_free_block_id(){
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

void BTree::set_block_id(int block_id, bool bit){
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

void BTree::print_used_block_id(){
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
            tree_write(fd, this);
        }       

        node_read(root_id, root);
        if(root->num_key >= m){            
            int new_root_id = get_free_block_id();
            BTreeNode* new_root = new BTreeNode(m, false, new_root_id);
            node_write(new_root_id, new_root);

            root_id = root->split(this, root_id, new_root_id, &rmlist);

            tree_write(fd, this);

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
        tree_write(fd, this);

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

void BTreeNode::stat(){
    cout << endl;
    cout << "Degree: " << m << endl;
    cout << "Minimun Key: " << min_num << endl;
    cout << "# keys: " << num_key << endl;
    cout << "Is leaf? : " << is_leaf << endl;
    cout << "Node ID: " << node_id << endl;
    cout << endl;
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

int BTreeNode::traverse_insert(BTree* t, int _k, char _v, BTree::removeList** list){
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
            *list = new BTree::removeList(node_id, *list);
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

int BTreeNode::direct_insert(BTree* t, int _k, char _v, BTree::removeList** list, int node_id1, int node_id2){
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

    *list = new BTree::removeList(node_id, *list);
    node_id = t->get_free_block_id();

    t->node_write(node_id, this);

    return node_id;
}

int BTreeNode::split(BTree*t, int spt_node_id, int parent_id, BTree::removeList** list){
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
    
    *list = new BTree::removeList(spt_node_id, *list);
    *list = new BTree::removeList(parent_id, *list);

    t->node_write(node->node_id, node);
    t->node_write(new_node_id, new_node);

    delete node;
    delete parent;
    delete new_node;

    return dup_par_id;
}

int BTreeNode::traverse_delete(BTree *t, int _k, BTree::removeList** list){
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
                *list = new BTree::removeList(node_id, *list);
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
                *list = new BTree::removeList(node_id, *list);
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
                *list = new BTree::removeList(node_id, *list);
                node_id = t->get_free_block_id();
                t->node_write(node_id, this);
            }

            return rebalance(t, i, list);
        }
    }
}

int BTreeNode::direct_delete(BTree* t, int _k, BTree::removeList** list){
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

    *list = new BTree::removeList(node_id, *list);
    node_id = t->get_free_block_id();

    t->node_write(node_id, this);

    return node_id;
}

int BTreeNode::rebalance(BTree* t, int idx, BTree::removeList** list){

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
        *list = new BTree::removeList(node_id, *list);
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
        *list = new BTree::removeList(node_id, *list);
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
        *list = new BTree::removeList(node_id, *list);
        node_id = t->get_free_block_id();
        t->node_write(node_id, this);

        *list = new BTree::removeList(right->node_id, *list);
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

BTree::removeList::removeList(int _id, removeList* _next){
    id = _id;
    next = _next;
}

void BTree::removeList::removeNode(BTree* t){
    if(next){
        next->removeNode(t);
        delete next;
    }
    t->set_block_id(id, false);  
}

