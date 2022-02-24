#include "b_tree.h"

BTree* tree_read(fstream* file){
    BTree* tree = (BTree*) malloc(sizeof(BTree));

    file->seekg(0, ios::beg);
    file->read((char*) tree, sizeof(BTree));

    return tree;
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

    set_node_id(0, true);
}


BTreeNode* BTree::node_read(int id){
    streamoff offset = id * block_size;
    BTreeNode* node = (BTreeNode*) malloc(sizeof(BTreeNode));
    
    file_ptr->seekg(offset, ios::beg);
    file_ptr->read((char*)node, sizeof(BTreeNode));

    node->key = new int[node->m];
    file_ptr->read((char*)node->key, node->m * sizeof(int));
    node->value = new char[node->m];
    file_ptr->read((char*)node->value, node->m * sizeof(char));
    node->child_id = new int[node->m + 1];
    file_ptr->read((char*)node->child_id, (node->m + 1) * sizeof(int));    

    return node;
}

void BTree::node_write(int id, BTreeNode* node){
    streamoff offset = id * block_size;

    file_ptr->seekp(offset, ios::beg);
    file_ptr->write((char*)node, sizeof(BTreeNode));

    file_ptr->write((char*)node->key, node->m * sizeof(int));
    file_ptr->write((char*)node->value, node->m * sizeof(char));
    file_ptr->write((char*)node->child_id, (node->m + 1) * sizeof(int));
}

int BTree::get_free_node_id(){
    file_ptr->seekg(sizeof(BTree), ios::beg);
    int id = 0;
    while(id < node_cap){
        uint8_t byte;
        file_ptr->read((char*) &byte, sizeof(uint8_t));
        for(int i = 0; i < 8; i++){
            if( !(byte & 1) ){
                set_node_id(id, true);
                return id;
            }
            byte >>= 1;
            id++;
        }
    }
    return 0;
}

void BTree::set_node_id(int block_id, bool bit){
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
}

void BTree::traverse(){
    cout << endl << "Tree Traversal: " << endl;

    if(root_id){
        BTreeNode* root = node_read(root_id);
        root->traverse(this, 0);
        delete root;
    }
    else
        cout << " The tree is empty! " << endl;

    cout << endl;
}

void BTree::insertion(int _k, char _v){

    if(root_id){
        BTreeNode* root = node_read(root_id);
        root->traverse_insert(this, _k, _v);
        delete root;

        /*
        if(root->num_key >= m){
            BTreeNode* new_root = new BTreeNode(m, false);
            root->split(root, new_root);
            root = new_root;
        }
        */
    }
    else{
        root_id = get_free_node_id();
        tree_write(file_ptr, this);            

        BTreeNode* root = new BTreeNode(m, true, root_id);
        node_write(root_id, root);
        delete root;
        insertion(_k, _v);     
    }
}

BTreeNode::BTreeNode(int _m, bool _is_leaf, int _node_id){
    m = _m;
    num_key = 0;
    key = new int[m];
    value = new char[m];
    child_id = new int[m+1];
    is_leaf = _is_leaf;
    node_id = _node_id;
}

void BTreeNode::traverse(BTree* t, int level){
    int i = 0;
    for(i = 0; i < num_key; i++){
        if(!is_leaf){
            BTreeNode* node = t->node_read(child_id[i]);
            node->traverse(t, level + 1);
            delete node;
        }
        for(int j = 0; j < level; j++) cout << '\t';
        cout << key[i] << '(' << value[i] << ')' << endl;;
    }
    if(!is_leaf){
        BTreeNode* node = t->node_read(child_id[i]);
        node->traverse(t, level + 1);
        delete node;
    }
}

void BTreeNode::traverse_insert(BTree* t, int _k, char _v){
    if(is_leaf)
        direct_insert(t, _k, _v);
    else{
        int i;
        for(i = 0; i < num_key; i++){
            if(_k == key[i])
                return;            
            if(_k < key[i])
                break;
        }
        
        BTreeNode* node = t->node_read(child_id[i]);
        node->traverse_insert(t, _k, _v);
        delete node;
        /*
        if(child[i]->num_key >= m)
            split(child[i], this);
        */
    }
}

void BTreeNode::direct_insert(BTree* t, int _k, char _v, int node_id1, int node_id2){
    /* Assume the list is not full */
    if(num_key >= m) return;

    int i;
    for(i = num_key; i > 0; i--){
        if(_k == key[i-1]) 
            return;     // No two keys are the same
        else if(_k < key[i-1]){
            key[i] = key[i-1];
            value[i] = value[i-1];
            if(!is_leaf) child_id[i+1] = child_id[i];
        }
        else
            break;
    }
    if(!is_leaf) child_id[i+1] = child_id[i];

    key[i] = _k;
    value[i] = _v;
    if(node_id1 != 0) child_id[i] = node_id1;
    if(node_id2 != 0) child_id[i+1] = node_id2;
    num_key++;

    t->node_write(node_id, this);
}
