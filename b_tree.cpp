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

BTreeNode* node_read(fstream* file, streamoff offset){
    BTreeNode* node = (BTreeNode*) malloc(sizeof(BTreeNode));
    
    file->seekg(offset, ios::beg);
    file->read((char*)node, sizeof(BTreeNode));

    node->key = new int[node->m];
    file->read((char*)node->key, node->m * sizeof(int));
    node->value = new char[node->m];
    file->read((char*)node->value, node->m * sizeof(char));
    node->child = new streamoff[node->m + 1];
    file->read((char*)node->child, (node->m + 1) * sizeof(streamoff));    

    return node;
}

void node_write(fstream* file, streamoff offset, BTreeNode* node){

    file->seekp(offset, ios::beg);
    file->write((char*)node, sizeof(BTreeNode));

    file->write((char*)node->key, node->m * sizeof(int));
    file->write((char*)node->value, node->m * sizeof(char));
    file->write((char*)node->child, (node->m + 1) * sizeof(streamoff));
}

BTree::BTree(string filename, int _block_size, fstream* _file){
    //file.open(filename, ios::in | ios::out | ios::binary);
    _file->open(filename, ios::in | ios::out | ios::trunc | ios::binary);
    file_ptr = _file;
    block_size = _block_size;
    m = (_block_size - sizeof(BTreeNode) - sizeof(streamoff)) / (sizeof(int) + sizeof(char) + sizeof(streamoff));
    node_cap = (_block_size - sizeof(BTree)) * 8;
    root_off = 0;

    tree_write(file_ptr, this);

    // Set the bit map to all 0
    void* empty_bytes = calloc(1, node_cap / 8);
    file_ptr->seekp(sizeof(BTree), ios::beg);
    file_ptr->write((char*)empty_bytes, node_cap / 8);
    free(empty_bytes);

    set_node_id(0, true);
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

    if(root_off){
        BTreeNode* root = node_read(file_ptr, root_off);
        root->traverse(0);
        delete root;
    }
    else
        cout << " The tree is empty! " << endl;

    cout << endl;
}

void BTree::insertion(int _k, char _v){

    if(root_off){
        BTreeNode* root = node_read(file_ptr, root_off);
        root->traverse_insert(_k, _v);
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
        int root_id = get_free_node_id();
        root_off = root_id * block_size;
        tree_write(file_ptr, this);

        BTreeNode* root = new BTreeNode(m, true, file_ptr, root_off);
        node_write(file_ptr, root_off, root);
        delete root;
        insertion(_k, _v);     
    }
}

BTreeNode::BTreeNode(int _m, bool _is_leaf, fstream* _file_ptr, streamoff _offset){
    m = _m;
    num_key = 0;
    key = new int[m];
    value = new char[m];
    child = new streamoff[m+1];
    is_leaf = _is_leaf;
    file_ptr = _file_ptr;
    offset = _offset;
}

void BTreeNode::traverse(int level){
    int i = 0;
    for(i = 0; i < num_key; i++){
        if(!is_leaf){
            BTreeNode* node = node_read(file_ptr, child[i]);
            node->traverse(level + 1);
            delete node;
        }
        for(int j = 0; j < level; j++) cout << '\t';
        cout << key[i] << '(' << value[i] << ')' << endl;;
    }
    if(!is_leaf){
        BTreeNode* node = node_read(file_ptr, child[i]);
        node->traverse(level + 1);
        delete node;
    }
}

void BTreeNode::traverse_insert(int _k, char _v){
    if(is_leaf)
        direct_insert(_k, _v);
    else{
        int i;
        for(i = 0; i < num_key; i++){
            if(_k == key[i])
                return;            
            if(_k < key[i])
                break;
        }
        
        BTreeNode* node = node_read(file_ptr, child[i]);
        node->traverse_insert(_k, _v);
        delete node;
        /*
        if(child[i]->num_key >= m)
            split(child[i], this);
        */
    }
}

void BTreeNode::direct_insert(int _k, char _v, streamoff node_off1, streamoff node_off2){
    /* Assume the list is not full */
    if(num_key >= m) return;

    int i;
    for(i = num_key; i > 0; i--){
        if(_k == key[i-1]) 
            return;     // No two keys are the same
        else if(_k < key[i-1]){
            key[i] = key[i-1];
            value[i] = value[i-1];
            if(!is_leaf) child[i+1] = child[i];
        }
        else
            break;
    }
    if(!is_leaf) child[i+1] = child[i];

    key[i] = _k;
    value[i] = _v;
    if(node_off1 != 0) child[i] = node_off1;
    if(node_off2 != 0) child[i+1] = node_off2;
    num_key++;

    node_write(file_ptr, offset, this);
}
