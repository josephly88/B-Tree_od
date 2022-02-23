#include "b_tree.h"

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

BTree::BTree(string filename, int block_size){
    //file.open(filename, ios::in | ios::out | ios::binary);
    file.open(filename, ios::in | ios::out | ios::trunc | ios::binary);
    block = block_size;
    m = (block_size - sizeof(BTreeNode) - sizeof(streamoff)) / (sizeof(int) + sizeof(char) + sizeof(streamoff));
    exist = false;
}

void BTree::traverse(){
    cout << endl << "Tree Traversal: " << endl;

    if(exist){
        BTreeNode* root = node_read(&file, 1 * block);
        root->traverse(0);
        delete root;
    }
    else
        cout << " The tree is empty! " << endl;

    cout << endl;
}

void BTree::insertion(int _k, char _v){

    if(exist){
        BTreeNode* root = node_read(&file, 1 * block);
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
        BTreeNode* root = new BTreeNode(m, true, &file, 1 * block);
        node_write(&file, 1 * block, root);
        exist = true;
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
