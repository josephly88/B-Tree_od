#include "b_tree.h"

BTree::BTree(string filename, int block_size){
    //file.open(filename, ios::out);
    file.open(filename, ios::out | ios::trunc);
    m = (block_size - sizeof(BTreeNode) - sizeof(streamoff)) / (sizeof(int) + sizeof(char) + sizeof(streamoff));
    exist = false;
}

fstream* BTree::get_file(){
    return &file;
}

int BTree::get_degree(){
    return m;
}

void BTree::traverse(){
    cout << endl << "Tree Traversal: " << endl;

    if(exist){
        //root->traverse(0);
    }
    else
        cout << " The tree is empty! " << endl;

    cout << endl;
}
