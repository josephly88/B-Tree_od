#include <iostream>
#include "../lib/cmb/b_tree.h"
using namespace std;

int main(){

    cout << "Size of BTree is : " << sizeof(BTree<char>) << endl;
    cout << "Size of BTreeNode is : " << sizeof(BTreeNode<char>) << endl;
    cout << "Size of int is : " << sizeof(int) << endl;

    return 0;
}