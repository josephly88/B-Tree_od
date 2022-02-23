#ifndef B_TREE_H //include guard
#define B_TREE_H

#include <iostream>
#include <string>
#include <fstream>
#include "b_tree.h"
using namespace std;

class BTreeNode{
	int m;				// degree
	int min_num; 		// minimun number of node
	int num_key;		// the number of keys
	int *key;			// keys array
	char *value; 		// value array
	streamoff *child;	// array of child pointers
	bool is_leaf;		// Is leaf or not

	public:
		BTreeNode(int _m, bool _is_leaf);

		void traverse(int level);
		
	// BTree class can now access the private members of BTreeNode
	friend class BTree;
};

class BTree{
	int m;				// degree
	streamoff root;		// Pointer to root node
	fstream file;
	bool exist;

	public:
		BTree(string filename, int block_size);
		fstream* get_file();
		int get_degree();
		
		void traverse();
		
};

#endif /* B_TREE_H */