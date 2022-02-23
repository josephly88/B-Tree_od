#ifndef B_TREE_H //include guard
#define B_TREE_H

#include <iostream>
#include <string>
#include <fstream>
#include "b_tree.h"
using namespace std;

class BTreeNode{

	public:
		int m;				// degree
		int min_num; 		// minimun number of node
		int num_key;		// the number of keys
		int *key;			// keys array
		char *value; 		// value array
		streamoff *child;	// array of child pointers
		bool is_leaf;		// Is leaf or not
		streamoff offset;

		fstream* file_ptr;
	
		BTreeNode(int _m, bool _is_leaf, fstream* _file_ptr, streamoff _offset);

		void traverse(int level);

		void traverse_insert(int _k, char _v);
		void direct_insert(int _k, char _v, streamoff node_off1 = 0, streamoff node_off2 = 0);
		
	// BTree class can now access the private members of BTreeNode
	friend class BTree;
};

class BTree{
	public:
		int m;				// degree
		streamoff root_off;		// Pointer to root node
		fstream* file_ptr;
		int block_size;
		bool root_exist;
		int node_cap;

		BTree(string filename, int _block_size, fstream* file);

		int get_free_node_id();
		void set_node_id(int block_id, bool bit);

		void traverse();

		void insertion(int _k, char _v);
		
};

BTree* tree_read(fstream* file);
void tree_write(fstream* file, BTree* tree);
BTreeNode* node_read(fstream* file, streamoff offset);
void node_write(fstream* file, streamoff offset, BTreeNode* node);

#endif /* B_TREE_H */