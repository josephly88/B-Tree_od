#ifndef B_TREE_H //include guard
#define B_TREE_H

#include <iostream>
#include <string>
#include <fstream>
#include "b_tree.h"
using namespace std;

class BTree;
class BTreeNode;
class removeList;

class BTree{
	public:
		int m;				// degree
		int root_id;		// Pointer to root node
		fstream* file_ptr;
		int block_size;
		int node_cap;

		BTree(string filename, int _block_size, fstream* file);

		void stat();

		void node_read(int id, BTreeNode* node);
		void node_write(int id, BTreeNode* node);

		int get_free_node_id();
		void set_node_id(int block_id, bool bit);
		void print_used_node_id();

		void traverse();
		char* search(int _k);

		void insertion(int _k, char _v);
		void deletion(int _k);		
};

class BTreeNode{

	public:
		int m;				// degree
		int min_num; 		// minimun number of node
		int num_key;		// the number of keys
		int *key;			// keys array
		char *value; 		// value array
		int *child_id;	// array of child pointers
		bool is_leaf;		// Is leaf or not
		int node_id;
	
		BTreeNode(int _m, bool _is_leaf, int _node_id);
		~BTreeNode();

		void traverse(BTree* t, int level);
		char* search(BTree* t, int _k);

		int traverse_insert(BTree* t, int _k, char _v, removeList** list);
		int direct_insert(BTree* t, int _k, char _v, removeList** list, int node_id1 = 0, int node_id2 = 0);
		int split(BTree* t, int node_id, int parent_id, removeList** list);

		int traverse_delete(BTree* t, int _k, removeList** _list);
		int direct_delete(BTree* t, int _k, removeList** _list);
		int rebalance(BTree* t, int idx, removeList** _list);
		int get_pred(BTree* t);
		int get_succ(BTree* t);	
		
	// BTree class can now access the private members of BTreeNode
	friend class BTree;
};

class removeList{
	int id;
	removeList* next;

	public:
		removeList(int _id, removeList* _next);

		void removeNode(BTree* t);
};

void tree_read(fstream* file, BTree* tree);
void tree_write(fstream* file, BTree* tree);

#endif /* B_TREE_H */