#ifndef B_TREE_H //include guard
#define B_TREE_H

#include <iostream>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include "b_tree.h"
using namespace std;

#define PAGE_SIZE 4096UL

class BTree;
class BTreeNode;

class BTree{
	int m;				// degree
	int root_id;		// Pointer to root node
	int fd;
	int block_size;
	int block_cap;
		
	public:
		class removeList{
			int id;
			removeList* next;

			public:
				removeList(int _id, removeList* _next);
				void removeNode(BTree* t);
		};

		BTree(char* filename, int degree);
		~BTree();

		void reopen(int _fd);

		void stat();

		void tree_read(int fd, BTree* tree);
		void tree_write(int fd, BTree* tree);

		void node_read(int block_id, BTreeNode* node);
		void node_write(int block_id, BTreeNode* node);

		int get_free_block_id();
		void set_block_id(int block_id, bool bit);
		void print_used_block_id();

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

		void stat();

		void traverse(BTree* t, int level);
		char* search(BTree* t, int _k);

		int traverse_insert(BTree* t, int _k, char _v, BTree::removeList** list);
		int direct_insert(BTree* t, int _k, char _v, BTree::removeList** list, int node_id1 = 0, int node_id2 = 0);
		int split(BTree* t, int node_id, int parent_id, BTree::removeList** list);

		int traverse_delete(BTree* t, int _k, BTree::removeList** _list);
		int direct_delete(BTree* t, int _k, BTree::removeList** _list);
		int rebalance(BTree* t, int idx, BTree::removeList** _list);
		int get_pred(BTree* t);
		int get_succ(BTree* t);	
};

#endif /* B_TREE_H */
