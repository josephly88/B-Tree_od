#pragma once

#include <iostream>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

const int VALUE_SIZE = 70;

const int BLOCK_SIZE = 512;
const int MAX_KEYS = (BLOCK_SIZE - sizeof(unsigned int) - sizeof(bool) - sizeof(int) - sizeof(uint32_t)) / (sizeof(uint64_t) + VALUE_SIZE + sizeof(uint32_t));
const int MIN_KEYS = (MAX_KEYS + 1) / 2 - 1;
const int MAX_CHILDREN = MAX_KEYS + 1;
const int MIN_CHILDREN = MIN_KEYS + 1;

struct BTreeNode {
    unsigned int id;
    bool leaf;
    int count;
    uint64_t keys[MAX_KEYS];
    char values[MAX_KEYS][VALUE_SIZE];
    uint32_t children[MAX_KEYS + 1];
};

class BTree {
public:
    BTree(const char* filename, bool create_new_tree);
    ~BTree();

    void insert(uint64_t key, const char* value);
    const char* search(uint64_t key);
    void update(uint64_t key, const char* value);
    void remove(uint64_t key);
    // Verification
    void print_btree();

private:
    // tree metadata
    uint64_t root_;
    bool tree_exist_;
    int height_;
    // file descriptor
    int fd_;
    // node usage bitmap
    uint8_t bitmap_[BLOCK_SIZE-sizeof(root_)-sizeof(tree_exist_)-sizeof(height_)-sizeof(fd_)];

    // metadata I/O
    void write_metadata();
    void read_metadata();
    // tree node I/O
    void write_node(int node_id, const BTreeNode& node);
    void read_node(int node_id, BTreeNode& node) const;
    // Node usage bitmap I/O
    int allocate_node();

    // Verification
    void print_node(const BTreeNode& node, int level);

    // for insertion
    int insert_non_full(BTreeNode& node, uint64_t key, const char* value);
    void split_child(BTreeNode& node, int child_index);

    int find_child(const BTreeNode& node, uint64_t key) const;
    void remove_from_leaf(BTreeNode& node, int index);
    void remove_from_non_leaf(BTreeNode& node, int index);
    uint64_t get_predecessor(BTreeNode& node, int index) const;
    uint64_t get_successor(BTreeNode& node, int index) const;
    void fill_child(BTreeNode& node, int index);
    void borrow_from_prev(BTreeNode& node, int index);
    void borrow_from_next(BTreeNode& node, int index);
    void merge_children(BTreeNode& node, int index);
    void remove_helper(BTreeNode& node, uint64_t key);

    void copy_node(int node_id, BTreeNode& node) const;
};
