#include "b_tree.h"

#include <stdexcept>

BTree::BTree(const char* filename, bool create_new_tree) :  root_(0), tree_exist_(false), height_(0), fd_(-1) {
    int flags = O_RDWR | O_DIRECT;

    fd_ = open(filename, flags, 0644);
    if (fd_ == -1) {
        perror("open");
        exit(EXIT_FAILURE);
    }

    if (create_new_tree) {
        std::cout << "Creating new BTree." << std::endl;
        memset(bitmap_, 0, sizeof(bitmap_));

        write_metadata();
    } else {
        std::cout << "Loading existing BTree." << std::endl;
        read_metadata();

        if (root_ == 0) {
            fprintf(stderr, "Error: empty BTree file.\n");
            exit(EXIT_FAILURE);
        }
    }

    std::cout << "MAX_KEYS = " << MAX_KEYS << std::endl;
    std::cout << "----------------------------------------" << std::endl;
}

BTree::~BTree() {
    // Save the root id, tree_exist_ value, and height to the file.
    write_metadata();

    if (fd_ != -1) {
        close(fd_);
    }
}

void BTree::write_metadata() {
    char metadata[BLOCK_SIZE];
    memcpy(metadata, &root_, sizeof(root_));
    memcpy(metadata + sizeof(root_), &tree_exist_, sizeof(tree_exist_));
    memcpy(metadata + sizeof(root_) + sizeof(tree_exist_), &height_, sizeof(height_));
    memcpy(metadata + sizeof(root_) + sizeof(tree_exist_) + sizeof(height_), &fd_, sizeof(fd_));
    uint64_t bitmap_offset = sizeof(root_) + sizeof(tree_exist_) + sizeof(height_) + sizeof(fd_);
    uint64_t bitmap_size = BLOCK_SIZE - bitmap_offset;
    memcpy(metadata + bitmap_offset, bitmap_, bitmap_size);

    // Ensure that the file descriptor is valid before writing
    if (fd_ < 0) {
        throw std::runtime_error("Invalid file descriptor");
    }

    ssize_t bytes_written = pwrite(fd_, metadata, sizeof(metadata), 0);

    // Check that pwrite wrote the expected number of bytes
    if (bytes_written != sizeof(metadata)) {
        throw std::runtime_error("Error writing metadata");
    }
}

void BTree::read_metadata() {
    char metadata[BLOCK_SIZE];

    // Ensure that the file descriptor is valid before reading
    if (fd_ < 0) {
        throw std::runtime_error("Invalid file descriptor");
    }

    ssize_t bytes_read = pread(fd_, metadata, sizeof(metadata), 0);

    // Check that pread read the expected number of bytes
    if (bytes_read != sizeof(metadata)) {
        throw std::runtime_error("Error reading metadata");
    }

    memcpy(&root_, metadata, sizeof(root_));
    memcpy(&tree_exist_, metadata + sizeof(root_), sizeof(tree_exist_));
    memcpy(&height_, metadata + sizeof(root_) + sizeof(tree_exist_), sizeof(height_));

    uint64_t bitmap_offset = sizeof(root_) + sizeof(tree_exist_) + sizeof(height_) + sizeof(fd_);
    uint64_t bitmap_size = BLOCK_SIZE - bitmap_offset;
    memcpy(bitmap_, metadata + bitmap_offset, bitmap_size);
}

void BTree::write_node(int node_id, const BTreeNode& node) {
    if (node_id <= 0) {
        fprintf(stderr, "Error: write_node() received an invalid node_id.\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = node_id * BLOCK_SIZE;
    ssize_t num_bytes = pwrite(fd_, &node, sizeof(BTreeNode), offset);

    if (num_bytes != sizeof(BTreeNode)) {
        perror("pwrite");
        exit(EXIT_FAILURE);
    }
}

void BTree::read_node(int node_id, BTreeNode& node) const {
    if (node_id <= 0) {
        fprintf(stderr, "Error: read_node() received an invalid node_id.\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = node_id * BLOCK_SIZE;
    ssize_t bytes_read = pread(fd_, &node, sizeof(BTreeNode), offset);
    
    if (bytes_read == -1) {
        perror("read_node");
        exit(EXIT_FAILURE);
    }
    if (bytes_read != sizeof(BTreeNode)) {
        fprintf(stderr, "Error: read_node() failed to read a full block.\n");
        exit(EXIT_FAILURE);
    }
}

int BTree::allocate_node() {
    int bitmap_size = (BLOCK_SIZE - sizeof(root_) - sizeof(tree_exist_) - sizeof(height_) - sizeof(fd_));

    // Find the first free block in the bitmap
    for (int i = 0; i < bitmap_size; i++) {
        uint8_t byte = bitmap_[i];
        if (byte != 0xFF) {
            // Find the first 0 bit in the byte
            for (int j = 0; j < 8; j++) {
                if (!(byte & (1 << j))) {
                    // Mark the bit as used
                    bitmap_[i] |= (1 << j);
                    // Write the bitmap to disk
                    write_metadata();
                    // Compute the block id
                    int node_id = i * 8 + j + 1;
                    return node_id;
                }
            }
        }
    }
    throw std::runtime_error("Failed to allocate node");
}

void BTree::deallocate_node(int node_id) {
    int bitmap_index = (node_id - 1) / 8;
    int bit_index = (node_id - 1) % 8;

    // Find the byte containing the bit to be cleared
    uint8_t byte = bitmap_[bitmap_index];
    // Clear the bit corresponding to the node_id
    byte &= ~(1 << bit_index);
    // Write the updated byte back to the bitmap
    bitmap_[bitmap_index] = byte;
    // Write the bitmap to disk
    write_metadata();
}

void BTree::print_btree() {
    if(tree_exist_ == false) {
        std::cout << "Empty tree" << std::endl;
        return;
    }
    BTreeNode root;
    read_node(root_, root);
    print_node(root, 0);
}

void BTree::print_node(const BTreeNode& node, int level) {
    // Print node ID
    for(int i = 0; i < level; i++) {
        std::cout << '\t';
    }
    std::cout << '[' << node.id << ']' << std::endl;

    // Print keys and child nodes
    int key_index;
    for(key_index = 0; key_index < node.count; key_index++) {
        if(!node.leaf) {
            // Print child node
            BTreeNode child;
            read_node(node.children[key_index], child);
            print_node(child, level + 1);
        }

        // Print key
        for(int level_index = 0; level_index < level; level_index++) {
            std::cout << '\t';
        }
        std::cout << node.keys[key_index] << " (" << node.values[key_index] << ")" << std::endl;
    }

    // Print last child node, if applicable
    if(!node.leaf) {
        BTreeNode child;
        read_node(node.children[key_index], child);
        print_node(child, level + 1);
    }
}

int BTree::binary_search(const uint64_t* arr, int low, int high, uint64_t key) {
    int last_low = -1;
    while (low <= high) {
        int mid = (low + high) / 2;

        if (arr[mid] == key) {
            // Key already exists in the tree
            return mid;
        }
        else if (key < arr[mid]) {
            high = mid - 1;
        }
        else {
            last_low = mid;
            low = mid + 1;
        }
    }
    // Key does not exist in the tree, return the index of predecessor
    return last_low;
}

void BTree::insert(uint64_t key, const char* value) {
    // Check if fd_ is valid
    if (fd_ < 0) {
        throw std::runtime_error("Invalid file descriptor");
    }

    // If the tree is empty, create a new root node
    if (!tree_exist_) {
        BTreeNode root_node;
        root_node.id = allocate_node();
        root_node.leaf = true;
        root_node.count = 1;
        root_node.keys[0] = key;
        strncpy(root_node.values[0], value, VALUE_SIZE);

        write_node(root_node.id, root_node);

        root_ = root_node.id;
        height_ = 1;
        tree_exist_ = true;

        write_metadata();

        return;
    }

    // Read the root node
    BTreeNode root_node;
    read_node(root_, root_node);

    // Call the recursive insert function to insert the key-value pair
    insert_recursive(root_node, key, value);

    // If the root node is full, split it into two nodes and create a new root node
    if (root_node.count == MAX_KEYS) {
        BTreeNode new_root_node;
        new_root_node.id = allocate_node();
        new_root_node.leaf = false;
        new_root_node.count = 0;
        new_root_node.children[0] = root_;

        split_child(new_root_node, 0);
        
        root_ = new_root_node.id;
        height_++;

        write_metadata();
    }
}

void BTree::insert_recursive(BTreeNode& node, uint64_t key, const char* value) {
    int i = node.count - 1;

    if (node.leaf) {
        // Insert the key into the leaf node
        while (i >= 0 && key < node.keys[i]) {
            node.keys[i+1] = node.keys[i];
            strncpy(node.values[i+1], node.values[i], VALUE_SIZE);
            i--;
        }
        node.keys[i+1] = key;
        strncpy(node.values[i+1], value, VALUE_SIZE);
        node.count++;
        write_node(node.id, node);
    }
    else {
        // Find the child node to insert the key
        i = binary_search(node.keys, 0, node.count - 1, key) + 1;

        BTreeNode child_node;
        read_node(node.children[i], child_node);

        insert_recursive(child_node, key, value);

        if (child_node.count == MAX_KEYS) {
            // Split the child node if it is full
            split_child(node, i);
        }
    }
}

void BTree::split_child(BTreeNode& parent, int i) {
    BTreeNode child_node, right_child;
    read_node(parent.children[i], child_node);

    // Create a new child node and move the right half of the keys to it
    right_child.id = allocate_node();
    right_child.leaf = child_node.leaf;
    right_child.count = MIN_KEYS;
    for (int j = 1; j <= MIN_KEYS; j++){
        right_child.keys[MIN_KEYS - j] = child_node.keys[MAX_KEYS - j];
        strncpy(right_child.values[MIN_KEYS - j], child_node.values[MAX_KEYS - j], VALUE_SIZE);
    }

    // Move the right half of the children to the new node, if it's not a leaf
    if (!child_node.leaf) {
        for (int j = 1; j <= MIN_CHILDREN; j++) {
            right_child.children[MIN_CHILDREN - j] = child_node.children[MAX_CHILDREN - j];
        }
    }

    write_node(right_child.id, right_child);

    // Update the child node's count and write it back to the disk
    child_node.count = MAX_KEYS - MIN_KEYS - 1;
    write_node(child_node.id, child_node);

    // Insert the middle key to the parent node
    for (int j = parent.count-1; j >= i; j--) {
        parent.keys[j+1] = parent.keys[j];
        strncpy(parent.values[j+1], parent.values[j], VALUE_SIZE);
    }
    parent.keys[i] = child_node.keys[MAX_KEYS - MIN_KEYS - 1];
    strncpy(parent.values[i], child_node.values[MAX_KEYS - MIN_KEYS - 1], VALUE_SIZE);
    parent.count++;

    // Insert the new child node to the parent node
    for (int j = parent.count; j >= i+1; j--) {
        parent.children[j+1] = parent.children[j];
    }
    parent.children[i+1] = right_child.id;

    write_node(parent.id, parent);
}

