#include <iostream>
#include <sys/mman.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
using namespace std;

#define FATAL do { fprintf(stderr, "Error at line %d, file %s (%d) [%s]\n", \
  __LINE__, __FILE__, errno, strerror(errno)); exit(1); } while(0)

#define MAP_SIZE 16384UL

typedef enum {
    BLACK,
    RED
} COLOR;

class RBTree;
class RBTreeNode;

class RBTree{
    u_int64_t root_idx;
    u_int64_t free_idx;
    void* map_base;

    public:
        RBTree(void* _map_base);

        void insert(u_int64_t key);
        void insertFix(u_int64_t idx);
        void remove(u_int64_t key);
        void removeFix(u_int64_t idx, u_int64_t cur_parent_idx);
        u_int64_t search(u_int64_t key);

        void display();

        u_int64_t get_free_idx();

        void readNode(u_int64_t idx, RBTreeNode* buf);
        void writeNode(u_int64_t idx, RBTreeNode* buf);

        void leafRotation(u_int64_t x_idx);
        void rightRotation(u_int64_t x_idx);

        u_int64_t succesor(u_int64_t idx);
};

class RBTreeNode{
    public:
        u_int64_t key;
        u_int64_t left_child_idx;
        u_int64_t right_child_idx;  
        u_int64_t color;    // Red - 1, Black - 0
        u_int64_t parent_idx;   

        RBTreeNode(u_int64_t key, u_int64_t color, u_int64_t parent_idx);   

        void display(void* map_base, int level);
};

RBTree::RBTree(void* _map_base){
    root_idx = 0;
    free_idx = 1;
    map_base = _map_base;

    RBTreeNode* zero = new RBTreeNode(0,0,0);
    writeNode(0, zero);
    delete zero;
}

RBTreeNode::RBTreeNode(u_int64_t _key, u_int64_t _color, u_int64_t _parent_idx){
    key = _key;
    color = _color;
    parent_idx = _parent_idx;
    left_child_idx = 0;
    right_child_idx = 0;
}

void RBTree::insert(u_int64_t key){
    if(root_idx == 0){
        RBTreeNode* new_node = new RBTreeNode(key, BLACK, 0);
        writeNode(free_idx, new_node);

        root_idx = free_idx;        
        free_idx++;

        delete new_node;
    }
    else{
        u_int64_t cur_idx = root_idx;
        RBTreeNode* cur_node = new RBTreeNode(0, 0, 0);
        u_int64_t *child_ptr;

        while(cur_idx){
            readNode(cur_idx, cur_node);

            if(key < cur_node->key){
                child_ptr = &cur_node->left_child_idx;
            }                
            else if(key > cur_node->key){
                child_ptr = &cur_node->right_child_idx;
            }

            if(*child_ptr != 0){
                cur_idx = *child_ptr;
            }
            else{
                RBTreeNode* new_node = new RBTreeNode(key, RED, cur_idx);
                writeNode(free_idx, new_node);      

                *child_ptr = free_idx;
                char* addr = (char*)map_base + cur_idx * sizeof(RBTreeNode) + ((char*)child_ptr - (char*)cur_node);
                memcpy(addr, child_ptr, sizeof(u_int64_t));
                
                free_idx++;

                delete new_node;
                break;
            }
        }
        insertFix(*child_ptr);
        
        delete cur_node;
    }
}

void RBTree::insertFix(u_int64_t idx){
    
    u_int64_t cur_idx = idx;
    while(cur_idx){
        RBTreeNode* cur = new RBTreeNode(0,0,0);
        readNode(cur_idx, cur);        

        u_int64_t parent_color;
        char* addr = (char*)map_base + cur->parent_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
        memcpy(&parent_color, addr, sizeof(u_int64_t));

        if(parent_color == RED){
            RBTreeNode* parent = new RBTreeNode(0,0,0);
            readNode(cur->parent_idx, parent);

            u_int64_t grand_left_child;
            addr = (char*)map_base + parent->parent_idx * sizeof(RBTreeNode) + ((char*)&cur->left_child_idx - (char*)cur);
            memcpy(&grand_left_child, addr, sizeof(u_int64_t));
            u_int64_t grand_right_child;
            addr = (char*)map_base + parent->parent_idx * sizeof(RBTreeNode) + ((char*)&cur->right_child_idx - (char*)cur);
            memcpy(&grand_right_child, addr, sizeof(u_int64_t));

            if(grand_left_child == cur->parent_idx){
                // Parent is on the left
                RBTreeNode* uncle = new RBTreeNode(0,0,0);
                readNode(grand_right_child, uncle);

                if(uncle->color == RED){
                    // Case 1
                    parent->color = BLACK;
                    addr = (char*)map_base + cur->parent_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &parent->color, sizeof(u_int64_t));
                    uncle->color = BLACK;
                    addr = (char*)map_base + grand_right_child * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &uncle->color, sizeof(u_int64_t));
                    // Grandparent->color = RED;
                    if(parent->parent_idx != root_idx){
                        addr = (char*)map_base + parent->parent_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                        memcpy(addr, &cur->color, sizeof(u_int64_t));
                    }                   

                    cur_idx = parent->parent_idx;
                }
                else{
                    // Case 2 & 3
                    if(cur_idx == parent->right_child_idx){
                        leafRotation(cur->parent_idx);
                        cur_idx = cur->parent_idx;
                        readNode(cur_idx, cur);
                        readNode(cur->parent_idx, parent);
                    }
                    parent->color = BLACK;
                    addr = (char*)map_base + cur->parent_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &parent->color, sizeof(u_int64_t));
                    // Grandparent->color = RED;
                    addr = (char*)map_base + parent->parent_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &cur->color, sizeof(u_int64_t));
                    rightRotation(parent->parent_idx);

                    cur_idx = 0;
                }
                
                delete uncle;
            }
            else{
                // Parent is on the right
                RBTreeNode* uncle = new RBTreeNode(0,0,0);
                readNode(grand_left_child, uncle);

                if(uncle->color == RED){
                    // Case 1
                    parent->color = BLACK;
                    addr = (char*)map_base + cur->parent_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &parent->color, sizeof(u_int64_t));
                    uncle->color = BLACK;
                    addr = (char*)map_base + grand_left_child * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &uncle->color, sizeof(u_int64_t));
                    // Grandparent->color = RED;
                    if(parent->parent_idx != root_idx){
                        addr = (char*)map_base + parent->parent_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                        memcpy(addr, &cur->color, sizeof(u_int64_t));
                    }
                    

                    cur_idx = parent->parent_idx;
                }
                else{
                    // Case 2 & 3
                    if(cur_idx == parent->left_child_idx){
                        rightRotation(cur->parent_idx);
                        cur_idx = cur->parent_idx;
                        readNode(cur_idx, cur);
                        readNode(cur->parent_idx, parent);
                    }
                    parent->color = BLACK;
                    addr = (char*)map_base + cur->parent_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &parent->color, sizeof(u_int64_t));
                    // Grandparent->color = RED;
                    addr = (char*)map_base + parent->parent_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &cur->color, sizeof(u_int64_t));
                    leafRotation(parent->parent_idx);

                    cur_idx = 0;
                }

                delete uncle;
            }

            delete parent;
        }
        else{
            delete cur;
            return;
        }

        delete cur;
    }
}

void RBTree::remove(u_int64_t key){
    u_int64_t delete_idx = search(key);
    if(delete_idx == 0){
        cout << "Data not found" << endl;
        return;
    }
    RBTreeNode* delete_node = new RBTreeNode(0,0,0);

    char* addr;
    u_int64_t target_idx = 0;
    RBTreeNode* target_node = new RBTreeNode(0,0,0);
    u_int64_t replace_idx = 0;

    readNode(delete_idx, delete_node);

    if(delete_node->left_child_idx == 0 || delete_node->right_child_idx == 0){
        // Has <= 1 child
        target_idx = delete_idx;
        memcpy(target_node, delete_node, sizeof(RBTreeNode));
    }
    else{
        // Has two children
        target_idx = succesor(delete_node->right_child_idx);
        readNode(target_idx, target_node);
    }

    // Set the replace_idx as the target's child
    if(target_node->left_child_idx != 0)
        replace_idx = target_node->left_child_idx;
    else
        replace_idx = target_node->right_child_idx;

    if(replace_idx != 0){
        // If the replace_idx is not NIL, set it's parent to target->parent
        addr = (char*)map_base + replace_idx * sizeof(RBTreeNode) + ((char*)&delete_node->parent_idx - (char*)delete_node);
        memcpy(addr, &target_node->parent_idx, sizeof(u_int64_t));
    }

    if(target_node->parent_idx == 0){
        // If the target is the root, set the root as the replacement
        root_idx = replace_idx;
    }
    else{
        u_int64_t parent_left;
        addr = (char*)map_base + target_node->parent_idx * sizeof(RBTreeNode) + ((char*)&delete_node->left_child_idx - (char*)delete_node);
        memcpy(&parent_left, addr, sizeof(u_int64_t));

        if(parent_left == target_idx){
            // If target is the left child, set the replace to the left child of target->parent
            memcpy(addr, &replace_idx, sizeof(u_int64_t));
        }
        else{
            addr = (char*)map_base + target_node->parent_idx * sizeof(RBTreeNode) + ((char*)&delete_node->right_child_idx - (char*)delete_node);
            memcpy(addr, &replace_idx, sizeof(u_int64_t));
        }
    }

    if(target_idx != delete_idx){
        addr = (char*)map_base + delete_idx * sizeof(RBTreeNode) + ((char*)&delete_node->key - (char*)delete_node);
        memcpy(addr, &target_node->key, sizeof(u_int64_t));
    }

    if(target_node->color == BLACK){
        removeFix(replace_idx, target_node->parent_idx);
    }
    
    delete delete_node;
    delete target_node;
}

void RBTree::removeFix(u_int64_t idx, u_int64_t cur_parent_idx){
    u_int64_t cur_idx = idx;
    u_int64_t parent_idx = cur_parent_idx;
    u_int64_t red = RED;
    u_int64_t black = BLACK;
    while(true){
        char* addr; 
        RBTreeNode* cur = new RBTreeNode(0,0,0);
        readNode(cur_idx, cur);

        if(cur_idx != root_idx && cur->color == BLACK){
            RBTreeNode* parent = new RBTreeNode(0,0,0);
            readNode(parent_idx, parent);

            if(cur_idx == parent->left_child_idx){
                // Current is the left child
                RBTreeNode* sibling = new RBTreeNode(0,0,0);
                u_int64_t sibling_idx = parent->right_child_idx;
                readNode(sibling_idx, sibling);

                if(sibling->color == RED){
                    //Case 1
                    sibling->color = BLACK;
                    addr = (char*)map_base + sibling_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &black, sizeof(u_int64_t));
                    parent->color = RED;
                    addr = (char*)map_base + parent_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &red, sizeof(u_int64_t));
                    leafRotation(parent_idx);
                    readNode(parent_idx, parent);
                    sibling_idx = parent->right_child_idx;
                    readNode(sibling_idx, sibling);
                }

                RBTreeNode* sibling_left_child = new RBTreeNode(0,0,0);
                readNode(sibling->left_child_idx, sibling_left_child);
                RBTreeNode* sibling_right_child = new RBTreeNode(0,0,0);
                readNode(sibling->right_child_idx, sibling_right_child);

                if(sibling_left_child->color == BLACK && sibling_right_child->color == BLACK){
                    // Case 2
                    sibling->color = RED;
                    addr = (char*)map_base + sibling_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &red, sizeof(u_int64_t));
                    cur_idx = parent_idx;
                    addr = (char*)map_base + cur_idx * sizeof(RBTreeNode) + ((char*)&cur->parent_idx - (char*)cur);
                    memcpy(&parent_idx, addr, sizeof(u_int64_t));
                }
                else{
                    if(sibling_right_child->color == BLACK){
                        // Case 3
                        sibling_left_child->color = BLACK;
                        addr = (char*)map_base + sibling->left_child_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                        memcpy(addr, &black, sizeof(u_int64_t));
                        sibling->color = RED;
                        addr = (char*)map_base + sibling_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                        memcpy(addr, &red, sizeof(u_int64_t));
                        rightRotation(sibling_idx);
                        readNode(parent_idx, parent);
                        sibling_idx = parent->right_child_idx;
                        readNode(sibling_idx, sibling);
                    }

                    // Case 4
                    sibling->color = parent->color;
                    addr = (char*)map_base + sibling_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &parent->color, sizeof(u_int64_t));
                    parent->color = BLACK;
                    addr = (char*)map_base + parent_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &black, sizeof(u_int64_t));
                    // sibling->right->color = BLACK
                    addr = (char*)map_base + sibling->right_child_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &black, sizeof(u_int64_t));
                    leafRotation(parent_idx);
                    cur_idx = root_idx;
                }    

                delete sibling;    
                delete sibling_left_child;
                delete sibling_right_child;        
            }
            else{
                // Current is the right child
                RBTreeNode* sibling = new RBTreeNode(0,0,0);
                u_int64_t sibling_idx = parent->left_child_idx;
                readNode(sibling_idx, sibling);

                if(sibling->color == RED){
                    //Case 1
                    sibling->color = BLACK;
                    addr = (char*)map_base + sibling_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &black, sizeof(u_int64_t));
                    parent->color = RED;
                    addr = (char*)map_base + parent_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &red, sizeof(u_int64_t));
                    rightRotation(parent_idx);
                    readNode(parent_idx, parent);
                    sibling_idx = parent->left_child_idx;
                    readNode(sibling_idx, sibling);
                }

                RBTreeNode* sibling_left_child = new RBTreeNode(0,0,0);
                readNode(sibling->left_child_idx, sibling_left_child);
                RBTreeNode* sibling_right_child = new RBTreeNode(0,0,0);
                readNode(sibling->right_child_idx, sibling_right_child);

                if(sibling_left_child->color == BLACK && sibling_right_child->color == BLACK){
                    // Case 2
                    sibling->color = RED;
                    addr = (char*)map_base + sibling_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &red, sizeof(u_int64_t));
                    cur_idx = parent_idx;
                    addr = (char*)map_base + cur_idx * sizeof(RBTreeNode) + ((char*)&cur->parent_idx - (char*)cur);
                    memcpy(&parent_idx, addr, sizeof(u_int64_t));
                }
                else{
                    if(sibling_left_child->color == BLACK){
                        // Case 3
                        sibling_right_child->color = BLACK;
                        addr = (char*)map_base + sibling->right_child_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                        memcpy(addr, &black, sizeof(u_int64_t));
                        sibling->color = RED;
                        addr = (char*)map_base + sibling_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                        memcpy(addr, &red, sizeof(u_int64_t));
                        leafRotation(sibling_idx);
                        readNode(parent_idx, parent);
                        sibling_idx = parent->left_child_idx;
                        readNode(sibling_idx, sibling);
                    }

                    // Case 4
                    sibling->color = parent->color;
                    addr = (char*)map_base + sibling_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &parent->color, sizeof(u_int64_t));
                    parent->color = BLACK;
                    addr = (char*)map_base + parent_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &black, sizeof(u_int64_t));
                    // sibling->left->color = BLACK
                    addr = (char*)map_base + sibling->left_child_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
                    memcpy(addr, &black, sizeof(u_int64_t));
                    rightRotation(parent_idx);
                    cur_idx = root_idx;
                }   

                delete sibling;    
                delete sibling_left_child;
                delete sibling_right_child;   
            }
            delete parent;
        }
        else{
            addr = (char*)map_base + cur_idx * sizeof(RBTreeNode) + ((char*)&cur->color - (char*)cur);
            memcpy(addr, &black, sizeof(u_int64_t));
            delete cur;
            break;
        }  

        delete cur;   
    }
}

u_int64_t RBTree::search(u_int64_t key){
    u_int64_t cur_idx = root_idx;

    u_int64_t* addr;
    u_int64_t cur_key;
    while(cur_idx){
        //Read the cur node
        addr = (u_int64_t*) ((char*)map_base + cur_idx * sizeof(RBTreeNode));
        memcpy(&cur_key, addr, sizeof(u_int64_t));

        if(key == cur_key){
            return cur_idx;
        }
        else if(key < cur_key){
            addr++;
            memcpy(&cur_idx, addr, sizeof(u_int64_t));
        }
        else{
            addr += 2;
            memcpy(&cur_idx, addr, sizeof(u_int64_t));
        }
    }

    return 0;
}

void RBTree::display(){
    if(root_idx != 0){
        RBTreeNode* node = new RBTreeNode(0,0,0);
        char* addr = (char*)map_base + root_idx * sizeof(RBTreeNode);
        memcpy(node, addr, sizeof(RBTreeNode));

        node->display(map_base, 0);

        delete node;
    }
}

void RBTreeNode::display(void* map_base, int level){
    if(right_child_idx != 0){
        RBTreeNode* node = new RBTreeNode(0,0,0);
        char* addr = (char*)map_base + right_child_idx * sizeof(RBTreeNode);
        memcpy(node, addr, sizeof(RBTreeNode));
        
        node->display(map_base, level + 1);
        delete node;
    }

    for(int i = 0; i < level; i++)
        cout << "\t";
    string color_code = (color == RED) ? "Red" : "Black";
    cout << "(" << key << ":" << color_code << ")" << endl;
    

    if(left_child_idx != 0){
        RBTreeNode* node = new RBTreeNode(0,0,0);
        char* addr = (char*)map_base + left_child_idx * sizeof(RBTreeNode);
        memcpy(node, addr, sizeof(RBTreeNode));
        node->display(map_base, level + 1);
        delete node;
    }
}

void RBTree::readNode(u_int64_t idx, RBTreeNode* buf){
    char* addr = (char*)map_base + idx * sizeof(RBTreeNode);
    memcpy(buf, addr, sizeof(RBTreeNode));
}

void RBTree::writeNode(u_int64_t idx, RBTreeNode* buf){
    char* addr = (char*)map_base + idx * sizeof(RBTreeNode);
    memcpy(addr, buf, sizeof(RBTreeNode));
}

void RBTree::leafRotation(u_int64_t x_idx){
    char* addr;
    bool root = false;
    RBTreeNode* x = new RBTreeNode(0,0,0);
    readNode(x_idx, x);
    u_int64_t y_idx = x->right_child_idx;
    RBTreeNode* y = new RBTreeNode(0,0,0);
    readNode(x->right_child_idx, y);
    RBTreeNode* parent = new RBTreeNode(0,0,0);
    if(x_idx == root_idx)
        root = true;
    else
        readNode(x->parent_idx, parent);

    x->right_child_idx = y->left_child_idx;
    addr = (char*)map_base + x_idx * sizeof(RBTreeNode) + ((char*)&x->right_child_idx - (char*)x);
    memcpy(addr, &y->left_child_idx, sizeof(u_int64_t));
    // y->leaf_child->parent = x
    addr = (char*)map_base + y->left_child_idx * sizeof(RBTreeNode) + ((char*)&x->parent_idx - (char*)x);
    memcpy(addr, &x_idx, sizeof(u_int64_t));

    // x->parent->child = y
    if(root){
        root_idx = y_idx;
    }
    else{
        if(parent->left_child_idx == x_idx)
            addr = (char*)map_base + x->parent_idx * sizeof(RBTreeNode) + ((char*)&x->left_child_idx - (char*)x);
        else
            addr = (char*)map_base + x->parent_idx * sizeof(RBTreeNode) + ((char*)&x->right_child_idx - (char*)x);
        memcpy(addr, &y_idx, sizeof(u_int64_t));    
    }  
    
    y->parent_idx = x->parent_idx;
    addr = (char*)map_base + y_idx * sizeof(RBTreeNode) + ((char*)&x->parent_idx - (char*)x);
    memcpy(addr, &x->parent_idx, sizeof(u_int64_t));

    y->left_child_idx = x_idx;
    addr = (char*)map_base + y_idx * sizeof(RBTreeNode) + ((char*)&x->left_child_idx - (char*)x);
    memcpy(addr, &x_idx, sizeof(u_int64_t));
    x->parent_idx = y_idx;
    addr = (char*)map_base + x_idx * sizeof(RBTreeNode) + ((char*)&x->parent_idx - (char*)x);
    memcpy(addr, &y_idx, sizeof(u_int64_t));

    delete x;
    delete y;
    delete parent;
}

void RBTree::rightRotation(u_int64_t x_idx){
    char* addr;
    bool root = false;
    RBTreeNode* x = new RBTreeNode(0,0,0);
    readNode(x_idx, x);
    u_int64_t y_idx = x->left_child_idx;
    RBTreeNode* y = new RBTreeNode(0,0,0);
    readNode(x->left_child_idx, y);
    RBTreeNode* parent = new RBTreeNode(0,0,0);
    if(x_idx == root_idx)
        root = true;
    else
        readNode(x->parent_idx, parent);

    x->left_child_idx = y->right_child_idx;
    addr = (char*)map_base + x_idx * sizeof(RBTreeNode) + ((char*)&x->left_child_idx - (char*)x);
    memcpy(addr, &y->right_child_idx, sizeof(u_int64_t));
    // y->leaf_child->parent = x
    addr = (char*)map_base + y->right_child_idx * sizeof(RBTreeNode) + ((char*)&x->parent_idx - (char*)x);
    memcpy(addr, &x_idx, sizeof(u_int64_t));

    // x->parent->child = y
    if(root){
        root_idx = y_idx;
    }
    else{
        if(parent->left_child_idx == x_idx)
            addr = (char*)map_base + x->parent_idx * sizeof(RBTreeNode) + ((char*)&x->left_child_idx - (char*)x);
        else
            addr = (char*)map_base + x->parent_idx * sizeof(RBTreeNode) + ((char*)&x->right_child_idx - (char*)x);
        memcpy(addr, &y_idx, sizeof(u_int64_t));    
    }
    
    y->parent_idx = x->parent_idx;
    addr = (char*)map_base + y_idx * sizeof(RBTreeNode) + ((char*)&x->parent_idx - (char*)x);
    memcpy(addr, &x->parent_idx, sizeof(u_int64_t));

    y->right_child_idx = x_idx;
    addr = (char*)map_base + y_idx * sizeof(RBTreeNode) + ((char*)&x->right_child_idx - (char*)x);
    memcpy(addr, &x_idx, sizeof(u_int64_t));
    x->parent_idx = y_idx;
    addr = (char*)map_base + x_idx * sizeof(RBTreeNode) + ((char*)&x->parent_idx - (char*)x);
    memcpy(addr, &y_idx, sizeof(u_int64_t));

    delete x;
    delete y;
    delete parent;
}

u_int64_t RBTree::succesor(u_int64_t idx){
    u_int64_t cur_idx = idx;
    
    while(cur_idx){
        RBTreeNode* ref = new RBTreeNode(0,0,0);
        u_int64_t left_child;
        char* addr = (char*)map_base + cur_idx * sizeof(RBTreeNode) + ((char*)&ref->left_child_idx - (char*)ref);
        memcpy(&left_child, addr, sizeof(u_int64_t));

        if(left_child){
            cur_idx = left_child;
            delete ref;
        }
        else{
            delete ref;
            return cur_idx;
        }
    }

    return 0;
}

int main(){

    int fd = open("TREE", O_RDWR | O_DIRECT);
    ftruncate(fd, MAP_SIZE * 2);
    void* map_base = mmap(0, MAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map_base == (void*)-1) FATAL;

    RBTree* rbtree = new RBTree(map_base);
    int array[] = {31,94,1,78,22, 7,38,65,30,93, 58,89,51,4,2};
    for(int i = 0; i < sizeof(array) / sizeof(int); i++){
        rbtree->insert(array[i]);
    }  

    rbtree->display();
    int arr[] = {4,7,65,89,2, 78,1,30,58,94, 31,51,22,93,38};
    for(int i = 0; i < sizeof(arr) / sizeof(int); i++){
        cout << "# Delete : " << arr[i] << endl;
        rbtree->remove(arr[i]);
        rbtree->display();
    }   

    delete rbtree;
    munmap(map_base, MAP_SIZE);
    close(fd);

    return 0;
}