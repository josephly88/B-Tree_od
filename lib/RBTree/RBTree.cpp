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
        void remove();
        void search();

        void display();

        u_int64_t get_free_idx();

        void readNode(u_int64_t idx, RBTreeNode* buf);
        void writeNode(u_int64_t idx, RBTreeNode* buf);

        void leafRotation(u_int64_t x_idx);
        void rightRotation(u_int64_t x_idx);
};

class RBTreeNode{
    public:
        u_int64_t key;
        u_int64_t color;    // Red - 1, Black - 0
        u_int64_t parent_idx;
        u_int64_t left_child_idx;
        u_int64_t right_child_idx;     

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

    munmap(map_base, MAP_SIZE);
    close(fd);

    return 0;
}