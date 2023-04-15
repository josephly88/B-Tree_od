#include <iostream>
#include <random>
#include <cstring>
#include "b_tree.h"

int main() {
    BTree tree("my_tree.dat", true); // create a new tree
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(1, 100);

    for (int i = 0; i < 30; i++) {
        int key = distrib(gen);
        char value[104];
        std::memset(value, 0, sizeof(value));
        std::sprintf(value, "value%d", i);
        tree.insert(key, value);

        std::cout << std::endl;           
        std::cout << "OP#" << i+1 <<" - Insert key: " << key << ", value: " << value << std::endl;
        tree.print_btree();
    }

    return 0;
}
