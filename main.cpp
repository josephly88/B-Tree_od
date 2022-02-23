#include <iostream>
#include <time.h>
#include "b_tree.h"
using namespace std;

#define INS 10
#define DEL 10

struct S{
    char value;
    bool exist;
}S[INS];

int random_num(int base, int max){
    return rand() % max + base;
}

char random_char(){
    return (rand() % 2 == 0) ? char( rand() % 26 + 65 ) : char( rand() % 26 + 97 );
}

int main(int argc, char** argv){

    if(argc < 3){
        cout << "-- Not enough input arguments. --" << endl;
        cout << "Format:" << endl;
        cout << "\t./program {filename} {block size}" << endl;
        return 0;
    }

    BTree* t = new BTree(argv[1], atoi(argv[2]));

    if (t->get_file()->is_open()){
        cout << "Size of BTreeNode is : " << sizeof(BTreeNode) << endl;
        cout << "Block size is : " << argv[2] << endl;
        cout << "Degree is : " << t->get_degree() << endl;
    }
    else{
        cout << "Unable to open file" << endl;
        return 0;
    }

    char v = random_char();
    //t->insertion(0, v);
    t->traverse();

	return 0;
}