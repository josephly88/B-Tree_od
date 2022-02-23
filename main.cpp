#include <iostream>
#include <time.h>
#include "b_tree.h"
using namespace std;

#define INS 5
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

    if (t->file.is_open()){
        cout << "Block size is : " << argv[2] << endl;
        cout << "Size of BTreeNode is : " << sizeof(BTreeNode) << endl;        
        cout << "Size of K-V is : " << sizeof(int) + sizeof(char) + sizeof(streamoff) << "(" << sizeof(streamoff) << ")" << endl;
        cout << "Degree is : " << t->m << endl;
        cout << endl;
    }
    else{
        cout << "Unable to open file" << endl;
        return 0;
    }

    for(int i = 0; i < INS; i++){
        char v = random_char();
        t->insertion(i, v);
        cout << "-Insertion: " << i  << '(' << v << ')' << endl;
        t->traverse();
    }
    

	return 0;
}