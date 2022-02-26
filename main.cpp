#include <iostream>
#include <time.h>
#include <sys/stat.h>
#include "b_tree.h"
using namespace std;

#define INS 10
#define DEL 1

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

bool fileExists(const char* file) {
    struct stat buf;
    return (stat(file, &buf) == 0);
}

void command(BTree* t, fstream* file){
    while(1){
        t->traverse();
        cout << "Type your instruction " << endl;

        char opt;
        cin >> opt;

        if(opt == 'i'){
            int key;
            cin >> key;

            char v = random_char();
            //int k = random_num(0,1000);
            t->insertion(key, v);
            cout << "-Insertion: " << key  << '(' << v << ')' << endl;
        }
        else if(opt == 'd'){
            int key;
            cin >> key;

            t->deletion(key);
            cout << "-Deletion: " << key  <<  endl;
        }
        else
            break;

        tree_read(file, t);
    }
}

int main(int argc, char** argv){

    BTree* t;
    fstream file;

    srand(time(0));

    if(argc < 2){
        cout << "-- Not enough input arguments. --" << endl;
        cout << "Format:" << endl;
        cout << "\t./program {filename}" << endl;
        return 0;
    }
    else{
        if(fileExists(argv[1])){
            cout << "Read file <" << argv[1] << ">" << endl;
            file.open(argv[1], ios::in | ios::out | ios::binary);
            t = (BTree*) calloc(1, sizeof(BTree));
            tree_read(&file, t);
            t->file_ptr = &file;
            tree_write(&file, t);
        }
        else{
            cout << "Create file <" << argv[1] << ">" << endl;
            cout << "Please input the block size: ";
            int block_size;
            cin >> block_size;
            t = new BTree(argv[1], block_size, &file);
        }
    }
    
    if (file.is_open()){
        cout << endl;
        cout << "Block size : " << t->block_size << endl;
        cout << "Size of BTreeNode : " << sizeof(BTreeNode) << endl;        
        cout << "Size of K-V : " << sizeof(int) + sizeof(char) + sizeof(streamoff) << "(" << sizeof(streamoff) << ")" << endl;
        cout << "Degree : " << t->m << endl;
        cout << "Node capacity : " << t->node_cap << endl;
        cout << "Root id : " << t->root_id << endl;
        cout << endl;
    }
    else{
        cout << "Unable to open file" << endl;
        return 0;
    }

/*
    int ins[] = {0, 1, 2, 3, 4};
    int del[] = {0};
    for(int i = 0; i < sizeof(ins) / sizeof(ins[0]); i++){
        char v = random_char();
        t->insertion(ins[i], v);
        cout << "-Insertion: " << ins[i]  << '(' << v << ')' << endl;
        t->traverse();
    }
    for(int i = 0; i < sizeof(del) / sizeof(del[0]); i++){
        t->deletion(del[0]);
        cout << "-Deletion: " << del[0]  <<  endl;
        t->traverse();
    }
    char v = random_char();
    t->insertion(0, v);
    cout << "-Insertion: " << 0  << '(' << v << ')' << endl;
    t->traverse();
*/

    command(t, &file);

    delete t;

    return 0;


}