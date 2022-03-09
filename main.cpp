#include <iostream>
#include <time.h>
#include <sys/stat.h>
#include <chrono>
#include "b_tree.h"
using namespace std;

#define INS 1
#define DEL 1
#define RANGE 1000

struct S{
    char value;
    bool exist;
}S[RANGE];

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

void insert_n_print(BTree* t, int k, char v){
    cout << "-Insertion: " << k  << '(' << v << ')' << endl;
    auto start = chrono::system_clock::now();
    t->insertion(k, v);
    auto end = std::chrono::system_clock::now();        
    chrono::duration<double> diff = end - start;

    cout << "-Duration: " << diff.count() << endl;
    
    t->traverse();
    t->print_used_node_id();
}

void loop_insert(BTree* t){

    for(int i = 0; i < INS; i++){
        int k;
        k = i;
        //do{ k = random_num(0, RANGE);}while(S[k].exist == true);
        char v = random_char();
        S[k].value = v;
        S[k].exist = true;

        insert_n_print(t, k, v);
    }
}

void delete_n_print(BTree* t, int k){
    cout << "-Deletion: " << k << endl;
    auto start = chrono::system_clock::now();
    t->deletion(k);
    auto end = std::chrono::system_clock::now();
    chrono::duration<double> diff = end - start;

    cout << "-Duration: " << diff.count() << endl;
    t->traverse();
    t->print_used_node_id();
}

void loop_delete(BTree* t){

    for(int i = 0; i < DEL; i++){
        int k;
        do{ k = random_num(0, RANGE);}while(S[k].exist == false);
        S[k].exist = false;

        delete_n_print(t, k);
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
    
    if (!file.is_open()){
        cout << "Unable to open file" << endl;
        return 0;
    }

    t->stat();

    loop_insert(t);
    loop_delete(t);

    cout << "Expected result: " << endl;
    for(int i = 0; i < RANGE; i++){
        if(S[i].exist == true){
            cout << " " << i << "(" << S[i].value << ")";
        }
    }
    cout << endl << endl;   

    delete t;

    return 0;


}