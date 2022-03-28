#include <iostream>
#include <time.h>
#include <sys/stat.h>
#include <chrono>
#include "../../lib/copy_on_write/b_tree.h"
using namespace std;

#define INS 5
#define DEL 2
#define RANGE 1000

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
    t->print_used_block_id();
}

void loop_insert(BTree* t){

    for(int i = 0; i < INS; i++){
        int k;
        do{ k = random_num(0, RANGE);}while(t->search(k) != NULL);  // Random Key
        /*
        int arr[] = {20,37,36,74,10,40,94,80,78,17,44,66,96,25,58};
        k = arr[i]; 
        */

        char v = random_char();

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
    t->print_used_block_id();
}

void loop_delete(BTree* t){

    for(int i = 0; i < DEL; i++){
        int k;
        do{ k = random_num(0, RANGE);}while(t->search(k) == NULL);
        /*
        int arr[] = {13, 7, 9, 0, 10, 11, 4, 2, 17, 1, 14, 3, 15, 6, 12, 5};        
        k = arr[i];
        */

        delete_n_print(t, k);
    }
}

int main(int argc, char** argv){

    BTree* t;
    //off_t cmb_addr = 0xc0000000;

    srand(time(0));

    if(argc < 2){
        cout << "-- Not enough input arguments. --" << endl;
        cout << "Format:" << endl;
        cout << "\t./program {filename}" << endl;
        return 0;
    }
    else{
	// Existed tree file
        if(fileExists(argv[1])){
            cout << "Read file <" << argv[1] << ">" << endl;
            int fd = open(argv[1], O_DIRECT | O_RDWR);
            t = (BTree*) calloc(1, sizeof(BTree));
            t->tree_read(fd, t);
			t->reopen(fd);
        }
        else{
	// Create a new tree file
            cout << "Create file <" << argv[1] << ">" << endl;
            t = new BTree(argv[1], 5);
        }
    }

    t->stat();

    loop_insert(t);
    loop_delete(t);

    delete t;

    return 0;


}