#include <iostream>
#include <time.h>
#include <sys/stat.h>
#include <chrono>
#include "../../lib/cmb/b_tree.h"
#include "../../lib/YCSB/ycsb_data.h"
using namespace std;

typedef struct TYPE{char str[100];} TYPE;

int random_num(int base, int max){
    return rand() % max + base;
}

bool fileExists(const char* file) {
    struct stat buf;
    return (stat(file, &buf) == 0);
}

int main(int argc, char** argv){

    BTree<TYPE>* t;
    //off_t cmb_addr = 0xc0000000;
    off_t cmb_addr = 0x0;   // fake_cmb

    srand(time(0));

    if(argc < 2){
        cout << "-- Not enough input arguments. --" << endl;
        cout << "Format:" << endl;
        cout << "\t./program {filename} <data_filename>" << endl;
        return 0;
    }
    else{
	// Existed tree file
        if(fileExists(argv[1])){
            cout << "Read file <" << argv[1] << ">" << endl;
            int fd = open(argv[1], O_DIRECT | O_RDWR);
            t = (BTree<TYPE>*) calloc(1, sizeof(BTree<TYPE>));
            t->tree_read(fd, t);
			t->reopen(fd);
        }
        else{
	// Create a new tree file
            cout << "Create file <" << argv[1] << ">" << endl;
            t = new BTree<TYPE>(argv[1], cmb_addr, 5);
        }
    }

    t->stat();

    // Has data file as input
// Has data file as input
    if(argc == 3){
        cout << "Processing Input Data..." << endl;
        int recordcount = YCSB_data_file(argv[2], (char*)"inter.dat");
        ifstream dataFile;
        dataFile.open("inter.dat");

        cout << "Operation Start!" << endl;
        string line;
        for(int i = 0; i < recordcount; i++){
            // Extract data            
            char op;
            u_int64_t key;
            TYPE val;

            getline(dataFile, line);
            ycsb_lexi(line, &op, &key, val.str);

            auto start = chrono::system_clock::now();
            auto end = std::chrono::system_clock::now();
            if(op == 'i'){
                // Insert data
                cout << "OP#" << i+1 << " - Insert : " << key << " " << val.str << endl;
                start = chrono::system_clock::now();
                t->insertion(key, val);
                end = std::chrono::system_clock::now();
            }
            else if(op == 'r'){
                // Read data
                cout << "OP#" << i+1 << " - Read : " << key;
                start = chrono::system_clock::now();
                t->search(key, &val);
                end = std::chrono::system_clock::now();
                cout << " >> " << val.str << endl;
            }
            else{
                continue;
            }            
                
            // Display tree sturcture for Debug
            //t->display_tree();
            //t->print_used_block_id();
            auto diff = end - start;
            cout << "-Duration: " << diff.count() << endl;
        }

        dataFile.close();
        
        t->inorder_traversal((char*)"tree.dat");
    }
    
    delete t;

    return 0;
}