#include <iostream>
#include <time.h>
#include <sys/stat.h>
#include <chrono>
#include "../../lib/copy_on_write/b_tree.h"
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

    srand(time(0));

    if(argc < 2){
        cout << "-- Not enough input arguments. --" << endl;
        cout << "Format:" << endl;
        cout << "\t./program {tree_filename} <data_filename>" << endl;
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
            t = new BTree<TYPE>(argv[1], 5);
        }
    }

    t->stat();

    // Has data file as input
    if(argc == 3){
        cout << "Processing Input Data..." << endl;
        int recordcount = YCSB_data_file(argv[2], (char*)"inter.dat");

        ifstream ycsb_inter_file;
        ycsb_inter_file.open("inter.dat");
        ofstream op_file;
        op_file.open("opr.dat", ios_base::app);

        cout << "Operation Start!" << endl;
        string line;
        for(int i = 0; i < recordcount; i++){
            // Extract data            
            char op;
            u_int64_t key;
            TYPE val;

            getline(ycsb_inter_file, line);
            ycsb_lexi(line, &op, &key, val.str);

            if(op == 'i'){
                // Insert data
                cout << "OP#" << i+1 << " - Insert : " << key << " >> " << val.str << endl;
                auto start = chrono::system_clock::now();
                t->insertion(key, val);
                auto end = std::chrono::system_clock::now();
                chrono::duration<double> diff = end - start;
                op_file << "i\t" << key << "\t" << val.str << "\t" << diff.count() << endl;
            }
            else if(op == 'r'){
                // Read data
                cout << "OP#" << i+1 << " - Read : " << key;
                auto start = chrono::system_clock::now();
                t->search(key, &val);
                auto end = std::chrono::system_clock::now();
                cout << " >> " << val.str << endl;
                chrono::duration<double> diff = end - start;
                op_file << "r\t" << key << "\t" << val.str << "\t" << diff.count() << endl;
            }
            else if(op == 'u'){
                // Update data
                cout << "OP#" << i+1 << " - Update : " << key << " >> " << val.str << endl;
                auto start = chrono::system_clock::now();
                t->update(key, val);
                auto end = std::chrono::system_clock::now();
                chrono::duration<double> diff = end - start;
                op_file << "u\t" << key << "\t" << val.str << "\t" << diff.count() << endl;
            }
            else if(op == 'd'){
                // Delete data
                cout << "OP#" << i+1 << " - Delete : " << key << endl;
                auto start = chrono::system_clock::now();
                t->deletion(key);
                auto end = std::chrono::system_clock::now();
                chrono::duration<double> diff = end - start;
                cout << "-Duration: " << diff.count() << endl;
                op_file << "d\t" << key << "\t" << diff.count() << endl;
            }
            else{
                continue;
            }            
                
            // Display tree sturcture for Debug
            //t->display_tree();
            //t->print_used_block_id();
        }

        ycsb_inter_file.close();
        remove("inter.dat");
        op_file.close();

        t->inorder_traversal((char*)"tree.dat");
    }

    delete t;

    return 0;
}