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
    //off_t cmb_addr = 0xc0000000;

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
        ifstream dataFile;
        dataFile.open("inter.dat");

        cout << "Operation Start!" << endl;
        string line;
        for(int i = 0; i < recordcount; i++){
            // Extract data
            getline(dataFile, line);
            int tab_pos = line.find('\t');
            u_int64_t key = stoll(line.substr(0, tab_pos));
            TYPE val;
            string val_str = line.substr(tab_pos + 1, line.length());
            strcpy(val.str, (char*) val_str.c_str());

            // Insert data
            cout << "Insert data " << i + 1 << ": " << key << " " << val.str << endl;
            auto start = chrono::system_clock::now();
            t->insertion(key, val);
            auto end = std::chrono::system_clock::now();
            chrono::duration<double> diff = end - start;

            // Display tree sturcture for Debug
            //t->display_tree();
            //t->print_used_block_id();
            cout << "-Duration: " << diff.count() << endl;
        }

        dataFile.close();
        
        t->inorder_traversal((char*)"out.dat");
    }

    delete t;

    return 0;


}