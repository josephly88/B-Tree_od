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

void usage(){
    cout << "Format:" << endl; cout << "Usage: ./program [OPTION]... {tree_filename}" << endl << endl; cout << "Options" << endl;
    cout << "  -d {degree}\t\t\t\tDegree of the B-Tree, only for creating new B-Tree (Default: 128)" << endl;
    cout << "  -l {log file}\t\t\t\tSave all log to newly created file" << endl;
    cout << "  -i {input file}\t\t\tRead input opreations from file" << endl;
}

int main(int argc, char** argv){

    BTree<TYPE>* t;

    srand(time(0));
    
    int degree = 128;
    char* log_file = NULL;
    char* input_file = NULL;
    char* tree_file = NULL;
    int start = 0;
    int end = 0;

    if(argc < 2){
        usage();
        return 0;
    }
    else{
        for(int i = 1; i < argc; i++){          
            if(strcmp(argv[i], "-d") == 0){
                degree = stoi(argv[i+1]);
                i++;
            }
            else if(strcmp(argv[i], "-l") == 0){
                log_file = argv[i+1];
                i++;
            }
            else if(strcmp(argv[i], "-i") == 0){
                input_file = argv[i+1];
                i++;
            }
            else if(strcmp(argv[i], "-r") == 0){
                start = atoi(argv[i+1]);
                end = atoi(argv[i+2]);
                i += 2;
            }
            else{
                tree_file = argv[i];
                break;
            }
        }

        if(tree_file == NULL){
            usage();
            return 0;
        }
    }

    if(log_file == NULL)
        mylog.open("/dev/null");
    else
        mylog.open(log_file);

    // Existed tree file
    if(fileExists(tree_file)){
        cout << "Read file from <" << tree_file << ">" << endl;
        int fd = open(tree_file, O_DIRECT | O_RDWR);
        t = (BTree<TYPE>*) calloc(1, sizeof(BTree<TYPE>));
        t->tree_read(fd, t);
        t->reopen(fd);
    }
    else{
    // Create a new tree file
        cout << "Create file <" << tree_file << ">" << endl;
        t = new BTree<TYPE>(tree_file, degree);
    }

    t->stat();

    // Has data file as input
    if(input_file){
        YCSB_file *ycsb_file = new YCSB_file(input_file);

		// Extract input filename
		string input_file_str(input_file);
		regex rgx_opf("(\\w+)\\.txt");
		smatch match;
		regex_search(input_file_str, match, rgx_opf);
		string op_file_name(match[1]);

        ofstream op_file;
        op_file.open(op_file_name + ".dat", ios_base::app);

        if(start == 0 && end == 0)
            end = ycsb_file->get_recordcount();

        cout << "Operation Start!" << endl;
        mylog << "Operation Start!" << endl;
        for(int i = 0; i < ycsb_file->get_recordcount(); i++){
            // Extract data           
            string line; 
            char op;
            u_int64_t key;
            TYPE val;

            if(i < start){
                ycsb_file->skipline();
                continue;
            }
            if(i >= end)
                break;

            line = ycsb_file->readline();
            ycsb_file->lexi(line, &op, &key, val.str);

            if(op == 'i'){
                // Insert data
                cout << '\r' << "OP#" << i+1 << " - Insert : " << key << " >> " << val.str;
                mylog << "\n" << "OP#" << i+1 << " - Insert : " << key << " >> " << val.str << endl;;
                auto start = chrono::system_clock::now();
                t->insertion(key, val);
                auto end = std::chrono::system_clock::now();
                chrono::duration<double, milli> diff = end - start;
                op_file << "i\t" << key << "\t" << val.str << "\t" << diff.count() << endl;
            }
            else if(op == 'r'){
                // Read data
                cout << '\r' << "OP#" << i+1 << " - Read : " << key;
                mylog << "\n" << "OP#" << i+1 << " - Read : " << key;
                auto start = chrono::system_clock::now();
                t->search(key, &val);
                auto end = std::chrono::system_clock::now();
                cout << " >> " << val.str;
                mylog << " >> " << val.str << endl;
                chrono::duration<double, milli> diff = end - start;
                op_file << "r\t" << key << "\t" << val.str << "\t" << diff.count() << endl;
            }
            else if(op == 'u'){
                // Update data
                cout << '\r' << "OP#" << i+1 << " - Update : " << key << " >> " << val.str;
                mylog << "\n" << "OP#" << i+1 << " - Update : " << key << " >> " << val.str << endl;
                auto start = chrono::system_clock::now();
                t->update(key, val);
                auto end = std::chrono::system_clock::now();
                chrono::duration<double, milli> diff = end - start;
                op_file << "u\t" << key << "\t" << val.str << "\t" << diff.count() << endl;
            }
            else if(op == 'd'){
                // Delete data
                cout << '\r' << "OP#" << i+1 << " - Delete : " << key;
                mylog << "\n" << "OP#" << i+1 << " - Delete : " << key << endl;
                auto start = chrono::system_clock::now();
                t->deletion(key);
                auto end = std::chrono::system_clock::now();
                chrono::duration<double, milli> diff = end - start;
                op_file << "d\t" << key << "\t" << diff.count() << endl;
            }
            else{
                continue;
            }            
                
            // Display tree sturcture for Debug
            //t->display_tree();
            //t->print_used_block_id();
        }
	    cout << endl << "Done!" << endl;;

        delete ycsb_file;
        op_file.close();
    }
    t->inorder_traversal((char*)"tree.dat");

    mylog.close();
    
    delete t;

    return 0;
}
