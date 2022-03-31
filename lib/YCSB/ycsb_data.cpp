#ifndef YCSB_DATA_H //include guard
#define YCSB_DATA_H

#include <iostream>
#include <fstream>
#include <string.h>
using namespace std;

#define RECORDCOUNT 10
#define FIELDCOUNT 1
#define FIELD_LENGTH 110

struct YCSB_DATA{
    u_int64_t key;
    char field0[FIELD_LENGTH];
};

void print_YCSB_data(YCSB_DATA dat){
    cout << dat.key << " " << dat.field0 << endl;
}

void YCSB_data_file(char* fileIn){
    ifstream ycsb_file;    
    string line;

    ycsb_file.open(fileIn);

    for(int i = 0; i < 15; i++)
        getline(ycsb_file, line);

    for(int i = 0; i < RECORDCOUNT; i++){
        int th = 1;
        YCSB_DATA dat;

        getline(ycsb_file, line);

        // Key
        line.erase(0,21);
        dat.key = atoll((char*)line.c_str());

        // Value
        line.erase(0,29);
        memcpy(dat.field0, (char*)line.c_str(), FIELD_LENGTH-1);

        print_YCSB_data(dat);
    }

    ycsb_file.close();
}

int main(int argc, char** argv){

    YCSB_data_file(argv[1]);

    return 0;
}

#endif