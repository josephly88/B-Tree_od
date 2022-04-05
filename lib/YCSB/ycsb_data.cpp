#ifndef YCSB_DATA_H //include guard
#define YCSB_DATA_H

#include <iostream>
#include <fstream>
#include <string.h>
#include <regex>
using namespace std;

// Only support 1 field count

void YCSB_data_file(int recordcount, char* fileIn, char* fileOut){
    ifstream ycsb_file;    
    ycsb_file.open(fileIn);

	ofstream processed;
	processed.open(fileOut);

    string line;
// Skip first 15 lines
    for(int i = 0; i < 15; i++)
        getline(ycsb_file, line);

// For each record

	for(int i = 0; i < recordcount; i++){

		getline(ycsb_file, line);

		smatch m;
		regex regexp_id("user[0-9]+");
		regex regexp_val("field0=[\\w|\\W]*\\s]");
		
		regex_search(line, m, regexp_id);
		string id(m[0]);
		processed << id.erase(0,4) << '\t';	// Remove "user"

		regex_search(line, m, regexp_val);
		string val(m[0]);
		processed << val.substr(7, val.length()-2-7) << endl; // Remove "field0=" and " ]"

	}

    ycsb_file.close();
	processed.close();
}

int main(int argc, char** argv){

    YCSB_data_file(10, argv[1], argv[2]);

    return 0;
}

#endif
