#ifndef YCSB_DATA_H //include guard
#define YCSB_DATA_H

#include <iostream>
#include <fstream>
#include <string.h>
#include <regex>
using namespace std;

// Only support 1 field count

int YCSB_data_file(char* fileIn, char* fileOut){
    ifstream ycsb_file(fileIn);    

	ofstream processed;
	processed.open(fileOut);

    string line;
	int recordcount;

	// Skip properties, except for recordcount we take the number
    while(true){
        getline(ycsb_file, line);

		if(line == "**********************************************")
			break;

		string item(line);
		item = item.substr(1, 11);
		if(item == "recordcount"){
			smatch m;
			regex regexp_cnt("[0-9]+");
			regex_search(line, m, regexp_cnt);
			string value(m[0]);
			recordcount = stoi(value);
		}
	}

	// For each record
	for(int i = 0; i < recordcount; i++){

		getline(ycsb_file, line);

		processed << (char) tolower(line[0]);	// Op code

		smatch m;
		
		regex regexp_id("user[0-9]+");
		regex_search(line, m, regexp_id);
		string id(m[0]);
		processed << id.erase(0,4) << '\t';	// Remove "user"

		if(line[0] != 'R'){
			regex regexp_val("field0=[\\w|\\W]*\\s]");
			regex_search(line, m, regexp_val);
			string val(m[0]);
			processed << val.substr(7, val.length()-2-7); // Remove "field0=" and " ]"
		}

		processed << endl;
	}

    ycsb_file.close();
	processed.close();

	return recordcount;
}

void ycsb_lexi(string line, char *op, u_int64_t *key, char *val){
	// Extract the opreation
	*op = line[0];
	
	// Extract the key
	int tab_pos = line.find('\t');
	*key = stoll(line.substr(1, tab_pos));

	if(*op == 'i' || *op == 'u'){
		// Extract the value
		string val_str = line.substr(tab_pos + 1, line.length());
		strcpy(val, (char*) val_str.c_str());
	}
}


#endif
