#ifndef YCSB_DATA_H //include guard
#define YCSB_DATA_H

#include <iostream>
#include <fstream>
#include <string.h>
#include <regex>
using namespace std;

class YCSB_file{

	ifstream ycsb_file;
	int recordcount;

	public:
		YCSB_file(char* fileIn);
		~YCSB_file();
		int get_recordcount();
		string readline();
        void skipline();
		void lexi(string line, char *op, u_int64_t *key, char *val);
};

YCSB_file::YCSB_file(char* fileIn){
	ycsb_file.open(fileIn);

	string line;
	while(true){
		getline(ycsb_file, line);

		if(line == "**********************************************")
			break;

		string item(line);
		item = item.substr(1, 14);
		if(item == "operationcount"){
			smatch m;
			regex regexp_cnt("[0-9]+");
			regex_search(line, m, regexp_cnt);
			string value(m[0]);
			recordcount = stoi(value);	
		}
	}
}

YCSB_file::~YCSB_file(){
	ycsb_file.close();
}

int YCSB_file::get_recordcount(){
	return recordcount;
}

void YCSB_file::skipline(){
    string line;
    getline(ycsb_file, line);
}

string YCSB_file::readline(){
	string line;
	getline(ycsb_file, line);

	string processed = "";
	processed += (char) tolower(line[0]);
	
	smatch m;
	regex regexp_id("user[0-9]+");
	regex_search(line, m, regexp_id);
	string id(m[0]);
	processed += id.erase(0,4); 	// Remove "user"
	processed += '\t';

	if(line[0] == 'I' || line[0] == 'U'){
		regex regexp_val("field0=[\\w|\\W]*\\s]");
		regex_search(line, m, regexp_val);
		string val(m[0]);
		processed += val.substr(7, val.length()-2-7); // Remove "field0=" and " ]"
	}

	return processed;
}

void YCSB_file::lexi(string line, char *op, u_int64_t *key, char *val){
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
