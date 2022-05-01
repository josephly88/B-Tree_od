CC = g++

cpw_btree: main_cmb.o
	${CC} main_cmb.o -o cmb_btree.out;	\rm -rf *.o tree;
main_cmb.o: main_cmb.cpp
	${CC} -std=c++11 -c main_cmb.cpp
clean:
	@rm -rf cmb_btree.out
