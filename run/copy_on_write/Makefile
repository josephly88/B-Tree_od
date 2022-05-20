CC=g++

cpw_btree: main_cpw.o
	${CC} main_cpw.o -o cpw_btree.out;	\rm -rf *.o tree;
main_cpw.o: main_cpw.cpp
	${CC} -std=c++11 -c -g main_cpw.cpp
clean:
	@rm -rf cpw_btree.out
