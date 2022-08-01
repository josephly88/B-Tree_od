CC=g++

merge_btree: main_merge.o
	${CC} main_merge.o -o merge_btree.out;	\rm -rf *.o;
main_merge.o: main_merge.cpp
	${CC} -std=c++11 -c -g main_merge.cpp
clean:
	@rm -rf merge_btree.out
