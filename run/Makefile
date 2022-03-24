CC = g++

cpw_btree: b_tree.o main_cpw.o
	${CC} main_cpw.o b_tree.o -o cpw_btree.out;	\rm -rf *.o tree;
%.o: ../lib/copy_on_write/%.cpp
	${CC} $< -Wall -g -c;
clean:
	@rm -rf cpw_btree.out
