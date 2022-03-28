CC = g++

cpw_btree: b_tree.o main_cmb.o
	${CC} main_cmb.o b_tree.o -o cmb_btree.out;	\rm -rf *.o tree;
%.o: ../../lib/cmb/%.cpp
	${CC} $< -Wall -g -c;
clean:
	@rm -rf cmb_btree.out
