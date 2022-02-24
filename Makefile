CC = g++

main: b_tree.o main.o
	${CC} main.o b_tree.o -o demo.out;	\rm -rf *.o tree.dat;
%.o: %.cpp
	${CC} $< -Wall -g -c;
clean:
	@rm -rf demo.out
