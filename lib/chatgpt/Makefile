# Compiler
CC = g++

# Compiler flags
CFLAGS = -std=c++11 -Wall -Wextra -pedantic -g

# Source files
SRCS = main.cpp b_tree.cpp

# Object files
OBJS = $(SRCS:.cpp=.o)

# Executable name
EXEC = my_program

all: $(EXEC)

$(EXEC): $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) -o $(EXEC)
	$(RM) $(OBJS)

%.o: %.cpp b_tree.h
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(EXEC)
