#!/usr/bin/python3

import sys

dataset = []
op_file_ls = sys.argv[1:]

i = 0

# read opr.dat
for FILE in op_file_ls:
    print(" Reading file: " + FILE)
    op_file = open(FILE, "r")
    while True:
        nstr = op_file.readline()
        # read until EOF
        if len(nstr) == 0:
            break
        token = nstr.split('\t')
        # Insert data
        if(token[0] == 'i'):
            dataset.append([int(token[1]), token[2], 1])
        elif(token[0] == 'r'):
            # Find the data with key K in the dataset
            data = [x for x in dataset if int(token[1]) in x][0]
            if(token[2] != data[1]):
                print("Read Key {}: Value Unmatched".format(token[1]))
        elif(token[0] == 'u'):
            idx = dataset.index([x for x in dataset if int(token[1]) in x][0])
            dataset[idx][1] = token[2]
        elif(token[0] == 'd'):
            idx = dataset.index([x for x in dataset if int(token[1]) in x][0])
            # 0 means deleted
            dataset[idx][2] = 0
        print(" #op: " + str(i), end="\r")
        i = i+1
    op_file.close()

# Remove the item in dataset with [2] == 0 (deleted)
print(" Deletion operation ")
rm_list = [x for x in dataset if x[2] == 0]
dataset = [x[0:2] for x in dataset if x not in rm_list]

# Sort the dataset
print(" Sorting ")
dataset.sort(key=lambda x: x[0])

# Write the dataset in to expected.dat 
print(" Export expected results ")
w_file = open("expected.dat", "w")
for x in dataset:
    w_file.write(str(x[0])+"\t"+x[1]+'\n')
w_file.close()

# diff with tree.dat
print(" Comparing ")
tree_file = open("tree.dat", "r")
idx = 0
while True:
    nstr = tree_file.readline()
    if len(nstr) == 0:
        break
    token = nstr.split('\t')
    # Compare data
    if(int(token[0]) != dataset[idx][0] and token[1] != dataset[idx][1]):
        print("Result: Key {}: Unmatched".format(token[0]))
        print("\tExpected Value : "+dataset[idx][1])
        print("\tTree Value : "+token[1])
    idx = idx + 1
tree_file.close()

