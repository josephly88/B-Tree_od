#!/usr/bin/python3

import sys
import math

def binary_search(arr, val, min, max):
    if min >= max:
        if len(arr) == 0:
            return 0
        elif max == len(arr):
            return len(arr)
        elif val == str(arr[min][0]):
            return min
        elif len(val) < len(str(arr[min][0])):
            return min
        elif len(val) == len(str(arr[min][0])) and val < str(arr[min][0]):
            return min
        else:
            return min+1

    mid = math.floor((min + max) / 2)

    if val == str(arr[mid][0]):
        return mid
    elif len(val) > len(str(arr[mid][0])):
        return binary_search(arr, val, mid+1, max)
    elif len(val) < len(str(arr[mid][0])):
        return binary_search(arr, val, min, mid-1)
    else:
        if val > str(arr[mid][0]):
            return binary_search(arr, val, mid+1, max)
        else:
            return binary_search(arr, val, min, mid-1)

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
            idx = binary_search(dataset, token[1], 0, len(dataset))
            dataset.insert(idx, [int(token[1]), token[2], 1])
        elif(token[0] == 'r'):
            # Find the data with key K in the dataset
            idx = binary_search(dataset, token[1], 0, len(dataset))
            data = dataset[idx]
            if(token[2] != data[1]):
                print("Read Key {}: Value Unmatched".format(token[1]))
        elif(token[0] == 'u'):
            idx = binary_search(dataset, token[1], 0, len(dataset))
            dataset[idx][1] = token[2]
        elif(token[0] == 'd'):
            idx = binary_search(dataset, token[1], 0, len(dataset))
            # 0 means deleted
            dataset[idx][2] = 0
        print(" #op: " + str(i), end="\r")
        i = i+1
    op_file.close()

# Remove the item in dataset with [2] == 0 (deleted)
print(" Deletion operation ")
new_dataset = []
for x in dataset:
    if x[2] != 0:
        new_dataset.append(x[0:2])
dataset = new_dataset

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

