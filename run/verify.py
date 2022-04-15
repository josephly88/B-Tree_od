#!/usr/bin/python3

dataset = []

op_file = open("opr.dat", "r")
while True:
    nstr = op_file.readline()
    # read until EOF
    if len(nstr) == 0:
        break
    token = nstr.split('\t')
    # Insert data
    if(token[0] == 'i'):
        dataset.append([int(token[1]), token[2]])
    elif(token[0] == 'r'):
        # Find the data with key K in the dataset
        data = [x for x in dataset if int(token[1]) in x][0]
        if(token[2] != data[1]):
            print("Key {}: Value Unmatched".format(token[1]))
    elif(token[0] == 'u'):
        idx = dataset.index([x for x in dataset if int(token[1]) in x][0])
        dataset[idx][1] = token[2]


dataset.sort(key=lambda x: x[0])

w_file = open("expected.dat", "w")
for x in dataset:
    w_file.write(str(x[0])+"\t"+x[1]+'\n')
w_file.close()

op_file.close()