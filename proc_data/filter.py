#!/usr/bin/python3

import sys

op_file = open(sys.argv[1], 'r')
i = 1
while True:
    lstr = op_file.readline()
    if len(lstr) == 0:
        break
    token = lstr.split('\t')

    if(token[0] == 'i'):
        if(float(token[3]) > float(sys.argv[2])):
            print(str(i) + ":\t" + lstr, end = "")
    
    i = i + 1