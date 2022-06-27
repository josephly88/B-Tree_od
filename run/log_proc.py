#!/usr/bin/python3

import sys

log_file = open(sys.argv[1], "r")

cur_op = -1
split_cnt = 0
while(True):
    nstr = log_file.readline()
    # read until EOF
    if len(nstr) == 0:
        break
    token = nstr.split(' ')
    if(token[0][0:3] == "OP#"):
        if(split_cnt > 0):
            print("{}\t{}".format(cur_op, split_cnt))
        cur_op = int(token[0][3:])
        split_cnt = 0
        continue
    else:
        if(token[0] == (sys.argv[2] + "()")):
            split_cnt += 1
            continue