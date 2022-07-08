#!/usr/bin/python3
from re import I
import numpy as np
import matplotlib.pyplot as plt
import sys
import math

file_list = []
opr_name = ["Insert", "Delete", "Update", "Read"]
rm_cnt = 0
mark_split_file = ""
mark_merge_file = ""

# Processing cmd arg
if len(sys.argv) > 1:
    state = 0
    for i in range(1, len(sys.argv)):
        # Number of removal
        if sys.argv[i] == "-r":
            state = 1
            continue
        # Mark special OP# file, s - split, m - merge
        if sys.argv[i] == "-s":
            state = 2
            continue
        if sys.argv[i] == "-m":
            state = 3
            continue

        if state == 0:
            file_list.append(sys.argv[i])
        elif state == 1:
            rm_cnt = int(sys.argv[i])
            if rm_cnt < 0:
                print("Error: #remove must be >= 0")
                sys.exit()
            state = 0
        elif state == 2:
            mark_split_file = sys.argv[i]
            state = 0
        elif state == 3:
            mark_merge_file = sys.argv[i]
            state = 0

else:
    print("Usage: {} input files ... [-r #remove] [-m mark OP# file]".format(sys.argv[0]))
    sys.exit()


perf = [[] for i in range(len(opr_name))]

for FILE in file_list:
    # read opr.dat
    file_opr = open(FILE, "r")

    i_perf = []
    d_perf = []
    r_perf = []
    u_perf = []

    while True:
        nstr = file_opr.readline()
        # read until EOF
        if len(nstr) == 0:
            break

        token = nstr.split('\t')

        # Operation data
        if(token[0][0] == 'i'):
            i_perf.append(float(token[3]))
        elif(token[0][0] == 'd'):
            d_perf.append(float(token[2]))
        elif(token[0][0] == 'u'):
            u_perf.append(float(token[3]))
        elif(token[0][0] == 'r'):
            r_perf.append(float(token[3]))

    if(len(i_perf) > 0):
        perf[0].append(i_perf)
    if(len(d_perf) > 0):
        perf[1].append(d_perf)
    if(len(u_perf) > 0):
        perf[2].append(u_perf)
    if(len(r_perf) > 0):
        perf[3].append(r_perf)

    file_opr.close()

# Create marked #OP list
mark_split_list = [[[] for j in range(2)] for i in range(len(file_list))]
if(len(perf[0]) > 0 and mark_split_file != ""):
    mfile = open(mark_split_file)
    while True:
        nstr = mfile.readline()
        # read until EOF
        if len(nstr) == 0:
            break

        token = nstr.split('\t')

        for i in range(len(file_list)):
            mark_split_list[i][0].append(int(token[0]))
            mark_split_list[i][1].append(perf[0][i][int(token[0])])
    mfile.close()

    for i in range(len(file_list)):
        print(file_list[i] + ": " + str(sum(mark_split_list[i][1])))

mark_merge_list = [[[] for j in range(2)] for i in range(len(file_list))]
if(len(perf[1]) > 0 and mark_merge_file != ""):
    mfile = open(mark_merge_file)
    while True:
        nstr = mfile.readline()
        # read until EOF
        if len(nstr) == 0:
            break

        token = nstr.split('\t')

        for i in range(len(file_list)):
            mark_merge_list[i][0].append(int(token[0]))
            mark_merge_list[i][1].append(perf[1][i][int(token[0])])
    mfile.close()

    for i in range(len(file_list)):
        print(file_list[i] + ": " + str(sum(mark_merge_list[i][1])))

# Remove the highest data
for opr in perf:
    if(len(opr) > 0):
        if(rm_cnt >= min([len(x) for x in opr])):
            print("Error: Number of remove >= data size")
            print("Number of remove = " + str(rm_cnt))
            print("Data size = " + str(min([len(x) for x in opr])))
            sys.exit()
        for i in range(rm_cnt):
            for j in opr:
                j.remove(max(j))

# plot data
for i in range(len(perf)):
    dataset = perf[i]
    if(len(dataset) > 0):
        fig, ax = plt.subplots()

        for j in range(len(dataset)):
            x = np.arange(0, len(dataset[j]), 1)
            y = np.array(dataset[j])
            ax.scatter(x, y, label="("+str(j+1)+") "+file_list[j], s=0.3, marker='o')

        # Insert
        if(i == 0 and mark_split_file != ""):
            for j in range(len(dataset)):
                x = np.array(mark_split_list[j][0])
                y = np.array(mark_split_list[j][1])
                ax.scatter(x, y, label=file_list[j]+" - Split", s=0.3, marker='x')

        if(i == 1 and mark_merge_file != ""):
            for j in range(len(dataset)):
                x = np.array(mark_merge_list[j][0])
                y = np.array(mark_merge_list[j][1])
                ax.scatter(x, y, label=file_list[j]+" - Split", s=0.3, marker='x')

        ax.set(xlabel='op#', ylabel='response time (us)', title=opr_name[i]+' Operation Response time')
        plt.title(opr_name[i])
        plt.figtext(0.01, 0.96, "Overral latency: ", horizontalalignment='left')
        for j in range(len(file_list)):
            plt.figtext(0.01, 0.96 - 0.03 * (j+1), "(" + str(j+1) + ") " + " - " + str(round(sum(dataset[j]),2)) + "us", horizontalalignment='left')
        plt.figtext(0.70, 0.96, "Average latency: ", horizontalalignment='left')
        for j in range(len(file_list)):
            plt.figtext(0.70, 0.96 - 0.03 * (j+1), "(" + str(j+1) + ") " +  " - " + str(round(sum(dataset[j])/len(dataset[j]),2)) + "us", horizontalalignment='left')
        plt.legend(loc='upper left')
        ax.grid

        fig.savefig(opr_name[i]+'.png')