#!/usr/bin/python3
from re import I
import numpy as np
import matplotlib.pyplot as plt
import sys
import math

file_list = []
opr_name = ["Insert", "Delete", "Read", "Update"]
rm_cnt = 0

if len(sys.argv) > 1:
    state = 0
    for i in range(1, len(sys.argv)):
        if sys.argv[i] == "-r":
            state = 1
            continue

        if state == 0:
            file_list.append(sys.argv[i])
        elif state == 1:
            rm_cnt = int(sys.argv[i])
            if rm_cnt < 0:
                print("Error: #remove must be >= 0")
                sys.exit()
            break
else:
    print("Usage: {} input files ... [-r #remove]".format(sys.argv[0]))
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

# plot update
for i in range(len(perf)):
    dataset = perf[i]
    if(len(dataset) > 0):
        fig, ax = plt.subplots()

        for j in range(len(dataset)):
            x = np.arange(0, len(dataset[j]), 1)
            y = np.array(dataset[j])
            ax.scatter(x, y, label="("+str(j+1)+") "+file_list[j], s=0.3)

        ax.set(xlabel='op#', ylabel='response time (ms)', title=opr_name[i]+' Operation Response time')
        plt.title(opr_name[i])
        plt.figtext(0.01, 0.96, "Overral latency: ", horizontalalignment='left')
        for j in range(len(file_list)):
            plt.figtext(0.01, 0.96 - 0.03 * (j+1), "(" + str(j+1) + ") " + " - " + str(round(sum(dataset[j]),2)) + "ms", horizontalalignment='left')
        plt.figtext(0.70, 0.96, "Average latency: ", horizontalalignment='left')
        for j in range(len(file_list)):
            plt.figtext(0.70, 0.96 - 0.03 * (j+1), "(" + str(j+1) + ") " +  " - " + str(round(sum(dataset[j])/len(dataset[j]),2)) + "ms", horizontalalignment='left')
        plt.legend(loc='upper left')
        ax.grid

        fig.savefig(opr_name[i]+'.png')