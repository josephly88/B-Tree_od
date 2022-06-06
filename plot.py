#!/usr/bin/python3
from re import I
import numpy as np
import matplotlib.pyplot as plt
import sys
import math

num = "_5M"

percent = 100.0
dir_list = ["copy_on_write", "cmb"]
#file_list = ["insert"+num+".dat", "delete"+num+".dat"]
file_list = ["insert"+num+".dat"]

if len(sys.argv) == 2:
    percent = float(sys.argv[1])
else:
    print("Usage: ./program {Percentage}")
    sys.exit()

# Plot per workload
for FILE in file_list:
    opr_perf = [[] for i in range(len(dir_list))]
    filename = FILE[:-4]

    file_opr = [None] * len(dir_list)
    for i in range(len(dir_list)):
        file_opr[i] = open(dir_list[i] + "/" + FILE, "r")
    
    while True:
        nstr = [None] * len(dir_list)
        for i in range(len(dir_list)):
            nstr[i] = file_opr[i].readline()
        # read until EOF
        if len(nstr[0]) == 0:
            break

        token = [None] * len(dir_list)
        for i in range(len(dir_list)):
            token[i] = nstr[i].split('\t')

        if(token[0][0] == 'd'):
            for i in range(len(dir_list)):
                opr_perf[i].append(float(token[i][2]))
        else:
            for i in range(len(dir_list)):
                opr_perf[i].append(float(token[i][3]))

    for i in range(len(dir_list)):
        file_opr[i].close()

    rm_cnt = math.floor((100 - percent) / 100 * len(opr_perf[0]))
    for i in range(rm_cnt):
        for j in range(len(dir_list)):
            opr_perf[j].remove(max(opr_perf[j]))
        print("{:.2f}%".format(i / rm_cnt * 100), end="\r")

    # plot
    x = np.arange(0, len(opr_perf[0]), 1)
    y = [[] for i in range(len(dir_list))]
    for i in range(len(dir_list)):
        y[i] = np.array(opr_perf[i])

    fig, ax = plt.subplots()
    for i in range(len(dir_list)):
        ax.scatter(x, y[i], label=dir_list[i], s=0.5)

    ax.set(xlabel='op#', ylabel='response time (ms)', title='Operation Response time')
    if FILE[0] == "A" or FILE[0] == "B" or FILE[0] == "C" or FILE[0] == "D" or FILE[0] == "F":
        plt.title("Workload " + FILE[0] + " - " + str(percent) + "%")
    else:
        plt.title(FILE[:-4] + " - " + str(percent) + "%")
    plt.figtext(0.01, 0.96, "Overral latency: ", horizontalalignment='left')
    for i in range(len(dir_list)):
        plt.figtext(0.01, 0.96 - 0.03 * (i+1), dir_list[i] + " - " + str(round(sum(opr_perf[i]),2)) + "ms", horizontalalignment='left')
    plt.figtext(0.70, 0.96, "Average latency: ", horizontalalignment='left')
    for i in range(len(dir_list)):
        plt.figtext(0.70, 0.96 - 0.03 * (i+1), dir_list[i] + " - " + str(round(sum(opr_perf[i])/len(opr_perf[i]),2)) + "ms", horizontalalignment='left')
    plt.legend(loc='upper left')
    ax.grid
    
    fig.savefig(filename + '.png')

sys.exit()

#Plot Read and Update
update_perf = [[] for i in range(len(dir_list))]
read_perf = [[] for i in range(len(dir_list))]
file_list = ["read_and_update"+num+".dat"]

for FILE in file_list:
    # read opr.dat
    file_opr = [None] * len(dir_list)
    for i in range(len(dir_list)):
        file_opr[i] = open(dir_list[i] + "/" + FILE, "r")

    while True:
        nstr = [None] * len(dir_list)
        for i in range(len(dir_list)):
            nstr[i] = file_opr[i].readline()
        # read until EOF
        if len(nstr[0]) == 0:
            break

        token = [None] * len(dir_list)
        for i in range(len(dir_list)):
            token[i] = nstr[i].split('\t')

        # Update data
        if(token[0][0] == 'u'):
            for i in range(len(dir_list)):
                update_perf[i].append(float(token[i][3]))

        if(token[0][0] == 'r'):
            for i in range(len(dir_list)):
                read_perf[i].append(float(token[i][3]))

    for i in range(len(dir_list)):
        file_opr[i].close()

# Remove certain percentage
rm_cnt = math.floor((100 - percent) / 100 * len(update_perf[0]))
for i in range(rm_cnt):
    for j in range(len(dir_list)):
        update_perf[j].remove(max(update_perf[j]))

# plot update
x = np.arange(0, len(update_perf[0]), 1)
y = [[] for i in range(len(dir_list))]
for i in range(len(dir_list)):
    y[i] = np.array(update_perf[i])

fig, ax = plt.subplots()
for i in range(len(dir_list)):
    ax.scatter(x, y[i], label=dir_list[i], s=0.5)

ax.set(xlabel='op#', ylabel='response time (ms)', title='Update Operation Response time')
plt.title("update - " + str(percent) + "%")
if len(update_perf[0]) > 0:
    plt.figtext(0.01, 0.96, "Overral latency: ", horizontalalignment='left')
    for i in range(len(dir_list)):
        plt.figtext(0.01, 0.96 - 0.03 * (i+1), dir_list[i] + " - " + str(round(sum(opr_perf[i]),2)) + "ms", horizontalalignment='left')
    plt.figtext(0.70, 0.96, "Average latency: ", horizontalalignment='left')
    for i in range(len(dir_list)):
        plt.figtext(0.70, 0.96 - 0.03 * (i+1), dir_list[i] + " - " + str(round(sum(opr_perf[i])/len(opr_perf[i]),2)) + "ms", horizontalalignment='left')
plt.legend(loc='upper left')
ax.grid

fig.savefig('update'+num+'.png')

# Remove certain percentage
rm_cnt = math.floor((100 - percent) / 100 * len(read_perf[0]))
for i in range(rm_cnt):
    for j in range(len(dir_list)):
        read_perf[j].remove(max(read_perf[j]))

# plot read
x = np.arange(0, len(read_perf[0]), 1)
y = [[] for i in range(len(dir_list))]
for i in range(len(dir_list)):
    y[i] = np.array(read_perf[i])

fig, ax = plt.subplots()
for i in range(len(dir_list)):
    ax.scatter(x, y[i], label=dir_list[i], s=0.5)

ax.set(xlabel='op#', ylabel='response time (ms)', title='Read Operation Response time')
plt.title("read - " + str(percent) + "%")
if len(read_perf[0]) > 0:
    plt.figtext(0.01, 0.96, "Overral latency: ", horizontalalignment='left')
    for i in range(len(dir_list)):
        plt.figtext(0.01, 0.96 - 0.03 * (i+1), dir_list[i] + " - " + str(round(sum(opr_perf[i]),2)) + "ms", horizontalalignment='left')
    plt.figtext(0.70, 0.96, "Average latency: ", horizontalalignment='left')
    for i in range(len(dir_list)):
        plt.figtext(0.70, 0.96 - 0.03 * (i+1), dir_list[i] + " - " + str(round(sum(opr_perf[i])/len(opr_perf[i]),2)) + "ms", horizontalalignment='left')
plt.legend(loc='upper left')
ax.grid

fig.savefig('read'+num+'.png')
