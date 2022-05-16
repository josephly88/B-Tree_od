#!/usr/bin/python3
import numpy as np
import matplotlib.pyplot as plt
import sys
import math

percent = 100.0
dir_list = ["copy_on_write", "cmb", "dram"]
file_list = ["A.dat", "B.dat", "C.dat", "D.dat", "F.dat", "insert.dat", "delete.dat"]

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
        for j in range(len(file_list)):
            opr_perf[j].remove(max(opr_perf[j]))

    # plot
    x = np.arange(0, len(opr_perf[0]), 1)
    y = [[] for i in range(len(dir_list))]
    for i in range(len(dir_list)):
        y[i] = np.array(opr_perf[i])

    fig, ax = plt.subplots()
    for i in range(len(dir_list)):
        ax.scatter(x, y[i], label=dir_list[i], s=0.7)

    ax.set(xlabel='op#', ylabel='response time (ms)', title='Operation Response time')
    if FILE[0] == "A" or FILE[0] == "B" or FILE[0] == "C" or FILE[0] == "D" or FILE[0] == "F":
        plt.title("Workload " + FILE[0] + " - " + str(percent) + "%")
    else:
        plt.title(FILE[:-4] + " - " + str(percent) + "%")
    plt.figtext(0.01, 0.96, "Overral latency: ", horizontalalignment='left')
    plt.figtext(0.01, 0.93, "Copy-on-write - " + str(round(sum(opr_perf[0]),2)) + "ms", horizontalalignment='left')
    plt.figtext(0.01, 0.90, "CMB - " + str(round(sum(opr_perf[1]), 2)) + "ms", horizontalalignment='left')
    plt.figtext(0.70, 0.96, "Average latency: ", horizontalalignment='left')
    plt.figtext(0.70, 0.93, "Copy-on-write - " + str(round(sum(opr_perf[0]) / len(opr_perf[0]),2)) + "ms", horizontalalignment='left')
    plt.figtext(0.70, 0.90, "CMB - " + str(round(sum(opr_perf[1]) / len(opr_perf[1]), 2)) + "ms", horizontalalignment='left')
    plt.legend(loc='upper left')
    ax.grid
    
    fig.savefig(filename + '.png')

#Plot Update
update_perf = [[] for i in range(len(dir_list))]
file_list = ["A.dat", "B.dat", "F.dat"]

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

        # Insert data
        if(token[0][0] == 'u'):
            for i in range(len(dir_list)):
                update_perf[i].append(float(token[i][3]))

    for i in range(len(dir_list)):
        file_opr[i].close()

rm_cnt = math.floor((100 - percent) / 100 * len(update_perf[0]))
for i in range(rm_cnt):
    for j in range(2):
        update_perf[j].remove(max(update_perf[j]))

# plot update
x = np.arange(0, len(update_perf[0]), 1)
y = [[] for i in range(len(dir_list))]
for i in range(len(dir_list)):
    y[i] = np.array(update_perf[i])

fig, ax = plt.subplots()
for i in range(len(dir_list)):
    ax.scatter(x, y[i], label=dir_list[i], s=0.7)

ax.set(xlabel='op#', ylabel='response time (ms)', title='Update Operation Response time')
plt.title("update - " + str(percent) + "%")
if len(update_perf[0]) > 0:
    plt.figtext(0.01, 0.96, "Overral latency: ", horizontalalignment='left')
    plt.figtext(0.01, 0.93, "Copy-on-write - " + str(round(sum(update_perf[0]),2)) + "ms", horizontalalignment='left')
    plt.figtext(0.01, 0.90, "CMB - " + str(round(sum(update_perf[1]), 2)) + "ms", horizontalalignment='left')
    plt.figtext(0.70, 0.96, "Average latency: ", horizontalalignment='left')
    plt.figtext(0.70, 0.93, "Copy-on-write - " + str(round(sum(update_perf[0])/len(update_perf[0]),2)) + "ms", horizontalalignment='left')
    plt.figtext(0.70, 0.90, "CMB - " + str(round(sum(update_perf[1])/len(update_perf[1]), 2)) + "ms", horizontalalignment='left')
plt.legend(loc='upper left')
ax.grid

fig.savefig('update.png')
