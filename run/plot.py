#!/usr/bin/python3
import numpy as np
import matplotlib.pyplot as plt
import sys
import math

percent = 100.0
file_list = ["A.dat", "B.dat", "C.dat", "D.dat", "F.dat", "insert.dat", "delete.dat"]

if len(sys.argv) == 2:
    percent = float(sys.argv[1])
else:
    print("Usage: ./program (-p=NUMBER) {file ...}")
    sys.exit()

# Plot per workload
for FILE in file_list:
    opr_perf = [[] for i in range(2)]
    filename = FILE[:-4]

    cpw_opr = open("copy_on_write/" + FILE, "r")
    cmb_opr = open("cmb/" + FILE, "r")
    while True:
        nstr_cpr = cpw_opr.readline()
        nstr_cmb = cmb_opr.readline()
        # read until EOF
        if len(nstr_cpr) == 0:
            break
        token_cpr = nstr_cpr.split('\t')
        token_cmb = nstr_cmb.split('\t')

        if(token_cpr[0] == 'd'):
            opr_perf[0].append(float(token_cpr[2]))
            opr_perf[1].append(float(token_cmb[2]))
        else:
            opr_perf[0].append(float(token_cpr[3]))
            opr_perf[1].append(float(token_cmb[3]))

    cpw_opr.close()
    cmb_opr.close()

    rm_cnt = math.floor((100 - percent) / 100 * len(opr_perf[0]))
    for i in range(rm_cnt):
        for j in range(2):
            opr_perf[j].remove(max(opr_perf[j]))

    # plot
    x = np.arange(0, len(opr_perf[0]), 1)
    cpw = np.array(opr_perf[0])
    cmb = np.array(opr_perf[1])

    fig, ax = plt.subplots()
    ax.scatter(x, cpw, label="copy_on_write", s=1)
    ax.scatter(x, cmb, label="cmb", s=1)

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
update_perf = [[] for i in range(2)]
file_list = ["A.dat", "B.dat", "F.dat"]

for FILE in file_list:
    # read opr.dat
    cpw_opr = open("copy_on_write/" + FILE, "r")
    cmb_opr = open("cmb/" + FILE, "r")
    while True:
        nstr_cpr = cpw_opr.readline()
        nstr_cmb = cmb_opr.readline()
        # read until EOF
        if len(nstr_cpr) == 0:
            break
        token_cpr = nstr_cpr.split('\t')
        token_cmb = nstr_cmb.split('\t')
        # Insert data
        if(token_cpr[0] == 'u'):
            update_perf[0].append(float(token_cpr[3]))
            update_perf[1].append(float(token_cmb[3]))
    cpw_opr.close()
    cmb_opr.close()

rm_cnt = math.floor((100 - percent) / 100 * len(update_perf[0]))
for i in range(rm_cnt):
    for j in range(2):
        update_perf[j].remove(max(update_perf[j]))

# plot update
x = np.arange(0, len(update_perf[0]), 1)
cpw = np.array(update_perf[0])
cmb = np.array(update_perf[1])

fig, ax = plt.subplots()
ax.scatter(x, cpw, label="copy_on_write", s=1)
ax.scatter(x, cmb, label="cmb", s=1)

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
