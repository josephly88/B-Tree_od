#!/usr/bin/python3
import numpy as np
import matplotlib.pyplot as plt
import sys

file_list = sys.argv[1:]

if len(sys.argv) == 1:
    sys.exit()

# Plot per workload
if len(sys.argv) == 2:
    opr_perf = [[] for i in range(2)]
    filename = sys.argv[1][:-4]

    cpw_opr = open("copy_on_write/" + sys.argv[1], "r")
    cmb_opr = open("cmb/" + sys.argv[1], "r")
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

    # plot
    x = np.arange(0, len(opr_perf[0]), 1)
    cpw = np.array(opr_perf[0])
    cmb = np.array(opr_perf[1])

    fig, ax = plt.subplots()
    ax.scatter(x, cpw, label="copy_on_write", s=1)
    ax.scatter(x, cmb, label="cmb", s=1)

    ax.set(xlabel='op#', ylabel='response time (ms)', title='Operation Response time')
    plt.title("Workload " + filename)
    plt.figtext(0.01, 0.96, "Overral latency: ", horizontalalignment='left')
    plt.figtext(0.01, 0.93, "Copy-on-write - " + str(round(sum(opr_perf[0]),2)) + "ms", horizontalalignment='left')
    plt.figtext(0.01, 0.90, "CMB - " + str(round(sum(opr_perf[1]), 2)) + "ms", horizontalalignment='left')
    plt.figtext(0.70, 0.96, "Average latency: ", horizontalalignment='left')
    plt.figtext(0.70, 0.93, "Copy-on-write - " + str(round(sum(opr_perf[0]) / len(opr_perf[0]),2)) + "ms", horizontalalignment='left')
    plt.figtext(0.70, 0.90, "CMB - " + str(round(sum(opr_perf[1]) / len(opr_perf[1]), 2)) + "ms", horizontalalignment='left')
    plt.legend(loc='upper left')
    ax.grid
    
    fig.savefig(filename + '.png')

#Plot per operation
else:
    insert_perf = [[] for i in range(2)]
    read_perf = [[] for i in range(2)]
    update_perf = [[] for i in range(2)]
    delete_perf = [[] for i in range(2)]

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
            if(token_cpr[0] == 'i'):
                insert_perf[0].append(float(token_cpr[3]))
                insert_perf[1].append(float(token_cmb[3]))
            elif(token_cpr[0] == 'r'):
                read_perf[0].append(float(token_cpr[3]))
                read_perf[1].append(float(token_cmb[3]))
            elif(token_cpr[0] == 'u'):
                update_perf[0].append(float(token_cpr[3]))
                update_perf[1].append(float(token_cmb[3]))
            elif(token_cpr[0] == 'd'):
                delete_perf[0].append(float(token_cpr[2]))
                delete_perf[1].append(float(token_cmb[2]))
        cpw_opr.close()
        cmb_opr.close()

    # plot insert
    x = np.arange(0, len(insert_perf[0]), 1)
    cpw = np.array(insert_perf[0])
    cmb = np.array(insert_perf[1])

    fig, ax = plt.subplots()
    ax.scatter(x, cpw, label="copy_on_write", s=1)
    ax.scatter(x, cmb, label="cmb", s=1)

    ax.set(xlabel='op#', ylabel='response time (ms)', title='Insertion Operation Response time')
    plt.title("Insertion")
    if len(insert_perf[0]) > 0:
        plt.figtext(0.01, 0.96, "Overral latency: ", horizontalalignment='left')
        plt.figtext(0.01, 0.93, "Copy-on-write - " + str(round(sum(insert_perf[0]),2)) + "ms", horizontalalignment='left')
        plt.figtext(0.01, 0.90, "CMB - " + str(round(sum(insert_perf[1]), 2)) + "ms", horizontalalignment='left')
        plt.figtext(0.70, 0.96, "Average latency: ", horizontalalignment='left')
        plt.figtext(0.70, 0.93, "Copy-on-write - " + str(round(sum(insert_perf[0])/len(insert_perf[0]),2)) + "ms", horizontalalignment='left')
        plt.figtext(0.70, 0.90, "CMB - " + str(round(sum(insert_perf[1])/len(insert_perf[1]), 2)) + "ms", horizontalalignment='left')
    plt.legend(loc='upper left')
    ax.grid

    fig.savefig('insert.png')

    # plot read
    x = np.arange(0, len(read_perf[0]), 1)
    cpw = np.array(read_perf[0])
    cmb = np.array(read_perf[1])

    fig, ax = plt.subplots()
    ax.scatter(x, cpw, label="copy_on_write", s=1)
    ax.scatter(x, cmb, label="cmb", s=1)

    ax.set(xlabel='op#', ylabel='response time (ms)', title='Read Operation Response time')
    plt.title("Read")
    if len(read_perf[0]) > 0:
        plt.figtext(0.01, 0.96, "Overral latency: ", horizontalalignment='left')
        plt.figtext(0.01, 0.93, "Copy-on-write - " + str(round(sum(read_perf[0]),2)) + "ms", horizontalalignment='left')
        plt.figtext(0.01, 0.90, "CMB - " + str(round(sum(read_perf[1]), 2)) + "ms", horizontalalignment='left')
        plt.figtext(0.70, 0.96, "Average latency: ", horizontalalignment='left')
        plt.figtext(0.70, 0.93, "Copy-on-write - " + str(round(sum(read_perf[0])/len(read_perf[0]),2)) + "ms", horizontalalignment='left')
        plt.figtext(0.70, 0.90, "CMB - " + str(round(sum(read_perf[1])/len(read_perf[1]), 2)) + "ms", horizontalalignment='left')
    plt.legend(loc='upper left')
    ax.grid

    fig.savefig('read.png')

    # plot update
    x = np.arange(0, len(update_perf[0]), 1)
    cpw = np.array(update_perf[0])
    cmb = np.array(update_perf[1])

    fig, ax = plt.subplots()
    ax.scatter(x, cpw, label="copy_on_write", s=1)
    ax.scatter(x, cmb, label="cmb", s=1)

    ax.set(xlabel='op#', ylabel='response time (ms)', title='Update Operation Response time')
    plt.title("Update")
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

    # plot delete
    x = np.arange(0, len(delete_perf[0]), 1)
    cpw = np.array(delete_perf[0])
    cmb = np.array(delete_perf[1])

    fig, ax = plt.subplots()
    ax.scatter(x, cpw, label="copy_on_write", s=1)
    ax.scatter(x, cmb, label="cmb", s=1)

    ax.set(xlabel='op#', ylabel='response time (ms)', title='Delete Operation Response time')
    plt.title("Deletion")
    if len(delete_perf[0]) > 0:
        plt.figtext(0.01, 0.96, "Overral latency: ", horizontalalignment='left')
        plt.figtext(0.01, 0.93, "Copy-on-write - " + str(round(sum(delete_perf[0]),2)) + "ms", horizontalalignment='left')
        plt.figtext(0.01, 0.90, "CMB - " + str(round(sum(delete_perf[1]), 2)) + "ms", horizontalalignment='left')
        plt.figtext(0.70, 0.96, "Average latency: ", horizontalalignment='left')
        plt.figtext(0.70, 0.93, "Copy-on-write - " + str(round(sum(delete_perf[0])/len(delete_perf[0]),2)) + "ms", horizontalalignment='left')
        plt.figtext(0.70, 0.90, "CMB - " + str(round(sum(delete_perf[1])/len(delete_perf[1]), 2)) + "ms", horizontalalignment='left')
    plt.legend(loc='upper left')
    ax.grid

    fig.savefig('delete.png')
