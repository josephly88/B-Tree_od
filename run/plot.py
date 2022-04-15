#!/usr/bin/python3
import numpy as np
import matplotlib.pyplot as plt

insert_perf = [[] for i in range(2)]
read_perf = [[] for i in range(2)]
update_perf = [[] for i in range(2)]
delete_perf = [[] for i in range(2)]

# read opr.dat
cpw_opr = open("copy_on_write/opr.dat", "r")
cmb_opr = open("cmb/opr.dat", "r")
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
plt.legend(loc='upper left')
ax.grid

fig.savefig('delete.png')
