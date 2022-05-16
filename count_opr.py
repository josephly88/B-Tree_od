#!/usr/bin/python3
import sys

FILE = open(sys.argv[1], "r")

i = 0
r = 0
u = 0

while True:
    nstr = FILE.readline()
    if len(nstr) == 0:
        break
    
    if nstr[0] == 'i':
        i = i + 1
    elif nstr[0] == 'u':
        u = u + 1
    elif nstr[0] == 'r':
        r = r + 1
    else:
        continue

print(" i: " + str(i) + " (" + str(round(i/(i+r+u)*100,2)) + "%)")
print(" r: " + str(r) + " (" + str(round(r/(i+r+u)*100,2)) + "%)")
print(" u: " + str(u) + " (" + str(round(u/(i+r+u)*100,2)) + "%)")

FILE.close()
