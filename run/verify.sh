#!/bin/bash

cat inter.dat | sed -r 's/^.//' >> inter_v2.dat
sort -t, -k1 -n inter_v2.dat > expect.dat
rm inter*
diff expect.dat tree.dat
