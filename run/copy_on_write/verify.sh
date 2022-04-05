#!/bin/bash

sort -t, -k1 -n inter.dat > expect.dat
rm inter.dat
diff expect.dat out.dat