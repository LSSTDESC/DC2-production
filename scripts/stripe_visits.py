#!/usr/bin/env python

"""
Take a list of visits and divided them up into num_files
by striping through the visit list.  E.g., a file with a list from 1-55
with num_files = 10
would get divided up as
1 11 21 31 41 51
2 12 22 32 42 52
3 13 23 33 43 53
4 14 24 34 44 54
5 15 25 55 45 55
6 16 26 66 46
7 17 27 77 47
8 18 28 88 48
9 19 29 99 49
10 20 30 40 50
"""

import sys
import os

visit_file = sys.argv[1]
num_files = int(sys.argv[2])

with open(visit_file, 'r') as f:
    visits = f.readlines()

basename, ext = os.path.splitext(visit_file)

lines_per_file = (len(visits) // num_files) + 1

for i in range(num_files):
    these_visits = visits[i::num_files]
    new_name = '{}_{}{}'.format(basename, i, ext)
    with open(new_name, 'w') as of:
        of.writelines(these_visits)
