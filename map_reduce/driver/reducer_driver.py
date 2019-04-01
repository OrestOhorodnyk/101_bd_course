#!/usr/bin/python3.6
import sys

# maps ip's to their counts
driver2count = {}

# input comes from STDIN
for line in sys.stdin:
    # split to ip and butas
    line = line.strip(',')

    # parse the input we got from map_ip.py
    driver, count = line.split(',', 1)
    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        continue

    try:
        driver2count[driver] = driver2count[driver] + count
    except:
        driver2count[driver] = count

    # write the tuples to stdout
    # Note: they are unsorted
for driver in driver2count.keys():
    print(f'{driver},{driver2count[driver]}')
