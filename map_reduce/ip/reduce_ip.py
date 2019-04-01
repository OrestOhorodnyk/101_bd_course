#!/usr/bin/python3.6
import sys

# maps ip's to their counts
ip2count = {}
ip2bytes = {}

# input comes from STDIN
for line in sys.stdin:
    # split to ip and butas
    line = line.strip(',')

    # parse the input we got from map_ip.py
    ip, b, count = line.split(',')
    # convert count (currently a string) to int
    try:
        count = int(count)
        b = int(b)
    except ValueError:
        continue

    try:
        ip2count[ip] = ip2count[ip] + count
        ip2bytes[ip] = ip2bytes[ip] + b
    except:
        ip2count[ip] = count
        ip2bytes[ip] = b

# write the tuples to stdout
# Note: they are unsorted
for ip in ip2count.keys():
    print(f'{ip},{ip2bytes[ip]/ip2count[ip]},{ip2bytes[ip]}')
