a = ['ip1591,6269,1',
     'ip1592,109331,1',
     'ip1593,40027,1',
     'ip1594,28206,1',
     'ip1591,46293,1',
     'ip1593,105424,1',
     'ip1594,16688,1',
     'ip1597,6757,1']

def reduce_driver(line: str, ip2count: dict, ip2bytes: dict):
    # parse the input we got from map_ip.py
    ip, b, count = line.split(',')
    # convert count (currently a string) to int
    try:
        count = int(count)
        b = int(b)
    except ValueError:
        pass

    try:
        ip2count[ip] = ip2count[ip] + count
        ip2bytes[ip] = ip2bytes[ip] + b
    except:
        ip2count[ip] = count
        ip2bytes[ip] = b

ip2count = {}
ip2bytes = {}
for line in a:
    reduce_ip(line,ip2count, ip2bytes)

for ip in ip2count.keys():
    print(f'{ip},{round(ip2bytes[ip]/ip2count[ip],2)},{ip2bytes[ip]}')






#!/usr/bin/python3.6
import sys

# maps ip's to their counts
driver2count = {}

a = ['Mozilla, 1',
     'Mozilla, 1',
     'Mozilla, 1',
     'ichiro, 1',
     'ichiro, 1',
     'Opera, 1',
     'Opera, 1',
     'Opera, 1',
     'Opera, 1',
     'Opera, 1',
     'Opera, 1',
     'Opera, 1',
     'Opera, 1',
     'Opera, 1',
     'ichiro, 1',
     'ichiro, 1',
     'ichiro, 1']

# input comes from STDIN
for line in a:
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
