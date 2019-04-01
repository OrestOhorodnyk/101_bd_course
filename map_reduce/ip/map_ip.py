#!/usr/bin/env python3.6
import sys

# --- get all lines from stdin ---
for line in sys.stdin:
    # --- split the line into words ---
    logs = line.split('"')
    # --- output strings 'ip, average bytes, total bytes' in comma-delimited format---
    if len(logs) == 7:
        print(f'{logs[0].split()[0]},{logs[2].split()[1]},1')
