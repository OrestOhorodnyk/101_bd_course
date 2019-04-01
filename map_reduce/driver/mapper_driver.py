#!/usr/bin/env python3.6
import sys

# --- get all lines from stdin ---
for line in sys.stdin:

    # --- split the line into words ---
    logs = line.split('"')

    # --- output strings 'driver, 1' in comma-delimited format---
    if len(logs) == 7:
        print(f"{logs[5].split('/')[0].split('+')[0]}, 1")



