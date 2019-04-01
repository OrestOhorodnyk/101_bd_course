import os

print(os.getcwd())

filepath = f'{os.getcwd()}/access_logs/input/000000'
with open(filepath) as fp:
    line = fp.readline()
    cnt = 1
    while line:
        # print("Line {}: {}".format(cnt, line.strip()))
        line = fp.readline()
        cnt += 1
        logs = line.split('"')
        if len(logs) == 7:
            print(f"{logs[5].split('/')[0].split('+')[0]},1")
        # for i,v in enumerate(logs):
        #     print(f'{i}, {v[]}')
#