#!/usr/bin/python3
import happybase
import fileinput
import time
import sys
from collections import defaultdict
def map(table_name):
    conn = happybase.Connection('localhost', 9090)
    table = conn.table(table_name)
    sub_dict = defaultdict(list)
    for key, data in table.scan():
        subreddit = str(data[b'subreddit:subreddit'])[2:-1] 
        if subreddit == 'announcements':
            continue 
        timestamp = str(data[b'time:time'])[2:-1]
        sub_dict[subreddit].append((subreddit, float(timestamp)))

    for key in sub_dict:
        #sub_dict[key].sort()
        for item in sub_dict[key]:
           print(item[0], item[1])
if __name__ == "__main__":
    map(str(sys.argv[1]))
