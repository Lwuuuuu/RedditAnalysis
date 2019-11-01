#!/usr/bin/python3

import time
import sys 
from datetime import datetime 
from collections import defaultdict
start_day = 1571457600
bi_hour = 7200
time_list = [start_day + (i * bi_hour) for i in range(85)]
def reduce(table_name):
    output_dict = defaultdict(dict)
    for line in sys.stdin:
        subreddit, timestamp = line.strip('\n ').split(' ')
        for i in range(len(time_list) - 1):
            #Find in which time range post submitted
            if time_list[i+1] >= float(timestamp) and float(timestamp) > time_list[i]:
                try:
                    output_dict[subreddit][(i+1) * 2] += 1
                except KeyError:
                    output_dict[subreddit][(i+1) * 2] = 1
                break
    #output_dict all subreddits
    for d in output_dict:
        #d will be each subreddit dict, timestamp is the bihour 
        for timestamp in output_dict[d]:
            print("Subreddit {0} has gotten {1} {2} between {3} and {4}".format(d, table_name, output_dict[d][timestamp], int(timestamp) - 2, timestamp))

if __name__ == "__main__":
    reduce(str(sys.argv[1]))
