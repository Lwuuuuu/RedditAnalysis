import happybase 
from collections import defaultdict
import praw
import sys
import time
from scrapReddit import browseReddit
from tqdm import tqdm
client_id = "NEpDdev8xeh__Q"
client_secret = "Qw0dKtMdN2QoQEwrGcINAdDASAo"
list_range = ["0", "1", "2", "3", "4", "5", "6", "7"]
class sub_dict():
    def __init__(self, name):
        self.name = name
        self.dict_list = [defaultdict(int) for i in range(8)] 
        self.bihourly_dif = []
def table_scanner(table_name):
    start_day = 1571457600-86400
    bi_hour = 7200
    time_list = [start_day + (i * bi_hour) for i in range(85)]
    conn = happybase.Connection('localhost', 9090)
    table = conn.table(table_name)
    total_upvotes = []
    for key, data in tqdm(table.scan()):
        subreddit = str(data[b'subreddit:subreddit'])[2:-1]
        timestamp = float(str(data[b'time:time'])[2:-1])
        upvotes = int(str(data[b'stats:upvotes'])[2:-1])
        foundSub = False
        for i in range(len(total_upvotes)):
            if subreddit == total_upvotes[i][0]:
                foundSub = True
                subreddit_location = i 
                break
        if foundSub == False:
            total_upvotes.append([subreddit, defaultdict(int)])
            subreddit_location = len(total_upvotes) - 1
        for j in range(len(time_list) - 1):
            if time_list[j+1] >= timestamp and timestamp > time_list[j]:
                total_upvotes[subreddit_location][1][(j+1) * 2] += upvotes
    return total_upvotes                
def extractRows(table_name):
    conn = happybase.Connection('localhost', 9090)
    rddit = browseReddit(client_id, client_secret, "USERAGENT", "bigDataProject", "asianswag")
    sub_list = rddit.get_subs().split('+')
    sub_list = [x.strip("+") for x in sub_list[:-1]]
    master_list = [sub_dict(sub) for sub in sub_list]
    submission_table = conn.table(table_name)
    for key, data in submission_table.scan():
        #locate day
        last_index = -4 if str(key)[-4] in list_range else -3
        for subreddit in master_list:
            if subreddit.name == str(key)[2:last_index]:
                hour = -2 if last_index == -3 else -3 
                subreddit.dict_list[int(str(key)[last_index])][str(key)[hour:-1]] = int(str(data[b'num:upvotes'])[2:-1])
                break
    if table_name == 'submission_upvotes':
        total_upvotes = table_scanner('submissions')
    else:
        total_upvotes = table_scanner('comments')
    #Subreddit List
    for i in range(len(master_list)):
        #7 day upvote list, skip the first day populated for sake of second day 
        for j in range(1, len(master_list[i].dict_list)):
            #Days upvote list (Hours List)
            for k in range(len(master_list[i].dict_list[j])):
                if k == 0:
                    current_hour = master_list[i].dict_list[j-1]["24"]
                    two_later = master_list[i].dict_list[j]["2"]
                    #Subtract j-1's upvotes from 0-2 from DB from current_hour
                    for l in range(len(total_upvotes)):
                        if total_upvotes[l][0] == master_list[i].name:
                            prev_day = total_upvotes[l][1][(j-1) * 24 + 2]
                            break
                    master_list[i].bihourly_dif.append(two_later - current_hour + prev_day)

                else:
                    current_hour = master_list[i].dict_list[j][str(k*2)]
                    two_later = master_list[i].dict_list[j][str((k+1)*2)]
                    #Subtract j-1's upvotes from (k+1)*2 - 2 to (k+1) * 2 hours from db 
                    for l in range(len(total_upvotes)):
                        if total_upvotes[l][0] == master_list[i].name:
                            prev_day = total_upvotes[l][1][((j-1) * 24) + ((k + 1) * 2)]
                            break
                    master_list[i].bihourly_dif.append(two_later - current_hour + prev_day)
                        
    for x in master_list:
        print(x.name, x.bihourly_dif)
if __name__ == "__main__":
    extractRows(str(sys.argv[1]))

