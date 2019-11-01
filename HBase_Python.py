from tqdm import tqdm
import random
import time
import os
import sys
from multiprocessing.pool import ThreadPool
import happybase 
from collections import defaultdict
from datetime import datetime
client_id = "NEpDdev8xeh__Q"
client_secret = "Qw0dKtMdN2QoQEwrGcINAdDASAo"
class Connector():
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connection = happybase.Connection(self.host, self.port)
    def insertSubmissions(self, submission_id, subreddit, upvotes, timestamp, body):
        connection = happybase.Connection(self.host, self.port)
        table = connection.table('submissions')
        table.put(str(submission_id), {'stats:upvotes' : str(upvotes), 'stats:comment' : str(body), 
                                     'subreddit:subreddit' : str(subreddit), 
                                     'time:time' : str(timestamp)})

    def insertComments(self, comment_id, subreddit, upvotes, timestamp, body): 
        connection = happybase.Connection(self.host, self.port)
        table = connection.table('comments')
        table.put(str(comment_id), {'stats:upvotes' :  str(upvotes),
                               'stats:comment' : str(body),
                               'subreddit:subreddit' : str(subreddit),
                               'time:time' : str(timestamp)}) 

    def scanTable(self, table_name):
        start_in_seconds, beginning_hour = get_start_time()
        for i in range(8):
            current_day_in_seconds = 1571371200 + i * 86400
            iterations = 13
            k = int(beginning_hour) // 2 if i == 0 else 1
            if i == 0 and k == 0:
                print("Script started before 2, waiting till 2 to begin")
                wait_next_hour(k, current_day_in_seconds)
                k += 1 
            for j in range(k, iterations):
                conn = happybase.Connection(self.host, self.port) 
                table = conn.table(table_name)
                #KEY IS ID, VALUE IS THE TIMESTAMP
                retries = 0 
                while 1:
                    if retries == 4:
                        print("Error occurred connecting to HBase")
                        return 
                    try:
                        table_dict = {} 
                        for key,data in tqdm(table.scan()):
                            table_dict[str(key)[2:-1]] = [data[b'time:time']]
                    except:
                        retries += 1 
                        continue
                    break                        
                print("Started iteration on Day", i, "Hour", j*2)
                #CHANGE LAST_HOUR TO ACTIVATE ON THE 12TH HOUR DURING THE RERUN if j == iterations//2
                last_hour = True if j == iterations - 1 else False
                self.relookReddit(table_dict, j, current_day_in_seconds, table_name, i, last_hour) 
                if i == 0 and j == 0:
                    print("Script Started, waiting for next hour")
                else:
                    print("Completed Iteration on Day", i, "Hour", j*2, "at exactly", datetime.today())
                wait_next_hour(j, current_day_in_seconds)

    def relookReddit(self, table_dict, current_hour, current_day_in_seconds, table_name, current_day_integer, last_hour):     
        from scrapReddit import browseReddit 
        relook = browseReddit(client_id, client_secret, "USERAGENT")
        if table_name == 'comments':
            def split_relook(dic):
                comment_dict = defaultdict(int)
                comment_upvotes = defaultdict(int)
                for key in tqdm(dic):
                    #if int(float(str(dic[key][0])[2:-1])) >= time.time() - 86400:
                    if int(float(str(table_dict[key][0])[2:-1])) >= time.time() - 86400:
                        comment = relook.reddit.comment(key)
                        try:
                            comment_dict[comment.subreddit] += int(comment.score)
                            if last_hour:
                                comment_upvotes[key] = comment.score    
                        except:
                            pass
                #comment_dict is for the upvote table, comment_upvotes for updating 'comments' last hour
                return comment_dict, comment_upvotes
            all_keys = list(table_dict.keys())
            random.shuffle(all_keys)
            d1 = all_keys[len(all_keys) // 2:]
            d2 = all_keys[:len(all_keys) // 2]
            d1_1 = d1[len(d1)//2:]
            d1_2 = d1[:len(d1)//2]
            d2_1 = d2[len(d2)//2:]
            d2_2 = d2[:len(d2)//2]
            d1_1_1 = d1_1[len(d1_1)//2:]
            d1_1_2 = d1_1[:len(d1_1)//2]
            d1_2_1 = d1_2[len(d1_2)//2:]
            d1_2_2 = d1_2[:len(d1_2)//2]
            d2_1_1 = d2_1[len(d2_1)//2:]
            d2_1_2 = d2_1[:len(d2_1)//2]
            d2_2_1 = d2_2[len(d2_2)//2:]
            d2_2_2 = d2_2[:len(d2_2)//2]
            pool = ThreadPool(processes=8)
            v1 = pool.apply_async(split_relook, (d1_1_1,))
            v2 = pool.apply_async(split_relook, (d1_1_2,))
            v3 = pool.apply_async(split_relook, (d1_2_1,))
            v4 = pool.apply_async(split_relook, (d1_2_2,))
            v5 = pool.apply_async(split_relook, (d2_1_1,))
            v6 = pool.apply_async(split_relook, (d2_1_2,))
            v7 = pool.apply_async(split_relook, (d2_2_1,))
            v8 = pool.apply_async(split_relook, (d2_2_2,))
            hours_score_1,last_update_1 = v1.get() 
            hours_score_2,last_update_2 = v2.get()
            hours_score_3,last_update_3 = v3.get()
            hours_score_4,last_update_4 = v4.get()
            hours_score_5,last_update_5 = v5.get()
            hours_score_6,last_update_6 = v6.get()
            hours_score_7,last_update_7 = v7.get()
            hours_score_8,last_update_8 = v8.get()
            comment_dict = defaultdict(int)
            for key in hours_score_1:
                comment_dict[key] = str(int(hours_score_1[key]) + int(hours_score_2[key]) 
                                        + int(hours_score_3[key]) + int(hours_score_4[key])
                                        + int(hours_score_5[key]) + int(hours_score_6[key])
                                        + int(hours_score_7[key]) + int(hours_score_8[key]))
            comment_upvotes = {**last_update_1, **last_update_2, **last_update_3, **last_update_4,                                              **last_update_5, **last_update_6, **last_update_7, **last_update_8}
            if last_hour == True:
            #Update all comments to their final upvote score for the day
                conn = happybase.Connection('localhost', 9090)
                table = conn.table('comments')
                batch = table.batch(batch_size = 1024)
                for key in comment_upvotes:
                    batch.put(key, {'stats:upvotes': str(comment_upvotes[key])})
                batch.send()
            #Put in the overall submission upvote scores up until the current hour
            conn = happybase.Connection('localhost', 9090)
            upvote_update = conn.table('comment_upvotes')
            comment_batch = upvote_update.batch(batch_size = 1024)
            for key in comment_dict:
                comment_batch.put(str(key) + str(current_day_integer) + str(current_hour * 2),
                                        {'time:day' : str(current_day_integer),
                                             'time:hour' : str(current_hour * 2), 
                                             'num:upvotes' : str(comment_dict[key])})
            comment_batch.send()
        if table_name == 'submissions':
            #Submission_dict is used bihourly
            submission_dict = defaultdict(int)
            #submission_upvotes is used at the end of the day 
            submission_upvotes = defaultdict(int)
            for key in tqdm(table_dict):
                if int(float(str(table_dict[key][0])[2:-1])) >= time.time() - 86400:
                    submission = relook.reddit.submission(key)
                    try:
                        submission_dict[submission.subreddit] += submission.score
                        if last_hour:
                            submission_upvotes[key] = submission.score        
                    except:
                        pass
            if last_hour:
                #Update all submissions to their final upvote score for the day
                conn = happybase.Connection('localhost', 9090)
                table = conn.table('submissions')
                batch = table.batch(batch_size = 1024)
                for key in submission_upvotes:
                    batch.put(str(key), {'stats:upvotes': str(submission_upvotes[key])})
                batch.send()
                #Put in the number of overall submission upvotes up until the current hour
            conn = happybase.Connection('localhost', 9090)
            upvote_update = conn.table('submission_upvotes')
            submission_batch = upvote_update.batch(batch_size = 1024)
            for key in submission_dict:
                submission_batch.put(str(key) + str(current_day_integer) + str(current_hour * 2), 
                                               {'time:day' : str(current_day_integer),
                                                'time:hour' : str(current_hour * 2), 
                                                'num:upvotes' : str(submission_dict[key])})
            submission_batch.send()
def wait_next_hour(current_hour, current_day_in_seconds):
    exit_seconds = current_day_in_seconds + (3600 * (current_hour * 2 + 2))
    while 1:
        dt = datetime.today()
        current_seconds = dt.timestamp()
        if current_seconds >= exit_seconds:
            return 0
        else:
            time.sleep(10)

def get_start_time():
    dt = datetime.today()
    dt_list = list(str(dt))
    hour_seconds = int(dt_list[11] +  dt_list[12]) * 3600
    minute_seconds = int(dt_list[14] + dt_list[15]) * 60 
    current_seconds = dt.timestamp()
    start_day_seconds = current_seconds - hour_seconds - minute_seconds - int(dt_list[17] + dt_list[18])
    return start_day_seconds, hour_seconds / 3600
    
    
if __name__ == '__main__':
    conn = Connector('localhost', 9090)
    conn.scanTable(sys.argv[1])
