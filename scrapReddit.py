from multiprocessing import Process
from collections import defaultdict 
from threading import Thread
import praw 
import time
import os
from HBase_Python import Connector  
import datetime
client_id = "NEpDdev8xeh__Q"
client_secret = "Qw0dKtMdN2QoQEwrGcINAdDASAo"
seconds_in_day = 86400
class browseReddit():
    def __init__(self, client_id, client_secret, user_agent, username = None, password = None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.user_agent = user_agent 
        self.reddit = praw.Reddit(client_id = client_id, client_secret = client_secret, username = username, password = password, user_agent = user_agent)
        self.comment_count = 0 
        self.submission_count = 0
        self.sub_list = self.get_subs()
    def get_subs(self):
        if self.username and self.password is not None:
            sub_list = self.reddit.user.subreddits()
            sub_names = [str(x) for x in sub_list]
            joined = ""
            for sub_name in sub_names:
                joined += sub_name + "+"
            return joined
    def browse_submissions(self):
        while 1:
            try:
                subreddits = self.reddit.subreddit(self.sub_list)
                HBaseConnector = Connector('localhost', 9090)
                for submission in subreddits.stream.submissions(skip_existing = True):
                    HBaseConnector.insertSubmissions(submission.id, submission.subreddit, submission.score, submission.created_utc, submission.title)
                    self.submission_count += 1
                    print("Parsed", self.submission_count, "Submissions")
            except:
                pass
    def browse_comments(self):
        while 1:
            try:
                subreddits = self.reddit.subreddit(self.sub_list)
                HBaseConnector = Connector('localhost', 9090)
                for comment in subreddits.stream.comments(skip_existing = True):
                    HBaseConnector.insertComments(comment.id, comment.subreddit, comment.score, comment.created_utc, comment.body)
                    self.comment_count += 1
                    print("Parsed", self.comment_count, "Comments")
            except:
                pass 

if __name__ == '__main__':
    Task = browseReddit(client_id, client_secret, "USERAGENT", "bigDataProject", "asianswag")    
    if sys.argv[1] == 'comments':
        Task.browse_comments()
    else:
        Task.browse_submissions()


