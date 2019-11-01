import re 
from sklearn.model_selection import train_test_split
from nltk.stem import WordNetLemmatizer
from sklearn.feature_extraction.text import TfidfVectorizer
import happybase
from sklearn.naive_bayes import MultinomialNB, GaussianNB, BernoulliNB
import os
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import os
import json
import time
import numpy as np
from collections import defaultdict
from tqdm import tqdm
from nltk.stem import PorterStemmer
class sub_dict():
    def __init__(self, name):
        self.name = name
        self.dict = {}
        self.sort_keys = None
def classify(X):
    examples, num_features = X.shape
    Y = np.zeros((examples, 1))
    Y[int(examples * .7):] = 1
    Y = np.ravel(Y)
    X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size = .2, shuffle = True)    
    clf = MultinomialNB()
    clf.fit(X_train, y_train)
    acc = clf.score(X_train, y_test)
    print(acc)
def retrieveRows(subreddit):
    conn = happybase.Connection('localhost', 9090)
    table = conn.table('comments')
    all_ids = []
    comments = []
    for key in subreddit.sort_keys:
        for identifcation in subreddit.dict[key]:
            all_ids.append(identifcation)
    for val in tqdm(all_ids):
            row = table.row(val)
            comment = str(row[b'stats:comment'])[2:-1]
            comments.append(comment)
    data = json.dumps(comments)
    with open("output.json", "w") as f:
        f.write(data)
    return comments

def vectorize(comments):
    stemmer = WordNetLemmatizer()
    corpus = []
    tfidfconverter = TfidfVectorizer(max_features = 1500, min_df = 10, 
    max_df = .7, stop_words = stopwords.words('english'))
    for comment in tqdm(comments):
        comment = re.sub(r'\W', ' ', comment)
        comment = re.sub(r'\s+[a-zA-Z]\s+', ' ', comment)
        comment = re.sub(r'\^[a-zA-Z]\s+', ' ', comment)
        comment = re.sub(r'\s+', ' ', comment, flags=re.I)
        comment = re.sub(r'^b\s+', ' ', comment)
        comment = comment.lower()
        comment = comment.split()
        comment = [stemmer.lemmatize(word) for word in comment]
        comment = ' '.join(comment)
        corpus.append(comment)
    z = (n for n in corpus)
    X = tfidfconverter.fit_transform(z).toarray()
    return X
def extractIDs():
    conn = happybase.Connection('localhost', 9090)
    table = conn.table('comments')
    sub_list = []
    for key, data in tqdm(table.scan()): 
        subreddit = str(data[b'subreddit:subreddit'])[2:-1]
        if subreddit == 'announcements':
            continue 
        identifcation = str(key)[2:-1]
        upvotes = int(str(data[b'stats:upvotes'])[2:-1])
        foundSub = False
        for sub in sub_list:
            if sub.name == subreddit:
                if upvotes in sub.dict.keys():
                    sub.dict[upvotes].append(identifcation)
                else:
                    sub.dict[upvotes] = [identifcation]
                foundSub = True 
        if foundSub == False:
            new = sub_dict(subreddit)
            sub_list.append(new) 
            sub_list[-1].dict[upvotes] = [identifcation]
    for x in sub_list:
        x.sort_keys = sorted(list(x.dict.keys()))
    return sub_list 
#    for x in sub_list:
#        print("SUBREDDIT", x.name)
#        for j in range(1, 30):
#            print("Comments with", x.sort_keys[-j],"Upvotes", x.dict[x.sort_keys[-j]])
if __name__ == "__main__":
    if not os.path.exists(os.getcwd() + "/output.json"):
        sub_list = extractIDs()
        comments = retrieveRows(sub_list[0])
        X = vectorize(comments)
        classify(X)
    else:
        with open(os.getcwd() + "/output.json") as f:
            comments = json.load(f)
            X = vectorize(comments)
            classify(X)
