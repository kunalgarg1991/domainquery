import pandas as pd
import requests
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn import metrics
import urllib.request
from bs4 import BeautifulSoup
from sklearn.neighbors import KNeighborsClassifier
from sklearn.cross_validation import train_test_split
import pickle
import argparse
import json
import time
import uuid

import googleapiclient.discovery
from pandas.io import gbq
import pandas as pd
import requests
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn import metrics
from bs4 import BeautifulSoup
from sklearn.neighbors import KNeighborsClassifier
#from sklearn.cross_validation import train_test_split
import pickle   
from threading import Thread
import queue
import lxml
from urllib.request import Request, urlopen
import datetime
from datetime import timedelta






df=pd.read_csv('domains.csv')
df["url"] = "url"
df["content"] = "content"
Count_Row=df.shape[0]


for x in range(Count_Row):  
    df["url"][x] = "http://www."+ str(df["domain"][x])


urls = df["url"]
df['category_num'] = df.category.map({'Y':1, 'N':0})


q = queue.Queue(maxsize=0)
num_theads = min(96, len(urls))

#this is where threads will deposit the results
results = [{} for x in urls];
#load up the queue with the urls to fetch and the index for each job (as a tuple):
for i in range(len(urls)):
    q.put((i,urls[i]))

def crawl(q, result, i):
    while True:
        work = q.get()  
        print(str(i) + "....." + str(work[0]) + "....." + str(work[1]))                    #fetch new work from the Queue
        try:
            
            urls=work[1]
            
            p=Request(urls, headers={'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'})
            html = urlopen(p).read()

            #html = urllib.request.urlopen(work[1]).read()
            soup = BeautifulSoup(html,"lxml")
            for script in soup(["script", "style"]):
                script.extract()
            text = soup.get_text()

            # break into lines and remove leading and trailing space on each
            lines = (line.strip() for line in text.splitlines())
            # break multi-headlines into a line each
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            # drop blank lines
            text = '\n'.join(chunk for chunk in chunks if chunk)
            result[work[0]] = text
            q.task_done()  
        except:
            result[work[0]] = "No Data"
            df["url"][work[0]]="google.com"
            df["category"][work[0]]='N'
            q.task_done()
        #signal to the queue that task has been processed
    return True


#set up the worker threads
for m in range(num_theads):
    worker = Thread(target=crawl, args=(q,results,m))
    worker.daemon = True    #setting threads as "daemon" allows main program to 
                              #exit eventually even if these dont finish 
                              #correctly.
    worker.start()

#now we wait until the queue has been processed
q.join()

df["category_num"]="no idea"

df["content"]=results
df['category_num'] = df.category.map({'Y':1, 'N':0})


df=df.dropna(axis=0, how='any')



X=df["content"]
y=df["category_num"]

print(df)

X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1)
vect = CountVectorizer()
vect.fit(X_train)
X_train_dtm = vect.transform(X_train)
X_test_dtm = vect.transform(X_test)

#print(X_test_dtm)
#print(y_train)


nb = MultinomialNB()
nb.fit(X_train_dtm, y_train)
y_pred_class = nb.predict(X_test_dtm)
print(metrics.accuracy_score(y_test, y_pred_class))
print(metrics.confusion_matrix(y_test, y_pred_class))


testdata=pd.DataFrame(data=X_test[1:,1:], index=data[1:,0], columns=data[0,1:])  # 1st row as the column names
testvalue=pd.DataFrame(data=y_test[1:,1:], index=data[1:,0], columns=data[0,1:])  # 1st row as the column names
testpred=pd.DataFrame(data=y_pred_class[1:,1:], index=data[1:,0], columns=data[0,1:])  # 1st row as the column names


m=pd.concat([testdata, testvalue, testpred], axis = 1)
m.to_csv("observe.csv")



data_struct = {'vectorizer1': vect, 'selector1': nb}
# use the 'with' keyword to automatically close the file after the dump
with open('storage2.bin', 'wb') as f: 
    pickle.dump(data_struct, f)
    


	













	







