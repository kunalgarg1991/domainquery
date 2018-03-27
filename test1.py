import pandas as pd
import requests
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn import metrics
from bs4 import BeautifulSoup
from sklearn.neighbors import KNeighborsClassifier
from sklearn.cross_validation import train_test_split
import pickle	
from threading import Thread
import queue
from urllib.request import Request, urlopen




# reload
with open('storage1.bin', 'rb') as f:
    data_struct = pickle.load(f)
    vect, model1 = data_struct['vectorizer1'], data_struct['selector1']


df=pd.read_csv('testdomains.csv')
df["url"] = "url"
df["content"] = "content"
df["category"]='category'
Count_Row=df.shape[0]


for x in range(Count_Row):	
    df["url"][x] = "http://www."+ str(df["domain"][x])


urls = df["url"]

q = queue.Queue(maxsize=0)
num_theads = min(25, len(urls))

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
            soup = BeautifulSoup(html, "lxml")
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


df["content"]=results



df["prediction"]=9
X_test=df["content"]
X_test_dtm = vect.transform(X_test)
y_pred_class = model1.predict(X_test_dtm)
print(y_pred_class)
df["prediction"]= y_pred_class
p = df.drop(['content'], axis=1)
print(df)

p.to_csv("resultsdomains3.csv")

