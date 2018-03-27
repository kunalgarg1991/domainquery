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





df=pd.read_csv('domains.csv')
df["url"] = "url"
df["content"] = "content"


Count_Row=df.shape[0]


for x in range(Count_Row):	
	df["url"][x] = "http://www."+ df["domain"][x]
	urls=df["url"][x]
	print(x,"  ",urls)
	try:
		html = urllib.request.urlopen(urls).read()
	except:
		print("---Exception---")
		urls="https://www.google.com"
		df["url"][x]="google.com"
		df["category"][x]='N'

		html = urllib.request.urlopen(urls).read()

	soup = BeautifulSoup(html)

	# kill all script and style elements
	for script in soup(["script", "style"]):
	    script.extract()    # rip it out

	# get text
	text = soup.get_text()

	# break into lines and remove leading and trailing space on each
	lines = (line.strip() for line in text.splitlines())
	# break multi-headlines into a line each
	chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
	# drop blank lines
	text = '\n'.join(chunk for chunk in chunks if chunk)


	
	df["content"][x] =text
	



df['category_num'] = df.category.map({'Y':1, 'N':0})
X=df["content"]
y=df["category_num"]
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1)
vect = CountVectorizer()
vect.fit(X_train)
X_train_dtm = vect.transform(X_train)
X_test_dtm = vect.transform(X_test)
nb = MultinomialNB()
nb.fit(X_train_dtm, y_train)
y_pred_class = nb.predict(X_test_dtm)
print(metrics.accuracy_score(y_test, y_pred_class))
print(metrics.confusion_matrix(y_test, y_pred_class))


data_struct = {'vectorizer1': vect, 'selector1': nb}
# use the 'with' keyword to automatically close the file after the dump
with open('storage1.bin', 'wb') as f: 
    pickle.dump(data_struct, f)
    

df=pd.read_csv('testdomains.csv')
df["url"] = "url"
df["content"] = "content"


Count_Row=df.shape[0]


for x in range(Count_Row):	
	df["url"][x] = "http://www."+ df["domain"][x]
	urls=df["url"][x]
	print(x,"  ",urls)
	try:
		html = urllib.request.urlopen(urls).read()
	except:
		print("---Exception---")
		urls="https://www.google.com"
		df["url"][x]="google.com"
		df["category"][x]='N'

		html = urllib.request.urlopen(urls).read()

	soup = BeautifulSoup(html)

	# kill all script and style elements
	for script in soup(["script", "style"]):
	    script.extract()    # rip it out

	# get text
	text = soup.get_text()

	# break into lines and remove leading and trailing space on each
	lines = (line.strip() for line in text.splitlines())
	# break multi-headlines into a line each
	chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
	# drop blank lines
	text = '\n'.join(chunk for chunk in chunks if chunk)


	
	df["content"][x] =text

df["prediction"]=9
X=df["content"]
X_test = vect.transform(X)

y=nb.predict(X_test)
df["prediction"] = y
p = df.drop(['content'], axis=1)
p.to_csv("qazedc1.csv")

	













	







