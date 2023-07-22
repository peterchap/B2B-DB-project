from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
import re
from ftfy import fix_text
import pandas as pd 
import pickle
import time

def ngrams(string, n=3):
    string = fix_text(string) # fix text
    string = string.encode("ascii", errors="ignore").decode() #remove non ascii chars
    string = string.lower()
    chars_to_remove = [")","(",".","|","[","]","{","}","'"]
    rx = '[' + re.escape(''.join(chars_to_remove)) + ']'
    string = re.sub(rx, '', string)
    string = string.replace('&', 'and')
    string = string.replace(',', ' ')
    string = string.replace('-', ' ')
    string = string.title() # normalise case - capital at start of each word
    string = re.sub(' +',' ',string).strip() # get rid of multiple spaces and replace with a single
    string = ' '+ string +' ' # pad names for ngrams...
    string = re.sub(r'[,-./]|\sBD',r'', string)
    ngrams = zip(*[string[i:] for i in range(n)])
    return [''.join(ngram) for ngram in ngrams]

directory1 = 'E:/Acquirze/'
file1 ='acquirz_jul20_cleaned_160720.csv'
directory2 = 'E:/Company house/'
file2 = 'BasicCompanyDataAsOneFile-2020-07-01.csv'

clean_org_names = pd.read_csv(directory2 + file2 ,encoding = "utf-8",low_memory=False, usecols=['CompanyName'])
clean_org_names = clean_org_names.iloc[:, 0:6]

org_name_clean = clean_org_names['CompanyName'].unique()

print('Vecorizing the data - this could take a few minutes for large datasets...')
vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams, lowercase=False)
tfidf = vectorizer.fit_transform(org_name_clean)
pickle.dump(vectorizer, open(directory2 + 'vectorizer.pickle', 'wb'))
print('Vecorizing completed...')

nbrs = NearestNeighbors(n_neighbors=1, n_jobs=-1).fit(tfidf)

names = pd.read_csv(directory1 +file1, encoding = "ISO-8859-1",low_memory=False, usecols=['Company Name'])
org_column = 'Company Name' #column to match against in the messy data
unique_org = set(names[org_column].values) # set used for increased performance


###matching query:
def getNearestN(query):
  queryTFIDF_ = vectorizer.transform(query)
  distances, indices = nbrs.kneighbors(queryTFIDF_)
  return distances, indices


t1 = time.time()
print('getting nearest n...')
distances, indices = getNearestN(unique_org)
t = time.time()-t1
print("COMPLETED IN:", t)

unique_org = list(unique_org) #need to convert back to a list
print('finding matches...')
matches = []
for i,j in enumerate(indices):
  temp = [round(distances[i][0],2), clean_org_names.values[j][0][0],unique_org[i]]
  matches.append(temp)

print('Building data frame...')  
matches = pd.DataFrame(matches, columns=['Match confidence (lower is better)','Matched name','Origional name'])
matches.to_csv(directory1 + 'nearest_neighbout_v1.csv', index=False)
print('Done') 