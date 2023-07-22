import pandas as pd 
import numpy as np
from string_grouper import match_strings, match_most_similar, group_similar_strings, StringGrouper

directory1 = 'E:/Acquirze/'
file1 ='unmatched companies.csv'
directory2 = 'E:/Company house/'
file2 = 'CHcleancocomplete.csv'

df1 = pd.read_csv(directory1 + file1,encoding = "ISO-8859-1",low_memory=False, usecols=['Company Name'])
df1['Company Name'] = df1['Company Name'].astype(str).unique()
new = pd.Series(df1['Company Name'].values)
print(new.shape)
df2 = pd.read_csv(directory1 + file2 ,encoding = "utf-8",low_memory=False, usecols=['Company'])
df2['Company'] = df2['Company'].astype(str)
chouse = pd.Series(df2['Company'].values)
print(df1.shape, df2.shape)

# Create all matches:
matches = match_most_similar(chouse, new)

# Display the results:
results= pd.DataFrame({'new_companies': df1, 'matched': matches})
results.to_csv(directory1 + 'finalunknownresults.csv', index=False)

print('Completed')