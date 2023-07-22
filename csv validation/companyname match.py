import pandas as pd
from cleanco import prepare_terms, basename
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def cleanco(name):
    business_name = name
    terms = prepare_terms()
    result = basename(name, terms, prefix=False, middle=False, suffix=True)
    return result

directory1 = 'E:/Acquirze/'
file1 ='Acquirz July20.csv'
directory2 = 'E:/Company house/'
file2 = 'BasicCompanyDataAsOneFile-2020-07-01.csv'

df1 = pd.read_csv(directory1 + file1,encoding = "ISO-8859-1",low_memory=False, usecols=['Company Name'])
df1['Company Name'] = df1['Company Name'].str.lower()
print(df1.shape)
df1.drop_duplicates(subset=['Company Name'], inplace=True)
print(df1.shape)
nan_value = float("NaN")
df1.replace("", nan_value, inplace=True)
df1.dropna(subset = ["Company Name"], inplace=True)

#df1['Company'] = df1['Company Name'].apply(lambda x : cleanco(x))
print(df1.shape)
print(list(df1.columns.values))

df2 = pd.read_csv(directory2 + file2 ,encoding = "utf-8",low_memory=False, usecols=['CompanyName'])
df2['CompanyName'] = df2['CompanyName'].str.lower()

print(df2.shape)

df = pd.merge(df1, df2, left_on=['Company Name'], right_on=['CompanyName'], how='inner')
print(df.shape)
print(df.head(5))

df1r = (df1.merge(df2, left_on=['Company Name'], right_on=['CompanyName'], how='left', indicator=True)
     .query('_merge == "left_only"')
     .drop('_merge', 1))
print(df1r.shape)    
df1r['Company'] = df1r['Company Name'].apply(lambda x : cleanco(x))
df2['Company'] = df2['CompanyName'].apply(lambda x : cleanco(x))

df3 = pd.merge(df1r, df2, left_on=['Company'], right_on=['Company'], how='inner')
print(df3.shape)
print(df3.head(5))
df3.to_csv(directory1 + 'matched0607.csv', index=False)

'''
remain = (df1r.merge(df2, left_on=['Company'], right_on=['Company'], how='left', indicator=True)
     .query('_merge == "left_only"')
     .drop('_merge', 1))

remain.to_csv(directory1 + 'unmatched companies.csv', index=False)
print(remain.shape)
#df2['mobile'] = df2['mobile'].apply(str)


df2['key']=df2.Company.apply(lambda x : [process.extract(x, df1.Company, limit=1)][0][0][0])

df2.merge(df1,left_on='key',right_on='Company', how='inner')



'''