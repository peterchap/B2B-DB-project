import pandas as pd
from sqlalchemy import create_engine 

directory1 = 'E:/Cleaning-todo/'
file1 = 'Experian rejected- and bad emails v.2 20210125.csv'
file2= 'experian2711.csv'

dbdirectory= 'E:/'
db = 'b2b.db'
tablename = 'combined_b2b270121'

sqlite_engine = create_engine('sqlite:///' + dbdirectory + db)


df1 = pd.read_csv(directory1 + file2,encoding = "ISO-8859-1",low_memory=False, usecols=['email'])
df2 = pd.read_csv(directory1 + file1,encoding = "utf-8",low_memory=False)
df2['status'] = 'Bad'
print(df1.shape, df2.shape)
print(df1.columns)
print(df2.columns)

df1.drop_duplicates(subset=['email'], inplace=True)
print(df1.shape)

df3 = (df1.merge(df2, left_on='email', right_on='email', how='left', indicator=True)
        .query('_merge == "left_only"')
        .drop('_merge', 1))
df3.loc[df3['status'].isnull(), 'status'] = 'Existing'
df3bad= df1.merge(df2, left_on='email', right_on='email', how='inner')
print(df3.shape)
print(df3.columns)
'''
dropcols = ['URN', 'CompanyName', 'first_name', 'last_name', 'job_title',
       'RegAddress.AddressLine1', ' RegAddress.AddressLine2',
       'RegAddress.PostTown', 'RegAddress.County', 'RegAddress.Postcode',
       'SICCode.SicText_1', 'SIC_5_Code1', 'SIC_2_Code1', 'generic',
       'md5']
df3.drop(columns=dropcols,inplace=True)
'''
df4 = pd.read_sql(tablename,sqlite_engine)
print('combinedb2b_270111', df4.shape)

df4.drop_duplicates(subset=['email'], inplace=True)
b2b = (df4.merge(df2, left_on='email', right_on='email', how='left', indicator=True)
        .query('_merge == "left_only"')
        .drop('_merge', 1))
print('b2bdb_new', b2b.shape)
#b2b.to_sql(tablename,sqlite_engine, index=False,if_exists='replace',chunksize=50000)

#new data
df7 = (b2b.merge(df1, left_on='email', right_on='email', how='left', indicator=True)
        .query('_merge == "left_only"')
        .drop('_merge', 1))
df7['status'] = 'New'



df5 = (df3.merge(df7['email'], left_on='email', right_on='email', how='left', indicator=True)
        .query('_merge == "left_only"')
        .drop('_merge', 1))
df5['status'] = 'Existing'

final = pd.concat([df7, df3bad, df5], ignore_index=True)

todropcols = [' CompanyNumber',  'telephone', 'title',  'employees', 'employee band',\
        'turnover', 'website','CompanyCategory', 'CompanyStatus',\
        'CountryOfOrigin', 'IncorporationDate', 'Returns.NextDueDate',\
        'SICCode.SicText_2', 'SICCode.SicText_3','SICCode.SicText_4',\
        'SIC_5_Code2', 'SIC_5_Code3', 'SIC_5_Code4', ]
final.drop(columns=todropcols,inplace=True)

final.to_csv(directory1 + 'Experian_270121_refesh.csv', index=False)
print(final.columns)

print(final['status'].value_counts())

print('new', df7.shape)
print('Existing', df5.shape)
print('bad', df3bad.shape)