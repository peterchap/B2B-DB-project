import pandas as pd 
from validate_email import validate_email
from datetime import datetime
from datetime import date
from sqlalchemy import create_engine

directory = 'E:/B2B DB project/'

dbdirectory= 'E:/'
db = 'b2b.db'
tablename = 'combined_b2b270121100221'

statusfile = 'combined_b2bmar21Stage1Complete.csv'

sqlite_engine = create_engine('sqlite:///' + dbdirectory + db)

month = '12feb21'

df1 = pd.read_sql(tablename,sqlite_engine)
print('Original', df1.shape)
df1 = df1.drop_duplicates(subset=['CompanyName','email'], keep ='first')
df1['email'] = df1['email'].str.lower()

print("deduped", df1.shape)




df2 = pd.read_csv(directory + statusfile, usecols= ['email', 'status'])

df2 = df2.drop_duplicates(subset='email', keep='first')
print("All Data Flag", df2.shape[0])
print(df2['status'].value_counts())
accept = ['OK', 'Not Checked']
df3 = df2['email'][df2['status'].isin(accept)]
print('accept', df3.shape)
df4 = df1.merge(df3, left_on='email', right_on='email', how='inner')

print(df4.columns)
print(df4.shape)
df4.to_sql(tablename, sqlite_engine, if_exists='replace',chunksize=500, index=False)
