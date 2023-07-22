import pandas as pd
from sqlalchemy import create_engine

dbdirectory= 'E:/'
directory='E:/Company house/'
file = 'BasicCompanyDataAsOneFile-2020-07-01.csv'
tablename = 'base_data' 
db = 'companies.db'

sqlite_engine = create_engine('sqlite:///' + dbdirectory + db)


df = pd.read_csv(directory + file, encoding = 'utf-8', low_memory=False)
print(df.shape)

df.to_sql(tablename, sqlite_engine, if_exists='replace',chunksize=500, index=False)