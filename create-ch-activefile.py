import pandas as pd 
from sqlalchemy import create_engine

dbdirectory= 'E:/'
directory = 'E:/Company house/'
file = 'BasicCompanyDataAsOneFile-2021-02-01.csv'
db = 'companyhouse.db'
tablename = 'ch_active_companies'

sqlite_engine = create_engine('sqlite:///' + dbdirectory + db)

df = pd.read_csv(directory+file,delimiter=',',encoding ="ISO-8859-1", low_memory=False)
df = df[df['CompanyStatus'] == 'Active']
df.to_sql(tablename, sqlite_engine, if_exists='replace',chunksize=500, index=False)