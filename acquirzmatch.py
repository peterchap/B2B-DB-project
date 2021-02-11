import pandas as pd
from sqlalchemy import create_engine 
import hashlib
import base_repr
import sys

directory = 'E:/Cleaning-todo/'
directory1 = 'E:/Acquirze/'
file1 = 'results.csv'
file2 = 'ESB_FEB05_reformat.csv'
file3 = 'acquirz100221Feb21Stage1Completev2.csv'


dbdirectory= 'E:/'
dbb2b = 'b2b.db'
tablename_b2b = 'combined_b2b270121'
dbch = 'companyhouse.db'
tablename_ch = 'ch_active_companies'

sqlite_engine_b2b = create_engine('sqlite:///' + dbdirectory + dbb2b)
sqlite_engine_ch = create_engine('sqlite:///' + dbdirectory + dbch)


df1 =  pd.read_csv(directory1 + file1,encoding = "utf-8",low_memory=False)
print(df1.shape)
df2 =  pd.read_csv(directory1 + file2,encoding = "utf-8",low_memory=False)
print(df2.shape)
df2.rename(columns={'employee_band': 'employee band'}, inplace=True)

df3 = df2.merge(df1, left_on= 'company_name', right_on= 'AZ', how='inner')

print(df3.shape)

df4 = pd.read_csv(directory1 + file3,encoding = "utf-8",low_memory=False, usecols=['email', 'status'])
df4.loc[df4['status'].isin(['OK','Recheck', 'Not Checked'])]
print(df4.shape)

df5 = df3.merge(df4, left_on='email', right_on= 'email', how = 'inner')

print(df5.columns)

colsremove = ['Unnamed: 0',  'company_urn', 'contact_urn','company_name', 'sic_2_code',\
     'sic_2_desc', 'sic_5_code', 'sic_5_desc','AZ', 'similarity','status']
df5.drop(columns= colsremove, inplace=True)

df5.rename(columns={'CH' :'CompanyName'}, inplace=True)

df5.loc[df5['personal_email'].notnull(), 'generic'] = 'N'
df5.loc[df5['personal_email'].isnull(), 'generic'] = 'Y'
df5['md5'] = [hashlib.md5(val.encode('utf-8')).hexdigest() for val in df5['email']]
df5['address line 1'] = ''
df5['address line 2'] = ''
df5['address line 3'] = ''
df5['town'] = ''
df5['county'] = ''
df5['telephone'] = ''
df5['turnover'] = ''
df5['website'] = ''
df5['URN'] = ''

print(df5.shape)
print(df5.columns)
df6 = pd.read_sql(tablename_ch,sqlite_engine_ch)
colstodrop = ['RegAddress.CareOf', 'RegAddress.POBox', 'CountryOfOrigin','DissolutionDate',\
       'Accounts.AccountRefDay', 'Accounts.AccountRefMonth', 'Accounts.NextDueDate',\
       'Accounts.LastMadeUpDate', 'Accounts.AccountCategory','Mortgages.NumMortCharges',\
       'Mortgages.NumMortOutstanding', 'Mortgages.NumMortPartSatisfied',\
       'Mortgages.NumMortSatisfied', 'Returns.LastMadeUpDate','LimitedPartnerships.NumGenPartners',\
       'LimitedPartnerships.NumLimPartners', 'URI', 'PreviousName_1.CONDATE',\
       ' PreviousName_1.CompanyName', ' PreviousName_2.CONDATE',\
       ' PreviousName_2.CompanyName', 'PreviousName_3.CONDATE',\
       ' PreviousName_3.CompanyName', 'PreviousName_4.CONDATE',\
       ' PreviousName_4.CompanyName', 'PreviousName_5.CONDATE',\
       ' PreviousName_5.CompanyName', 'PreviousName_6.CONDATE',\
       ' PreviousName_6.CompanyName', 'PreviousName_7.CONDATE',\
       ' PreviousName_7.CompanyName', 'PreviousName_8.CONDATE',\
       ' PreviousName_8.CompanyName', 'PreviousName_9.CONDATE',\
       ' PreviousName_9.CompanyName', 'PreviousName_10.CONDATE',\
       ' PreviousName_10.CompanyName', 'ConfStmtNextDueDate',\
       ' ConfStmtLastMadeUpDate']
df6.drop(columns=colstodrop, inplace=True)
df6['SIC_5_Code1'] = df6['SICCode.SicText_1'].str.split(' -').str[0]
df6['SIC_5_Code2'] = df6['SICCode.SicText_2'].str.split(' -').str[0]
df6['SIC_5_Code3'] = df6['SICCode.SicText_3'].str.split(' -').str[0]
df6['SIC_5_Code4'] = df6['SICCode.SicText_4'].str.split(' -').str[0]
df6['SIC_2_Code1'] = df6['SICCode.SicText_1'].str[-2:]
df6.drop_duplicates(subset=['CompanyName'], inplace=True)
df7 = df5.merge(df6, left_on='CompanyName', right_on='CompanyName', how='left')
print(df7.shape)
print(df7.columns)

df8 = df7[['URN', 'email','md5','generic','CompanyName',' CompanyNumber','address line 1',\
    'address line 2', 'address line 3', 'town', 'county', 'postcode','telephone',\
    'title', 'first_name', 'last_name', 'job_title', 'employees', 'employee band',\
    'turnover', 'website', 'RegAddress.AddressLine1',' RegAddress.AddressLine2',\
    'RegAddress.PostTown', 'RegAddress.County',\
    'RegAddress.Country', 'RegAddress.PostCode', 'CompanyCategory',\
    'CompanyStatus', 'IncorporationDate', 'Returns.NextDueDate',\
    'SICCode.SicText_1', 'SICCode.SicText_2', 'SICCode.SicText_3',\
    'SICCode.SicText_4', 'SIC_5_Code1', 'SIC_5_Code2', 'SIC_5_Code3',\
    'SIC_5_Code4', 'SIC_2_Code1']]
print(df8.shape)
print(df8.columns)

df9 = pd.read_sql(tablename_b2b,sqlite_engine_b2b)
print(df9.shape)
print(df9.columns)


df10 = pd.concat([df9,df8], ignore_index=True)
print(df10.shape)
df10.drop_duplicates(subset=['CompanyName', 'email'], keep='last', inplace=True)

df10['URN'] = df10['email'].apply(lambda x: base_repr.str_to_repr(x, base=62, byteorder='little', encoding='utf-8'))
print(df10.shape)
print(df10.columns)
df10.to_sql(tablename_b2b+'100221',sqlite_engine_b2b, index=False,if_exists='replace',chunksize=50000)
'''
df9 = pd.read_sql(tablename_b2b,sqlite_engine_b2b, columns=mail'])
df7 = (df5.merge(df6, left_on='email', right_on='email', how='left', indicator=True)
    .query('_merge == "left_only"')
    .drop('_merge', 1))
print(df7.shape)
df7.rename(columns={'CH' :'company_name'}, inplace=True)
print(df7.columns)
df7.to_csv(directory1 + 'acquirzfeb21_chmatched.csv', index=False)
'''