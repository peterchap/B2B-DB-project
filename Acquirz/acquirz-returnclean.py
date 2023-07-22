import pandas as pd 
from validate_email import validate_email


directory = 'E:/Acquirze/'
file1 = 'ESB_FEB05_reformat.csv'
file2 = 'ESB_FEB05.csv'
statusfile = 'acquirz100221Feb21Stage1Completev2.csv'

month = '12feb21'

df1 = pd.read_csv(directory+file1,encoding ='utf-8',usecols= ['contact_urn', 'email'],low_memory=False) 
print('reformat', df1.shape)
df1 = df1.drop_duplicates(subset=['email'], keep ='first')
df1['email'] = df1['email'].str.lower()

print("deduped", df1.shape)

df2 = pd.read_csv(directory + statusfile, usecols= ['email', 'status'])

df2 = df2.drop_duplicates(subset='email', keep='first')
print("All Data Flag", df2.shape[0])

accept = ['OK', 'Not Checked', 'Gov domain']
df3 = df2[['email','status']][~df2['status'].isin(accept)]
print('accept', df3.shape)

df4 = df1.merge(df3, left_on='email', right_on='email', how='inner')
df4['dataflag'] = 'Remove'
statuscounts = df2['status'].value_counts(dropna=False).rename_axis('Status').reset_index(name='Count')
print(statuscounts)
#statuscounts.to_csv(directory + "Acquirz Status counts 120221.csv", index=False)
print(df4.columns)
print(df4.shape)

df5 = pd.read_csv(directory+file2,sep='\t',encoding ="ISO-8859-1",engine='python', error_bad_lines=False)
print('original', df5.shape)
df6 = df5.merge(df4, left_on='Contact URN', right_on='contact_urn', how='left')
df6.loc[df6['dataflag'].isnull(), 'dataflag'] = 'OK'
print(df6.columns)
print('final', df6.shape)
print('dataflag',df6['dataflag'].value_counts())
df6.drop(columns=['contact_urn', 'email'], inplace=True)
df6.loc[df6['status'].isnull(), 'status'] = 'OK'
df6.loc[df6['status'] == 'Parked Domain', 'status'] = 'Catchall'
df6.loc[df6['status'] == 'Spam Trap', 'status'] = 'Catchall'
print(df6['status'].value_counts(dropna=False))
print(df6.columns)
df6.to_csv(directory + "ESB_FEB05_datacleanflag_withstatus.csv", index=False)