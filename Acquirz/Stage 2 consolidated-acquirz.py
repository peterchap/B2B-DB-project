import pandas as pd 
from validate_email import validate_email
from datetime import datetime
from datetime import date

import hashlib

#Remove invalid email formats

def invalid_emails(data):
    data.loc[~data.email.str.contains("@",na=False), 'data flag'] ='remove'
    data.loc[~data.email.str.contains("@",na=False), 'status'] ='Invalid email address'
    char = '\+|\*|\'| |\%|,|\"|\/'
    data.loc[data.email.str.contains(char,regex=True,na=False), 'data flag'] = 'remove'
    data.loc[data.email.str.contains(char,regex=True,na=False), 'status'] = 'Invalid email address'
    return data

# Remove Bad statuss

def junk_emails(data):
    patternDel = "abuse|backup|cancel|comp|contact|crap|\
        fake|free|garbage|generic|invalid|\
        junk|loan|penis|person|phruit|police|postmaster|random|recep|\
        shit|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
    data.loc[data['email'].str.contains(patternDel, na=False), 'status'] = 'Junk Email'
    data.loc[data['first_name'].str.contains(patternDel, na=False), 'status'] = 'Junk Email'
    data.loc[data['last_name'].str.contains(patternDel, na=False), 'status'] = 'Junk Email'
    return data

def report_ISP_groups(data, ispgroup):
        
    new = data['email'].str.split(pat="@", expand=True)
    data.loc[:,'Left']= new.iloc[:,0]
    data.loc[:,'Domain'] = new.iloc[:,1]

    ispdata = pd.merge(data, ispgroup, on='Domain', how='left')
    ispdata.loc[:,'Group'].fillna("Other", inplace = True)
    stat = pd.DataFrame(ispdata['Group'].value_counts()).reset_index()
    stat.rename(columns={'index' : 'ISP', 'Group' : 'count'}, inplace=True)
    return stat

# End of Functions

directory = 'E:/Acquirze/'
onedrive= 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/'

file = 'Acquirzall.csv'
statusfile = 'acquirzall06jul20Stage1Completev2.csv'
#hashfile = '20200428 all TJ domains from selligent_hashed v2.txt'

month = '06jul20'
'''
cols = ['Company Name', 'Address Line 1', 'Address Line 2', 'Address Line 3','Town', 'County', 'Postcode',\
    'Telephone', 'Title', 'First Name','Surname', 'Job Role', 'Email', 'Employee Band', 'Turnover',\
    'SIC Sector', 'SIC5 Code & Description', 'Website']
'''
df = pd.read_csv(directory + file, encoding = "ISO-8859-1", low_memory=False)
df.rename(columns={'combined_email' : 'email'},inplace=True)
df['email'] = df['email'].str.lower()
#df['Generic_email'] = df['Generic_email'].str.lower()
print(df.columns)
print("Original", df.shape)
#gens = df[df['email'].isnull() & df['Generic_email'].notnull()].copy()
#gens.drop_duplicates(subset='Generic_email', keep='first',inplace=True)
#gens.drop(columns=['email'],inplace=True)
#print("Gens", gens.shape)
#gens.rename(columns={'Generic_email' : 'email'},inplace=True)
#print(gens.columns)


#personal = df[df['email'].notnull()].copy()
#personal.drop_duplicates(subset='email', keep='first',inplace=True)
#personal.drop(columns=['Generic_email'],inplace=True)
#print("Personal", personal.shape)
#personal.rename(columns={'email' : 'email'},inplace=True)
#print(personal.columns)


#df1 = personal.append(gens)
#print(df1.columns)
df.drop_duplicates(subset=['email'], inplace=True)
print("All",df.shape)
#df1.columns = map(str.lower, df1.columns)



df2 = pd.read_csv(directory + statusfile, usecols= ['email', 'domain','status', 'data flag'])
ispgroups = pd.read_csv(onedrive+'ISP Group domains.csv',encoding = "ISO-8859-1")

print('Gross', df2.shape[0])
df2 = df2.drop_duplicates(subset='email', keep='first')
print("All Data Flag", df2.shape[0])

df3 = pd.merge(df, df2, left_on='email', right_on='email', how='left')
print(df3.columns)
print(df3.shape)


df3 = invalid_emails(df3)    
df4 = junk_emails(df3)


dataflags = df3['data flag'].value_counts()
report1 = dataflags.rename_axis('Description').reset_index(name='Count')
print(report1)
statusflags= df3['status'].value_counts()
report2 = statusflags.rename_axis('Description').reset_index(name='Count')
report2.to_csv(directory +'acquirz_jul20_status_counts.csv')
print(report2)

#Remove bad emails

df5 = df4[df4['data flag'] != 'remove']
print('emails', df5.shape)
print(df5.columns)    

to_mdropcols = ['data flag']
df5.drop(columns=to_mdropcols,  inplace=True)

# Remove hash data
'''
df5['hash'] = df5['domain'].astype(str).str.encode('UTF-8')\
          .apply(lambda x: (hashlib.md5(x).hexdigest()))


df6 = pd.read_csv(directory + hashfile,header=0, names=['domain'])
df6 = df6.apply(lambda x: x.astype(str).str.lower())

df7 = (df5.merge(df6, left_on='hash', right_on='domain', how='left', indicator=True)
     .query('_merge == "left_only"')
     .drop('_merge', 1))

print('final',df7.shape)
print(df7.columns)
print(df7.head())
'''
df5.to_csv(directory + 'acquirz_all_cleaned_060720.csv', index=False)
