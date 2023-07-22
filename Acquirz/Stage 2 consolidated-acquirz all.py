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
    patternDel = "abuse|backup|cancel|comp|crap|\
        fake|free|garbage|generic|invalid|\
        junk|loan|penis|person|phruit|police|postmaster|random|\
        shit|signup|spam|stuff|survey|test|trash|webmaster|xx"
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

file = 'esb_2805.csv'
statusfile = 'esb_280512Jun20Stage1Complete.csv'
hashfile = '20200428 all TJ domains from selligent_hashed v2.txt'

month = '12JUn20'

df = pd.read_csv(directory + file, encoding = 'utf-8')
df['Personal_Email'] = df['Personal_Email'].str.lower()
df['Generic_Email'] = df['Generic_Email'].str.lower()
print(df.columns)

gens = df[df['Personal_Email'].isnull() & df['Generic_Email'].notnull()].copy()
gens.drop_duplicates(subset='Generic_Email', keep='first',inplace=True)
print("Gens", gens.shape)
gens.rename(columns={'Generic_Email' : 'email'},inplace=True)
print(gens.columns)


personal = df[df['Personal_Email'].notnull()].copy()
personal.drop_duplicates(subset='Personal_Email', keep='first',inplace=True)
print("Personal", personal.shape)
personal.rename(columns={'Personal_Email' : 'email'},inplace=True)
print(personal.columns)


df1 = personal.append(gens)
print(df1.columns)
df1.drop_duplicates(subset=['email'], inplace=True)
print("All",df1.shape)
df1.columns = map(str.lower, df1.columns)

print(df1.shape)
print(df1.columns)

print(df1.shape)

df2 = pd.read_csv(directory + statusfile, usecols= ['email', 'domain','status', 'data flag'])
ispgroups = pd.read_csv(onedrive+'ISP Group domains.csv',encoding = "ISO-8859-1")

print('Gross', df2.shape[0])
df2 = df2.drop_duplicates(subset='email', keep='first')
print("All Data Flag", df2.shape[0])

df3 = pd.merge(df1, df2, left_on='email', right_on='email', how='left')
print(df3.columns)
print(df3.shape)


df3 = invalid_emails(df3)    
df4 = junk_emails(df3)


dataflags = df4['data flag'].value_counts()
report1 = dataflags.rename_axis('Description').reset_index(name='Count')
print(report1)
statusflags= df4['status'].value_counts()
report2 = statusflags.rename_axis('Description').reset_index(name='Count')
print(report2)

#Remove bad emails

   
df4.to_csv(directory + 'allstatus_' + file, index=False)
