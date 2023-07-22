import pandas as pd
import datetime

directory1 = 'E:/Acquirze/'
file1 ='acquirz_all_cleaned_060720.csv'
file2 = 'matcheddetail_part2.csv'

df1_cols = ['company_name', 'postcode','title', 'first_name', 'last_name', 'job_title', 'personal_email',\
       'generic_email', 'sic_5_code', 'sic_5_desc', 'employees', 'employee_band', 'email']
df1 = pd.read_csv(directory1 + file1,encoding = "ISO-8859-1",low_memory=False, usecols=df1_cols)

print(df1.shape)
print(df1.columns)

df2_cols = ['address line 1', 'address line 2', 'address line 3',\
       'town', 'county', 'postcode', 'telephone', 'title', 'first_name',\
       'last_name', 'job_title', 'email', 'employee band', 'turnover',\
       'website', 'companyname', 'regaddress.postcode', 'companystatus',\
       'incorporationdate', 'accounts.nextduedate', 'sic_5_code', 'sic_5_desc']
df2 = pd.read_csv(directory1 + file2,encoding = "ISO-8859-1",low_memory=False, usecols=df2_cols)
df2 = df2[df2['companystatus'] == 'Active']
print(df2.shape)
print(df2.columns)

new_df1_cols = ['address line 1', 'address line 2', 'address line 3',\
       'town', 'county', 'telephone',  'turnover',\
       'website', 'companyname', 'regaddress.postcode', 'companystatus',\
       'incorporationdate', 'accounts.nextduedate']

df1 = df1.reindex(columns=[*df1.columns.tolist(), *new_df1_cols], fill_value="")
df1['companyname'] = df1['company_name']

neworder = ['companyname','address line 1', 'address line 2', 'address line 3',\
       'town', 'county', 'postcode', 'telephone', 'title', 'first_name',\
       'last_name', 'job_title', 'email','employees', 'employee band', 'turnover',\
       'website', 'regaddress.postcode', 'companystatus',\
       'incorporationdate', 'accounts.nextduedate', 'sic_5_code', 'sic_5_desc']

df1=df1.reindex(columns=neworder)
print(df1.columns)
print(df1.shape)

new_df2_cols = ['personal_email', 'generic_email', 'employees']
df2 = df2.reindex(columns=[*df2.columns.tolist(), *new_df2_cols], fill_value="")
df2=df2.reindex(columns=neworder)
print(df2.columns)
print(df2.shape)

final = df1.append(df2, ignore_index=True)
print(final.shape)
final.drop_duplicates(subset=['companyname', 'email'],keep='last', inplace=True, ignore_index=True)
final.to_csv(directory1 + 'Aquirezall030820.csv', index=False)
print(final.shape)
overall = pd.DataFrame({'Total Database' : final.shape[0], 'Number of Unique Businesses' : final['companyname'].unique().shape[0]}, index=[0])
final['incorpyear'] = pd.to_datetime(final['incorporationdate'], errors='coerce',dayfirst=True).dt.to_period('Y')
final['accts_due'] = pd.to_datetime(final['accounts.nextduedate']).dt.to_period('M')
counts1 = final['sic_5_code'].value_counts().rename_axis('Sic Code').reset_index(name='Counts')
sicodes = final.groupby('sic_5_desc')['sic_5_code'].value_counts()
counts2 = final['employee band'].value_counts().rename_axis('Employee Band').reset_index(name='Counts')
counts3 = final['incorpyear'].value_counts().rename_axis('Year incorporated').reset_index(name='Counts')
counts4 = final['accts_due'].value_counts().rename_axis('Accounts Next Due').reset_index(name='Counts')

print(counts1)
print(counts2)
print(counts3)
print(counts4)
print(overall)