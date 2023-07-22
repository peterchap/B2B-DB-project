import pandas as pd

directory = "E:/Acquirze/"
file1 = "ESB_FEB05.csv"


df1 = pd.read_csv(directory+file1, sep='\t', encoding ="ISO-8859-1", low_memory=False)

print(list(df1.columns.values))

renamecols = {'Site URN' : 'company_urn','Contact URN' : 'contact_urn',  'Company_name' : 'company_name',\
    'Postcode_area' : 'postcode', 'Salutation' : 'title', 'First_Name' : 'first_name', 'Last_Name' : 'last_name',\
    'Job_Title' : 'job_title', 'Personal_Email' : 'personal_email', 'Generic_Email' : 'generic_email',\
    'SIC_2007_2' : 'sic_2_desc','SIC_2007_2_CODE' : 'sic_2_code',  'SIC_2007_5' : 'sic_5_desc',\
     'SIC_2007_5_CODE' : 'sic_5_code', 'Employees' : 'employees', 'Employee_Band' : 'employee_band'}

df1.rename(columns = renamecols, inplace=True)

print('new', list(df1.columns.values))

df1ok = df1[['company_urn','contact_urn',  'company_name', 'postcode', 'title', 'first_name', 'last_name', 'job_title',\
    'personal_email', 'generic_email', 'sic_2_code', 'sic_2_desc', 'sic_5_code', 'sic_5_desc', 'employees', 'employee_band']]

print(df1ok.shape)
print(list(df1ok.columns.values))



df1ok['email'] = df1ok['personal_email']
df1ok.loc[df1ok['personal_email'].isnull(), 'email'] = df1ok['generic_email']

df1ok.to_csv(directory + file1[:-4] + "_reformat.csv", index=True)