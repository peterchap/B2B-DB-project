import pandas as pd

directory1 = 'E:/Acquirze/'
file1 ='Aquirezall030820.csv'
file2 = 'matcheddetail_part2.csv'

df = pd.read_csv(directory1 + file1,encoding = "ISO-8859-1",low_memory=False)

df['incorpyear'] = pd.to_datetime(df['incorporationdate'], errors='coerce',dayfirst=True).dt.to_period('Y')
df['accts_due'] = pd.to_datetime(df['accounts.nextduedate']).dt.to_period('M')
df['postal_region'] = df['postcode'].str[:4]
counts5 = df['job_title'].value_counts().rename_axis('Job Title').reset_index(name='Counts')
overall = pd.DataFrame({'Total Database' : df.shape[0], 'Number of Unique Businesses' : df['companyname'].unique().shape[0]}, index=[0])
comps = df.drop_duplicates(subset='companyname', ignore_index=True)

counts1 = comps['sic_5_code'].value_counts().rename_axis('Sic Code').reset_index(name='Counts')
sicodes = comps.groupby('sic_5_desc')['sic_5_code'].value_counts().reset_index(name='Counts').sort_values(by=['Counts'], ascending=False)
postregion = comps['postal_region'].value_counts().rename_axis('Postal Region').reset_index(name='Counts')
counts2 = comps['employee band'].value_counts().rename_axis('Employee Band').reset_index(name='Counts')
counts3 = comps['incorpyear'].value_counts().rename_axis('Year incorporated').reset_index(name='Counts')
counts4 = comps['accts_due'].value_counts().rename_axis('Accounts Next Due').reset_index(name='Counts')


print(counts1)
print(sicodes)
print(postregion)
print(counts2)
print(counts3)
print(counts4)

writer = pd.ExcelWriter(directory1 + 'data_counts_v5.xlsx', engine='xlsxwriter')

# Position the dataframes in the worksheet.
overall.to_excel(writer, sheet_name='summary')  # Default position, cell A1.
sicodes.to_excel(writer, sheet_name='siccodes')  # Default position, cell A1.
postregion.to_excel(writer, sheet_name='postal region')
counts2.to_excel(writer, sheet_name='employee band')
counts3.to_excel(writer, sheet_name='incorpyear')
counts4.to_excel(writer, sheet_name='accts_due')
counts5.to_excel(writer, sheet_name='job_title')

writer.save()