import pandas as pd

directory = 'E:/Acquirze/'
filename = 'acquirz_all_cleaned_060720.csv'

df = pd.read_csv(directory+filename,encoding ="ISO-8859-1",low_memory=False,usecols=['company_name','employees'])
print(df.shape)
df.drop_duplicates(subset=['company_name'], inplace=True)
df['company_name'] = df['company_name'].str.lower()
print(df.shape)
#selection = df[df['job_title']
#.str.contains('hr|human|recruitment|people|payroll|benefits|person|talent|ceo|cfo|cro|md|director',na=False, case=False)]
#print(selection.shape)
final = df[df['employees'].between(1,100)]
print(final.shape)


file = 'acquirz_jul20_cleaned_160720.csv'

df1 = pd.read_csv(directory+file,encoding ="ISO-8859-1",low_memory=False,usecols=['Company Name','Employee Band'])

df1 = df1[df1['Employee Band'].str.contains('A|B|C|D|E',na=False)]
df1.drop_duplicates(subset=['Company Name'], inplace=True)
df1['Company Name'] = df1['Company Name'].str.lower()
print(df1.shape)

df2 = df1.merge(final, left_on=['Company Name'], right_on=['company_name'], how='outer')
print(df2.shape)
#final.to_csv(directory + 'hr_selection_ 150720.csv', index = False)