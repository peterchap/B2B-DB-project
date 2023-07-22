import pandas as pd 

directory1 = 'E:/Acquirze/'
file1 ='acquirz_jul20_cleaned_160720.csv'
file2 = 'matched.csv'
directory2 = 'E:/Company house/'
file3 = 'BasicCompanyDataAsOneFile-2020-07-01.csv'

df1 = pd.read_csv(directory1 + file1,encoding = "ISO-8859-1",low_memory=False)
df1['Company Name'] = df1['Company Name'].str.lower()

print(df1.shape)

df2 = pd.read_csv(directory1 + file2,encoding = "ISO-8859-1",low_memory=False)
#df2['right_side'] = df2['right_side'].str.lower()
#df2['left_side'] = df2['left_side'].str.lower()
#df2 = df2[df2['similarity'] >= 0.93]
df2.drop_duplicates(subset='CompanyName_y', keep='first', inplace=True)


matched = df1.merge(df2, left_on='Company Name', right_on='Company Name',how='inner')
print('matched', matched.shape)

cols = ['CompanyName', 'CompanyStatus','IncorporationDate','RegAddress.PostCode','Accounts.NextDueDate', 'SICCode.SicText_1']
df3 = pd.read_csv(directory2 + file3 ,encoding = "utf-8",low_memory=False, usecols=cols)
df3['CompanyName'] = df3['CompanyName'].str.lower()

final = matched.merge(df3, left_on='CompanyName_y', right_on='CompanyName', how='inner')
print(final.shape)

final.to_csv(directory1 + 'matcheddetail.csv', index=False)