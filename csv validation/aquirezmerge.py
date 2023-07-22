import pandas as pd
import recordlinkage

directory1 = 'E:/Acquirze/'
file1 ='acquirz_jul20_cleaned_160720.csv'
directory2 = 'E:/Company house/'
file2 = 'BasicCompanyDataAsOneFile-2020-07-01.csv'

df1 = pd.read_csv(directory1 +file1, encoding = "ISO-8859-1",low_memory=False)
df2 = pd.read_csv(directory2 +file2, encoding = "ISO-8859-1",low_memory=False)
print(df1.shape, df2.shape)

df1['Company Name'] = df1['Company Name'].astype(str)
df1['sort_gp'] = df1['Company Name'].apply(lambda x: x[0])
df1.drop_duplicates(subset='Company Name', inplace=True, ignore_index=True)
df1.set_index('Company Name', inplace=True, drop=False)
print(df1.shape)

df2['CompanyName'] = df2['CompanyName'].astype(str)
df2['sort_gp'] = df2['CompanyName'].apply(lambda x: x[0])
df2.drop_duplicates(subset='CompanyName', inplace=True, ignore_index=True)
df2.set_index('CompanyName', inplace=True, drop=False)

print(df2.shape)

indexer = recordlinkage.Index()
indexer.full()

candidates = indexer.index(df2, df1)
print(len(candidates))

compare = recordlinkage.Compare()
compare.exact('sort_gp', 'sort_gp', label='sort_gp')
compare.string('CompanyName',
            'Company Name',
            method='jarowinkler',
            threshold=0.9,
            label='Company Name')
compare.string('RegAddress.PostCode',
            'Postcode',
            threshold=0.85,
            label='postcode')
features = compare.compute(candidates, df2,df1)
features.to_csv(directory1  + 'reordlinkage results', index=False)