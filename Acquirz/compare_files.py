import pandas as pd 

directory = "E:/Acquirze/"
file1 = 'ESB_FEB05_reformat.csv'
file2 = 'ESB_JAN21_reformat.csv'

df1 = pd.read_csv(directory+file1, low_memory=False)
df2 = pd.read_csv(directory+file2, low_memory=False)
print(df1.shape)
print(df2.shape)

df3 = (df1.merge(df2, on='company_urn', how='left', indicator=True)
     .query('_merge == "left_only"')
     .drop('_merge', 1))
print(df3.shape)