import pandas as pd

directory = 'E:/Experian/'
file1 = 'Experian_270121_refesh.csv'
file2 = 'Experian_Refresh_Bad_20210225.csv'

df1 = pd.read_csv(directory + file1,encoding ='utf-8', low_memory=False)
df2 = pd.read_csv(directory + file2,encoding ='utf-8', low_memory=False)
print('df1', df1.shape)
print('df2', df2.shape)
print(df1['status'].value_counts())
df3 = df1.merge(df2, on='email', how='inner')
print(df3.shape)
df3['status'] = 'Bad'
df4 = (df1.merge(df2, on='email', how='left', indicator=True)
     .query('_merge == "left_only"')
     .drop('_merge', 1))
print(df4.shape)
final = pd.concat([df3,df4], ignore_index=True)
print(final.shape)
print(final['status'].value_counts())
final.to_csv(directory + 'Experian_260221_refesh.csv', index=False)
print('Completed Successfully')