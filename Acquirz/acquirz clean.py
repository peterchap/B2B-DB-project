import pandas as pd

directory = 'E:/Acquirze/'
filename = 'ESB_FEB05.csv'

df = pd.read_csv(directory+filename,sep='\t',encoding ="ISO-8859-1",engine='python', error_bad_lines=False) 


print(list(df.columns.values))
print("Overall", df.shape)
print(df['Personal_Email'].describe())
print(df['Generic_Email'].describe())
print(df['SIC_2007_2'].describe())
print('No email records', df[df['Personal_Email'].isnull() & df['Generic_Email'].isnull()].shape)

gens = df[df['Personal_Email'].isnull() & df['Generic_Email'].notnull()].copy()
print("Gens", gens.shape)
gens.drop_duplicates(subset='Generic_Email', keep='first',inplace=True)

gens.rename(columns={'Generic_Email' : 'email'},inplace=True)
gens.drop(columns=['Personal_Email'], inplace=True)




personal = df[df['Personal_Email'].notnull()].copy()
print("Personal", personal.shape)
personal.drop_duplicates(subset='Personal_Email', keep='first',inplace=True)

personal.rename(columns={'Personal_Email' : 'email'},inplace=True)
personal.drop(columns=['Generic_Email'], inplace=True)
print("Personal - deduped", personal.shape)
print("Gens - deduped", gens.shape)
all = personal.append(gens, ignore_index=True)
print("All",all.shape)
all.drop_duplicates(subset='email',inplace=True)
print(all.shape)
'''
counts=all['Sic07_2_desc'].value_counts().rename_axis('SIC Code').reset_index(name='Count')
counts.to_csv(directory + "SICcodecounts100221.csv", index=False)
'''
all.to_csv(directory + 'acquirz100221.csv', index=False)
