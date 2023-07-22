import pandas as pd

directory = 'E:/Acquirze/'
filename = 'acquirz_all_cleaned_060720.csv'

df = pd.read_csv(directory+filename,encoding ="ISO-8859-1",engine='python', error_bad_lines=False) 

selection = ['56101', '56102', '56103', '56301', '56302']

count = df[df['sic_5_code'].isin(selection)]

print(count['sic_5_code'].value_counts())

print(count.shape)