import pandas as pd
import numpy as np 
from cleanco import prepare_terms, basename
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def cleanco(name):
    business_name = name
    terms = prepare_terms()
    result = basename(name, terms, prefix=False, middle=False, suffix=True)
    return result

directory1 = 'E:/Acquirze/'
file1 ='acquirz_all_cleaned_060720.csv'
directory2 = 'E:/Company house/'
file2 = 'CHcleancocomplete.csv'

df1 = pd.read_csv(directory1 + file1,encoding = "ISO-8859-1",low_memory=False, usecols=['company_name'])
df2 = pd.read_csv(directory1 + file2 ,encoding = "utf-8",low_memory=False)

df1['company_name'] = df1['company_name'].str.lower()
df2['Company'] = df2['Company'].str.lower()
print(df1.shape)
print(df2.shape)

df1['company_name'] = df1['company_name'].apply(str)
df1['Company'] = df1['company_name'].apply(lambda x : cleanco(x))
df1['Company'].replace('', np.nan, inplace=True)
df1 = df1[df1['Company'].notna()]

#df2.to_csv(directory1 + 'CHcleancocomplete.csv', index=False)

all_comphouse = pd.DataFrame(columns=['sort_gp','CompanyName','Company',])
all_comp = df2['Company'].astype(str).unique()
all_comp.sort()
all_comphouse['Company'] = all_comp

print(all_comphouse.head(3))
all_comphouse['sort_gp'] = all_comphouse['Company'].apply(lambda x: x[0])
comphouse_sort_gp = all_comphouse['sort_gp'].unique()

all_unknowns = pd.DataFrame(columns=['sort_gp','name','company','score'])
all_unknowns['company'] = df1['Company']
all_unknowns['sort_gp'] = all_unknowns['company'].apply(lambda x: x[0])

all_sort_gp = all_unknowns['sort_gp'].unique()

print(all_unknowns.head(5))
finalresult = []

for sortgp in all_sort_gp:
    print(sortgp)
    if sortgp in comphouse_sort_gp:
        comp_gp = all_comphouse.groupby(['sort_gp']).get_group(sortgp)
        choices = comp_gp['Company'].tolist()
        
        this_gp = all_unknowns.groupby(['sort_gp']).get_group(sortgp)
        print(this_gp.shape)
        gp_start = this_gp.index.min()
        gp_end = this_gp.index.max()
        print(gp_start, gp_end)
        y = []
        for company in this_gp['company']:
            result = process.extractOne(company, choices, scorer=fuzz.token_sort_ratio)
            x = [company]
            x.extend(result)
            y.append(x)
            print(company,x)
    else:
        comp_gp = ['none', 'none','nomatch']
        print(comp_gp)
    finalresult.append(y)

df3 = pd.DataFrame(finalresult, columns= ['company', 'match', 'score'])
df3.to_csv(directory1 + 'finalresults0607.csv', index=False)
print('Completed processing')


    
