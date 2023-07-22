import pandas as pd 
from fuzzywuzzy import fuzz
from collections import Counter


all_names = df['CompanyName'][df['RegAddress.PostTown']=='CAMBRIDGE'].unique()
names_freq = Counter()
for name in all_names:
    names_freq.update(str(name).split(" "))
key_words = [word for (word,_) in names_freq.most_common(30)]
print(key_words)

all_main_name = pd.DataFrame(columns=['sort_gp','names','alias','score'])
all_names.sort()
all_main_name['names'] = all_names
all_main_name['sort_gp'] = all_main_name['names'].apply(lambda x: x[0])

all_sort_gp = all_main_name['sort_gp'].unique()

def no_key_word(name):
    """check if the name contain the keywords in travel company"""
    output = True
    for key in key_words:
        if key in name:
            output = False
    return output

for sortgp in all_sort_gp:
    this_gp = all_main_name.groupby(['sort_gp']).get_group(sortgp)
    gp_start = this_gp.index.min()
    gp_end = this_gp.index.max()
    for i in range(gp_start,gp_end+1):
    
        # if self has not got alias, asign to be alias of itself
        if pd.isna(all_main_name['alias'].iloc[i]):
            all_main_name['alias'].iloc[i] = all_main_name['names'].iloc[i]
            all_main_name['score'].iloc[i] = 100
        
        # if the following has not got alias and fuzzy match, asign to be alias of this one
        for j in range(i+1,gp_end+1):
            if pd.isna(all_main_name['alias'].iloc[j]):
                fuzz_socre = fuzz.token_sort_ratio(all_main_name['names'].iloc[i],all_main_name['names'].iloc[j])
                if not no_key_word(all_main_name['names'].iloc[j]):
                    fuzz_socre -= 10
                if (fuzz_socre > 85):
                    all_main_name['alias'].iloc[j] = all_main_name['alias'].iloc[i]
                    all_main_name['score'].iloc[j] = fuzz_socre
                    
        if i % (len(all_names)//10) == 0:
            print("progress: %.2f" % (100*i/len(all_names)) + "%")