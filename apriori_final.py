import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
from sqlalchemy import create_engine
import os

output_path = ' ' # {p}
sample_size = 

def assure_path_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)

def encode_units(x):
    if x <= 0:
        return 0
    if x >= 1:
        return 1

assure_path_exists(output_path)

engine = create_engine('  ')

rules_final = pd.DataFrame()
for country in ('Hong Kong', 'Singapore', 'Philippines', 'Thailand'):
    if country == 'Hong Kong':
        cnt = 'hk'
    elif country == 'Singapore':
        cnt = 'sg'
    elif country == 'Philippines':
        cnt = 'ph'
    elif country == 'Thailand':
        cnt = 'th'

    print('Working on {C}...'.format(C=country))

    user_title = pd.read_sql_query("""select device_sk, pd.group_series_name, pd.area_name from 
    (select device_sk, pd_sk from bi.vv_hist_det_usr 
    where vv_date_hk > current_date-91
    and screen_name = 'Video Player' 
    and device_sk in 
    (select distinct device_sk from bi.vv_hist_det_usr 
    where vv_date_hk > current_date-91
    and screen_name = 'Video Player' 
    and platform_name = 'APP'
    and country = '{C}'
    order by random()
    limit '{s}')
    group by 1,2
    ) vv
    left join bi.vw_pd_mst pd on vv.pd_sk = pd.pd_sk
    group by 1,2,3
    having area_name = '{C}'
    order by 1,3,2""".format(C=country,s=sample_size), engine)

    user_title.insert(3, 'watched', 1, allow_duplicates=True)
    user_title.to_csv('{p}/{c}_user_title.csv'.format(p=output_path, c=cnt), index=False)

    basket = (user_title[user_title['area_name'] == country]
              .groupby(['device_sk', 'group_series_name'])['watched']
              .sum().unstack().reset_index().fillna(0)
              .set_index('device_sk'))

    basket_sets = basket.applymap(encode_units)
    frequent_itemsets = apriori(basket_sets, min_support=0.005, use_colnames=True)
    rules = association_rules(frequent_itemsets, metric="lift", min_threshold=0.1)

    rules.insert(9, 'country', country, allow_duplicates=True)
    rules_final = rules_final.append(rules)

print('Output in progress...')
rules_final.to_csv('{p}/output.csv'.format(p=output_path),index=False)
print('Finished!')

