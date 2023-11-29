# ====================================================================================================
# Python Test and Dataset | Mong | Python 3.10.6
# https://docs.google.com/spreadsheets/d/1J5p2t4mOHgdeq-p3g214JLr7wws-06KCA1v8nI2hazs/
# ----------------------------------------------------------------------------------------------------
# Import

import pandas as pd


# ----------------------------------------------------------------------------------------------------
# Prepare

## Input data
df1 = pd.read_excel('Python test and Dataset.xlsx', sheet_name='pricing_project_dataset') # PK = sku_id
df2 = pd.read_excel('Python test and Dataset.xlsx', sheet_name='platform_number') # PK = grass_region
df3 = pd.read_excel('Python test and Dataset.xlsx', sheet_name='exchange_rate') # PK = grass_region

## Rename columns
df2.rename(index=str, columns={'region':'grass_region', 'platform order':'platform_order'}, inplace=True)


# ----------------------------------------------------------------------------------------------------
# Run Question 1

## Calculate fields
# df1['sku_id'] = df1['shopee_item_id'].astype(str) + '_' + df1['shopee_model_id'].astype(str)
df1['is_cheaper'] = df1['shopee_model_competitiveness_status'].apply(lambda x: 1 if x=='Shopee < CPT' else 0)
df1['is_higher'] = df1['shopee_model_competitiveness_status'].apply(lambda x: 1 if x=='Shopee > CPT' else 0)
df1['num_sku'] = 1

## Aggregate data (PK = grass_region)
df1_coverage = df1[['grass_region','shopee_order']].groupby(['grass_region']).agg('sum').reset_index()
df1_coverage = pd.merge(df1_coverage, df2[['grass_region','platform_order']], how='left', on='grass_region')
df1_coverage['net_coverage'] = df1_coverage['shopee_order'] / df1_coverage['platform_order']

df1_competit = df1[['grass_region','is_cheaper','is_higher','num_sku']].groupby(['grass_region']).agg('sum').reset_index()
df1_competit['net_competitiveness'] = (df1_competit['is_cheaper'] - df1_competit['is_higher']) / df1_competit['num_sku']

df_answer_q1 = pd.DataFrame({'grass_region':['SG','TH','VN','ID','PH','MY']})
df_answer_q1 = pd.merge(df_answer_q1, df1_coverage[['grass_region','net_coverage']], how='left', on='grass_region')
df_answer_q1 = pd.merge(df_answer_q1, df1_competit[['grass_region','net_competitiveness','num_sku']], how='left', on='grass_region')


# ----------------------------------------------------------------------------------------------------
# Run Question 2

## Define priority
list_competitiveness = {1:'Shopee < CPT', 2:'Shopee = CPT', 3:'Shopee > CPT', 4:'Others'}
def map_competitivity(x):
	if x == 'Shopee < CPT':
		return 1
	if x == 'Shopee = CPT':
		return 2
	if x == 'Shopee > CPT':
		return 3
	else:
		return 4
## Calculate fields
df1_temp_q2 = df1.loc[:,['grass_region','category_group','seller_type','shopee_item_id']]
# df1_temp_q2['item_id_last2'] = df1['shopee_item_id'].apply(lambda x: x % 100) # For test only
df1_temp_q2['competitivity'] = df1['shopee_model_competitiveness_status'].apply(map_competitivity)

## Re-arrange data
df_answer_q2 = df1_temp_q2 \
	.groupby(['grass_region','category_group','seller_type','shopee_item_id']).agg('min').reset_index()
df_answer_q2 = df_answer_q2 \
	.sort_values(['grass_region','category_group','seller_type','shopee_item_id'])
df_answer_q2['competitiveness'] = df_answer_q2['competitivity'].apply(lambda x: list_competitiveness[x])


# ----------------------------------------------------------------------------------------------------
# Run Question 3

df1_temp_q3 = df1.loc[:,['grass_region','shopee_order']]
num_order_pct70 = float(df1_temp_q3.quantile(0.7)) # 140.0
df1_temp_q3['num_item'] = df1_temp_q3['shopee_order'].apply(lambda x: 1 if x>=num_order_pct70 else 0)
df1_temp_q3_agg = df1_temp_q3[['grass_region','num_item']].groupby(['grass_region']).agg('sum').reset_index()

df_answer_q3 = pd.DataFrame({'grass_region':['SG','TH','VN','ID','PH','MY']})
df_answer_q3 = pd.merge(df_answer_q3, df1_temp_q3_agg, how='left', on='grass_region')


# ----------------------------------------------------------------------------------------------------
# Write DF to CSV

df_answer_q1.to_csv('test_answer_q1.csv', encoding='utf-8-sig', index=False)
df_answer_q2.to_csv('test_answer_q2.csv', encoding='utf-8-sig', index=False)
df_answer_q3.to_csv('test_answer_q3.csv', encoding='utf-8-sig', index=False)

print('Completed')

