import pandas as pd

df = pd.read_csv(r'L:\Workspace\StreamLine\ESA\UnionFiles_Winter2018\CriticalHabitat\Lookup_CH_Clipped_Union_CntyInter_HUC2ABInter_20180612'
                 r'\CH_Flowering_Plants_Union_20180110_ClippedRegions_20180110_20180914.csv')

state = ['AL', 'AR', 'CO', 'DE', 'GA', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'MD', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE',
         'NJ', 'NM', 'NC', 'ND', 'OH', 'OK', 'PA', 'SC', 'SD', 'TN', 'TX', 'VA', 'WI', 'WV',  'WY']

df_states = df.loc[(df['STUSPS']).isin(state)]
df_states.to_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects'
                 r'\Risk Assessments\GMOs\isoxaflutole\Counties-Spring2019\CH_Flowering_27state.csv')

df = pd.read_csv(r'C:\Users\JConno02\Documents\Counties-Spring2019\Counties_Registration_EPA.csv')
df_cnty = df[['State','NAME']]
pivot = df_cnty.pivot(columns ='State')['NAME']
pivot.to_csv(r'C:\Users\JConno02\Documents\Counties-Spring2019\EPA_pivot_noplant.csv')

