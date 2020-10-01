import pandas as pd
import os
species_zone_lookup = r'D:\Species\UnionFile_Spring2020\Range\LookUp_Grid_byProjections_20200427'
# D:\Species\UnionFile_Spring2020\CriticalHabitat\LookUp_Grid_byProjections_20200427
out_path =r''
table_name ='species_cnty_area_3.csv'
list_csv = os.listdir(species_zone_lookup)
sp_out = pd.DataFrame()
for t in list_csv:
    sp_data = pd.read_csv(species_zone_lookup + os.sep + t)
    # sp_df = sp_data[['EntityID', 'GEOID', 'COUNT']].copy()
    sp_df = sp_data[['EntityID', 'GEOID', 'Acres']].copy()
    sp_df['GEOID'] = sp_df['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
    sp_df['EntityID'] = sp_df['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
    filter_sp = sp_df.groupby(['EntityID', 'GEOID']).sum().reset_index()
    # print set(filter_sp['EntityID'].values.tolist())
    sp_out = pd.concat([sp_out, filter_sp], axis=0)
    print t, len(filter_sp), len(sp_out)

if 'Acres' not in sp_out.columns.values.tolist():
    sp_out['EntityID'] = sp_out['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
    sp_out['GEOID'] = sp_out['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
    sp_out['MSQ'] = sp_out['COUNT'] * 900
    sp_out['SP_Cnt_Acres'] = sp_out['MSQ'] * 0.000247
    sp_out.to_csv(out_path +os.sep+ table_name)
else:
    sp_out['SP_Cnt_Acres']=sp_out['Acres']
    sp_out.drop('Acres',axis=1,inplace=True)
    sp_out.to_csv(out_path +os.sep+ table_name)