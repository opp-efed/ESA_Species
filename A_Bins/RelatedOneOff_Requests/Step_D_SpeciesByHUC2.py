import pandas as pd
huc_12_cross = r'L:\Workspace\ESA_Species\Range\HUC12\AllSpe\HUC12\GDB\R_AllSpe_FWS_NMFS_ByHUC12_20170328.txt'

df_cross = pd.read_csv(huc_12_cross)
[df_cross.drop(v, axis=1, inplace=True) for v in df_cross.columns.values.tolist() if v.startswith('Unnamed')]
huc2_df = pd.DataFrame(index=(list(range(0, 1800))),columns=['EntityID', 'HUC_2'])
huc_12_crosswalk_dict = df_cross.set_index('EntityID').T.to_dict('list')
spe_keys = huc_12_crosswalk_dict.keys()

row = 0
for i in spe_keys:
    try:
        huc12 = (huc_12_crosswalk_dict[i])[0].split(',')
    except AttributeError:
        huc12 =['nan']

    huc12 =['0' + str(x) if len (v) == 11 else x for x in huc12]
    huc_2= list(set([p[:2] if p[:2] != 'na' else 'nan' for p in huc12]))
    huc_2 = [x for x in huc_2 if len(x) != 0 ]
    huc2_df.iloc[row,0] = i
    huc2_df.iloc[row,1] = huc_2
    row +=1
print huc2_df.loc[huc2_df['EntityID'] == '24', 'HUC_2']

