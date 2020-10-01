import pandas as pd
import datetime
import os

WholeRange= True
in_tables_nona = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\Agg_layers\NonAg\CriticalHabitat\CH_L48_NonAg_SprayInterval_20170508_WholeRange.csv'
in_table_ag = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\Agg_layers\Ag\CriticalHabitat\CH_L48_Ag_SprayInterval_20170508_WholeRange.csv'
in_table_aa ='L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\Agg_layers\AAs\CriticalHabitat\CH_L48_AAs_SprayInterval_20170508_WholeRange.csv'

outlocation = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\Agg_layers'
out_csv = outlocation + os.sep + 'tCH_MagTool_SprayDrift_WholeRange_20170508.csv'
master_list = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_June2016_20170216.xlsx'

in_acres_list = [r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tables\CH_Acres_by_region_20170208.csv',
                  r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tables\R_Acres_by_region_20161216.csv']


master_col = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'CH_GIS','Source of Call final BE-Range','WoE Summary Group','Source of Call final BE-Critical Habitat']

region = 'CONUS'


useLookup = {'10x2': 'Corn',
             '20x2': 'Cotton',
             '30x2': 'Rice',
             '40x2': 'Soybeans',
             '50x2': 'Wheat',
             '60x2': 'Vegetables and Ground Fruit',
             '70x2': 'Orchards and Vineyards',
             '80x2': 'Other Grains',
             '90x2': 'Other RowCrops',
             '100x2': 'Other Crops',
             '110x2': 'Pasture',
             'Ag': 'Ag',
             'CattleEarTag': 'Cattle Eartag',
             'Developed': 'Developed',
             'ManagedForests': 'Managed Forest',
             'Nurseries': 'Nurseries',
             'OSD': 'Open Space Developed',
             'ROW': 'Right of Way',
             'Rangeland': 'Cattle Eartag',
             'CullPiles': 'Cull Piles',
             'Cultivated': 'Cultivated',
             'NonCultivated': 'Non Cultivated',
             'PineSeedOrchards': 'Pine seed orchards',
             'OrchardsVineyards': 'Orchards and vineyards',
             'OtherCrops': 'Other crops',
             'OtherGrains': 'Other grains',
             'Pasture': 'Pasture/Hay/Forage',
             'VegetablesGroundFruit': 'Veg Ground Fruit',
             'Diazinon' : 'Diazinon_AA',
             'Carbaryl': 'Carbaryl_AA',
             'Chlorpyrifos':'Chlorpyrifos_AA',
             'Methomyl':'Methomyl_AA',
             'Malathion':'Malathion_AA',
             'bermudagrass2' : 'Bermuda Grass',
             'usa':'Golfcourses',
             'XmasTrees': 'Christmas Trees',
             'alleycropping2': 'Alley Cropping',
             'wheat2': 'zMethomylWheat'



             }
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

path_check_ch_r, sum_table = os.path.split(in_tables_nona)
path, ch_r_folder = os.path.split(path_check_ch_r)
if ch_r_folder == 'Range':
    in_acres_table = in_acres_list[1]
else:
    in_acres_table = in_acres_list[0]
# master_list_df =  pd.read_csv(master_list)
master_list_df = pd.read_excel(master_list)
master_list_df = master_list_df.ix[:, master_col]
final_df = master_list_df
final_df_working = final_df
final_col = final_df.columns.values.tolist()

acres_df = pd.read_csv(in_acres_table, dtype= object)
acres_df['EntityID'] = acres_df['EntityID'].map(lambda x: x).astype(str)

if WholeRange:
    in_acres = acres_df[('TotalAcresOnLand')].map(lambda x: x).astype(float).map(lambda x: x).astype(
    float)
    list_acres = in_acres.values.tolist()
    se = pd.Series(list_acres)
    final_df[('TotalAcresOnLand')] = se.values

else:
    in_acres = acres_df[('Acres_' + str(region))].map(lambda x: x).astype(float).map(lambda x: x).astype(
    float)
    list_acres = in_acres.values.tolist()
    se = pd.Series(list_acres)
    final_df[('Acres_' + str(region))] = se.values




final_df['EntityID'] = final_df['EntityID'].map(lambda x: x).astype(str)

in_df_ag = pd.read_csv(in_table_ag, dtype= object)
in_df_ag['EntityID'] = in_df_ag['EntityID'].map(lambda x: x).astype(str)
in_df_ag_col = in_df_ag.columns.values.tolist()

for v in in_df_ag_col:
    if v == 'EntityID':
        pass
    else:
        if v in master_col:
            print v
            try:
                in_df_ag_col.remove(v)
            except:
                pass
        if not v.startswith(region):
            try:
                in_df_ag_col.remove(v)
            except:
                pass

in_df_ag = in_df_ag.reindex(columns=in_df_ag_col)



# in_df_non_ag = pd.read_csv(in_tables_nona, dtype= object)
# in_df_non_ag['EntityID'] = in_df_non_ag['EntityID'].map(lambda x: x).astype(str)
# in_df_nonag_col = in_df_non_ag.columns.values.tolist()
# for v in in_df_nonag_col:
#     if v == 'EntityID':
#         pass
#     else:
#         if v in master_col:
#             try:
#                 in_df_nonag_col.remove(v)
#             except:
#                 pass
#         if not v.startswith(region):
#             try:
#                 in_df_nonag_col.remove(v)
#                 continue
#             except:
#                 pass
# in_df_non_ag = in_df_non_ag.reindex(columns=in_df_nonag_col)
#
# in_df_aa= pd.read_csv(in_table_aa, dtype= object)
# in_df_aa['EntityID'] = in_df_aa['EntityID'].map(lambda x: x).astype(str)
# in_df_aa_col = in_df_aa.columns.values.tolist()
#
# for v in in_df_aa_col:
#     if v == 'EntityID':
#         pass
#     else:
#         if v in master_col:
#             try:
#                 in_df_aa_col.remove(v)
#             except:
#                 pass
#         if not v.startswith(region):
#             try:
#                 in_df_aa_col.remove(v)
#                 continue
#             except:
#                 pass
# in_df_aa = in_df_aa.reindex(columns=in_df_aa_col)

# out_df = pd.merge( in_df_aa,in_df_ag, on='EntityID', how='inner')
# out_df = pd.merge( out_df ,in_df_non_ag, on='EntityID', how='inner')
# out_df.to_csv(outlocation + os.sep + 'Temp.csv')
#
# final_df = pd.merge(final_df, out_df, on='EntityID', how='outer')
# final_df = final_df.fillna(0)
final_col = final_df .columns.values.tolist()
for col in final_col:
    if col == 'Acres_CONUS':
        pass
    elif col.startswith('Acres'):
        final_df.drop(col,axis=1, inplace=True)
        print 'dropped: '+ col
    else:
        pass
final_col = final_df .columns.values.tolist()
col_reindex =[]
for col in final_col:
    if col.startswith('CONUS_CDL'):
        use_value = col.split("_")[3]
        distance = col.split("_")[4]
        use = useLookup[use_value]
        new_col = 'CONUS_' + use+ "_"+distance
        col_reindex.append(new_col)
    elif col.startswith('CONUS_'):
        split_col = col.split("_")
        use_value = col.split("_")[1]
        distance = col.split("_")[(len(split_col)-1)]
        use = useLookup[use_value]
        new_col = 'CONUS_' + use+ "_"+distance
        col_reindex.append(new_col)
    else:
        col_reindex.append(col)
print col_reindex
final_df.columns = col_reindex
final_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
