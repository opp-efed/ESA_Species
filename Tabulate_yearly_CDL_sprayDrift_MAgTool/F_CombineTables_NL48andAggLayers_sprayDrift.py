import pandas as pd
import datetime
import os

in_table_ag = r'E:\Tabulated_NewComps\NL48\CriticalHabitat\CH_NL48_SprayInterval_20170109_All.csv'

outlocation = 'E:\Tabulated_NewComps\NL48\CriticalHabitat'
out_csv = outlocation + os.sep + 'CH_NL48_SprayInterval_20170109_All_cleaned.csv'

master_list = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_June2016_20170216.xlsx'

master_col = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'CH_GIS','Source of Call final BE-Range','	WoE Summary Group','Source of Call final BE-Range']

regions = ['AK', 'GU', 'HI', 'AS', 'PR', 'VI', 'CNMI','AS','CONUS']

useLookup = {
             'Ag': 'Ag',
             'CattleEarTag': 'Cattle Eartag',
             'Developed': 'Developed',
             'ManagedForests': 'Managed Forests',
             'Nurseries': 'Nurseries',
             'OSD': 'Open Space Developed',
             'ROW': 'Right of Way',
             'Rangeland': 'Cattle Eartag',
             'NonCultivated': 'Non Cultivated',
             'OrchardsVineyards': 'Orchards and vineyards',
             'OtherCrops': 'Other crops',
             'OtherGrains': 'Other grains',
             'Pasture': 'Pasture/Hay/Forage',
             'VegetablesGroundFruit': 'Veg Ground Fruit',
             'Diazinon': 'Diazinon_AA',
             'Carbaryl': 'Carbaryl_AA',
             'Chlorpyrifos': 'Chlorpyrifos_AA',
             'Methomyl': 'Methomyl_AA',
             'Malathion': 'Malathion_AA',
             'usa': 'Golf Courses',
             'CCAP': 'NonCultivated',
             'NLCD': 'NonCultivated',
             }


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# master_list_df =  pd.read_csv(master_list)
master_list_df = pd.read_excel(master_list)
master_list_df = master_list_df.ix[:, master_col]

in_df_ag = pd.read_csv(in_table_ag, dtype=object)
in_df_ag['EntityID'] = in_df_ag['EntityID'].map(lambda x: x).astype(str)
in_df_ag_col = in_df_ag.columns.values.tolist()

for col in in_df_ag_col:
    if col.startswith('Acres'):
        in_df_ag.drop(col, axis=1, inplace=True)
        print 'dropped: ' + col
    else:
        pass
in_df_ag_col = in_df_ag.columns.values.tolist()

col_reindex = []

for col in in_df_ag_col:
    region = col.split("_")[0]
    if col in master_col:
        col_reindex.append(col)

    elif region in regions:
        split_col = col.split("_")
        use_value = col.split("_")[1]
        distance = col.split("_")[(len(split_col) - 1)]
        use = useLookup[use_value]
        new_col = region + '_' + use + "_" + distance
        col_reindex.append(new_col)
    else:
        in_df_ag.drop(col, axis=1, inplace=True)
        print 'dropped: ' + col


in_df_ag.columns = col_reindex
in_df_ag.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
