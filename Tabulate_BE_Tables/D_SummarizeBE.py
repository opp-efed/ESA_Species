import datetime
import os

import pandas as pd

# Title - Transforms out results by zone and summarize totals by species - final output is a master sum table of results
# by use and interval for each species

# TODO set up separate script so that it will check for missing runs, right now if there is not datat in the master tables
# it eill not incluude use.  SEt up dict that will list the uses by region and then it can check to see if the run is missing
# see archiveds step 7
# inlocation
in_table = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\FinalTables_Range\MagTool\R_NL48_MagTool_SprayDrift_20170209_clean.csv'
master_col = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'CH_GIS','Source of Call final BE-Range','WoE Summary Group','Source of Call final BE-Range']
# master list
temp_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\FinalTables_Range\BETables'

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

out_csv = temp_folder + os.sep + 'R_NL48_BE_SprayDriftIntervals_' +date +'.csv'

#regions = ['CONUS']  # CONUS
regions = ['AK', 'GU', 'HI', 'AS', 'PR', 'VI', 'CNMI','AS'] #NL48

sp_index_cols = 10
bins = [0, 305, 765]  # meter conversion of 1000 and 2500 foot buffer round up to the nearest 5

useLookup = {'Corn': 'Corn',
             'Cotton': 'Cotton',
             'Rice': 'Rice',
             'Soybeans': 'Soybeans',
             'Wheat': 'Wheat',
             'Vegetables and Ground Fruit': 'Vegetables and Ground Fruit',
             'Orchards and Vineyards': 'Orchards and Vineyards',
             'Other Grains': 'Other Grains',
             'Other RowCrops': 'Other RowCrops',
             'Other Crops': 'Other Crops',
             'Pasture': 'Pasture',
             'Cattle Eartag': 'Cattle Eartag',
             'Developed': 'Developed',
             'Managed Forests': 'Managed Forests',
             'Nurseries': 'Nurseries',
             'Open Space Developed': 'Open Space Developed',
             'Right of Way': 'Right of Way',
             'Cull Piles': 'Cull Piles',
             'Cultivated': 'Cultivated',
             'Non Cultivated': 'Non Cultivated',
             'Pine seed orchards': 'Pine seed orchards',
             'Bermuda Grass': 'Bermuda Grass',
             'Golfcourses': 'Golfcourses',
             'Christmas Trees': 'Christmas Trees',
             'Carbaryl': 'Carbaryl_AA',
             'Diazinon': 'Diazinon_AA',
             'Chlorpyrifos': 'Chlorpyrifos_AA',
             'Malathion': 'Malathion_AA',
             'Methomyl': 'Methomyl_AA',
             'Alley Cropping': 'Alley Cropping',
             'zMethomylWheat': 'zMethomylWheat',
             'Ag':'Ag',

                 }





# #####FUNCTION

def make_list_uses_byregion(list_uses_from_dict):

    use_list_region = []
    for use in list_uses_from_dict:
        use_final = use
        for region in regions:
            regional_use = region + "_" + use_final
            if regional_use not in use_list_region:
                    use_list_region.append(regional_use)

    return use_list_region


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Sets up the intervals that are of interests for each of the uses
uses = sorted(useLookup.keys())
sp_table_df = pd.read_csv(in_table, dtype=object)
# print sp_table_df
sp_info_df = sp_table_df.iloc[:, :sp_index_cols]
use_df = sp_table_df.iloc[:, sp_index_cols:]

# print use_df
use_list = make_list_uses_byregion(uses)

collapsed_df = pd.DataFrame()

list_col = sp_table_df.columns.values.tolist()

for i in use_list:
    print i
    break_use = i.split("_")
    #print break_use
    use_lookup_value = break_use[1]
    #print use_lookup_value

    use_group = break_use[0] + "_" + useLookup[use_lookup_value]
    #print use_group
    current_group = [use for use in list_col if use.startswith(i)]

    if i == 'CONUS_Methomyl':

        for j in current_group:

            check = j.split("_")[1]

            if check=='MethomylWheat':
                current_group.remove(j)


        print current_group

    if len(current_group) == 0:
        continue
    grouped_use = use_df[current_group]
    #error_check = grouped_use.iloc[0, 0]
    current_cols = grouped_use.columns.values.tolist()
    previous_col = []
    for value in bins:

        new_df = pd.DataFrame()
        binned_col = []
        # NOTE IF ELSE OUTTER WITH GIVE USE SEPARATE FROM DRIFT
        # if value == bins[0]:
        #     for col in current_cols:
        #         get_interval = col.split('_')
        #         interval = int(get_interval[(len(get_interval) - 1)])
        #         if interval == bins[0]:
        #
        #             binned_col.append(col)
        # else:

        for col in current_cols:
            get_interval = col.split('_')
            interval = int(get_interval[(len(get_interval) - 1)])
            # if interval == 0:
            #     continue
            # else:
            if interval <= value and col not in previous_col:
                    binned_col.append(col)
        # NOTE UN COMMENT THIS LOOP TO MAKE INTEVERAL NON CUMMALTIVE, ie only the percent in the interval
        # for p in binned_col:
        #     if p in previous_col:
        #         binned_col.remove(p)
        #
        #     previous_col.append(p)
        print binned_col
        binned_df = grouped_use[binned_col]
        # print binned_df

        use_results_df = binned_df.apply(pd.to_numeric, errors='coerce')

        new_df[(str(use_group) + '_' + str(value))] = use_results_df.sum(axis=1)

        #new_df = new_df.fillna(error_check)

        collapsed_df = pd.concat([collapsed_df, new_df], axis=1)

final_df = pd.concat([sp_info_df, collapsed_df], axis=1)

in_df_ag_col = final_df.columns.values.tolist()
col_reindex = []
for col in in_df_ag_col:
    region_col = col.split("_")[0]
    if col in master_col:
        pass
    elif region_col in regions:
        split_col = col.split("_")
        use_value = col.split("_")[1]
        distance = col.split("_")[(len(split_col) - 1)]
        use = useLookup[use_value]
        new_col =region_col+ '_' + use + "_" + distance
        col_reindex.append(new_col)
    else:
        final_df.drop(col, axis=1, inplace=True)
        print 'dropped: ' + col
col_final = sorted(col_reindex)

col_reindex = []
for i in master_col:
    col_reindex.append(i)
for i in col_final:
    col_reindex.append(i)

final_df = final_df.reindex(columns=col_reindex)
final_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
