import datetime
import os

import pandas as pd

# Title - Transforms out results by zone and summarize totals by species - final output is a master sum table of results
# by use and interval for each species

# TODO set up separate script that will spit out chem specific table with different interval include aerial and group
# inlocation
in_table = r'E:\Tabulated_NewComps\FinalBETables\CriticalHabitat\AllIntervals\CH_NL48_SprayInterval_20170109_All_cleaned.csv'
master_col = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'CH_GIS']
# master list
temp_folder = r'E:\Tabulated_NewComps\FinalBETables\CriticalHabitat\BE_intervals'
out_csv = temp_folder + os.sep + 'CH_NL48_Ag_NonAg_BE_Sum_CoOccur_SprayDriftIntervals_20170109.csv'
regions = ['AK', 'GU', 'HI', 'PR', 'VI', 'CNMI']  # CritHab NL48

#regions = ['AK', 'GU', 'HI', 'PR', 'VI', 'CNMI','AS'] #RANGE NL48
sp_index_cols = 8
bins = [0, 305, 765]  # meter conversion of 1000 and 2500 foot buffer round up to the nearest 5

useLookup = {
    'Ag': 'Ag',
    'Cattle Eartag': 'Cattle Eartag',
    'Developed': 'Developed',
    'Managed Forests': 'Managed Forests',
    'Nurseries': 'Nurseries',
    'Open Space Developed': 'Open Space Developed',
    'Right of Way': 'Right of Way',
    'Orchards and vineyards': 'Orchards and vineyards',
    'Other crops': 'Other crops',
    'Other grains': 'Other grains',
    'Pasture/Hay/Forage': 'Pasture/Hay/Forage',
    'Veg Ground Fruit': 'Veg Ground Fruit',
    'Carbaryl': 'Carbaryl_AA',
    'Diazinon': 'Diazinon_AA',
    'Chlorpyrifos': 'Chlorpyrifos_AA',
    'Malathion': 'Malathion_AA',
    'Methomyl': 'Methomyl_AA',
    'Golf Courses': 'Golf Courses',
    'NonCultivated': 'NonCultivated'
}

regionLookup = {
    'Ag': ['AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
    'Cattle Eartag': ['AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
    'Developed': ['AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
    'Managed Forests': ['AK', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
    'Nurseries': ['AK', 'PR', 'HI', 'VI'],
    'Open Space Developed': ['AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
    'Right of Way': ['AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
    'Orchards and vineyards': ['HI', 'PR'],
    'Other crops': ['HI', 'PR'],
    'Other grains': ['HI', 'PR'],
    'Pasture/Hay/Forage': ['HI', 'AK'],
    'Veg Ground Fruit': ['HI', 'PR'],
    'NonCultivated': ['HI', 'CNMI', 'PR'],
    'Golf Courses': ['AK', 'GU', 'HI', 'PR'],
    'Carbaryl': ['AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
    'Chlorpyrifos': ['AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
    'Diazinon': ['AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
    'Malathion': ['AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
    'Methomyl': ['AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI']}

# cols to include from master
col_included = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'CH_GIS']


# #####FUNCTION

def make_list_uses_byregion(list_uses_from_dict):
    use_list_region = []
    for use in list_uses_from_dict:
        use_final = use

        for region in regions:
            check_region = regionLookup[use]
            if region not in check_region:
                continue
            else:
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
    # print i
    break_use = i.split("_")
    use_lookup_value = break_use[1]
    print use_lookup_value

    use_group = break_use[0] + "_" + useLookup[use_lookup_value]

    current_group = [use for use in list_col if use.startswith(i)]
    grouped_use = use_df[current_group]
    error_check = grouped_use.iloc[0, 0]
    current_cols = grouped_use.columns.values.tolist()
    previous_col = []
    for value in bins:
        new_df = pd.DataFrame()
        binned_col = []
        for col in current_cols:
            get_interval = col.split('_')
            interval = int(get_interval[(len(get_interval) - 1)])

            if interval <= value and col not in previous_col:
                binned_col.append(col)
        # NOTE UN COMMENT THIS LOOP TO MAKE INTEVERAL NON CUMMALTIVE, ie only the percent in the interval
        # for p in binned_col:
        #     if p in previous_col:
        #         binned_col.remove(p)
        #
        #     previous_col.append(p)

        binned_df = grouped_use[binned_col]
        # print binned_df

        use_results_df = binned_df.apply(pd.to_numeric, errors='coerce')

        new_df[(str(use_group) + '_' + str(value))] = use_results_df.sum(axis=1)

        new_df = new_df.fillna(error_check)

        collapsed_df = pd.concat([collapsed_df, new_df], axis=1)

final_df = pd.concat([sp_info_df, collapsed_df], axis=1)

in_df_ag_col = final_df.columns.values.tolist()
col_reindex = []
for col in in_df_ag_col:
    region = col.split("_")[0]
    if col in master_col:
        pass
    elif region in regions:
        split_col = col.split("_")
        use_value = col.split("_")[1]
        distance = col.split("_")[(len(split_col) - 1)]
        use = useLookup[use_value]
        new_col = region + '_' + use + "_" + distance
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
