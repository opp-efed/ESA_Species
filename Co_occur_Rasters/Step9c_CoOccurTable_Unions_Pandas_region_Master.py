import datetime
import os

import numpy as np
import pandas as pd

# Title - Transforms out results by zone and summarize totals by species - final output is a master sum table of results
# by use and interval for each species

# TODO set up separate script that will spit out chem specific table with different interval include aerial and group
# inlocation
in_folder = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\tabulated_results\byspecies'

# master list
master_list = 'C:\Users\Admin\Documents\Jen\Workspace\MasterLists\MasterListESA_June2016_20160907.xlsx'
temp_folder = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\tabulated_results\Master'

# TODO set up a dict to read in the use index base on layer name
group_index = 1  # place to extract species group from tablename
SkipUses = []

# cols to include from master
col_included = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'CH_GIS']
regions = ['AK', 'GU', 'HI', 'AS', 'PR', 'VI', 'CONUS', 'CNMI']
# species groups that can be skipped
group_skip = []
# breaks out the intervals into bin
col_start = 1
labelCol = 0
interval_step = 30
max_dis = 1501
use_index = 1  # place to extract use from tablename this is not in a standard position
cdl_index = 3

useLookup = {'10x2': 'Corn',
             '20x2': 'Cotton',
             '30x2': 'Rice',
             '40x2': 'Soybean',
             '50x2': 'Wheat',
             '60x2': 'Veg Ground Fruit',
             '70x2': 'Orchards and vineyards',
             '80x2': 'Other grains',
             '90x2': 'Other row crops',
             '100x2': 'Other crops',
             '110x2': 'Pasture/Hay/Forage',
             'Ag': 'Ag',
             'CattleEarTag': 'Cattle Eartag',
             'Developed': 'Developed',
             'ManagedForests': 'Managed Forest',
             'Nurseries': 'Nurseries',
             'OSD': 'OSD',
             'ROW': 'ROW',
             'Rangeland': 'Cattle Eartag',
             'CullPiles': 'Cull Piles',
             'Cultivated_2015': 'Cultivated',
             'NonCultivated_2015': 'Non Cultivated',
             'PineSeedOrchards': 'Pineseed Orchards',
             'XmasTrees': 'Xmas Tree',
             'OrchardsVineyards': 'Orchards and vineyards',
             'OtherCrops': 'Other crops',
             'OtherGrains': 'Other grains',
             'Pasture': 'Pasture/Hay/Forage',
             'VegetablesGroundFruit': 'Veg Ground Fruit'
             }

regionLookup = {'10x2': ['CONUS'],
                '20x2': ['CONUS'],
                '30x2': ['CONUS'],
                '40x2': ['CONUS'],
                '50x2': ['CONUS'],
                '60x2': ['CONUS'],
                '70x2': ['CONUS'],
                '80x2': ['CONUS'],
                '90x2': ['CONUS'],
                '100x2': ['CONUS'],
                '110x2': ['CONUS'],
                'Ag': ['AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
                'CattleEarTag': ['CONUS', 'AK'],
                'Developed': ['CONUS', 'AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
                'ManagedForests': ['CONUS', 'AK', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
                'Nurseries': ['CONUS', 'AK', 'PR', 'HI', 'VI'],
                'OSD': ['CONUS', 'AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
                'ROW': ['CONUS', 'AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
                'Rangeland': ['CONUS', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI'],
                'CullPiles': ['CONUS'],
                'Cultivated_2015': ['CONUS'],
                'NonCultivated_2015': ['CONUS'],
                'PineSeedOrchards': ['CONUS'],
                'XmasTrees': ['CONUS'],
                'OrchardsVineyards': ['HI', 'PR'],
                'OtherCrops': ['HI', 'PR'],
                'OtherGrains': ['HI', 'PR'],
                'Pasture': ['HI'],
                'VegetablesGroundFruit': ['HI', 'PR']}

bins = np.arange((0 - interval_step), max_dis, interval_step)


def extract_species_info(master_in_table, col_from_master):
    check_extention = (master_in_table.split('.'))[1]
    if check_extention == 'xlsx':
        master_list_df = pd.read_excel(master_in_table)
    else:
        master_list_df = pd.read_csv(master_in_table)
    master_list_df['EntityID'] = master_list_df['EntityID'].astype(str)
    ent_df = master_list_df['EntityID']
    ent_list_included = master_list_df['EntityID'].values.tolist()
    sp_info_df = pd.DataFrame(master_list_df, columns=col_from_master)
    sp_info_header = sp_info_df.columns.values.tolist()

    return sp_info_df, sp_info_header, ent_list_included, ent_df


def set_up_intervals(use_int, bins_set_up_interval, out_header_int, use_intervals_dict):
    for i in bins_set_up_interval:
        if i < 0:
            continue
        nm_interval = str(use_int) + '_' + str(i)
        use_interval_list = use_intervals_dict.get(use_int)
        if use_interval_list is None:
            use_intervals_dict[use_int] = [nm_interval]
        else:
            use_interval_list.append(nm_interval)
        if nm_interval not in out_header_int:
            out_header_int.append(nm_interval)

    return use_intervals_dict, out_header_int


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Reads in all information for species that should considered
main_out_header = []

main_species_infoDF, main_sp_header, ent_list_master, ent_df = extract_species_info(master_list, col_included)
main_out_header.extend(main_sp_header)

# Sets up the intervals that are of interests for each of the uses
uses = sorted(useLookup.keys())
use_intervals = {}
for use in uses:
    if use.endswith('x2'):
        use_final = 'CDL_1015_' + use
    else:
        use_final = use

    for region in regions:
        check_region = regionLookup[use]
        if region not in check_region:
            continue
        else:
            regional_use = region + "_" + use_final
            use_intervals, main_out_header = set_up_intervals(regional_use, bins, main_out_header, use_intervals)

final_df = pd.DataFrame(columns=main_out_header)

final_df.drop('EntityID', axis=1, inplace=True)
final_df.insert(0, 'EntityID', ent_df)
final_df['EntityID'] = final_df['EntityID'].astype(str)

main_list_tables = os.listdir(in_folder)
main_list_table = [v for v in main_list_tables if v.endswith('.csv')]

for table in main_list_table:
    print table
    sp_table = in_folder + os.sep + table
    sp_table_df = pd.read_csv(sp_table, dtype=object)
    sp_table_df.drop('Unnamed: 0', axis=1, inplace=True)
    sp_table_df['EntityID'] = sp_table_df['EntityID'].astype(str)

    current_col = sp_table_df.columns.values.tolist()
    final_col = final_df.columns.values.tolist()

    add_col = []
    for j in final_col:
        if j not in current_col:
            add_col.append(j)

    sp_table_df = pd.concat([sp_table_df, pd.DataFrame(columns=add_col)])

    df_final = sp_table_df.reindex(columns=final_col)
    df_final = df_final.fillna(-33333)



    #df_final = pd.merge(sp_info_df, num, on='EntityID',how='outer')


    group_overlap_csv = temp_folder + os.sep + str(table)
    df_final.to_csv(group_overlap_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
