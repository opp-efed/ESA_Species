import datetime
import os

import pandas as pd

# Title - Transforms out results by zone and summarize totals by species - final output is a master sum table of results
# by use and interval for each species

# TODO set up separate script that will spit out chem specific table with different interval include aerial and group
# inlocation
in_table = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\tabulated_results\CH_Use_byinterval_20161003.csv'

# master list
temp_folder = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\tabulated_results'
out_csv = temp_folder + os.sep + 'Sum_CoOccur_SprayDriftIntervals_20161003.csv'
regions = ['AK', 'GU', 'HI', 'AS', 'PR', 'VI', 'CONUS', 'CNMI']

bins = [0, 305, 765]  # meter conversion of 1000 and 2500 foot buffer round up to the nearest 5

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
             'Cultivated': 'Cultivated',
             'NonCultivated': 'Non Cultivated',
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
                'Cultivated': ['CONUS'],
                'NonCultivated': ['CONUS'],
                'PineSeedOrchards': ['CONUS'],
                'XmasTrees': ['CONUS'],
                'OrchardsVineyards': ['HI', 'PR'],
                'OtherCrops': ['HI', 'PR'],
                'OtherGrains': ['HI', 'PR'],
                'Pasture': ['HI'],
                'VegetablesGroundFruit': ['HI', 'PR']}

# cols to include from master
col_included = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'CH_GIS']


# #####FUNCTION

def make_list_uses_byregion(list_uses_from_dict):
    use_list_region = []
    for use in list_uses_from_dict:
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
            if regional_use not in use_list_region:
                use_list_region.append(regional_use)

    return use_list_region


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Sets up the intervals that are of interests for each of the uses
uses = sorted(useLookup.keys())
sp_table_df = pd.read_csv(in_table, dtype=object)
sp_info_df = sp_table_df.iloc[:, :7]
use_df = sp_table_df.iloc[:, 7:]
use_list = make_list_uses_byregion(uses)

collapsed_df = pd.DataFrame()

list_col = sp_table_df.columns.values.tolist()

for i in use_list:

    break_use = i.split("_")
    use_lookup_value = break_use[1]
    if use_lookup_value == 'CDL':
        use_lookup_value = break_use[3]
    use_group = break_use[0]+"_"+useLookup[use_lookup_value]


    current_group = [use for use in list_col if use.startswith(i)]
    #print current_group
    grouped_use = use_df[current_group]
    error_check = grouped_use.iloc[0,0]
    print error_check
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

        for p in binned_col:
            if p in previous_col:
                binned_col.remove(p)

            previous_col.append(p)

        binned_df = grouped_use[binned_col]
        #print binned_df

        use_results_df = binned_df.apply (pd.to_numeric, errors='coerce')

        new_df[(str(use_group) + '_' + str(value))] = use_results_df.sum(axis=1)

        new_df=new_df.fillna(error_check)

        collapsed_df = pd.concat([collapsed_df, new_df], axis=1)

final_df = pd.concat([sp_info_df, collapsed_df ], axis=1)


final_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
