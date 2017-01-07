import datetime
import os

import pandas as pd

# Title - Transforms out results by zone and summarize totals by species - final output is a master sum table of results
# by use and interval for each species

# TODO set up separate script that will spit out chem specific table with different interval include aerial and group

# inlocation
in_table = r'E:\Tabulated_NewComps\FinalBETables\CriticalHabitat\BE_intervals\CH_AllUses_BE_20170106.csv'
master_col = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'CH_GIS']
# master list
temp_folder = r'E:\Tabulated_NewComps\FinalBETables\WideArea'
out_csv = temp_folder + os.sep + 'CH_FinalBE_WideAreaOnly_20170106.csv'
sp_index_cols = 12
col_reindex = ['EntityID', 'comname', 'sciname', 'family', 'status_text', 'pop_abbrev', 'Des_CH', 'Critical_Habitat_',
               'Migratory', 'Migratory_', 'WideAreaOnly', 'Corn', 'Cotton', 'Orchards and Vineyards', 'Other Crops',
               'Other Grains', 'Other RowCrops', 'Pasture', 'Rice', 'Soybeans', 'Vegetables and Ground Fruit', 'Wheat',
               'Developed', 'Managed Forest', 'Nurseries', 'Open Space Developed', 'Pine seed orchards', 'Right of Way',
               'Christmas Trees', 'Golf Courses', 'Cattle Eartag', 'Cull Piles', 'Ag', 'Mosquito Control',
               'Wide Area Use', 'CH_GIS', 'Group']

NE_Extinct = ['19', '26', '68', '122', '141', '6345', '9433', '9435', '9437', '9445', '9447', '9451', '9455', '9463',
              '9481', '10582']

NLAA_Extinct = ['16', '23', '64', '77', '93', '100', '105', '109', '191', '1953', '78', '1302', '91']

LAA_QualReport = ['7', '459', '461', '463', '471', '485', '2891', '7115', '9126', '9707', '10485', '11175', '11176',
                  '11191', '11192', '11193', '154', '153']
NLAA_QualReport = ['2510', '5232', '10144', '10145', '460', '474']

NE_QualReport = ['472', '473']
# check on last 3 that occur on Moa why did we excluded in draft
NLAA_OutsideUse = ['598', '499', '606','153','164','165','177']

collapses_dict = {
    'Ag': ['AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765', 'VI_Ag_765'],
    'Cattle Eartag': ['AK_Cattle Eartag_765',  'CNMI_Cattle Eartag_765',
                      'CONUS_Cattle Eartag_765', 'GU_Cattle Eartag_765', 'HI_Cattle Eartag_765',
                      'PR_Cattle Eartag_765'],
    'Christmas Trees': ['CONUS_Christmas Trees_765'],
    'Corn': ['CONUS_Corn_765'],
    'Cotton': ['CONUS_Cotton_765'],
    'Cull Piles': ['CONUS_Cull Piles_765'],
    'Developed': ['AK_Developed_765',  'CNMI_Developed_765', 'CONUS_Developed_765',
                  'GU_Developed_765', 'HI_Developed_765', 'PR_Developed_765'],
    'Golf Courses': ['AK_Golf Courses_765', 'GU_Golf Courses_765', 'HI_Golf Courses_765', 'PR_Golf Courses_765',
                     'CONUS_Golfcourses_765'],
    'Managed Forest': ['CONUS_Managed Forest_765', 'AK_Managed Forests_765', 'CNMI_Managed Forests_765',
                       'GU_Managed Forests_765', 'HI_Managed Forests_765', 'PR_Managed Forests_765',
                       'VI_Managed Forests_765'],
    'Nurseries': ['AK_Nurseries_765', 'CONUS_Nurseries_765', 'HI_Nurseries_765', 'PR_Nurseries_765',
                  'VI_Nurseries_765'],
    'Open Space Developed': ['AK_Open Space Developed_765',
                             'CNMI_Open Space Developed_765', 'CONUS_Open Space Developed_765',
                             'GU_Open Space Developed_765', 'HI_Open Space Developed_765',
                             'PR_Open Space Developed_765'],
    'Orchards and Vineyards': ['CONUS_Orchards and Vineyards_765', 'HI_Orchards and vineyards_765',
                               'PR_Orchards and vineyards_765'],
    'Other Crops': ['CONUS_Other Crops_765', 'HI_Other crops_765', 'PR_Other crops_765'],
    'Other Grains': ['CONUS_Other Grains_765', 'HI_Other grains_765', 'PR_Other grains_765'],
    'Other RowCrops': ['CONUS_Other RowCrops_765'],
    'Pasture': ['CONUS_Pasture_765', 'AK_Pasture/Hay/Forage_765', 'HI_Pasture/Hay/Forage_765', ],
    'Pine seed orchards': ['CONUS_Pine seed orchards_765'],
    'Rice': ['CONUS_Rice_765'],
    'Right of Way': ['AK_Right of Way_765', 'CNMI_Right of Way_765', 'CONUS_Right of Way_765',
                     'GU_Right of Way_765', 'HI_Right of Way_765', 'PR_Right of Way_765'],
    'Soybeans': ['CONUS_Soybeans_765'],
    'Vegetables and Ground Fruit': ['HI_Veg Ground Fruit_765', 'PR_Veg Ground Fruit_765',
                                    'CONUS_Vegetables and Ground Fruit_765'],
    'Wheat': ['CONUS_Wheat_765'],
    'Mosquito Control': [],
    'Wide Area Use': []

}


def wide_area_check(row):
    if row['Ag'] <= 0.4 and row['Cattle Eartag'] <= 0.4 and row['Christmas Trees'] <= 0.4 and row['Corn'] <= 0.4 \
            and row['Cotton'] <= 0.4 and row['Cull Piles'] <= 0.4 and row['Developed'] <= 0.4 \
            and row['Golf Courses'] <= 0.4 and row['Managed Forest'] <= 0.4 and row['Nurseries'] <= 0.4 \
            and row['Open Space Developed'] <= 0.4 and row['Orchards and Vineyards'] <= 0.4 \
            and row['Other Crops'] <= 0.4 and row['Other Grains'] <= 0.4 and row['Other RowCrops'] <= 0.4 \
            and row['Pasture'] <= 0.4 and row['Pine seed orchards'] <= 0.4 and row['Right of Way'] <= 0.4 \
            and row['Rice'] <= 0.4 and row['Soybeans'] <= 0.4 and row['Ag'] <= 0.4 and row['Ag'] <= 0.4:
        if row['EntityID'] not in NE_Extinct:
            if row['EntityID'] not in NLAA_Extinct:
                if row['EntityID'] not in NLAA_OutsideUse:
                    if row['EntityID'] not in LAA_QualReport:
                        if row['EntityID'] not in NLAA_QualReport:
                            return 'Yes'
        else:
            return 'No'

def clean_up_columns(row, column):
    if row['Des_CH'] == 'Not Prudent':
        return 'CritHab Found Not Prudent'
    elif row['Critical_Habitat_'] == 'No':
        return 'No CritHab'
    elif row['CH_GIS'] == 'FALSE':
        return 'No GIS'
    else:
        value = (row[column])
        return value

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
list_uses = collapses_dict.keys()
print list_uses
# Sets up the intervals that are of interests for each of the uses

sp_table_df = pd.read_csv(in_table, dtype=object)
# print sp_table_df
sp_info_df = sp_table_df.iloc[:, :sp_index_cols]
use_df = sp_table_df.iloc[:, sp_index_cols:]
# print use_df

collapsed_df = pd.DataFrame(data=sp_info_df)

for use in list_uses:
    print use
    binned_col = list(collapses_dict[use])

    if not use == 'Mosquito Control':
        if not use == 'Wide Area Use':
            binned_df = use_df[binned_col]
            # print binned_df
            use_results_df = binned_df.apply(pd.to_numeric, errors='coerce')
            collapsed_df[(str(use))] = use_results_df.sum(axis=1)
        else:
            collapsed_df.ix[:, str(use)] = 100
    else:
        collapsed_df.ix[:, str(use)] = 100
# Step 1
collapsed_df['WideAreaOnly'] = collapsed_df.apply(lambda row: wide_area_check(row), axis=1)
collapsed_df['WideAreaOnly'] = collapsed_df.apply(lambda row: clean_up_columns(row, 'WideAreaOnly'), axis=1)

final_df = collapsed_df.reindex(columns=col_reindex)
final_df.to_csv(out_csv)
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
