import datetime
import os

import pandas as pd

# ## TODO if the there is missing data in any of the PWC columns should those rows be excluded from random sample
# ## append to single output table - clear memory in between regions
# TODO update file names when looping between regions

# Original Data P:\GIS_Data\SAM_2018\bin and P:\GIS_Data\ScenarioTables2018
qa_table = r'P:\GIS_Data\SAM_2018\bin\Tables\fields_and_qc.csv'
matrix_table = r'P:\GIS_Data\ScenarioTables2018\r07_2010.csv'
outlocation = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\SAM\PWC_Selection'
meta_data_table = outlocation + os.sep + 'r07_sample_metadata.csv'
random_sample_size = 100

# TODO check the crop lookups to SAM table; possible replace this with that table
useLookup = {'10': 'Corn',
             '20': 'Cotton',
             '22': 'Drum Wheat',
             '23': 'Spring Wheat',
             '24': 'Winter Wheat',
             '30': 'Rice',
             '40': 'Soybeans',
             '60': 'Vegetables and Ground Fruit',
             '70': 'Other Orchards',
             '71': 'Grapes',
             '72': 'Citrus',
             '75': 'Other Trees',
             '80': 'Other Grains',
             '90': 'Other RowCrops',
             '100': 'Other Crops',
             '110': 'Pasture'
             }


def filter_table(table, value, col, sample_size, track_df, key):
    # key = [k for k, v in useLookup.iteritems() if v == value][0]

    filter_df = table.loc[(table[col] == key)].copy()
    if len(filter_df) < sample_size:
        sample = filter_df.copy()
        sample_size = len(filter_df)
    else:
        sample = filter_df.sample(n=sample_size)
    print sample_size

    return sample, track_df, sample_size, len(filter_df)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

# TODO Loop through all NHD regions?
meta_df = pd.DataFrame(columns=['Crop', 'Total scenarios in region', 'Sample size'])
input_df = pd.read_csv(qa_table)
print 'Loading input table {0}'.format(os.path.basename(matrix_table))
data_df = pd.read_csv(matrix_table, dtype=object)

# data_df['cdl'] = (data_df['cdl']).map(lambda x: x).astype(int)  # TODO Check col name against Trips matrix and QA Table
# list_cdl = list(set(data_df['cdl'].values.tolist()))
# print list_cdl

# if gen_class col is blank it is filtered - see email to Trip- these should be not be meaningful for PWC  - everything
# over 110 is also filtered only the keys in the useLookup are in the selections process.  If we need to included more
# 'crops' we can add it ot the lookup so it is no longer filtered

data_df['gen_class'] = (data_df['gen_class']).map(lambda x: x).astype(
    str)  # TODO Check col name against Trips matrix and QA Table
data_df['gen_class'] = (data_df['gen_class']).map(lambda x: x.split('.')[0]).astype(str)
# data_df['gen_class'] = (data_df['gen_class']).map(lambda x: x).astype(int)  # TODO add back and update dict if blanks will be resolved


list_cdl = list(set(data_df['gen_class'].values.tolist()))
filtered_dict = {k: v for k, v in useLookup.items() if k in list_cdl}

for crop in filtered_dict.keys():
    sample_size = random_sample_size
    print 'Working on crop {0}'.format(useLookup[crop])
    ran_sample, meta_df, sample_size, total_sce = filter_table(data_df, useLookup[crop], 'gen_class', sample_size,
                                                               meta_df, crop)
    ran_sample = ran_sample.reindex(
        columns=['state', 'cokey', 'mukey', 'weather_grid', 'bloom_begin', 'bloom_end', 'crop_prac', 'harvest_begin',
                 'harvest_begin_active', 'harvest_end', 'harvest_end_active', 'irr_pct', 'irr_type', 'maxcover_begin',
                 'maxcover_end', 'plant_begin', 'plant_end', 'season', 'root_zone_max', 'amxdr', 'cdl', 'cfact_cov',
                 'cfact_fal', 'cintcp', 'covmax', 'gen_class', 'slope', 'hydro_group', 'kwfact', 'mannings_n',
                 'slope_length', 'anetd', 'rainfall', 'sfac', 'cn_ag', 'cn_fallow', 'uslels', 'uslep', 'scenario_id',
                 'Numbre_of_horizons', 'thck1', 'thck2', 'thck3', 'thck4', 'thck5', 'thck6', 'thck7', 'thck8', 'thck9',
                 'thck10', 'thck11', 'thck12', 'thck13', 'bd_1', 'bd_2', 'bd_3', 'bd_4', 'bd_5', 'bd_6', 'bd_7', 'bd_8',
                 'bd_9', 'bd_10', 'bd_11', 'bd_12', 'bd_13', 'fc_1', 'fc_2', 'fc_3', 'fc_4', 'fc_5', 'fc_6', 'fc_7',
                 'fc_8', 'fc_9', 'fc_10', 'fc_11', 'fc_12', 'fc_13', 'wp_1', 'wp_2', 'wp_3', 'wp_4', 'wp_5', 'wp_6',
                 'wp_7', 'wp_8', 'wp_9', 'wp_10', 'wp_11', 'wp_12', 'wp_13', 'orgC_1', 'orgC_2', 'orgC_3', 'orgC_4',
                 'orgC_5', 'orgC_6', 'orgC_7', 'orgC_8', 'orgC_9', 'orgC_10', 'orgC_11', 'orgC_12', 'orgC_13', 'pH_1',
                 'pH_2', 'pH_3', 'pH_4', 'pH_5', 'pH_6', 'pH_7', 'pH_8', 'pH_9', 'pH_10', 'pH_11', 'pH_12', 'pH_13',
                 'sand_1', 'sand_2', 'sand_3', 'sand_4', 'sand_5', 'sand_6', 'sand_7', 'sand_8', 'sand_9', 'sand_10',
                 'sand_11', 'sand_12', 'sand_13', 'clay_1', 'clay_2', 'clay_3', 'clay_4', 'clay_5', 'clay_6', 'clay_7',
                 'clay_8', 'clay_9', 'clay_10', 'clay_11', 'clay_12', 'clay_13'])

    ran_sample.to_csv(outlocation + os.sep + 'r07_' + useLookup[crop] + "_" + 'random_sample' + str(
        sample_size) + '.csv')  # TODO dd in the  NHD region when
print 'Random sample can be found at {0}'.format(
    outlocation + os.sep + 'r07_' + useLookup[crop] + "_" + 'random_sample' + str(sample_size) + '.csv')
meta_df = meta_df.append(
    {'Crop': useLookup[crop], 'Total scenarios in region': total_sce, 'Sample size': str(sample_size)},
    ignore_index=True)

meta_df.to_csv(meta_data_table)
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)

# User drive crop selection - cut when we decided to make a table for each genclass
# possAnswer = filtered_dict.values()
# askQ = True
#
# # while askQ:
# #     print 'Crops to select from {0}'.format(sorted(filtered_dict.values()))
# #     user_input = raw_input('What crop would you like to look at: ')
# #     if user_input not in possAnswer:
# #         print 'This is not a valid answer'
# #     else:
# #         break
