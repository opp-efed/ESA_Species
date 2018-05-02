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
meta_data_table = outlocation + os.sep +'r07_sample_metadata.csv'
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
        sample =filter_df.copy()
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

data_df['gen_class'] = (data_df['gen_class']).map(lambda x: x).astype(str)  # TODO Check col name against Trips matrix and QA Table
data_df['gen_class'] = (data_df['gen_class']).map(lambda x: x.split('.')[0]).astype(str)
# data_df['gen_class'] = (data_df['gen_class']).map(lambda x: x).astype(int)  # TODO add back and update dict if blanks will be resolved

list_cdl = list(set(data_df['gen_class'].values.tolist()))
filtered_dict = {k: v for k, v in useLookup.items() if k in list_cdl}


for crop in filtered_dict.keys():
    sample_size = random_sample_size
    print 'Working on crop {0}'.format(useLookup[crop])
    ran_sample, meta_df, sample_size, total_sce  = filter_table(data_df, useLookup[crop], 'gen_class', sample_size , meta_df, crop )
    ran_sample.to_csv(outlocation + os.sep + 'r07_'+ useLookup[crop]+ "_" + 'random_sample' + str(sample_size )+'.csv') # TODO dd in the  NHD region when
    print 'Random sample can be found at {0}'.format(outlocation + os.sep + 'r07_'+ useLookup[crop]+ "_" + 'random_sample' + str(sample_size )+'.csv')
    meta_df = meta_df .append({'Crop': useLookup[crop], 'Total scenarios in region': total_sce , 'Sample size': str(sample_size)}, ignore_index=True)
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