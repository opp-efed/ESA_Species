import datetime
import os

import numpy as np
import pandas as pd

# Title- Generate distance interval tables per user input from master percent overlap tables - this script is
# interchangeable with the on under GAP_PilotSpecies but kept  separate
#               1) Generates Spray Drift table tables for aggregated layers, AA, Ag and NonAG
#                   1a) NOTE this will sum steps in between intervals to generate a value for the interval step
# Static variables are updated once per update; user input variables update each  run

# ###############user input variables
species_file_type = 'Range'   # can be 'Range' or 'CH'
p_region = 'L48'  # can be L48 or NL48
folder_name_csv = 'MO_Region'  # can be MO_NL48Range or MO_FullRange NL48 or MO_Region
# 'MergeOverlap_Region' L48 or NL48
interval_step = 30
max_dis = 1501

# ########### Updated once per run-variables
master_list = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']


out_root = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
           r'\_ED_results\Tabulated' + os.sep + p_region + os.sep + species_file_type
csv_folder =  out_root + os.sep +'Agg_Layers' + os.sep + folder_name_csv


look_up_use = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
                  r'\_ExternalDrive\_CurrentSupportingTables\Uses_lookup_20180430.csv'
# look_up_use = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
#                   r'\_ExternalDrive\_CurrentSupportingTables\RangeUses_lookup.csv'

bins = np.arange((0 - interval_step), max_dis, interval_step)
regions = ['AK', 'AS', 'CNMI', 'CONUS', 'GU', 'HI', 'PR', 'VI']


# ###########Static variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

find_file_type = csv_folder.split(os.sep)
if 'Range' in find_file_type or 'range' in find_file_type:
    file_flag = 'R'
    species_file_type = 'Range'
else:
    file_flag = 'CH'
    species_file_type = 'CriticalHabitat'
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
file_type, dir_folder = os.path.split(csv_folder)
out_folder = file_type + os.sep + 'SprayInterval_IntStep_{0}_MaxDistance_{1}'.format(str(interval_step), str(max_dis))

out_csv = out_folder + os.sep + file_flag+ "_SprayInterval_" + date + "_" + \
          dir_folder.split("_")[1] + '.csv'



species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]

base_sp_df['EntityID'] = base_sp_df['EntityID'].map(lambda x: x).astype(str)
out_df = base_sp_df.copy()

use_lookup = pd.read_csv(look_up_use)


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        # print "created directory {0}".format(DBF_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_directory(out_folder)
list_csv = os.listdir(csv_folder)
list_csv = [csv for csv in list_csv if csv.endswith('.csv')]

for csv in list_csv:
    print csv
    region = csv.split('_')[1]
    split_csv = csv.split("_")
    use_nm = split_csv[2]
    for t in split_csv:
        if t == region or t == use_nm or t == 'euc' or t == 'Merge':
            pass
        else:
            use_nm = use_nm + "_" + t
    use_nm = region + "_" + use_nm
    use_nm = use_nm.replace('_euc.csv', '')
    final_col_value = use_lookup.loc[use_lookup['FullName'] == use_nm, 'FinalColHeader'].iloc[0]

    in_csv = csv_folder + os.sep + csv
    in_df = pd.read_csv(in_csv, dtype=object)
    [in_df.drop(m, axis=1, inplace=True) for m in in_df.columns.values.tolist() if m.startswith('Unnamed')]
    columns_in_df_numeric = [t for t in in_df.columns.values.tolist() if t.split("_")[0] in regions]
    in_df.ix[:, columns_in_df_numeric] = in_df.ix[:, columns_in_df_numeric].apply(pd.to_numeric)

    in_df['sum'] = in_df[columns_in_df_numeric].sum(axis=1)
    columns_in_df_numeric.insert(0, 'EntityID')
    in_table_w_values = in_df.loc[in_df['sum'] != 0].copy()
    in_table_w_values = in_table_w_values.loc[:, columns_in_df_numeric]

    transformed = in_table_w_values.T.reset_index()

    transformed.columns = transformed.iloc[0]
    transformed = transformed.reindex(transformed.index.drop(0))
    update_cols = transformed.columns.values
    update_cols[0] = 'Use_Interval'
    transformed.columns = update_cols

    transformed['Use_Interval'] = transformed['Use_Interval'].map(
        lambda x: str(x).split('_')[len(x.split('_')) - 1] if len(x.split('_')) > 2 else 'NaN').astype(int)
    transformed.ix[:, :] = transformed.ix[:, :].apply(pd.to_numeric)

    bin_labels = bins.tolist()
    bin_labels.remove((0 - interval_step))

    # breaks out into binned intervals and sums
    binned_df = transformed.groupby(pd.cut(transformed['Use_Interval'], bins, labels=bin_labels)).sum()
    binned_df.drop('Use_Interval', axis=1, inplace=True)
    binned_df = binned_df.reset_index()

    group_df_by_zone_sum = binned_df.transpose()  # transposes so it is species by interval and not interval by species

    # Makes the interval values the col header then drops the row with those values
    group_df_by_zone_sum.columns = group_df_by_zone_sum.iloc[0]
    group_df_by_zone_sum = group_df_by_zone_sum.reindex(group_df_by_zone_sum.index.drop('Use_Interval')).reset_index()
    # EntityID was the index, after resetting index EntityID can be added to col headers
    update_cols = group_df_by_zone_sum.columns.values.tolist()
    update_cols[0] = 'EntityID'
    group_df_by_zone_sum.columns = update_cols
    group_df_by_zone_sum['EntityID'] = group_df_by_zone_sum['EntityID'].map(lambda x: x).astype(str)

    # sets up list that will be used to populate the final col header the use name and interval value for
    # non-species cols ie use_nm "_" + interval, then re-assigns col header
    cols = group_df_by_zone_sum.columns.values.tolist()
    out_col = ['EntityID']

    for i in cols:
        if i in out_col:
            pass
        else:
            col = final_col_value + "_" + str(i)
            out_col.append(col)
    group_df_by_zone_sum.columns = out_col

    # merges current use to running out_df that included the desired species cols from master
    out_df = pd.merge(out_df, group_df_by_zone_sum, on='EntityID', how='left')
    out_df.fillna(0, inplace=True)  # any species w/o a value did not overlap with use
    out_df.to_csv(out_csv)  # saves a working df of all currently completed uses

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
