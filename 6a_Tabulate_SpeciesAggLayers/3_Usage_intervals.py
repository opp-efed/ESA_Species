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
master_list = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_Feb2017_20170410_b.csv'
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_']
chem_name = 'Carbaryl'
indiv_regions = ['CONUS']
nl48_regions = ['AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI']

csv_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\Range_Streamline_test\Range\Agg_Layers'
look_up_use = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\RangeUses_lookup.csv'
interval_step = 30
max_dis = 1501
bins = np.arange((0 - interval_step), max_dis, interval_step)

# ###########Static variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

csv_folder_FullRange = csv_folder + os.sep + 'MergeOverlap_FullRange'
csv_folder_RegionRange = csv_folder + os.sep + 'MergeOverlap_Region'
csv_folder_NL48Range = csv_folder + os.sep + 'MergeOverlap_NL48'
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
out_folder_final = file_type + os.sep + 'SprayInterval_IntStep_{0}_MaxDistance_{1}_{2}'.format(str(interval_step),
                                                                                               str(max_dis),
                                                                                               dir_folder.split('_')[1])
out_folder_temp = out_folder_final + os.sep + 'Intermediate_Tables'

nl48_other = out_folder_temp + os.sep + file_flag + "_NL48_SprayInterval_" + date + "_" + dir_folder.split("_")[
    1] + '.csv'
nl48_aa = out_folder_temp + os.sep + file_flag + "_NL48_AA_SprayInterval_" + date + "_" + dir_folder.split("_")[
    1] + '.csv'
out_csv = out_folder_temp + os.sep + file_flag + "_Working_SprayInterval_" + date + "_" + dir_folder.split("_")[
    1] + '.csv'

species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]

use_lookup = pd.read_csv(look_up_use)
use_lookup = use_lookup.loc[(use_lookup['chemical_use'] == 'x')]


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        # print "created directory {0}".format(DBF_dir)


def create_intervals(csv_list, csv_folder_location, regions_list, sp_df):
    out_df = sp_df.copy()
    out_df['EntityID'] = out_df['EntityID'].map(lambda x: x).astype(str)
    for c_csv in csv_list:
        print c_csv
        region = c_csv.split('_')[1]
        split_csv = c_csv.split("_")
        use_nm = split_csv[2]
        for t in split_csv:
            if t == region or t == use_nm or t == 'euc' or t == 'Merge':
                pass
            else:
                use_nm = use_nm + "_" + t
        use_nm = region + "_" + use_nm
        use_nm = use_nm.replace('_euc.csv', '')

        if use_nm not in use_lookup['FullName'].values.tolist():
            pass
        else:
            in_csv = csv_folder_location + os.sep + c_csv
            in_df = pd.read_csv(in_csv, dtype=object)

            if interval_step == 1:
                final_col_value = use_lookup.loc[use_lookup['FullName'] == use_nm, 'FinalColHeader'].iloc[0]
                sel_cols = [x for x in in_df.columns.values.tolist() if
                            x == 'EntityID' or x.startswith(final_col_value)]
                group_df_by_zone_sum = in_df.loc[:, sel_cols]
                out_df = pd.merge(out_df, group_df_by_zone_sum, on='EntityID', how='left')
                out_df.fillna(0, inplace=True)
            else:
                final_col_value = use_lookup.loc[use_lookup['FullName'] == use_nm, 'FinalColHeader'].iloc[0]
                [in_df.drop(m, axis=1, inplace=True) for m in in_df.columns.values.tolist() if m.startswith('Unnamed')]
                columns_in_df_numeric = [t for t in in_df.columns.values.tolist() if t.split("_")[0] in regions_list]
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
                group_df_by_zone_sum = group_df_by_zone_sum.reindex(
                    group_df_by_zone_sum.index.drop('Use_Interval')).reset_index()
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
    return out_df


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
create_directory(out_folder_final)
create_directory(out_folder_temp)

# Step 1 generate Max dirft for AA l48 and NL48 and calulate species with less than 1% overlap and 5%

# Step 1 drift ; move action area drift tables here filter 1% overlap species

#Step 2
#generate Max dirft for use layer l48 and NL48 remoce 1 and 5 % ovelrap species

for region in indiv_regions:
    aa_indiv_region = out_folder_final + os.sep + file_flag + "_{0}_{3}_AA_SprayInterval_{1}_{2}.csv".format(region, date,dir_folder.split("_")[1], chem_name)
    indiv_other = out_folder_final  + os.sep + file_flag + "_{0}_{3}_SprayInterval_{1}_{2}.csv".format(region, date,dir_folder.split("_")[
                                                                                                     1],chem_name)
    regions_list = [region]

    list_csv = os.listdir(csv_folder_RegionRange)
    list_csv = [csv for csv in list_csv if csv.endswith('.csv') and csv.split('_')[1] == region]

    df_out = create_intervals(list_csv,csv_folder_RegionRange, regions_list, base_sp_df)

    #TODO filter species with less than 1% overlap
    indiv_aa_cols = [x for x in df_out.columns.values.tolist() if x in col_include_output or (x.split('_')[0] == region and x.split('_')[2] == 'ActionArea' and x.split('_')[1] == chem_name)]
    #TODO filter species with less than 5% overlap
    indiv_other_cols = [x for x in df_out.columns.values.tolist() if
                        x in col_include_output or (x.split('_')[0] == region and x.split('_')[2] != 'ActionArea')]
    out_indiv_aa = df_out.loc[:, indiv_aa_cols]
    out_indiv_aa.to_csv(aa_indiv_region)
    out_conus_other = df_out.loc[:, indiv_other_cols]
    out_conus_other.to_csv(indiv_other)

aa_nl48_region = out_folder_temp + os.sep + file_flag + "_NL48_{2}_AA_SprayInterval_{0}_{1}_byregion.csv".format(date,
                                                                                                                 dir_folder.split(
                                                                                                                     "_")[
                                                                                                                     1],
                                                                                                                 chem_name)
nl48_other = out_folder_temp + os.sep + file_flag + "_NL48_{2}_SprayInterval_{0}_{1}_byregion.csv".format(date,
                                                                                                          dir_folder.split(
                                                                                                              "_")[1],
                                                                                                          chem_name)

list_csv = os.listdir(csv_folder_RegionRange)
list_csv = [csv for csv in list_csv if csv.endswith('.csv') and csv.split('_')[1] in nl48_regions]
df_out = create_intervals(list_csv, csv_folder_RegionRange, nl48_regions, base_sp_df)
nl48_aa_cols = [x for x in df_out.columns.values.tolist() if
                x in col_include_output or (x.split('_')[0] in nl48_regions and x.split('_')[2] == 'ActionArea')]
nl48_other_cols = [x for x in df_out.columns.values.tolist() if
                   x in col_include_output or (x.split('_')[0] in nl48_regions and x.split('_')[2] != 'ActionArea')]
out_nl48_aa = df_out.loc[:, nl48_aa_cols]
out_nl48_aa.to_csv(aa_nl48_region)
out_conus_other = df_out.loc[:, nl48_other_cols]
out_conus_other.to_csv(nl48_other)

aa_nl48_region_sum = out_folder_final + os.sep + file_flag + "_NL48_{2}_AA_SprayInterval_{0}_{1}.csv".format(date,
                                                                                                             dir_folder.split(
                                                                                                                 "_")[
                                                                                                                 1],
                                                                                                             chem_name)
nl48_other_sum = out_folder_final + os.sep + file_flag + "_NL48_{2}_SprayInterval_{0}_{1}.csv".format(date,
                                                                                                      dir_folder.split(
                                                                                                          "_")[1],
                                                                                                      chem_name)
aa_col = []
for col in out_nl48_aa.columns.values.tolist():
    if col not in col_include_output:
        if col.replace(col.split('_')[0], '', ) not in aa_col:
            aa_col.append((col.replace(col.split('_')[0], '', )))

other_col = []
for col in out_conus_other.columns.values.tolist():
    if col not in col_include_output:
        if col.replace(col.split('_')[0], '', ) not in other_col:
            other_col.append((col.replace(col.split('_')[0], '', )))

f_nl48_aa = base_sp_df.copy()
f_nl48_other = base_sp_df.copy()
for col in aa_col:
    sum_col = [x for x in out_nl48_aa.columns.values.tolist() if col.endswith(col)]
    f_nl48_aa[('NL48' + col)] = out_nl48_aa[sum_col].sum(axis=1)

for col in other_col:
    sum_col = [x for x in out_conus_other.columns.values.tolist() if col.endswith(col)]
    f_nl48_other[('NL48' + col)] = out_conus_other[sum_col].sum(axis=1)

f_nl48_aa.to_csv(aa_nl48_region_sum)
f_nl48_other.to_csv(nl48_other_sum)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)

## TODO reorganize into step 1, Step2 and step 3
## TODO check out put for ind region and sum NL48
## TODO Add in look to do an output for the FullRANGE
## ADD Include Acres Columns to the output tables

## TODO Generate AA max drift
    # Remove species that 1% from AA drift table
    # make list of species with 5% overlap would be removed from step 2, 3 tables tables
    # remove 1% and 5% filtered species from Step3_drift tables before outputs
##ADD in max drift and filter based on 1% overlap for Atrep 1
## Generare list of specices that are mad dirf 5% overlap these plus species with 1% overlap removed from step 2 and 3 drift tables
## Set up step 2 usage and NL48 max drive summary tables (combining step 4 and 5