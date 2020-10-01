import datetime
import os

import numpy as np
import pandas as pd

# Author J.Connolly USEPA

# Description: Generates the drift interval percent overlap tables by species that includes all UDLs for the chemical.

# This script has been approved for release by the U.S. Environmental Protection Agency (USEPA). Although
# the script has been subjected to rigorous review, the USEPA reserves the right to update the script as needed
# pursuant to further analysis and review. No warranty, expressed or implied, is made by the USEPA or the U.S.
# Government as to the functionality of the script and related material nor shall the fact of release constitute
# any such warranty. Furthermore, the script is released on condition that neither the USEPA nor the U.S. Government
# shall be held liable for any damages resulting from its authorized or unauthorized use.

# User input variables
chemical_name = 'Glyphosate'  # chemical name used for tracking
file_type = "Range"  # CriticalHabitat, Range

# flags to apply to the end of output file name for the different iterations of overlap.  This should always include
# 'census' for the standard no adjustment and aggregated PCT runs; but user can had in additional flags to other
# overlap scenarios that included supplemental information just as habitat
suffixes = ['census', 'adjHab']  # 'census' covers the no adjustment table can include others e.g. 'adjHab' result suffixes
pct_groups = ['no adjustment','max', 'min', 'avg']  # pct groups to include 'no adjustment', 'max', 'min', 'avg'

run_noadjust = True  # include unadjusted table; set to false to exclude

# out tabulated root path - ie Tabulated_[suffix] folder
root_path = r'D:\Tabulated_Habitat'

# Tables directory  where the working/intermediate tables are found; this will start:
# SprayInterval_IntStep_30_MaxDistance_1501
# script sets the output file structure
folder_path = 'SprayInterval_IntStep_30_MaxDistance_1501\ParentTables'
in_location = root_path + os.sep + chemical_name + os.sep + file_type + os.sep + folder_path

# input table identifying the uses and drift limits for the chemical
use_lookup = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs" \
             + os.sep + chemical_name + os.sep + "GLY_Uses_lookup_June2020_v2.csv"

# Acres tables for the species location calculation by pixel, and adjusted for the on/off site where the site
# locations are removed from the total area' DON'T USE _ in acres_type identifier
#Be sure acres table names starts with the CH_ or R_
if file_type == 'Range':
    # in_acres_table = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\R_Acres_Pixels_20200628.csv"
    #Be sure acres table names starts with the CH_ or R_
    in_acres_table = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\R_Glyphosate_OnOffAdjust_20200811.csv"
    acres_type = 'On OffField'  # Full Range, On OffField'  # Full Range, On OffField
else:
    in_acres_table = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\CH_Acres_Pixels_20200628.csv"
    acres_type = 'Full CH'  # Full CH

# location of master species list
master_list = "C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\MasterListESA_Dec2018_June2020.csv"
# out column header from the master species list
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS', 'WoE Summary Group', 'Critical_Habitat_YesNo', 'Migratory',
                      'Migratory_YesNo', 'CH_Filename', 'Range_Filename', 'L48/NL48']

# STATIC VARIABLES NO INPUT NEEDED
file_type_marker = os.path.basename(in_acres_table).split("_")[0]
interval_step = 30
max_dis = 1501
out_folder = os.path.dirname(in_location)

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

dir_folder = os.path.dirname(in_location)
# DATAFRAME USED TO TRACK INPUT USED FOR RESULTS
parameters_used = pd.DataFrame(columns=['Chemical Name', 'File Type', 'In Location', 'Use Lookup', 'Acres Table',
                                        'Acre Type', 'Species List', 'Out Base Location'])
parameters_used.loc[0, 'Chemical Name'] = chemical_name
parameters_used.loc[0, 'File Type'] = file_type
parameters_used.loc[0, 'In Location'] = in_location
parameters_used.loc[0, 'Use Lookup'] = use_lookup
parameters_used.loc[0, 'Acres Table'] = in_acres_table
parameters_used.loc[0, 'Acre Type'] = acres_type
parameters_used.loc[0, 'Species List'] = master_list
parameters_used.loc[0, 'Out Base Location'] = out_folder

# intervals used to summarize off-site transport
bins = np.arange((0 - interval_step), max_dis, interval_step)
regions = ['AK', 'AS', 'CNMI', 'CONUS', 'GU', 'HI', 'PR', 'VI']

# use lookup information
use_lookup_df = pd.read_csv(use_lookup)
usage_lookup_df = use_lookup_df.ix[:, ['FullName', 'Region', 'Usage lookup', 'FinalColHeader', 'Type', 'Cell Size']].copy()
# set up output table names from the use lookup table
usage_lookup_df['Filename'] = usage_lookup_df['FullName'].map(lambda x: str(x) + "_euc.csv").astype(str)

# FUNCTIONS


def roll_up_table(df, dis_cols, use_nm, c_region):
    # set the output column order for the drift columns
    cols = df.columns.values.tolist()
    reindex_col = []
    out_cols = ['EntityID']
    for col in cols:
        if col in dis_cols:
            int_col = col.split("_")[1]
            new_col = c_region + "_" + use_nm + "_" + str(int_col)
            out_cols.append(new_col)
            reindex_col.append(new_col)
        else:
            reindex_col.append(col)
    df.columns = reindex_col
    df = df.reindex(columns=out_cols)
    return df


def create_directory(dbf_dir):
    # creates a new folder
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        # print "created directory {0}".format(DBF_dir)


def apply_distance_interval(in_df, final_col_value, c_type,  c_acres_for_calc, c_region):
    # summarizes to the distance intervals set by the uses
    [in_df.drop(h, axis=1, inplace=True) for h in in_df.columns.values.tolist() if h.startswith('Unnamed')]
    columns_in_df_numeric = [t for t in in_df.columns.values.tolist() if t.split("_")[0] in regions]
    in_df.ix[:, columns_in_df_numeric] = in_df.ix[:, columns_in_df_numeric].apply(pd.to_numeric)
    in_table_w_values = in_df.groupby(['EntityID', ], as_index=False).sum()

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
    group_df_by_zone_sum['EntityID'] = group_df_by_zone_sum['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)

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

    # merges current use to running acres table to get to percent overlap from the summed area information
    sum_df_acres = pd.merge(group_df_by_zone_sum, c_acres_for_calc, on='EntityID', how='left')
    sum_df_acres.fillna(0, inplace=True)
    if c_region == 'CONUS':
        type_overlap = 'regional range'
    else:
        type_overlap = 'NL48 range'
    # calculates the percent overlap
    percent_overlap = calculation(c_type, sum_df_acres, c_region, type_overlap)
    return percent_overlap


def calculation(type_fc, in_sum_df, c_region, percent_type):
    # calculates the percent overlap
    # ASSUMES ONLY NUMERIC COLS ARE USED FOR USE COLS AND ACRES COLS
    acres_col = ''  # place holder for acres columns
    use_cols = in_sum_df.select_dtypes(include=['number']).columns.values.tolist()
    # columns from the acres table to use as the total area for the species is set by the user
    if percent_type == 'full range':
        acres_col = 'TotalAcresOnLand'
        in_sum_df.ix[:, use_cols] = in_sum_df.ix[:, use_cols].apply(pd.to_numeric, errors='coerce')
        use_cols.remove(acres_col)
        in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]
    elif percent_type == 'NL48 range':
        acres_col = 'TotalAcresNL48'
        in_sum_df.ix[:, use_cols] = in_sum_df.ix[:, use_cols].apply(pd.to_numeric, errors='coerce')
        use_cols.remove(acres_col)
        in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]
    elif percent_type == 'regional range':
        acres_col = 'Acres_' + str(c_region)
        in_sum_df.ix[:, use_cols] = in_sum_df.ix[:, use_cols].apply(pd.to_numeric, errors='coerce')
        use_cols.remove(acres_col)
        in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]

    if type_fc == "Raster":
        overlap = in_sum_df.copy()
        # Convert to acres
        overlap.ix[:, use_cols] *= 0.000247

        # generate percent overlap by taking acres of use divided by total acres of the species range
        overlap[use_cols] = overlap[use_cols].div(overlap[acres_col], axis=0)
        overlap.ix[:, use_cols] *= 100
        # Drop excess acres col- both regional and full range are included on input df; user defined parameter
        # percent_type determines which one is used in overlap calculation
        if percent_type == 'full range':
            overlap.drop('TotalAcresNL48', axis=1, inplace=True)
            overlap.drop(('Acres_' + str(c_region)), axis=1, inplace=True)

        elif percent_type == 'NL48 range':
            overlap.drop(('Acres_' + str(c_region)), axis=1, inplace=True)
            overlap.drop('TotalAcresOnLand', axis=1, inplace=True)

        elif percent_type == 'regional range':
            overlap.drop('TotalAcresOnLand', axis=1, inplace=True)
            overlap.drop('TotalAcresNL48', axis=1, inplace=True)

        try:
            overlap.drop(acres_col, inplace=True)
        except ValueError:
            pass
        return overlap


    else:
        print "ERROR ERROR"


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# reads in species information from master species list
species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]
base_sp_df['EntityID'] = base_sp_df['EntityID'].map(lambda x: x.split(".")[0]).astype(str)
# creates output folder
create_directory(out_folder)
# reads in the acres tables and sets entityid as a string
acres_df = pd.read_csv(in_acres_table)
acres_df['EntityID'] = acres_df['EntityID'].map(lambda x: x.split(".")[0]).astype(str)

for group in pct_groups:  # loops over pct groups
    print out_folder, group
    if group == 'no adjustment':
        if run_noadjust:
            # starts the output df with the base species information from the master species list
            out_df = base_sp_df
            replace_value = '_noadjust.csv'
            in_location_group = in_location + os.sep + group + os.sep + 'noadjust'
            list_csv = os.listdir(in_location_group)
            c_list = [csv for csv in list_csv if csv.endswith('.csv')]  # gets a list of results csv
            # set output location and csv for unadjusted summarized table
            out_csv = out_folder + os.sep + 'noadjust' + os.sep + file_type_marker + '_UnAdjusted' + "_SprayInterval_noadjust_" + acres_type + "_" + date + '.csv'
            if not os.path.exists(out_csv):
                # loops over results csvs
                for csv in c_list:
                    region = csv.split("_")[0]
                    csv_lookup = csv.replace(replace_value, ".csv")
                    if csv_lookup not in usage_lookup_df['Filename'].values.tolist():
                        pass
                    else:
                        acres_for_calc = acres_df.ix[:, ['EntityID', ('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']].copy()
                        acres_for_calc.ix[:,[('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']] = acres_for_calc.ix[:,[('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']].apply(pd.to_numeric)
                        use_lookup_df_value = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'Usage lookup'].iloc[0]
                        final_col_v = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'FinalColHeader'].iloc[0]
                        type_use = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'Type'].iloc[0]
                        # reads in the result csv
                        use_df = pd.read_csv(in_location_group + os.sep + csv)
                        # prints the current csv
                        print in_location_group + os.sep + csv
                        # confirms the EntityID col is a string
                        use_df['EntityID'] = use_df['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
                        drop_col = [m for m in use_df.columns.values.tolist() if m.startswith('Unnamed')]
                        use_df.drop(drop_col, axis=1, inplace=True)
                        # calculates the distance columns from overlap in the current csv
                        distance_cols = [c for c in use_df.columns.values.tolist() if c.startswith("VALUE")]
                        # sets the distance columns as numeric
                        use_df.ix[:, distance_cols] = use_df.ix[:, distance_cols].apply(pd.to_numeric)
                        # Set up no adjustment
                        use_df = roll_up_table(use_df, distance_cols, use_lookup_df_value, region)
                        # applies the distance intervals and calculates the percent overlap
                        per_overlap_interval = apply_distance_interval(use_df, final_col_v, type_use,
                                                                       acres_for_calc, region)
                        # merges use to the working out_df
                        out_df = pd.merge(out_df, per_overlap_interval, how='left', on='EntityID')
                # create output folder
                create_directory(out_folder + os.sep + 'noadjust')
                # prints where the out_csv is saved
                print("\nFinal tables can be found at:".format(out_csv))
                out_df.fillna(0, inplace=True)
                # saves the out csv
                out_df.to_csv(out_csv)

    else:
        # loop over the different overlap scenarios set by the user using the suffixes list
        for value in suffixes:
            print value, group   # prints the current scenario
            # started the three working dfs with the base species information from the master species list
            # each use is appended to this df before saving the final output
            out_upper = base_sp_df.copy()
            out_lower = base_sp_df.copy()
            out_uniform = base_sp_df.copy()
            # sets the scenario suffix
            if value == 'adjHab':
                replace_value = "_adjHabcensus_" + value + "_" + group + '.csv'
            else:
                replace_value = "_" + value + "_" + group + '.csv'
            # in locations
            in_location_group = in_location + os.sep + group + os.sep + value
            list_csv = os.listdir(in_location_group)
            c_list = [csv for csv in list_csv if csv.endswith('.csv')]  # get list of result csvs
            # output csvs
            out_upper_csv = out_folder + os.sep + value + os.sep + file_type_marker + "_Upper_SprayInterval_" + acres_type + "_" + value + "_" + group + "_" + date + '.csv'
            out_lower_csv = out_folder + os.sep + value + os.sep + file_type_marker + "_Lower_SprayInterval_" + acres_type + "_" + value + "_" + group + "_" + date + '.csv'
            out_uniform_csv = out_folder + os.sep + value + os.sep + file_type_marker + "_Uniform_SprayInterval_" + acres_type + "_" + value + "_" + group + "_" + date + '.csv'
            # confims output has not been created
            if not os.path.exists(out_upper_csv) and not os.path.exists(out_lower_csv) and not os.path.exists(
                    out_uniform_csv):
                for csv in c_list:  # loops over result csv
                    region = csv.split("_")[0]
                    csv_lookup = csv.replace(replace_value, ".csv")
                    if csv_lookup not in usage_lookup_df['Filename'].values.tolist():
                        pass
                    else:
                        # loads acres information
                        acres_for_calc = acres_df.ix[:, ['EntityID', ('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']]
                        acres_for_calc.ix[:, [('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']] = acres_for_calc.ix[:, [('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']].apply(pd.to_numeric)
                        # loads the use look up table
                        use_lookup_df_value = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'Usage lookup'].iloc[0]
                        final_col_v = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'FinalColHeader'].iloc[0]
                        type_use = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'Type'].iloc[0]
                        # print progress for user
                        print in_location_group + os.sep + csv
                        # reads in the current result table and sets the EntityID as a string
                        use_df = pd.read_csv(in_location_group + os.sep + csv)
                        use_df['EntityID'] = use_df['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
                        drop_col = [m for m in use_df.columns.values.tolist() if m.startswith('Unnamed')]
                        use_df.drop(drop_col, axis=1, inplace=True)
                        # extracts the distance values from the results csv and sets those columns to numeric
                        distance_cols = [c for c in use_df.columns.values.tolist() if c.startswith("VALUE")]
                        use_df.ix[:, distance_cols] = use_df.ix[:, distance_cols].apply(pd.to_numeric)
                        # extract the upper, lower and uniform distribution of treat area from the working results
                        # then summarizes to the spray drift intervals and calculations the percent overlap
                        # Set up upper bound
                        upper_df = use_df.copy()
                        upper_df.ix[:, 'VALUE_0'] = upper_df.ix[:, 'Upper in species range'].map(lambda x: x).astype(float)
                        # rolls us to the spray drift intervals and calculates the percent overlap
                        upper_df = roll_up_table(upper_df, distance_cols, use_lookup_df_value, region)
                        per_overlap_interval = apply_distance_interval(upper_df, final_col_v, type_use,
                                                                       acres_for_calc, region)
                        # merges to working out table
                        out_upper = pd.merge(out_upper, per_overlap_interval, how='left', on='EntityID')
                        # Set up lower bound
                        lower_df = use_df.copy()
                        lower_df.ix[:, 'VALUE_0'] = lower_df.ix[:, 'Lower in species range'].map(lambda x: x).astype(float)
                        # rolls us to the spray drift intervals and calculates the percent overlap
                        lower_df = roll_up_table(lower_df, distance_cols, use_lookup_df_value, region)
                        per_overlap_interval = apply_distance_interval(lower_df, final_col_v, type_use,
                                                                       acres_for_calc, region)
                        # merges to working out table
                        out_lower = pd.merge(out_lower, per_overlap_interval, how='left', on='EntityID')
                        # Set up uniform
                        uniform_df = use_df.copy()
                        uniform_df.ix[:, 'VALUE_0'] = uniform_df.ix[:, 'Uniform in species range'].map(lambda x: x).astype(float)
                        # rolls us to the spray drift intervals and calculates the percent overlap
                        uniform_df = roll_up_table(uniform_df, distance_cols, use_lookup_df_value, region)
                        per_overlap_interval = apply_distance_interval(uniform_df, final_col_v, type_use,
                                                                       acres_for_calc, region)
                        # merges to working out table
                        out_uniform = pd.merge(out_uniform, per_overlap_interval, how='left', on='EntityID')
                # creates the output locations
                create_directory(out_folder + os.sep + value)
                # saves the three output tables that includes all uses for the chemicals for the three different
                # treated area distributions
                out_upper.fillna(0, inplace=True)
                out_upper.to_csv(out_upper_csv)
                out_lower.fillna(0, inplace=True)
                out_lower.to_csv(out_lower_csv)
                out_uniform.fillna(0, inplace=True)
                out_uniform.to_csv(out_uniform_csv)
                # prints the output location for the user
                print("\nFinal tables can be found at:")
                print out_uniform_csv
                print out_upper_csv
                print out_lower_csv

# saves a csv with parameters used to generate tables
parameters_used.to_csv(out_folder + os.sep + 'Parameters_used_' + file_type + "_" + acres_type + "_" + date + '.csv')
print("Parameter file can be found at {0}".format(
    out_folder + os.sep + 'Parameters_used_' + file_type + "_" + acres_type + "_" + date + '.csv'))

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
