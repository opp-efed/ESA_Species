import pandas as pd
import os
import datetime
import numpy as np


# Author J.Connolly
# Internal deliberative, do not cite or distribute

# This script generates the species percent overlap tables by chemical from parent tables -
# summed to species and generated the summarized tables used for 7b_Generate Final Chemical Tables and Step4_ SummarizeBE
# can't skip over previously generated tables because summary table is also generated - is there a work around?

# runtime carbaryl 2 hours
chemical_name = '' # 'Carbaryl' 'Methomyl'
file_type = "Range"  # CriticalHabitat, Range

# if running tables not adjusted for the census change census to euc, and if adjusted by species change to census_species
suffixes = ['census']  # result options 'census','adjEle','adjEleHab','adjHab'

run_noadjust = True  # include unadjusted tables
# root path directory

# out tabulated root path
root_path  = r'out locations'
#Tables directory  one level done from chemical

folder_path = 'SprayInterval_IntStep_30_MaxDistance_1501\ParentTables'
in_location = root_path + os.sep + chemical_name + os.sep + file_type + os.sep + folder_path

use_lookup = r"path"+ os.sep + chemical_name + "Uses_lookup_20190409.csv"


if file_type =='Range':
    in_acres_table = r'path\R_Acres.csv'
    acres_type = 'Full Range' # DONT USE _ in identifier  Full Range, On OffField, Habitat, Elevation, Habitat_Elevations,
else:
    in_acres_table ="paths\CH_Acres.csv"
    acres_type = 'Full CH'  # Full CH

master_list = r"\MasterListESA_Feb2017_20190130.csv"
# out column header
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']

file_type_marker = os.path.basename(in_acres_table).split("_")[0]
interval_step = 30
max_dis = 1501
out_folder = os.path.dirname(in_location)
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
dir_folder = os.path.dirname(in_location)

bins = np.arange((0 - interval_step), max_dis, interval_step)
regions = ['AK', 'AS', 'CNMI', 'CONUS', 'GU', 'HI', 'PR', 'VI']

use_lookup_df = pd.read_csv(use_lookup)
usage_lookup_df = use_lookup_df.ix[:, ['FullName', 'Region','Usage lookup', 'FinalColHeader', 'Type', 'Cell Size']].copy()
usage_lookup_df['Filename'] = usage_lookup_df['FullName'].map(lambda x: str(x) + "_euc.csv").astype(str)



def roll_up_table(df, dis_cols, use_nm, c_region):
    cols = df.columns.values.tolist()
    reindex_col = []
    out_cols = ['EntityID']
    for col in cols:
        if col in dis_cols:
            int_col = col.split("_")[1]
            new_col = c_region+"_" + use_nm + "_" + str(int_col)
            out_cols.append(new_col)
            reindex_col.append(new_col)
        else:
            reindex_col.append(col)
    df.columns = reindex_col
    df = df.reindex(columns=out_cols)
    return df


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        # print "created directory {0}".format(DBF_dir)


def apply_distance_interval(in_df, final_col_value, type, cell_size, acres_for_calc, c_region):
    # apply_distance_interval(max, final_col_v, base_sp_df, type_use, r_cell_size, acres_for_calc)
    [in_df.drop(m, axis=1, inplace=True) for m in in_df.columns.values.tolist() if m.startswith('Unnamed')]
    columns_in_df_numeric = [t for t in in_df.columns.values.tolist() if t.split("_")[0] in regions]
    # print columns_in_df_numeric
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

    # merges current use to running acres table to get to percent overlap

    sum_df_acres = pd.merge(group_df_by_zone_sum, acres_for_calc, on='EntityID', how='left')
    sum_df_acres.fillna(0, inplace=True)
    if c_region == 'CONUS':
        type_overlap = 'regional range'
    else:
        type_overlap ='NL48 range'

    percent_overlap = calculation(type, sum_df_acres, cell_size, c_region, type_overlap)
    return percent_overlap


def calculation(type_fc, in_sum_df, cell_size, c_region, percent_type):

    # ASSUMES ONLY NUMERIC COLS ARE USE COLS AND ACRES COLS
    use_cols = in_sum_df.select_dtypes(include=['number']).columns.values.tolist()
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
        except:
            pass

        return overlap

    else:
        # TODO ADD IN VECTOR OVERLAP
        print "ERROR ERROR"


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]
base_sp_df['EntityID'] = base_sp_df['EntityID'].map(lambda x: x).astype(str)

create_directory(out_folder)

acres_df = pd.read_csv(in_acres_table)

for group in [ 'no adjustment', 'max', 'min', 'avg' ]:   # summarize unadjusted values
    print out_folder, group
    if group == 'no adjustment':
        if run_noadjust:
            out_df = base_sp_df
            replace_value = '_noadjust.csv'
            in_location_group = in_location + os.sep + group + os.sep+ 'noadjust'
            list_csv = os.listdir(in_location_group)
            c_list = [csv for csv in list_csv if csv.endswith('.csv')]
            for csv in c_list:
                # print csv
                region = csv.split("_")[0]
                csv_lookup = csv.replace(replace_value, ".csv")
                if csv_lookup not in usage_lookup_df['Filename'].values.tolist():
                    pass
                else:
                    acres_for_calc = acres_df.ix[:, ['EntityID', ('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']]
                    acres_for_calc.ix[:, [ ('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']] = acres_for_calc .ix[:, [ ('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']].apply(pd.to_numeric)
                    # print csv_lookup

                    use_lookup_df_value = \
                        usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'Usage lookup'].iloc[0]
                    final_col_v = \
                    usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'FinalColHeader'].iloc[0]
                    type_use = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'Type'].iloc[0]
                    r_cell_size = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'Cell Size'].iloc

                    # TODO Set up dtype lookup for read in csv
                    use_df = pd.read_csv(in_location_group + os.sep + csv)
                    print in_location_group + os.sep + csv
                    use_df ['EntityID'] = use_df ['EntityID'].map(lambda x: x).astype(str)
                    drop_col = [m for m in use_df .columns.values.tolist() if m.startswith('Unnamed')]
                    use_df.drop(drop_col, axis=1, inplace=True)
                    distance_cols = [c for c in use_df.columns.values.tolist() if c.startswith("VALUE")]

                    use_df.ix[:, distance_cols] = use_df.ix[:, distance_cols].apply(pd.to_numeric)
                    # Set up no adjustment

                    use_df = roll_up_table(use_df, distance_cols, use_lookup_df_value, region)

                    per_overlap_interval = apply_distance_interval(use_df , final_col_v, type_use, r_cell_size,acres_for_calc, region)

                    out_df =  pd.merge(out_df, per_overlap_interval, how='left', on='EntityID')
            create_directory(out_folder + os.sep + 'noadjust')
            out_csv = out_folder + os.sep + 'noadjust' + os.sep + file_type_marker+ '_UnAdjusted' + "_SprayInterval_noadjust_" + acres_type  + "_"+ date + '.csv'
            print out_csv
            out_df.fillna(0, inplace = True)
            out_df.to_csv(out_csv)

    else:
        for value in suffixes:
            print value, group
            out_upper = base_sp_df.copy()
            out_lower = base_sp_df.copy()
            out_uniform = base_sp_df.copy()
            if value == 'adjHab':
                replace_value = "_adjHabcensus_"+value + "_" + group + '.csv'
            else:
                replace_value ="_"+value + "_" + group + '.csv'

            in_location_group = in_location + os.sep + group + os.sep+ value
            list_csv = os.listdir(in_location_group)
            # print in_location_group
            c_list = [csv for csv in list_csv if csv.endswith('.csv')]
            for csv in c_list:
                # print csv
                region = csv.split("_")[0]
                csv_lookup = csv.replace(replace_value, ".csv")
                # print csv_lookup

                if csv_lookup not in usage_lookup_df['Filename'].values.tolist():
                    pass
                else:
                    # print csv_lookup
                    acres_for_calc = acres_df.ix[:, ['EntityID', ('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']]
                    acres_for_calc.ix[:, [ ('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']] = acres_for_calc .ix[:, [ ('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']].apply(pd.to_numeric)

                    use_lookup_df_value = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'Usage lookup'].iloc[0]
                    final_col_v = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'FinalColHeader'].iloc[0]
                    type_use = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'Type'].iloc[0]
                    r_cell_size = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'Cell Size'].iloc[0]

                    # TODO Set up dtype lookup for read in csv
                    use_df = pd.read_csv(in_location_group + os.sep + csv)
                    use_df ['EntityID'] = use_df ['EntityID'].map(lambda x: x).astype(str)
                    drop_col = [m for m in use_df .columns.values.tolist() if m.startswith('Unnamed')]
                    use_df.drop(drop_col, axis=1, inplace=True)
                    distance_cols = [c for c in use_df.columns.values.tolist() if c.startswith("VALUE")]
                    # print distance_cols
                    use_df.ix[:, distance_cols] = use_df.ix[:, distance_cols].apply(pd.to_numeric)
                    # use_df.to_csv(r'L:\Workspace\StreamLine\ESA\Tabulated_TabArea_HUCAB_Usage\Methomyl\Summarized Tables' + os.sep + csv +'_test.csv')
                    # print 'L:\Workspace\StreamLine\ESA\Tabulated_TabArea_HUCAB_Usage\Methomyl\Summarized Tables' + os.sep + csv +'_test.csv'

                    # Set up upper bound
                    upper_df = use_df.copy()
                    upper_df.ix[:, 'VALUE_0'] = upper_df.ix[:, 'Max in species range'].map(lambda x: x).astype(float)
                    upper_df = roll_up_table(upper_df, distance_cols, use_lookup_df_value, region)
                    per_overlap_interval = apply_distance_interval(upper_df, final_col_v, type_use, r_cell_size,
                                                                   acres_for_calc, region)
                    out_upper = pd.merge(out_upper, per_overlap_interval, how='left', on='EntityID')
                    # Set up lower bound
                    lower_df = use_df.copy()
                    lower_df.ix[:, 'VALUE_0'] = lower_df.ix[:, 'Min in Species range'].map(lambda x: x).astype(float)
                    lower_df = roll_up_table(lower_df, distance_cols, use_lookup_df_value, region)
                    per_overlap_interval = apply_distance_interval(lower_df, final_col_v, type_use, r_cell_size,
                                                                   acres_for_calc, region)
                    out_lower = pd.merge(out_lower, per_overlap_interval, how='left', on='EntityID')
                    # Set up uniform
                    uniform_df = use_df.copy()
                    uniform_df.ix[:, 'VALUE_0'] = uniform_df.ix[:, 'Uniform'].map(lambda x: x).astype(float)
                    uniform_df = roll_up_table(uniform_df, distance_cols, use_lookup_df_value, region)
                    per_overlap_interval = apply_distance_interval(uniform_df, final_col_v, type_use, r_cell_size,
                                                                   acres_for_calc, region)
                    out_uniform = pd.merge(out_uniform, per_overlap_interval, how='left', on='EntityID')
            create_directory(out_folder + os.sep + value)
            out_upper_csv = out_folder + os.sep + value + os.sep + file_type_marker + "_Upper_SprayInterval_" + acres_type  + "_"+ value + "_" + group + "_" + date + '.csv'
            out_lower_csv = out_folder + os.sep + value + os.sep + file_type_marker + "_Lower_SprayInterval_" + acres_type  + "_"+ value + "_" + group + "_" + date + '.csv'
            out_uniform_csv = out_folder + os.sep + value + os.sep + file_type_marker + "_Uniform_SprayInterval_" + acres_type  + "_"+value + "_" + group + "_" + date + '.csv'


            out_upper.fillna(0, inplace = True)
            out_upper.to_csv(out_upper_csv)
            out_lower.fillna(0, inplace = True)
            out_lower.to_csv(out_lower_csv)
            out_uniform.fillna(0, inplace = True)
            out_uniform.to_csv(out_uniform_csv)
            print out_uniform_csv
            print out_upper_csv
            print out_lower_csv


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
