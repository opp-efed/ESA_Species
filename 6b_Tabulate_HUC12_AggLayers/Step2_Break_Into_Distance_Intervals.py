import datetime
import os

import numpy as np
import pandas as pd


# Title- Generate distance interval tables per user input from master percent overlap tables for HUC12 =
#               1) Generates Spray Drift table tables for aggregated layers, AA, Ag and NonAG and HUC12a
#                   1a) NOTE this will sum steps in between intervals to generate a value for the interval step
# Static variables are updated once per update; user input variables update each  run

# ###############user input variables
master_list = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\NHD_acresHUC2\acres_sum\AllHUC_acres.csv'
col_include_output = ['HUC_12']

csv_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\Range_Gap\HUC12\Agg_Layers\MergeOverlap_HUC12'
look_up_use = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\RangeUses_lookup.csv'
interval_step = 30
max_dis = 1501
bins = np.arange((0 - interval_step), max_dis, interval_step)

# ###########Static variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

file_type, dir_folder = os.path.split(csv_folder)
out_folder = file_type + os.sep + 'SprayInterval_IntStep_{0}_MaxDistance_{1}'.format(str(interval_step), str(max_dis))

out_csv = out_folder + os.sep +   "HUC12_SprayInterval_" + date + "_" + \
          dir_folder.split("_")[1] + '.csv'

huc_df = pd.read_csv(master_list, dtype=object)
[huc_df.drop(m, axis=1, inplace=True) for m in huc_df.columns.values.tolist() if m.startswith('Unnamed')]
huc_df['HUC_12'] = huc_df['HUC12'].map(lambda x: x).astype(str)
base_huc = huc_df.loc[:, col_include_output]
base_huc['HUC_12'] = base_huc['HUC_12'].map(lambda x: x).astype(str)

out_df = base_huc.copy()
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
bin_labels = bins.tolist()
bin_labels.remove((0 - interval_step))
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
    in_df['HUC_12'] = in_df['HUC_12'].map(lambda x: str(x)).astype(str)
    in_df['HUC_12'] = in_df['HUC_12'].map(lambda x: str(x) if len(x) == 12 else '0' + str(x))
    [in_df.drop(m, axis=1, inplace=True) for m in in_df.columns.values.tolist() if m.startswith('Unnamed')]
    columns_in_df_numeric = [t for t in in_df.columns.values.tolist() if t.split("_")[0] == 'CONUS']
    in_df.ix[:, columns_in_df_numeric] = in_df.ix[:, columns_in_df_numeric].apply(pd.to_numeric)

    in_df['sum'] = in_df[columns_in_df_numeric].sum(axis=1)
    columns_in_df_numeric.insert(0, 'HUC_12')
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


    # breaks out into binned intervals and sums
    binned_df = transformed.groupby(pd.cut(transformed['Use_Interval'], bins, labels=bin_labels)).sum()
    binned_df.drop('Use_Interval', axis=1, inplace=True)
    binned_df = binned_df.reset_index()


    group_df_by_zone_sum = binned_df.T  # transposes so it is species by interval and not interval by species
    # Makes the interval values the col header then drops the row with those values
    group_df_by_zone_sum.columns = group_df_by_zone_sum.iloc[0]
    group_df_by_zone_sum = group_df_by_zone_sum.reindex(group_df_by_zone_sum.index.drop('Use_Interval')).reset_index()

    # HUC_12 was the index, after resetting index HUC_12 can be added to col headers
    update_cols = group_df_by_zone_sum.columns.values.tolist()
    update_cols[0] = 'HUC_12'
    group_df_by_zone_sum.columns = update_cols
    group_df_by_zone_sum['HUC_12'] = group_df_by_zone_sum['HUC_12'].map(lambda x: x).astype(str)

    # sets up list that will be used to populate the final col header the use name and interval value for
    # non-species cols ie use_nm "_" + interval, then re-assigns col header
    cols = group_df_by_zone_sum.columns.values.tolist()
    out_col = ['HUC_12']

    for i in cols:
        if i in out_col:
            pass
        else:
            col = final_col_value + "_" + str(i)
            out_col.append(col)
    group_df_by_zone_sum.columns = out_col

    # merges current use to running out_df that included the desired species cols from master

    out_df = pd.merge(out_df, group_df_by_zone_sum, on='HUC_12', how='left')

    out_df.fillna(0, inplace=True)  # any species w/o a value did not overlap with use
    out_df.to_csv(out_csv)  # saves a working df of all currently completed uses

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
