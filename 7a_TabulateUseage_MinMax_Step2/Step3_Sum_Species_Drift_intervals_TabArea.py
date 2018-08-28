import pandas as pd
import os
import datetime
import numpy as np

in_location = r'L:\ESA\Tabulates_Usage\Carbaryl'
suffixes = ['noadjust', 'adjEle','adjEleHab','adjHab']
use_lookup = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\SupportingTables\Carbaryl_Uses_lookup_20180430.csv'
in_acres_table = r'L:\ESA\CompositeFiles_Winter2018\R_Acres_by_region_20180110_GAP.csv'
region = 'CONUS'

master_list = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country','Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']


interval_step = 30
max_dis = 1501
out_folder = in_location + os.sep + 'SprayInterval_IntStep_{0}_MaxDistance_{1}'.format(str(interval_step), str(max_dis))
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
dir_folder = os.path.dirname(in_location)

bins = np.arange((0 - interval_step), max_dis, interval_step)
regions = ['AK', 'AS', 'CNMI', 'CONUS', 'GU', 'HI', 'PR', 'VI']

use_lookup_df = pd.read_csv(use_lookup)
usage_lookup_df = use_lookup_df.ix[:, ['FullName', 'Usage lookup', 'FinalColHeader', 'Type', 'Cell Size']].copy()
usage_lookup_df['Filename'] = usage_lookup_df['FullName'].map(lambda x: str(x) + "_euc.csv").astype(str)


def roll_up_table(df, dis_cols, use_nm):
    cols = df.columns.values.tolist()
    reindex_col = []
    out_cols = ['EntityID']
    for col in cols:
        if col in dis_cols:
            int_col = col.replace(',', '')
            new_col = "CONUS_" + use_nm + "_" + str(int_col)
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


def apply_distance_interval(in_df, final_col_value, out_df, type, cell_size, acres_for_calc):
    # apply_distance_interval(max, final_col_v, base_sp_df, type_use, r_cell_size, acres_for_calc)
    [in_df.drop(m, axis=1, inplace=True) for m in in_df.columns.values.tolist() if m.startswith('Unnamed')]
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

    # merges current use to running acres table to get to percent overlap

    sum_df_acres = pd.merge(group_df_by_zone_sum, acres_for_calc, on='EntityID', how='left')
    sum_df_acres.fillna(0, inplace =True)
    percent_overlap = calculation(type, sum_df_acres, cell_size, 'CONUS', 'regional range')

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
        msq_conversion = cell_size * cell_size
        # convert pixels to msq
        overlap = in_sum_df.copy()
        # overlap.ix[:, use_cols] *= msq_conversion
        # convert msq to acres
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
            overlap.drop(acres_col, inplace= True)
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
out_max = base_sp_df.copy()
out_min = base_sp_df.copy()
out_uniform = base_sp_df.copy()
create_directory(out_folder)

acres_df = pd.read_csv(in_acres_table)
acres_for_calc = acres_df.ix[:, ['EntityID', ('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']]
for group in ['min','max','avg']:
    in_location_group = in_location +os.sep+group
    list_csv = os.listdir(in_location_group)
    list_csv = [csv for csv in list_csv if csv.endswith('.csv')]
    for value in suffixes:
        if not os.path.exists(out_folder +os.sep +value):
            os.mkdir(out_folder +os.sep +value)
        c_list = [v for v in list_csv if v.endswith(value +"_"+group+ '.csv')]
        for csv in c_list:
            csv_lookup = csv.replace("_" + value +"_"+group+  '.csv', ".csv")
            for v in csv_lookup.split("_"):
                if v not in regions:
                    csv_lookup = csv_lookup.replace(v + "_", "")
                else:
                    break

            use_lookup_df_value = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'Usage lookup'].iloc[0]
            final_col_v = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'FinalColHeader'].iloc[0]

            type_use = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'Type'].iloc[0]
            r_cell_size = usage_lookup_df.loc[(usage_lookup_df['Filename'] == csv_lookup), 'Cell Size'].iloc[0]

            max = pd.read_csv(in_location_group + os.sep + csv)

            distance_cols = [c for c in max.columns.values.tolist() if c.startswith("VALUE")]
            max.ix[:, distance_cols] = max.ix[:, distance_cols].apply(pd.to_numeric)

            max.ix[:, 'VALUE_0'] = max.ix[:, 'Max in species range'].map(lambda x: x).astype(float)
            max = roll_up_table(max, distance_cols, use_lookup_df_value)
            per_overlap_interval = apply_distance_interval(max, final_col_v, base_sp_df, type_use, r_cell_size,
                                                           acres_for_calc)
            out_max = pd.merge(out_max, per_overlap_interval, on='EntityID', how='left')

            min = pd.read_csv(in_location_group + os.sep + csv)
            min.ix[:, 'VALUE_0'] = min.ix[:, 'Min in Species range'].map(lambda x: x).astype(float)
            min = roll_up_table(min, distance_cols, use_lookup_df_value)
            per_overlap_interval_min = apply_distance_interval(min, final_col_v, base_sp_df, type_use, r_cell_size,
                                                               acres_for_calc)
            out_min = pd.merge(out_min, per_overlap_interval_min, on='EntityID', how='left')

            uniform = pd.read_csv(in_location_group + os.sep + csv)
            uniform.ix[:, 'VALUE_0'] = uniform.ix[:, 'Uniform'].map(lambda x: x).astype(float)
            uniform = roll_up_table(uniform, distance_cols, use_lookup_df_value)
            per_overlap_interval_uni = apply_distance_interval(uniform, final_col_v, base_sp_df, type_use, r_cell_size,
                                                               acres_for_calc)
            out_uniform = pd.merge(out_uniform, per_overlap_interval_uni, on='EntityID', how='left')

        out_max_csv = out_folder +os.sep +value+ os.sep + 'Upper' + "_SprayInterval_"+value+ "_"+group+"_" + date + '.csv'
        out_max.to_csv(out_max_csv)

        out_min_csv = out_folder +os.sep +value+ os.sep + 'Lower' + "_SprayInterval_" +value+ "_"  +group+"_"+date + '.csv'
        out_min.to_csv(out_min_csv)

        out_uniform_csv = out_folder +os.sep +value+ os.sep + 'Uniform' + "_SprayInterval_"+value+ "_"+group+"_"  + date + '.csv'
        out_uniform.to_csv(out_uniform_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
