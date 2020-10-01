import datetime
import os
import arcpy

import pandas as pd

# Title- Generate overlap tables from zone species rasters to use layers results;
#               1) Generates tables for aggregated layers, AA, Ag and NonAG
#                       1a) The final merged output are used to generate distance interval table for spray drift; and
#                           summarized BE table (0, 305m and 765)
# TODO Add look to read in vector table once vector overlap final

# Static variables are updated once per update; user input variables update each  run

# ASSUMPTIONS
# Species group is found in index position 1 of all input result tables when split by '_'
# All raster are 30 meter cells - note previously VI and CNMI has some use with a different cell size

# ###############user input variables
overwrite_inter_data = True

raw_results_csv = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
                  r'\_ED_results\Results\Results_PolBoundaries\Agg_layers'

# raw_results_csv = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects' \
#                   r'\ESA\_ExternalDrive\_CurrentResults\Results_diaz\L48\Agg_Layers\Range'

ground = '305'
aerial = '763'
find_file_type = raw_results_csv.split(os.sep)
# ########### Updated once per run-variables
master_list = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename']

if 'Range' in find_file_type or 'range' in find_file_type:
    look_up_fc = r'L:\ESA\UnionFiles_Winter2018\R_Clipped_union_IntersectCntys.gdb'
                  r'\_ExternalDrive\_CurrentSupportingTables\RangeUses_lookup.csv'
    file_type = 'R_'
    species_file_type = 'Range'
    in_acres_table = r'L:\ESA\CompositeFiles_Winter2018\R_Acres_by_region_20180110_GAP.csv'
    look_up_fips = ''
else:
    look_up_fc = r'L:\ESA\UnionFiles_Winter2018\CriticalHabitat\CH_Clipped_Union_20180110.gdb'
    look_up_use = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects' \
              r'\ESA\_ExternalDrive\_CurrentSupportingTables\CH_Uses_lookup.csv'
    species_file_type = 'CH'
    file_type = 'CH_'
    in_acres_table = r'L:\ESA\CompositeFiles_Winter2018\CH_Acres_by_region_20180110.csv'
    look_up_fips = ''



find_file_type = raw_results_csv.split(os.sep)

out_root = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
           r'\_ED_results' + os.sep + species_file_type

out_results = out_root + os.sep + 'Agg_Layers'

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

arcpy.env.workspace = look_up_fc
list_fc = arcpy.ListFeatureClasses()

arcpy.env.workspace = look_up_fips
list_fips_fc = arcpy.ListFeatureClasses()

species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]

list_sp = species_df['EntityID'].values.tolist()
acres_df = pd.read_csv(in_acres_table)
acres_df['EntityID'] = acres_df['EntityID'].astype(str)
use_lookup = pd.read_csv(look_up_use)


# ###Functions


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        # print "created directory {0}".format(dbf_dir)


def melt_df(df_melt):
    cols = df_melt.columns.values.tolist()
    id_vars_melt = []
    val_vars = []
    for k in cols:
        val_vars.append(k) if type(k) is long else id_vars_melt.append(k)

    df_melt_row = pd.melt(df_melt, id_vars=id_vars_melt, value_vars=val_vars, var_name='melt_var',
                          value_name='EntityID')

    df_melt_row['EntityID'].fillna('None', inplace=True)
    df_melt_row = df_melt_row.loc[df_melt_row['EntityID'] != 'None']
    df_melt_row.drop('melt_var', axis=1, inplace=True)
    df_melt_row.ix[:, id_vars_melt] = df_melt_row.ix[:, id_vars_melt].apply(pd.to_numeric)
    sum_by_ent = df_melt_row.groupby('EntityID').sum()

    return sum_by_ent


def collapse_state(df):
    entity_id = df['EntityID'].values.tolist()
    df.drop('EntityID', axis=1, inplace=True)
    out_df_state = df.T.reset_index()
    out_df_state['STUSPS'] = out_df_state['index'].map(lambda x: str(x)[:2]).astype(str)
    out_df_state.drop('index', axis=1, inplace=True)
    sum_by_state = out_df_state.groupby('STUSPS').sum()

    df_out_state_t = sum_by_state.T
    df_out_state_t_reindex = df_out_state_t.reset_index()
    final_columns = df_out_state_t_reindex.columns.values.tolist()
    del final_columns[0]

    final_columns.insert(0, 'EntityID')
    df_out_state_t_reindex.columns = final_columns

    se = pd.Series(entity_id )
    df_out_state_t_reindex['EntityID'] = se.values
    return df_out_state_t_reindex


def parse_tables(in_table, in_row_sp):
    in_row_sp['ZoneSpecies'] = in_row_sp['ZoneSpecies'].apply(
        lambda x: x.replace('[', '').replace(']', '').replace('u', '').replace(' ', '').replace("'", ""))
    spl = in_row_sp['ZoneSpecies'].str.split(',', expand=True)
    spl['ZoneID'] = in_row_sp['ZoneID'].map(lambda x: x.replace(',', '')).astype(str)

    merged_df = pd.merge(in_table, spl, on='ZoneID', how='left')
    merged_df.drop('ZoneID', axis=1, inplace=True)

    sum_by_ent = melt_df(merged_df)
    df_out = sum_by_ent.reset_index()

    return df_out


def extract_overlap_interval(max_value, use_df):
    index_value = use_df[use_df['LABEL'] == max_value].index.values.astype(int)[0]
    # removed all extraneous columns only import columns are the VALUE_[zoneID] and default Label col from tool export
    drop_cols = [z for z in use_df.columns.values.tolist() if not z.startswith('V')]
    drop_cols.remove('LABEL')
    [use_df.drop(j, axis=1, inplace=True) for j in drop_cols if j in drop_cols]
    # transform table so it is zones by distance interval; rest index; update column header and remove 'VALUE' form
    # zone ID- value term is the default output of the tool
    overlap_df = use_df.iloc[0:(index_value + 1), :]
    overlap_df = overlap_df.T
    overlap_df = overlap_df.reset_index()
    overlap_df.columns = overlap_df.iloc[0]
    overlap_df = overlap_df.reindex(overlap_df.index.drop(0))
    update_cols = overlap_df.columns.values
    update_cols[0] = 'InterID'
    overlap_df.columns = update_cols
    overlap_df['InterID'] = overlap_df['InterID'].map(lambda x: str(x).split('_')[1]).astype(str)
    return overlap_df


def use_by_fips(use_df, sp_group_abb, max_value):
    overlap_df = extract_overlap_interval(max_value, use_df)
    fips_zone_fc = [j for j in list_fips_fc if j.startswith(file_type + sp_group_abb.title())]
    fips_zone_array = arcpy.da.TableToNumPyArray(look_up_fips + os.sep + fips_zone_fc[0],
                                                 ['InterID', 'ZoneID', 'GEOID'])
    fips_zone_df = pd.DataFrame(data=fips_zone_array, dtype=object)
    fips_zone_df['InterID'] = fips_zone_df['InterID'].map(lambda x: str(x).split('.')[0]).astype(str)
    merge_fips = pd.merge(overlap_df, fips_zone_df, on='InterID', how='left')
    pivot = (merge_fips.pivot(index='ZoneID', columns='GEOID', values='0')).reset_index()
    pivot['ZoneID'] = pivot['ZoneID'].map(lambda x: str(x).split('.')[0]).astype(str)

    c_zones = pivot['ZoneID'].values.tolist()

    # break out species from csv name to know which FC attribute table to pull in; read in table and set correct
    # dtypes for columns
    if len(c_zones) == 0:
        spe_by_fips = pd.DataFrame()
    else:
        sp_zone_fc = [j for j in list_fc if j.startswith(file_type + sp_group_abb.title())]
        sp_zone_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + sp_zone_fc[0], ['ZoneID', 'ZoneSpecies'])
        sp_zone_df = pd.DataFrame(data=sp_zone_array, dtype=object)
        sp_zone_df['ZoneID'] = sp_zone_df['ZoneID'].map(lambda x: str(x).split('.')[0]).astype(str)
        # Filter so on the zone from the current use table is in the working df
        sp_zone_df = sp_zone_df[sp_zone_df['ZoneID'].isin(c_zones)]

        spe_by_fips = parse_tables(pivot, sp_zone_df)
    return spe_by_fips


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
s_out_folder = out_results

sub_root_folder = os.path.dirname(out_root)
create_directory(sub_root_folder)
create_directory(out_root)
create_directory(s_out_folder)

s_results_folder = raw_results_csv
# Set up output root folder file structure

out_folder_sum = s_out_folder + os.sep + 'SumSpecies'
create_directory(out_folder_sum)

# Loop through the input results
list_folders = os.listdir(s_results_folder)
for folder in list_folders:
    region = folder.split('_')[0]
    split_folder = folder.split("_")

    use_nm = region + "_" + split_folder[1]  # at csv and not folder so the yearly CDL will also have a use name
    for t in split_folder:
        if t == region or t == use_nm or t == 'euc':
            pass
        else:
            use_nm = use_nm + "_" + t

    # Set up within folder file structure
    out_folder_sum_use_fips = out_folder_sum + os.sep + 'Counties' + os.sep + folder
    out_folder_sum_use_state = out_folder_sum + os.sep + 'States' + os.sep + folder

    create_directory(os.path.dirname(out_folder_sum_use_fips))
    create_directory(out_folder_sum_use_fips)
    create_directory(os.path.dirname(out_folder_sum_use_state))
    create_directory(out_folder_sum_use_state)

    # parse out use name and region from folder name load use support info from look_up_use and acres info for region
    # and whole range of species

    print '\nWorking on {0}: {1} of {2}'.format(folder, (list_folders.index(folder)) + 1, len(list_folders))
    # set up list of result csv files
    list_csv = os.listdir(raw_results_csv + os.sep + folder)
    list_csv = [csv for csv in list_csv if csv.endswith('.csv')]

    # Loop through raw use result tables
    if len(list_csv) == 0:  # out use folder create but not runs complete
        pass
    else:
        out_df_direct = pd.DataFrame()
        out_df_ground = pd.DataFrame()
        out_df_aerial = pd.DataFrame()
        for csv in list_csv:
            # load outside function due to double _ added by filename by default w. arcpy- corrected moving forward
            df_use = pd.read_csv(raw_results_csv + os.sep + folder + os.sep + csv, dtype=object)
            csv = csv.replace('__', '_')  # error correction for file names with double _ in name- remove in futurr

            # Step 1: Sum by species- step up parse interID into zone ID by FIPS then convert the zoneIDs to the
            # species in the zones, then sum so there is one value per species

            out_sum_direct = out_folder_sum_use_fips + os.sep + folder + "_0" + '.csv'
            out_sum_ground = out_folder_sum_use_fips + os.sep + folder + "_" + ground + '.csv'
            out_sum_aerial = out_folder_sum_use_fips + os.sep + folder + "_" + aerial + '.csv'
            if not overwrite_inter_data and (
                    os.path.exists(out_sum_direct) and os.path.exists(out_sum_ground) and os.path.exists(
                    out_sum_aerial)):
                pass
            else:
                print '   Summing tables species by fips...species group:{0} for use {1}'.format(csv.split('_')[1],
                                                                                                 folder)
                use_fips_direct = use_by_fips(df_use, csv.split('_')[1], "0")
                use_fips_ground = use_by_fips(df_use, csv.split('_')[1], "0")
                use_fips_aerial = use_by_fips(df_use, csv.split('_')[1], "0")

                out_df_direct = pd.concat([out_df_direct, use_fips_direct], axis=0)
                out_df_ground = pd.concat([out_df_ground, use_fips_ground], axis=0)
                out_df_aerial = pd.concat([out_df_direct, use_fips_aerial], axis=0)

        if not overwrite_inter_data and (
                os.path.exists(out_sum_direct) and os.path.exists(out_sum_ground) and os.path.exists(out_sum_aerial)):
            out_df_direct = pd.read_csv(out_sum_direct)
            [use_fips_direct.drop(m, axis=1, inplace=True) for m in use_fips_direct.columns.values.tolist() if
             m.startswith('Unnamed')]
            out_df_dground = pd.read_csv(out_sum_ground)
            [out_df_dground.drop(m, axis=1, inplace=True) for m in out_df_dground.columns.values.tolist() if
             m.startswith('Unnamed')]
            out_df_aerial = pd.read_csv(out_sum_aerial)
            [out_df_aerial.drop(m, axis=1, inplace=True) for m in out_df_aerial.columns.values.tolist() if
             m.startswith('Unnamed')]
        else:
            out_df_direct.to_csv(out_sum_direct)
            out_df_ground.to_csv(out_sum_ground)
            out_df_aerial.to_csv(out_sum_aerial)

        # Step 2: Sum by to state from FIPS
        print '   Summing tables species by state...use {0}'.format(folder)
        out_sum_direct_st = out_folder_sum_use_state + os.sep + folder + "_0" + '.csv'
        out_sum_ground_st = out_folder_sum_use_state + os.sep + folder + "_" + ground + '.csv'
        out_sum_aerial_st = out_folder_sum_use_state + os.sep + folder + "_" + aerial + '.csv'
        if not os.path.exists(out_sum_direct_st):
            out_df_state_use = collapse_state(out_df_direct)
            out_df_state_use.to_csv(out_sum_direct_st)
        if not os.path.exists(out_sum_ground_st):
            out_df_state_use = collapse_state(out_df_ground)
            out_df_state_use.to_csv(out_sum_ground_st)
        if not os.path.exists(out_sum_aerial_st):
            out_df_state_use = collapse_state(out_df_aerial)
            out_df_state_use.to_csv(out_sum_aerial_st)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
