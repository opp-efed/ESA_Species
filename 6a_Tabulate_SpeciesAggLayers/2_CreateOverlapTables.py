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
overwrite_inter_data = False
master_list = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_Feb2017_20170410_b.csv'
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_']
raw_results_csv = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\test_gap\Range' \
                  r'\Agg_Layers'

find_file_type = raw_results_csv.split(os.sep)
# ###########Static variables
if 'Range' in find_file_type or 'range' in find_file_type:
    look_up_fc = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range' \
                 r'\R_Clipped_Union_MAG_20161102.gdb'
    look_up_use = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\RangeUses_lookup.csv'
    species_file_type = 'Range'
else:
    look_up_fc = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\CriticalHabitat' \
                 r'\CH_Clipped_Union_MAG_20161102.gdb'
    look_up_use = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\CH_Uses_lookup.csv'
    species_file_type = 'CriticalHabitat'

in_acres_list = [
    r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tables\CH_Acres_by_region_20170208.csv',
    r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tables\R_Acres_by_region_20161216.csv']

out_root = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\Range_Gap' + os.sep + \
           species_file_type
out_results = out_root + os.sep + 'Agg_Layers'

in_acres_table = ''
for v in in_acres_list:
    path, tail = os.path.split(v)
    if species_file_type == 'Range' and (tail[0] == 'R' or tail[0] == 'r'):
        in_acres_table = v
    elif species_file_type == 'CriticalHabitat' and (tail[0] == 'C' or tail[0] == 'C'):
        in_acres_table = v
    else:
        pass

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

arcpy.env.workspace = look_up_fc
list_fc = arcpy.ListFeatureClasses()

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


def calculation(type_fc, in_sum_df, cell_size, c_region, percent_type):
    # ASSUMES ONLY NUMERIC COLS ARE USE COLS AND ACRES COLS
    use_cols = in_sum_df.select_dtypes(include=['number']).columns.values.tolist()
    if percent_type:
        acres_col = 'Acres_' + str(c_region)
        in_sum_df.ix[:, use_cols] = in_sum_df.ix[:, use_cols].apply(pd.to_numeric, errors='coerce')
        use_cols.remove(acres_col)
        in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]
    else:
        acres_col = 'TotalAcresOnLand'
        in_sum_df.ix[:, use_cols] = in_sum_df.ix[:, use_cols].apply(pd.to_numeric, errors='coerce')
        use_cols.remove(acres_col)
        in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]

    if type_fc == "Raster":
        msq_conversion = cell_size * cell_size
        # convert pixels to msq
        overlap = in_sum_df.copy()
        overlap.ix[:, use_cols] *= msq_conversion
        # convert msq to acres
        overlap.ix[:, use_cols] *= 0.000247

        # generate percent overlap by taking acres of use divided by total acres of the species range
        overlap[use_cols] = overlap[use_cols].div(overlap[acres_col], axis=0)
        overlap.ix[:, use_cols] *= 100
        # Drop excess acres col- both regional and full range are included on input df; user defined parameter
        # percent_type determines which one is used in overlap calculation
        if percent_type:
            overlap.drop('TotalAcresOnLand', axis=1, inplace=True)
        else:
            overlap.drop(('Acres_' + str(c_region)), axis=1, inplace=True)
        return overlap

    else:
        # TODO ADD IN VECTOR OVERLAP
        print "ERROR ERROR"


def parse_tables(in_table, in_row_sp, final_header):
    in_table['ZoneID'] = in_table['ZoneID'].map(lambda x: x.replace(',', '')).astype(str)
    in_row_sp['ZoneSpecies'] = in_row_sp['ZoneSpecies'].apply(
        lambda x: x.replace('[', '').replace(']', '').replace('u', '').replace(' ', '').replace("'", ""))
    spl = in_row_sp['ZoneSpecies'].str.split(',', expand=True)
    spl['ZoneID'] = in_row_sp['ZoneID'].map(lambda x: x.replace(',', '')).astype(str)

    merged_df = pd.merge(in_table, spl, on='ZoneID', how='left')
    merged_df.drop('ZoneID', axis=1, inplace=True)

    sum_by_ent = melt_df(merged_df)
    df_out = sum_by_ent.reset_index()
    new_columns_col = df_out.columns.values.tolist()
    new_columns_col[0] = 'EntityID'
    out_col = ['EntityID']
    [out_col.append(final_header + "_" + str(j.replace(',', ''))) for j in new_columns_col if j != 'EntityID']

    df_out.columns = out_col

    return df_out


def use_by_species(use_df, sp_group_abb):
    # removed all extraneous columns only import columns are the VALUE_[zoneID] and default Label col from tool
    # export
    drop_cols = [z for z in use_df.columns.values.tolist() if not z.startswith('VALUE')]
    drop_cols.remove('LABEL')
    [use_df.drop(j, axis=1, inplace=True) for j in drop_cols if j in drop_cols]
    # transform table so it is zones by distance interval; rest index; update column header and remove 'VALUE' form
    # zone ID- value term is the default output of the tool
    use_df = use_df.T
    use_df = use_df.reset_index()
    use_df.columns = use_df.iloc[0]
    use_df = use_df.reindex(use_df.index.drop(0))
    update_cols = use_df.columns.values
    update_cols[0] = 'ZoneID'
    use_df.columns = update_cols
    use_df['ZoneID'] = use_df['ZoneID'].map(lambda x: str(x).split('_')[1]).astype(str)
    # list of zoneIDs to in output table; used to filter out zone from FC attribute table not found in use table
    c_zones = use_df['ZoneID'].values.tolist()
    # break out species from csv name to know which FC attribute table to pull in; read in table and set correct
    # dtypes for columns

    sp_zone_fc = [j for j in list_fc if j.startswith('R_' + sp_group_abb.title())]
    sp_zone_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + sp_zone_fc[0], ['ZoneID', 'ZoneSpecies'])
    sp_zone_df = pd.DataFrame(data=sp_zone_array, dtype=object)
    sp_zone_df['ZoneID'] = sp_zone_df['ZoneID'].map(lambda x: str(x).split('.')[0]).astype(str)
    # Filter so on the zone from the current use table is in the working df
    sp_zone_df = sp_zone_df[sp_zone_df['ZoneID'].isin(c_zones)]
    return use_df, sp_zone_df, c_zones


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
s_out_folder = out_results

create_directory(out_root)
sub_root_folder = os.path.dirname(s_out_folder)
create_directory(sub_root_folder)
create_directory(s_out_folder)

s_results_folder = raw_results_csv
# Set up output root folder file structure
out_folder_sum = s_out_folder + os.sep + 'SumSpecies'
out_folder_percent = s_out_folder + os.sep + 'PercentOverlap_FullRange'
out_folder_percent_region = s_out_folder + os.sep + 'PercentOverlap_RegionRange'
out_folder_merge = s_out_folder + os.sep + 'MergeOverlap_FullRange'
out_folder_merge_region = s_out_folder + os.sep + 'MergeOverlap_Region'

create_directory(out_folder_sum)
create_directory(out_folder_percent)
create_directory(out_folder_percent_region)
create_directory(out_folder_merge)
create_directory(out_folder_merge_region)

# Loop through the input results
list_folders = os.listdir(s_results_folder)
for folder in list_folders:
    region = folder.split('_')[0]
    split_folder = folder.split("_")

    use_nm = split_folder[1]  # at csv and not folder so the yearly CDL will also have a use name
    for t in split_folder:
        if t == region or t == use_nm or t == 'euc':
            pass
        else:
            use_nm = use_nm + "_" + t
    use_nm = region + "_" + use_nm

    # Set up within folder file structure
    out_folder_sum_use = out_folder_sum + os.sep + folder
    out_folder_percent_use = out_folder_percent + os.sep + folder
    out_folder_percent_region_use = out_folder_percent_region + os.sep + folder
    create_directory(out_folder_percent_use)
    create_directory(out_folder_percent_region_use)
    create_directory(out_folder_sum_use)
    # parse out use name and region from folder name load use support info from look_up_use and acres info for region
    # and whole range of species

    acres_for_calc = acres_df.ix[:, ['EntityID', ('Acres_' + str(region)), 'TotalAcresOnLand']]
    print '\nWorking on {0}: {1} of {2}'.format(folder, (list_folders.index(folder)) + 1, len(list_folders))
    # set up list of result csv files
    list_csv = os.listdir(raw_results_csv + os.sep + folder)
    list_csv = [csv for csv in list_csv if csv.endswith('.csv')]

    # Loop through raw use result tables
    if len(list_csv) == 0:  # out use folder create but not runs complete
        pass
    else:
        for csv in list_csv:
            # load outside function due to double _ added by filename by default w. arcpy- corrected moving forward
            df_use = pd.read_csv(raw_results_csv + os.sep + folder + os.sep + csv, dtype=object)
            csv = csv.replace('__', '_')  # error correction for file names with double _ in name- remove in future
            split_csv = csv.split("_")
            sp_group = split_csv[1]
            # use name on csv and not folder to  be constanst across all runs; TODO update raw result file structure for
            # to be at the folder level

            type_use = use_lookup.loc[use_lookup['FullName'] == use_nm, 'Type'].iloc[0]
            r_cell_size = use_lookup.loc[use_lookup['FullName'] == use_nm, 'Cell Size'].iloc[0]
            final_col_header = use_lookup.loc[use_lookup['FullName'] == use_nm, 'FinalColHeader'].iloc[0]

            out_use_pixel_by_species = out_folder_sum_use + os.sep + csv
            # Step 1: Sum by species- convert the zoneIDs to the species in the zones, then sum so there is one value per
            # species
            if not overwrite_inter_data and os.path.exists(out_use_pixel_by_species):
                use_array = pd.read_csv(out_use_pixel_by_species)
                [use_array.drop(m, axis=1, inplace=True) for m in use_array.columns.values.tolist() if
                 m.startswith('Unnamed')]
            else:
                print '   Summing tables by species...species group:{0}'.format(csv.split('_')[1])
                use_df_transformed, sp_zone_df_fc, zones = use_by_species(df_use, csv.split('_')[1])
                if len(zones) == 0:  # no zones in the raw output table no overlap; 0 for these species add at end
                    pass
                else:
                    use_array = parse_tables(use_df_transformed, sp_zone_df_fc, final_col_header)
                    use_array.to_csv(out_use_pixel_by_species)  # save and intermediate tables' pixel count for  use and
                    # interval by species

            # STEP 2: Run percent overlap for both the full range and the regional; set up acres table to include only
            # species found on the current use table (use_array)
            sum_df_acres = pd.merge(use_array, acres_for_calc, on='EntityID', how='left')
            full_range_overlap_out = out_folder_percent_use + os.sep + csv
            regional_range_overlap_out = out_folder_percent_region_use + os.sep + csv

            if not overwrite_inter_data and os.path.exists(full_range_overlap_out):
                pass
            elif len(zones) == 0:  # no zones in the raw output table no overlap; 0 for these species add at end
                pass
            else:
                print '         Generating full range percent overlap...species group:{0}'.format(csv.split('_')[1])
                percent_overlap_df_full = calculation(type_use, sum_df_acres, r_cell_size, region, False)
                [percent_overlap_df_full.drop(m, axis=1, inplace=True) for m in
                 percent_overlap_df_full.columns.values.tolist() if m.startswith('Unnamed')]
                percent_overlap_df_full.to_csv(full_range_overlap_out)

            if not overwrite_inter_data and os.path.exists(regional_range_overlap_out):
                pass
            elif len(zones) == 0:  # no zones in the raw output table no overlap; 0 for these species add at end
                pass
            else:
                print '         Generating regional range percent overlap...species group:{0}'.format(csv.split('_')[1])
                percent_overlap_df_region = calculation(type_use, sum_df_acres, r_cell_size, region, True)
                [percent_overlap_df_region.drop(m, axis=1, inplace=True) for m in
                 percent_overlap_df_region.columns.values.tolist() if m.startswith('Unnamed')]
                percent_overlap_df_region.to_csv(regional_range_overlap_out)

        # STEP 3: Merge Tables by use for the full range and regional rang
        out_csv = out_folder_merge + os.sep + 'Merge_' + str(folder) + '.csv'
        if not overwrite_inter_data and os.path.exists(out_csv):
            pass
        else:
            print'Merging overlap table full range...{0}'.format(use_nm)
            list_percent_csv = os.listdir(out_folder_percent_use)
            list_percent_csv = [csv for csv in list_percent_csv if csv.endswith('.csv')]
            out_df = pd.DataFrame()
            for csv in list_percent_csv:
                current_csv = out_folder_percent_use + os.sep + csv
                in_df = pd.read_csv(current_csv, dtype=object)

                out_df = pd.concat([out_df, in_df], axis=0)

            out_df = pd.merge(base_sp_df, out_df, on='EntityID', how='left')
            [out_df.drop(m, axis=1, inplace=True) for m in out_df.columns.values.tolist() if m.startswith('Unnamed')]
            out_df.fillna(0, inplace=True)
            out_df.to_csv(out_csv)

        # STEP 3: Merge Tables regional range
        out_csv = out_folder_merge_region + os.sep + 'Merge_' + str(folder) + '.csv'
        if not overwrite_inter_data and os.path.exists(out_csv):
            pass
        else:
            print'Merging overlap table regional range...{0}'.format(use_nm)
            list_percent_region_csv = os.listdir(out_folder_percent_region_use)
            list_percent_region_csv = [csv for csv in list_percent_region_csv if csv.endswith('.csv')]
            out_df = pd.DataFrame()
            for csv in list_percent_region_csv:
                current_csv = out_folder_percent_region_use + os.sep + csv
                in_df = pd.read_csv(current_csv, dtype=object)
                out_df = pd.concat([out_df, in_df], axis=0)
            out_df = pd.merge(base_sp_df, out_df, on='EntityID', how='left')
            [out_df.drop(m, axis=1, inplace=True) for m in out_df.columns.values.tolist() if m.startswith('Unnamed')]
            out_df.fillna(0, inplace=True)
            out_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
