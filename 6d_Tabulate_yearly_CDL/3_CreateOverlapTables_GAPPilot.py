import datetime
import os
import arcpy

import pandas as pd
# Title- Generate overlap tables from zone species rasters to individual year results
#               1) Generates tables for individual years
#                       1a) The final merged output are used MagTool

# TODO Add look to read in vector table once vector overlap final
# ASSUMPTION
# Species group is found in index position 1 of all input result tables when split by '_'
# All raster are 30 meter cells - note previously VI and CNMI has some use with a different cell size
overwrite_inter_data = False


master_list = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_Feb2017_20170410_b.csv'
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_']

raw_results_csv = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\test_gap\Range' \
                  r'\PilotGAP\Indiv_Year_raw'
find_file_type = raw_results_csv.split(os.sep)


if 'Range' in find_file_type or 'range' in find_file_type:
    look_up_fc = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range' \
                 r'\R_Clipped_Union_MAG_20161102.gdb'
    look_up_use = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\Indiv_year_lookup.csv'
    species_file_type = 'Range'
else:
    look_up_fc = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\CriticalHabitat' \
                 r'\CH_Clipped_Union_MAG_20161102.gdb'
    look_up_use = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\Indiv_year_lookup.csv'
    species_file_type = 'CriticalHabitat'

in_acres_list = [
    r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tables\CH_Acres_by_region_20170208.csv',
    r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tables\R_Acres_by_region_20170131.csv']

# ## Static variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
out_root = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\Range_Gap' + os.sep + \
           species_file_type
out_results = out_root + os.sep + 'PilotGAP\Indiv_Year_raw'


in_acres_table = ''
for v in in_acres_list:
    path, tail = os.path.split(v)
    if species_file_type == 'Range' and (tail[0] == 'R' or tail[0] == 'r'):
        in_acres_table = v
    elif species_file_type == 'CriticalHabitat' and (tail[0] == 'C' or tail[0] == 'C'):
        in_acres_table = v
    else:
        pass

arcpy.env.workspace = look_up_fc
list_fc = arcpy.ListFeatureClasses()

species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]

list_sp = species_df['EntityID'].values.tolist()
acres_df = pd.read_csv(in_acres_table)
acres_df['EntityID'] = acres_df['EntityID'].astype(str)
use_lookup = pd.read_csv(look_up_use, dtype = object)
use_lookup['Value'] = use_lookup['Value'].map(lambda x: str(x).split('.')[0]).astype(str)


# ###Functions


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        # print "created directory {0}".format(dbf_dir)


def calculation(type_fc, in_sum_df, cell_size, c_region, percent_type):
    # ASSUMES ONLY NUMERIC COLS ARE USE COLS AND ACRES COLS
    use_cols = in_sum_df.columns.values.tolist()
    use_cols.remove('EntityID')
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


def use_by_species(use_df, sp_entid, working_sp_df, use_lookup_table):
    # removed all extraneous columns only import columns are the VALUE_[zoneID] and default Label col from tool
    # export
    drop_cols = [z for z in use_df.columns.values.tolist() if not z.startswith('Value') or z.startswith('VALUE')]
    drop_cols.remove('LABEL')
    [use_df.drop(j, axis=1, inplace=True) for j in drop_cols if j in drop_cols]
    # transform table so it is zones by distance interval; rest index; update column header and remove 'VALUE' form
    # zone ID- value term is the default output of the tool
    use_df = use_df.T
    use_df = use_df.reset_index()
    use_df.columns = use_df.iloc[0]
    use_df = use_df.reindex(use_df.index.drop(0))
    update_cols = use_df.columns.values
    update_cols[0] = 'EntityID'
    use_df.columns = update_cols
    if len(use_df) != 0:
        use_df['EntityID'] = use_df['EntityID'].map(lambda x: str(sp_entid)).astype(str)
    else:
        use_df = use_df.append({'EntityID': entid}, ignore_index=True)
        use_df.fillna(0, inplace=True)

    use_columns = use_df.columns.values.tolist()
    for b in use_columns:
        if b == 'EntityID':
            pass
        else:
            use_nm = use_lookup_table.loc[use_lookup_table['Value'] == b, 'CDL_General_Class'].iloc[0]
            index_pos = use_columns.index(b)
            use_columns[index_pos] = use_nm
    use_df.columns = use_columns

    use_df = working_sp_df.append(use_df)

    return use_df


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
out_folder_merge = s_out_folder + os.sep + 'MergeOverlap_FullRange'
out_folder_merge_region = s_out_folder + os.sep + 'MergeOverlap_Region'

create_directory(out_folder_sum)
create_directory(out_folder_merge)
create_directory(out_folder_merge_region)

sub_results_folders = os.listdir(s_results_folder)
for sub_folder in sub_results_folders:
    c_results_folder = s_results_folder + os.sep + sub_folder

    # parse out use name and region from folder name load use info from look_up_use
    region = sub_folder.split('_')[0]
    split_folder = sub_folder.split("_")

    break_bool = False
    use_nm = region  # starting point that will be used for use_nm_folder

    for v in split_folder:
        if v == region:
            break_bool = True
        if break_bool:
            if v == region or v == 'rec' or v == use_nm:
                pass
            else:
                use_nm = use_nm + "_" + v

    type_use = 'Raster'
    r_cell_size = 30


    # Set up within folder file structure
    out_folder_sum_use = out_folder_sum + os.sep + sub_folder
    create_directory(out_folder_sum_use)

    # Extract acres information for whole range and regional of species

    acres_for_calc = acres_df.ix[:, ['EntityID', ('Acres_' + str(region)), 'TotalAcresOnLand']]
    print '\nWorking on {0}: {1} of {2}'.format(sub_folder, (sub_results_folders.index(sub_folder)) + 1,
                                                len(sub_folder))
    # set up list of result csv files
    list_csv = os.listdir(c_results_folder)
    list_csv = [csv for csv in list_csv if csv.endswith('.csv')]
    if len(list_csv) == 0:
        pass
    else:
        # Loop through raw use result tables
        sum_species_df = pd.DataFrame()
        out_use_pixel_by_species = out_folder_sum_use + os.sep + use_nm + '.csv'
        if not overwrite_inter_data and os.path.exists(out_use_pixel_by_species):
            sum_species_df = pd.read_csv(out_use_pixel_by_species)
            [sum_species_df.drop(m, axis=1, inplace=True) for m in sum_species_df.columns.values.tolist() if
             m.startswith('Unnamed')]
        else:

            for csv in list_csv:
                print '   Summing tables by species...species :{0}'.format(csv.split('_')[1])
                # loops through tables that are indiv species,merges into single output; row = species

                df_use = pd.read_csv(c_results_folder + os.sep + csv, dtype=object)
                csv = csv.replace('__', '_')  # error correction file name, double _ - remove in future
                split_csv = csv.split("_")
                entid = split_csv[1]
                sum_species_df = use_by_species(df_use, csv.split('_')[1], sum_species_df, use_lookup)
                sum_species_df.to_csv(out_use_pixel_by_species)  # save intermediate tables' pixel
                # count for use and interval by species

    # STEP 2: Run percent overlap for both the full range and regional; set up acres table

    sum_df_acres = pd.merge(sum_species_df, acres_for_calc, on='EntityID', how='left')

    full_range_overlap_out = out_folder_merge + os.sep + use_nm + '.csv'
    regional_range_overlap_out = out_folder_merge_region + os.sep + use_nm + '.csv'

    if not overwrite_inter_data and os.path.exists(full_range_overlap_out):
        pass

    else:
        print '         Generating full range percent overlap...for use {0}'.format(use_nm)
        percent_overlap_df_full = calculation(type_use, sum_df_acres, r_cell_size, region, False)
        [percent_overlap_df_full.drop(m, axis=1, inplace=True) for m in percent_overlap_df_full.columns.values.tolist()
         if m.startswith('Unnamed')]
        percent_overlap_df_full = pd.merge(base_sp_df, percent_overlap_df_full, on='EntityID', how='inner')
        percent_overlap_df_full.fillna(0, inplace=True)

        percent_overlap_df_full.to_csv(full_range_overlap_out)

    if not overwrite_inter_data and os.path.exists(regional_range_overlap_out):
        pass
    else:
        print '         Generating regional range percent overlap...for use {0}'.format(use_nm)
        percent_overlap_df_region = calculation(type_use, sum_df_acres, r_cell_size, region, True)
        [percent_overlap_df_region.drop(m, axis=1, inplace=True) for m in
         percent_overlap_df_region.columns.values.tolist() if m.startswith('Unnamed')]
        percent_overlap_df_region = pd.merge(base_sp_df, percent_overlap_df_region, on='EntityID', how='inner')
        percent_overlap_df_region.fillna(0, inplace=True)
        percent_overlap_df_region.to_csv(regional_range_overlap_out)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
