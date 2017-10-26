import datetime
import os
import arcpy

import pandas as pd

# Title- Generate overlap tables from feature huc 12 to use layers results;
#               1) Generates tables for aggregated layers, AA, Ag and NonAG
#                       1a) The final merged output are used to generate distance interval table for spray drift; and
#                           summarized BE table (0, 305m and 765)
# Static variables are updated once per update; user input variables update each  run
# TODO Add look to read in vector table once vector overlap final
# ASSUMPTION
# Species group is found in index position 1 of all input result tables when split by '_'
# All raster are 30 meter cells - note previously VI and CNMI has some use with a different cell size
overwrite_inter_data = True

master_list = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\NHD_acresHUC2\acres_sum\AllHUC_acres.csv'
col_include_output = ['HUC_12']
raw_results_csv = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\test_gap\HUC12\Indiv_Year_raw'

# ###########Static variables

look_up_use = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\Indiv_year_lookup.csv'
out_root = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\Range_Gap' +os.sep + 'HUC12'

out_results = out_root + os.sep + 'Indiv_Year_raw'

# Location of CORRECTED acres tables NHD
in_huc_acres = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\NHD_acresHUC2\acres_sum'
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')


huc_df = pd.read_csv(master_list, dtype=object)
huc_df['HUC_12'] = huc_df['HUC12']
base_huc = huc_df.loc[:, col_include_output]
use_lookup = pd.read_csv(look_up_use, dtype = object)
use_lookup['Value'] = use_lookup['Value'].map(lambda x: str(x).split('.')[0]).astype(str)


# ###Functions


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        # print "created directory {0}".format(dbf_dir)


def calculation(type_fc, in_sum_df, cell_size):
    # ASSUMES ONLY NUMERIC COLS ARE USE COLS AND ACRES COLS
    use_cols = in_sum_df.columns.values.tolist()
    use_cols.remove('HUC_12')
    acres_col = 'Acres'

    in_sum_df.ix[:, use_cols] = in_sum_df.ix[:, use_cols].apply(pd.to_numeric, errors='coerce')
    use_cols.remove(acres_col)
    # in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]

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

        overlap.drop('Acres', axis=1, inplace=True)
        return overlap

    else:
        # TODO ADD IN VECTOR OVERLAP
        print "ERROR ERROR"


def use_by_species(use_df, use_lookup_table):
    # removed all extraneous columns only import columns are the VALUE_[zoneID] and default Label col from tool
    # export
    drop_cols = [z for z in use_df.columns.values.tolist() if not z.startswith('HUC')]
    drop_cols.remove('LABEL')
    [use_df.drop(j, axis=1, inplace=True) for j in drop_cols if j in drop_cols]
    # transform table so it is zones by distance interval; rest index; update column header and remove 'VALUE' form
    # zone ID- value term is the default output of the tool
    use_df = use_df.T
    use_df = use_df.reset_index()
    use_df.columns = use_df.iloc[0]
    use_df = use_df.reindex(use_df.index.drop(0))
    update_cols = use_df.columns.values
    update_cols[0] = 'HUC_12'
    use_df.columns = update_cols
    use_df['HUC_12'] = use_df['HUC_12'].map(lambda x: str(x).split('_')[2]).astype(str)


    use_columns = use_df.columns.values.tolist()
    for b in use_columns:
        if b == 'HUC_12':
            pass
        else:
            use_nm = use_lookup_table.loc[use_lookup_table['Value'] == b, 'CDL_General_Class'].iloc[0]
            index_pos = use_columns.index(b)
            use_columns[index_pos] = use_nm
    use_df.columns = use_columns

    use_df.columns = use_columns
    # list of zoneIDs to in output table; used to filter out zone from FC attribute table not found in use table
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
out_folder_percent = s_out_folder + os.sep + 'PercentOverlap_HUC12'
out_folder_merge = s_out_folder + os.sep + 'MergeOverlap_HUC12'

create_directory(out_folder_sum)
create_directory(out_folder_percent)
create_directory(out_folder_merge)

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
    print use_nm
    # Set up within folder file structure
    out_folder_sum_use = out_folder_sum + os.sep + folder
    out_folder_percent_use = out_folder_percent + os.sep + folder
    create_directory(out_folder_percent_use)
    create_directory(out_folder_sum_use)
    # parse out use name and region from folder name load use support info from look_up_use and acres info for region
    # and whole range of species

    print '\nWorking on {0}: {1} of {2}'.format(folder, (list_folders.index(folder)) + 1, len(list_folders))
    # set up list of result csv files
    list_csv = os.listdir(raw_results_csv + os.sep + folder)
    list_csv = [csv for csv in list_csv if csv.endswith('.csv')]

    # Loop through raw use result tables
    for csv in list_csv:
        pares_csv = csv.split('_')
        huc_2 = pares_csv[(len(pares_csv) - 1)].replace('.csv', '')
        look_up_acres = in_huc_acres + os.sep + 'NHDPlus' + huc_2 + '.csv'
        acres_df = pd.read_csv(look_up_acres)
        acres_df['HUC_12'] = acres_df['HUC_12'].astype(str)
        acres_for_calc = acres_df.ix[:, ['HUC_12', 'Acres']]
        acres_for_calc['HUC_12'] = acres_for_calc['HUC_12'].map(lambda x: str(x) if len(x) == 12 else '0' + str(x))

        # load outside function due to double _ added by filename by default w. arcpy- corrected moving forward
        df_use = pd.read_csv(raw_results_csv + os.sep + folder + os.sep + csv, dtype=object)
        csv = csv.replace('__', '_')  # error correction for file names with double _ in name- remove in future
        split_csv = csv.split("_")

        # use name on csv and not folder to  be constant across all runs; TODO update raw result file structure for
        # to be at the folder level

        type_use = 'Raster'
        r_cell_size = 30

        out_use_pixel_by_huc12 = out_folder_sum_use + os.sep + csv
        # Step 1: Sum by species- convert the zoneIDs to the species in the zones, then sum so there is one value per
        # species
        if not overwrite_inter_data and os.path.exists(out_use_pixel_by_huc12):
            use_df_transformed = pd.read_csv(out_use_pixel_by_huc12)
            use_df_transformed['HUC_12'] = use_df_transformed['HUC_12'].astype(str)
            [use_df_transformed.drop(m, axis=1, inplace=True) for m in use_df_transformed.columns.values.tolist() if
             m.startswith('Unnamed')]
        else:
            print '   Summing tables by species...use {0} and HUC {1}'.format(csv.split('_')[1],
                                                                              csv.split('_')[len(csv.split('_')) - 1])
            use_df_transformed = use_by_species(df_use, use_lookup)
            use_df_transformed.to_csv(out_use_pixel_by_huc12)  # save and intermediate tables' pixel count for  use and
            # interval by species

        # STEP 2: Run percent overlap for both the full range and the regional; set up acres table to include only
        # species found on the current use table (use_df_transformed)

        sum_df_acres = pd.merge(use_df_transformed, acres_for_calc, on='HUC_12', how='left')
        full_range_overlap_out = out_folder_percent_use + os.sep + csv
        if not overwrite_inter_data and os.path.exists(full_range_overlap_out):
            pass

        else:
            print '         Generating HUC 12 percent overlap...species group:{0}'.format(csv.split('_')[1])
            percent_overlap_df_full = calculation(type_use, sum_df_acres, r_cell_size)
            [percent_overlap_df_full.drop(m, axis=1, inplace=True) for m in
             percent_overlap_df_full.columns.values.tolist() if m.startswith('Unnamed')]
            percent_overlap_df_full.to_csv(full_range_overlap_out)

    # STEP 3: Merge Tables by use for all HUCs
    out_csv = out_folder_merge + os.sep + 'Merge_' + str(folder) + '.csv'
    if not overwrite_inter_data and os.path.exists(out_csv):
        pass
    else:
        print'Merging overlap table HUC2...{0}'.format(use_nm)
        list_percent_csv = os.listdir(out_folder_percent_use)
        list_percent_csv = [csv for csv in list_percent_csv if csv.endswith('.csv')]
        out_df = pd.DataFrame()
        for csv in list_percent_csv:
            current_csv = out_folder_percent_use + os.sep + csv
            in_df = pd.read_csv(current_csv, dtype=object)
            out_df = pd.concat([out_df, in_df], axis=0)

        out_df = pd.merge(base_huc, out_df, on='HUC_12', how='left')
        [out_df.drop(m, axis=1, inplace=True) for m in out_df.columns.values.tolist() if m.startswith('Unnamed')]
        out_df.fillna(0, inplace=True)
        out_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
