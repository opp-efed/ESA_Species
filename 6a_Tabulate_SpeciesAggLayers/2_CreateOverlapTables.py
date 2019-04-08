import datetime
import os

import pandas as pd

# Title- Generate overlap tables from zone species rasters to use layers results using tabulate area; Author J Connolly
#               1) Generates tables for aggregated layers, AA, Ag and NonAG
#                       1a) The final merged output are used to generate distance interval table for spray drift; and
#                           summarized BE table (0, 305m and 765)
# TODO Add look to read in vector table once vector overlap final


# Static variables are updated once per update; user input variables update each  run
# ASSUMPTIONS
# Species group is found in index position 1 of all input result tables when split by '_'

# NOTE there is a limit to the number of characters in a path (255) be sure to save input files in a location where you
# will not hist the limit.  If the limit is hit you will receive and error that the file does not exist.  Can over ride
# error by pausing syncing

# ###############user input variables

# overwrite boolean - when set to FALSE tables previously generated will not be updated but  new ones need to be added.
# If a use layer was updated delete or archive the tables for the dated version and set this to false.  If this variable
# is set to TRUE than all tables will be recalculated.
overwrite_inter_data = False

# File structure is standard for raw result outputs and tabulated results outputs
# Changes include L48 v NL48  and Range and CriticalHabitat in the path

# Location of raw results
raw_results_csv = r'L:\Workspace\StreamLine\ESA\Results_HUCAB\NL48\Range\Agg_Layers'
# Root location where the transformed tables should be saved; 'Tabulated' results this locations should be the same for
# all steps
out_root_dir = r'L:\Workspace\StreamLine\ESA\Tabulated_TabArea_HUCAB'

# ########### Variables to be updated once per update
# NOTE there is a limit to the number of characters in a path (255) be sure to save input files in a location where you
# will not hist the limit.  If the limit is hit you will receive and error that the file does not exist.  Can over ride
# error by pausing syncing

# location of master species list
master_list = r'C:\Users\JConno02' \
              r'\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'
# Columns from the master species list that should be included in the output tables
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']

find_file_type = raw_results_csv.split(os.sep)  # identifies inputs as range or critical habit by splitting path
if 'Range' in find_file_type or 'range' in find_file_type:
    # Location of look-up table to transform from zone to species EntityID range files; Look directory needs to match
    # the input species files see InputFiles current runs for questions
    look_up_fc = r'L:\Workspace\StreamLine\ESA\UnionFiles_Winter2018\Range\Lookup_R_Clipped_Union_CntyInter_HUC2ABInter_20180612'
    # Common col from lookup table to output table, this is the column in the look up table that matches
    # the VALUE column of the species input raster
    join_col = 'HUCID'  # typical vlaues ZoneID, InterID, HUCID
    # Table will all of the uses, use layer, raster properties, usage columns and and final column headers for parent
    # tables
    look_up_use = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\SupportingTables\Malathion_Census_Uses_lookup_20181106_test.csv"
    file_type = 'R_'  # R_ for Range runs
    species_file_type = 'Range'  # Range for  Range runs
    # Location of range acres tables to be use to calculate percent overlap
    in_acres_table = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\R_Acres_Pixels_20180428.csv"
else:
    # Location of look-up table to transform from zone to species EntityID range files; Look directory needs to match
    # the input species files see InputFiles current runs for questions
    look_up_fc = r'L:\Workspace\StreamLine\ESA\UnionFiles_Winter2018\CriticalHabitat\Lookup_CH_Clipped_Union_CntyInter_HUC2ABInter_20180612'
    # Common col from lookup table to output table, this is the column in the look up table that matches
    # the VALUE column of the species input raster
    join_col = 'HUCID'
    # Table will all of the uses, use layer, raster properties, usage columns and and final column headers for parent
    # tables
    look_up_use = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ExternalDrive\_CurrentSupportingTables\Uses_lookup_20180430.csv'
    species_file_type = 'CH'  # CH for critical habitat runs
    file_type = 'CH_'  # CH_ for critical habit
    # Location of critical habitat acres tables to be use to calculate percent overlap
    in_acres_table = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\CH_Acres_Pixels_20180430.csv'

find_file_type = raw_results_csv.split(os.sep)
if 'L48' in find_file_type:
    p_region = 'L48'
else:
    p_region = 'NL48'

out_root = out_root_dir + os.sep + p_region + os.sep + species_file_type
out_results = out_root + os.sep + 'Agg_Layers'

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

list_lookup = os.listdir(look_up_fc)

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


def calculation(type_fc, in_sum_df, c_region, percent_type):
    # ASSUMES ONLY NUMERIC COLS ARE USE COLS AND ACRES COLS
    use_cols = [f for f in in_sum_df.columns.values.tolist() if len(f.split("_")) == 3]

    if percent_type == 'full range':
        acres_col = 'TotalAcresOnLand'
        in_sum_df.ix[:, use_cols +[acres_col]] = in_sum_df.ix[:, use_cols +[acres_col]].apply(pd.to_numeric, errors='coerce')
        in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]
    elif percent_type == 'NL48 range':
        acres_col = 'TotalAcresNL48'
        in_sum_df.ix[:, use_cols +[acres_col]] = in_sum_df.ix[:, use_cols +[acres_col]].apply(pd.to_numeric, errors='coerce')
        in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]
    elif percent_type == 'regional range':
        acres_col = 'Acres_' + str(c_region)
        in_sum_df.ix[:, use_cols +[acres_col]] = in_sum_df.ix[:, use_cols +[acres_col]].apply(pd.to_numeric, errors='coerce')
        in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]

    if type_fc == "Raster":
        overlap = in_sum_df.copy()
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

        return overlap

    else:
        # TODO ADD IN VECTOR OVERLAP
        print ("ERROR ERROR")


start_time = datetime.datetime.now()
print ("Start Time: " + start_time.ctime())

# create output file structure
s_out_folder = out_results

create_directory(out_root_dir)
sub_root_folder = os.path.dirname(out_root)
create_directory(sub_root_folder)
create_directory(out_root)
create_directory(s_out_folder)

s_results_folder = raw_results_csv
# Set up output root folder file structure
out_folder_sum = s_out_folder + os.sep + 'SumSpecies'
out_folder_percent = s_out_folder + os.sep + 'PO_FullRange'
out_folder_percent_region = s_out_folder + os.sep + 'PO_RegionRange'
out_folder_percent_NL48 = s_out_folder + os.sep + 'PO_NL48Range'

out_folder_merge = s_out_folder + os.sep + 'MO_FullRange'
out_folder_merge_region = s_out_folder + os.sep + 'MO_Region'
out_folder_merge_NL48 = s_out_folder + os.sep + 'MO_NL48Range'

create_directory(out_folder_sum)

create_directory(out_folder_percent)
create_directory(out_folder_percent_region)
create_directory(out_folder_percent_NL48)

create_directory(out_folder_merge)
create_directory(out_folder_merge_region)
create_directory(out_folder_merge_NL48)

# Loop through the input results
list_results_directory = os.listdir(raw_results_csv)

for folder in list_results_directory:
    print ('\nWorking on {0}: {1} of {2}'.format(folder, (list_results_directory.index(folder)) + 1,
                                                 len(list_results_directory)))
    region = folder.split('_')[0]
    split_folder = folder.split("_")

    use_nm = split_folder[1]  # at csv and not folder so the yearly CDL will also have a use name
    for t in split_folder:
        if t == region or t == use_nm or t == 'euc':
            pass
        else:
            use_nm = use_nm + "_" + t
    use_nm = region + "_" + use_nm
    out_folder_sum_use = out_folder_sum + os.sep + folder
    out_folder_percent_use = out_folder_percent + os.sep + folder
    out_folder_percent_region_use = out_folder_percent_region + os.sep + folder
    out_folder_percent_nl48_use = out_folder_percent_NL48 + os.sep + folder
    create_directory(out_folder_percent_use)
    create_directory(out_folder_percent_region_use)
    create_directory(out_folder_sum_use)
    create_directory(out_folder_percent_nl48_use)

    csv_list = os.listdir(raw_results_csv + os.sep + folder)
    csv_list = [csv for csv in csv_list if csv.endswith('.csv')]

    acres_for_calc = acres_df.ix[:, ['EntityID', ('Acres_' + str(region)), 'TotalAcresNL48', 'TotalAcresOnLand']]
    acres_for_calc['EntityID'] = acres_for_calc['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
    for csv in csv_list:
        # load outside function due to double _ added by filename by default w. arcpy- corrected moving forward
        print ('   Transforming and summing tables by species...species group:{0}'.format(csv.split('_')[1]))
        csv = csv.replace('__', '_')  # error correction for file names with double _ in name- remove in future
        # out csv
        out_use_pixel_by_species = out_folder_sum_use + os.sep + csv

        # pulls in use information from the use table
        print use_nm
        type_use = use_lookup.loc[use_lookup['FullName'] == use_nm, 'Type'].iloc[0]

        final_col_header = use_lookup.loc[use_lookup['FullName'] == use_nm, 'FinalColHeader'].iloc[0]

        # Step 1: Sum by species- convert the zoneIDs to the species in the zones, then sums so there is one value
        # per species
        if not overwrite_inter_data and not os.path.exists(out_use_pixel_by_species):
            if not os.path.exists(out_folder_merge_NL48 + os.sep + 'Merge_' + str(folder) + '.csv'):
                results_df = pd.read_csv(raw_results_csv + os.sep + folder + os.sep + csv, dtype=object)
                results_df['VALUE'] = results_df['VALUE'].map(lambda x: str(x).split(".")[0]).astype(str)
                val_cols = [v for v in results_df.columns.values.tolist() if v.startswith('VALUE')]
                if 'VALUE' in val_cols:
                    val_cols.remove('VALUE')
                results_df.ix[:, val_cols] = results_df.ix[:, val_cols].apply(pd.to_numeric, errors='coerce')
                list_zones = results_df['VALUE'].values.tolist()  # list of zones in raw output table

                lookup_csv = [t for t in list_lookup if
                              t.startswith(csv.split("_")[0].upper() + "_" + csv.split("_")[1].capitalize())]
                lookup_df = pd.read_csv(look_up_fc + os.sep + lookup_csv[0])
                lookup_df[join_col] = lookup_df[join_col].map(lambda x: str(x).split(".")[0]).astype(str)
                lookup_df = lookup_df[lookup_df[join_col].isin(list_zones)]  # filter lookup to just zones in current output
                merged_df = pd.merge(results_df, lookup_df, how='outer', left_on='VALUE', right_on=join_col)
                # drop cols that start with unnamed
                [merged_df.drop(col, axis=1, inplace=True) for col in merged_df.columns.values.tolist() if col.startswith('Un')]
                merged_df['EntityID'] = merged_df['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
                merged_df = merged_df.fillna(0)
                merged_df = merged_df.drop_duplicates()  # drop duplicate rows generate from outer join

                out_cols = ['EntityID'] + val_cols
                sp_df = merged_df[out_cols].copy()  # subset to desired columns
                sp_df = sp_df.drop_duplicates()
                sp_df.ix[:, val_cols] = sp_df.ix[:, val_cols].apply(pd.to_numeric, errors='coerce')
                sp_df = sp_df.groupby('EntityID').sum().reset_index()

                h_cols = [final_col_header + "_" + v.split("_")[1] for v in val_cols]
                final_cols = ['EntityID'] + h_cols

                sp_df.columns = final_cols
                zones = sp_df['EntityID'].values.tolist()
                if len(sp_df['EntityID'].values.tolist()) == 0:
                    zones = []
                sp_df.to_csv(out_use_pixel_by_species)

        elif not overwrite_inter_data and os.path.exists(out_use_pixel_by_species):
                sp_df = pd.read_csv(out_use_pixel_by_species)
                val_cols = [v for v in sp_df.columns.values.tolist() if v.startswith(final_col_header)]
                sp_df.ix[:, val_cols] = sp_df.ix[:, val_cols].apply(pd.to_numeric, errors='coerce')
                [sp_df.drop(m, axis=1, inplace=True) for m in sp_df.columns.values.tolist() if
                 m.startswith('Unnamed')]
                if len(sp_df) == 0:
                    zones = []
                else:
                    zones = sp_df['EntityID'].values.tolist()

                # STEP 2: Run percent overlap for both the full range and the regional; set up acres table to include only
                # species found on the current use table (use_array)

                # output full acres as denominator for % overlap
        full_range_overlap_out = out_folder_percent_use + os.sep + csv
        # output regional acres as denominator for % overlap
        regional_range_overlap_out = out_folder_percent_region_use + os.sep + csv
        # output NL48 acres as denominator for % overlap
        NL48_range_overlap_out = out_folder_percent_nl48_use + os.sep + csv


        if len(zones) == 0:  # no zones in the raw output table no overlap; 0 for these species add at end
            pass
        else:
            val_cols = [v for v in sp_df.columns.values.tolist() if v.startswith(final_col_header)]
            sp_df['EntityID'] = sp_df['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)

            sum_df_acres = pd.merge(sp_df, acres_for_calc, on='EntityID', how='left')
            sum_df_acres.ix[:, val_cols] = sum_df_acres.ix[:, val_cols].apply(pd.to_numeric, errors='coerce')

            if not overwrite_inter_data and os.path.exists(full_range_overlap_out):
                pass
            else:
                print ('         Generating full range percent overlap...species group:{0}'.format(csv.split('_')[1]))
                percent_overlap_df_full = calculation(type_use, sum_df_acres, region, 'full range')
                [percent_overlap_df_full.drop(m, axis=1, inplace=True) for m in
                 percent_overlap_df_full.columns.values.tolist() if m.startswith('Unnamed')]
                percent_overlap_df_full.to_csv(full_range_overlap_out)

        if not overwrite_inter_data and os.path.exists(regional_range_overlap_out):
            pass
        elif len(zones) == 0:  # no zones in the raw output table no overlap; 0 for these species add at end
            pass
        else:
            print ('         Generating regional range percent overlap...species group:{0}'.format(csv.split('_')[1]))
            percent_overlap_df_region = calculation(type_use, sum_df_acres, region, 'regional range')
            [percent_overlap_df_region.drop(m, axis=1, inplace=True) for m in
             percent_overlap_df_region.columns.values.tolist() if m.startswith('Unnamed')]
            percent_overlap_df_region.to_csv(regional_range_overlap_out)

        if not overwrite_inter_data and os.path.exists(NL48_range_overlap_out):
            pass
        elif len(zones) == 0:  # no zones in the raw output table no overlap; 0 for these species add at end
            pass
        else:
            print ('         Generating NL48 range percent overlap...species group:{0}'.format(csv.split('_')[1]))
            percent_overlap_df_NL48 = calculation(type_use, sum_df_acres, region, 'NL48 range')
            [percent_overlap_df_NL48.drop(m, axis=1, inplace=True) for m in
             percent_overlap_df_NL48.columns.values.tolist() if m.startswith('Unnamed')]
            percent_overlap_df_NL48.to_csv(NL48_range_overlap_out)

    # STEP 3: Merge Tables by use for the full range and regional range
    out_csv = out_folder_merge + os.sep + 'Merge_' + str(folder) + '.csv'
    if not overwrite_inter_data and os.path.exists(out_csv):
        pass
    else:
        print('           Merging overlap table full range...{0}'.format(use_nm))
        list_percent_csv = os.listdir(out_folder_percent_use)
        list_percent_csv = [csv for csv in list_percent_csv if csv.endswith('.csv')]
        out_df = pd.DataFrame()
        for csv_p in list_percent_csv:
            current_csv = out_folder_percent_use + os.sep + csv_p
            in_df = pd.read_csv(current_csv, dtype=object)
            out_df = pd.concat([out_df, in_df], axis=0)
        if len(out_df) == 0:  # No overlap for any species in the at region use
            pass
        else:
            out_df = pd.merge(base_sp_df, out_df, on='EntityID', how='left')
            [out_df.drop(m, axis=1, inplace=True) for m in out_df.columns.values.tolist() if
             m.startswith('Unnamed')]
            out_df.fillna(0, inplace=True)
            out_df.to_csv(out_csv)

    # STEP 3: Merge Tables regional range
    out_csv = out_folder_merge_region + os.sep + 'Merge_' + str(folder) + '.csv'
    if not overwrite_inter_data and os.path.exists(out_csv):
        pass
    else:
        print('             Merging overlap table regional range...{0}'.format(use_nm))
        list_percent_region_csv = os.listdir(out_folder_percent_region_use)
        list_percent_region_csv = [csv for csv in list_percent_region_csv if csv.endswith('.csv')]
        out_df = pd.DataFrame()
        for csv_r in list_percent_region_csv:
            # print out_folder_percent_region_use + os.sep + csv_r
            current_csv = out_folder_percent_region_use + os.sep + csv_r
            in_df = pd.read_csv(current_csv, dtype=object)
            out_df = pd.concat([out_df, in_df], axis=0)
        if len(out_df) == 0:  # No overlap for any species in the at region use
            pass
        else:
            out_df = pd.merge(base_sp_df, out_df, on='EntityID', how='left')
            [out_df.drop(m, axis=1, inplace=True) for m in out_df.columns.values.tolist() if
             m.startswith('Unnamed')]
            out_df.fillna(0, inplace=True)
            out_df.to_csv(out_csv)

    # STEP 3: Merge Tables NL48 range
    out_csv = out_folder_merge_NL48 + os.sep + 'Merge_' + str(folder) + '.csv'

    if not overwrite_inter_data and os.path.exists(out_csv):
        pass
    else:
        print'                 Merging overlap table NL48 range...{0}'.format(use_nm)
        list_percent_nl48_csv = os.listdir(out_folder_percent_nl48_use)
        list_percent_nl48_csv = [csv for csv in list_percent_nl48_csv if csv.endswith('.csv')]
        out_df = pd.DataFrame()
        for csv_nl48 in list_percent_nl48_csv:
            current_csv = out_folder_percent_nl48_use + os.sep + csv_nl48
            in_df = pd.read_csv(current_csv, dtype=object)
            out_df = pd.concat([out_df, in_df], axis=0)

        if len(out_df) == 0:  # No overlap for any species in the at region use
            pass
        else:
            out_df = pd.merge(base_sp_df, out_df, on='EntityID', how='left')
            [out_df.drop(m, axis=1, inplace=True) for m in out_df.columns.values.tolist()
             if m.startswith('Unnamed')]
            out_df.fillna(0, inplace=True)
            out_df.to_csv(out_csv)

end = datetime.datetime.now()
print ("End Time: " + end.ctime())
elapsed = end - start_time
print ("Elapsed  Time: " + str(elapsed))
