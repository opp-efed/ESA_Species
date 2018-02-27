import datetime
import os
import arcpy

import pandas as pd

# Title- Generate overlap tables from geoid to use layers results;
#               1) Generates tables for aggregated layers, AA, Ag and NonAG
#                       1a) The final merged output are used to generate distance interval table for spray drift; and
#                           summarized BE table (0, 305m and 765)
# TODO Add look to read in vector table once vector overlap final

# Static variables are updated once per update; user input variables update each  run

# ASSUMPTIONS


# ###############user input variables
overwrite_inter_data = True

raw_results_csv = r'L:\ESA\Results\diazinon\Usage\Results_PolBoundaries\Agg_Layers'

# raw_results_csv = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects' \
#                   r'\ESA\_ExternalDrive\_CurrentResults\Results_diaz\L48\Agg_Layers\Range'


find_file_type = raw_results_csv.split(os.sep)
# ########### Updated once per run-variables

look_up_fips = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
               r'\_ExternalDrive\_CurrentSpeciesSpatialFiles\Boundaries.gdb\Counties_all_overlap_albers'
look_up_use = r'L:\ESA\Results\diazinon\RangeUses_lookup.csv'

find_file_type = raw_results_csv.split(os.sep)

out_root = r'L:\ESA\Results\diazinon\Tabulated_PolBoundaries' + os.sep + 'PoliticalBoundaries'

out_results = out_root + os.sep + 'Agg_Layers'

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

use_lookup = pd.read_csv(look_up_use)


# ###Functions


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


def sum_df(df):
    str_cols = ['GEOID', 'STUSPS', 'STATEFP']
    num_cols = [v for v in df.columns.values.tolist() if v not in str_cols]
    df.ix[:, num_cols] = df.ix[:, num_cols].apply(pd.to_numeric)
    sum_by_geoid = df.groupby(['GEOID', 'STUSPS', 'STATEFP'], as_index=False).sum()
    return sum_by_geoid


def collapse_state(df):
    sum_by_state = df.groupby(['STATEFP', 'STUSPS'], as_index=False).sum()
    return sum_by_state


def extract_overlap_interval(use_df):
    # removed all extraneous columns only import columns are the VALUE_[zoneID] and default Label col from tool export
    drop_cols = [z for z in use_df.columns.values.tolist() if not z.startswith('G')]
    drop_cols.remove('LABEL')
    [use_df.drop(j, axis=1, inplace=True) for j in drop_cols if j in drop_cols]
    # transform table so it is geoid by distance interval; rest index; update column header and remove 'VALUE' form

    overlap_df = use_df.T
    overlap_df = overlap_df.reset_index()
    overlap_df.columns = overlap_df.iloc[0]
    overlap_df = overlap_df.reindex(overlap_df.index.drop(0))
    update_cols = overlap_df.columns.values
    update_cols[0] = 'GEOID'
    overlap_df.columns = update_cols
    overlap_df['GEOID'] = overlap_df['GEOID'].map(lambda x: str(x).split('_')[1]).astype(str)

    return overlap_df


def use_by_fips(use_df):
    overlap_df = extract_overlap_interval(use_df)

    fips_zone_array = arcpy.da.TableToNumPyArray(look_up_fips, ['GEOID', 'STUSPS', 'STATEFP', 'Acres', 'Region'])
    fips_zone_df = pd.DataFrame(data=fips_zone_array, dtype=object)
    fips_zone_df.drop('Region', axis=1, inplace=True)
    merged_df = pd.merge(overlap_df, fips_zone_df, on='GEOID', how='left')

    sum_by_fips = sum_df(merged_df)

    return sum_by_fips


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
s_out_folder = out_results

sub_root_folder = os.path.dirname(out_root)
create_directory(sub_root_folder)
create_directory(out_root)
create_directory(s_out_folder)

s_results_folder = raw_results_csv
# Set up output root folder file structure


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
    out_folder_sum_use_fips = out_results + os.sep + 'Counties'
    out_folder_sum_use_state = out_results + os.sep + 'States'


    create_directory(out_folder_sum_use_fips)
    create_directory(out_folder_sum_use_state)

    # parse out use name and region from folder name load use support info from look_up_use and acres info for region
    # and whole range of species

    print '\nWorking on {0}: {1} of {2}'.format(folder, (list_folders.index(folder)) + 1, len(list_folders))
    # set up list of result csv files
    list_csv = os.listdir(raw_results_csv + os.sep + folder)
    list_csv = [csv for csv in list_csv if csv.endswith('_County.csv')]

    # Loop through raw use result tables
    if len(list_csv) == 0:  # out use folder create but not runs complete
        pass
    else:
        out_df_direct = pd.DataFrame()
        out_df = pd.DataFrame()

        for csv in list_csv:
            # load outside function due to double _ added by filename by default w. arcpy- corrected moving forward
            df_use = pd.read_csv(raw_results_csv + os.sep + folder + os.sep + csv, dtype=object)
            csv = csv.replace('__', '_')  # error correction for file names with double _ in name- remove in futurr

            # Step 1: Sum by species- step up parse  FIPS  then sum so there is one value per species

            out_sum_direct = out_folder_sum_use_fips + os.sep + folder + '.csv'

            if not overwrite_inter_data and (os.path.exists(out_sum_direct)):
                pass
            else:
                print '   Summing tables by fips...for use {0}'.format(folder)
                use_fips_direct = use_by_fips(df_use)
                out_df_direct = pd.concat([out_df_direct, use_fips_direct], axis=0)

        if not overwrite_inter_data and (os.path.exists(out_sum_direct)):
            out_df_direct = pd.read_csv(out_sum_direct)
            [use_fips_direct.drop(m, axis=1, inplace=True) for m in use_fips_direct.columns.values.tolist() if
             m.startswith('Unnamed')]
        else:
            out_df_direct.to_csv(out_sum_direct)

        # Step 2: Sum by to state from FIPS
        print '   Summing tables by state...use {0}'.format(folder)
        out_sum_direct_st = out_folder_sum_use_state + os.sep + folder + '.csv'

        if not os.path.exists(out_sum_direct_st):
            out_df_state_use = collapse_state(out_df_direct)
            out_df_state_use.to_csv(out_sum_direct_st)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
