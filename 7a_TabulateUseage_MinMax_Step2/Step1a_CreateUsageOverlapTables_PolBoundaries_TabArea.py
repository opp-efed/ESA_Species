import datetime
import os
import arcpy

import pandas as pd

# **TOOL uses converts political boundary tables that were generated using Tabulate area into the needed input tables*
# Title- Generate overlap tables from geoid to use layers results; - overlap found within political boundaries
# this is used as an input variable fo usage


# TODO Check if needed - can I just copy the raw outputs to the new location or are the tables being transformed

# Static variables are updated once per update; user input variables update each  run

# ASSUMPTIONS

# # https://usepa-my.sharepoint.com/personal/connolly_jennifer_epa_gov/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fconnolly%5Fjennifer%5Fepa%5Fgov%2FDocuments%2FDocuments%5FC%5Fdrive%2FProjects%2FESA%2F%5FED%5Fresults%2FArchive%2FTabulated%5FJan2018%2FPolticalBoundaries%2FPolticalBoundaries%2FAgg%5FLayers%2FCounties
# ###############user input variables
overwrite_inter_data = False

raw_results_csv = r'L:\Workspace\StreamLine\ESA\Results_Usage\PolBoundaries\Agg_layers'


find_file_type = raw_results_csv.split(os.sep)
# ########### Updated once per run-variables

look_up_fips = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
               r'\_ExternalDrive\_CurrentSpeciesSpatialFiles\Boundaries.gdb\Counties_all_overlap_Mercator'


find_file_type = raw_results_csv.split(os.sep)

out_root = r'L:\Workspace\StreamLine\ESA\Tabulated_PolBoundaries' + os.sep + 'PoliticalBoundaries'

out_results = out_root + os.sep + 'Agg_Layers'

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')


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
    df['STATEFP'] = df['GEOID'].map(lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)
    sum_by_state = df.groupby(['STATEFP', 'STUSPS'], as_index=False).sum()
    return sum_by_state


def extract_overlap_interval(use_df):
    # removed all extraneous columns only import columns are the VALUE_[zoneID] and default Label col from tool export
    drop_cols = [z for z in df_use.columns.values.tolist() if not z.startswith('VALUE')]
    drop_cols.remove('GEOID')
    [use_df.drop(j, axis=1, inplace=True) for j in drop_cols if j in drop_cols]
    # Limits table to GeoID and use overlap by distance
    use_df['GEOID'] = use_df['GEOID'].map(lambda x: str(x) if len(x)==5 else '0'+str(x)).astype(str)

    return use_df


def use_by_fips(use_df):
    overlap_df = extract_overlap_interval(use_df)

    fips_zone_array = arcpy.da.TableToNumPyArray(look_up_fips, ['GEOID', 'STUSPS', 'STATEFP', 'Acres'])
    fips_zone_df = pd.DataFrame(data=fips_zone_array, dtype=object)
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
            csv = csv.replace('__', '_')  # error correction for file names with double _ in name- remove in future

            # Step 1: Sum by FIPS, for each interval and add acres columns, export table to Counties folder

            out_sum_direct = out_folder_sum_use_fips + os.sep + folder + '.csv'

            if not overwrite_inter_data and (os.path.exists(out_sum_direct)):
                pass
            else:
                print '   Summing tables by fips...for use {0}'.format(folder)
                if len(df_use)> 0:
                    use_fips_direct = use_by_fips(df_use)
                    out_df_direct = pd.concat([out_df_direct, use_fips_direct], axis=0)
                else:
                    print"****Check overlap tables for above run, no row confirm this is correct***"

        if not overwrite_inter_data and (os.path.exists(out_sum_direct)):
            out_df_direct = pd.read_csv(out_sum_direct)
            [out_df_direct.drop(m, axis=1, inplace=True) for m in out_df_direct.columns.values.tolist() if
             m.startswith('Unnamed')]
        else:
            out_df_direct.to_csv(out_sum_direct)

        # Step 2: Sum by to state from ; export table to Counties folder
        print '   Summing tables by state...use {0}'.format(folder)
        out_sum_direct_st = out_folder_sum_use_state + os.sep + folder + '.csv'

        if not os.path.exists(out_sum_direct_st):
            if len(out_df_direct)> 0:
                out_df_state_use = collapse_state(out_df_direct)
                out_df_state_use.to_csv(out_sum_direct_st)
            else:
                print"****Check overlap tables for above run, no row confirm this is correct***"

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
