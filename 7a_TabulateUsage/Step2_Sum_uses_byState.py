import datetime
import os
import sys

import numpy as np
import pandas as pd

# Description: Summarizes chemical results by use, all species are found in the single output table for a given UDL.
# Generates the Parent table for the chemical - summed msq to a species/state combo includes PCT data and the upper,
# lower ,and uniform treated area for a species /state by UDL

# This script has been approved for release by the U.S. Environmental Protection Agency (USEPA). Although
# the script has been subjected to rigorous review, the USEPA reserves the right to update the script as needed
# pursuant to further analysis and review. No warranty, expressed or implied, is made by the USEPA or the U.S.
# Government as to the functionality of the script and related material nor shall the fact of release constitute
# any such warranty. Furthermore, the script is released on condition that neither the USEPA nor the U.S. Government
# shall be held liable for any damages resulting from its authorized or unauthorized use.

# This script generates the Parent table for the chemical - summed msq to a species/state combo includes PCT data
# and the upper, lower ,and uniform direct overlap area for a species /state


chemical_name = 'Glyphosate'  # chemical name used for tracking
# flags to apply to the end of output file name for the different iterations of overlap.  This should always include
# 'census' for the standard no adjustment and aggregated PCT runs; but user can had in additional flags to other
# iterations of the overlap that included supplemental information just as habitat
suffixes = ['census', 'adjHab']  # 'census' covers the no adjustment table can include others e.g. 'adjHab' result suffixes
pct_groups = ['no adjustment','max','min','avg']   # 'no adjustment 'max','min','avg'
# input table identifying the uses and drift limits for the chemical
use_lookup = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs" \
             + os.sep + chemical_name + os.sep + "GLY_Uses_lookup_June2020_v2.csv"
# in location for either range or critical habitat change the end of the path for file type
# path is where the files where save from the previous step
in_location = r'D:\Tabulated_Habitat' + os.sep + chemical_name + os.sep + 'Range'
"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\MasterListESA_Dec2018_June2020.csv"
# columns to include
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS',  'WoE Summary Group', 'Critical_Habitat_YesNo', 'Migratory',
                      'Migratory_YesNo', 'CH_Filename', 'Range_Filename', 'L48/NL48']

# Static Variables not user input needed
interval_step = 30
max_dis = 1501
out_folder = in_location + os.sep + 'SprayInterval_IntStep_{0}_MaxDistance_{1}'.format(str(interval_step), str(max_dis))
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
dir_folder = os.path.dirname(in_location)

bins = np.arange((0 - interval_step), max_dis, interval_step)
# list regions
nl_48_regions = ['AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI']
# read in use look up tables
use_lookup_df = pd.read_csv(use_lookup)
usage_lookup_df = use_lookup_df.ix[:, ['FullName', 'Usage lookup', 'FinalColHeader', 'Type', 'Cell Size']].copy()
# loads the format for the final results
usage_lookup_df['Filename'] = usage_lookup_df['FullName'].map(lambda x: str(x) + "_euc.csv").astype(str)


# Functions
def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        # print "created directory {0}".format(DBF_dir)


def loop_use(c_list, outpath_use, c_group, c_value, replace_value):
    # roll up all species in the CONUS by use; ie one table per use for all species
    use_list = []
    for csv in c_list:
        csv_lookup = csv.replace(replace_value, ".csv")
        remove_sp_abb = csv_lookup.split("_")[0] + "_" + csv_lookup.split("_")[1] + "_"
        csv_lookup = csv_lookup.replace(remove_sp_abb, '')
        if csv_lookup not in use_list:
            use_list.append(csv_lookup)
    use_list = list(set(use_list))

    for use in use_list:
        # print( '\n Working on use {0}: {1} of {2}').format(use, use_list.index(use)+1, len(use_list))
        merge_use = pd.DataFrame()
        if c_group == 'no adjustment':
            nam_merg = use.replace('.csv', "_noadjust" + '.csv')
        else:
            if c_group not in use.split("_"):
                nam_merg = use.replace('.csv', "_" + c_value + "_" + c_group+'.csv')
                print nam_merg, use
            else:
                nam_merg = use
                print nam_merg, use

        if not os.path.exists(outpath_use + os.sep + nam_merg):
            for csv in c_list:
                csv_lookup = csv.replace(replace_value, ".csv")
                remove_sp_abb = csv_lookup.split("_")[0] + "_" + csv_lookup.split("_")[1] + "_"
                csv_lookup = csv_lookup.replace(remove_sp_abb, '')
                if csv_lookup == use:
                    use_df = pd.read_csv(in_location_group + os.sep + csv)
                    # drops the unnamed columns included for tracking
                    drop_col = [m for m in use_df.columns.values.tolist() if m.startswith('Unnamed')]
                    use_df.drop(drop_col, axis=1, inplace=True)
                    # Set col data type for merges
                    use_df['EntityID'] = use_df['EntityID'].map(lambda x: str(x).split('.')[0]).astype(str)
                    distance_cols = [c for c in use_df.columns.values.tolist() if c.startswith("VALUE")]
                    dis_values = [int(x.split("_")[1]) for x in distance_cols]
                    dis_values = sorted(dis_values)
                    if len(dis_values) != 644:  # QC check on all distance columns
                        print 'Rerun {0} some of the distance columns are missing; script will exit, check all use'.format(csv)
                        sys.exit()

                    col_order = [c for c in use_df.columns.values.tolist() if not c.startswith("VALUE")]
                    for x in dis_values:
                        col_order.append("VALUE_" + str(x))
                    # sets data types of distance columns to numeric for summing
                    use_df.ix[:, distance_cols] = use_df.ix[:, distance_cols].apply(pd.to_numeric)
                    # merges the current species table to the working use table for all species
                    current_length = len(merge_use)
                    merge_use = pd.concat([merge_use, use_df], axis=0)
                    # confirms the merge loaded correctly - concat is faster than merger, but see concat can load blank
                    # rows if the different tables don't align properly
                    check_merge = merge_use.dropna(subset=['VALUE_0'])
                    new_length = len(check_merge)
                    if new_length != current_length + len(use_df):
                        print('Merge did not complete correctly for csv {0} check the column headers and blank rows'.format(csv))
                        sys.exit()

            out_use = merge_use.reindex(columns=col_order)  # reindex to desired columns order
            out_use.fillna(0, inplace=True)  # add in 0 for no overlap
            print outpath_use + os.sep + nam_merg  # prints out location for tracking
            # saves table
            out_use.to_csv(outpath_use + os.sep + nam_merg)
            print("Table can be found at {0}".format(outpath_use + os.sep + nam_merg))

        else:
            print("Already created {0}".format(outpath_use + os.sep + nam_merg))


def loop_use_nl48(c_list, outpath_use, c_group, c_value, replace_value, nl48_uses):
    # roll up all species in the nl48 by use; ie one table per use for all species
    for use in nl48_uses:
        print('\n Working on use {0}: {1} of {2}'.format(use, nl48_uses.index(use) + 1, len(nl48_uses)))
        merge_nl48 = pd.DataFrame()
        if c_group == 'no adjustment':
            nam_merg = "NL48_" + use
        else:
            if c_group not in use.split("_"):
                nam_merg = "NL48_" + use + "_" + c_value + "_" + c_group+'.csv'
                print nam_merg, use
            else:
                nam_merg = "NL48_" + use
                print nam_merg, use

        for csv in c_list:  # loop over all use results
            region = csv.split("_")[0]
            # identifies key to link chemical name to use
            csv_lookup = csv.replace(replace_value, "")
            csv_lookup = csv_lookup.replace(region + '_', "")

            if csv_lookup == use and not csv.startswith('NL48'):
                use_df = pd.read_csv(outpath_use + os.sep + csv)
                # drops the unnamed columns included for tracking
                drop_col = [m for m in use_df.columns.values.tolist() if m.startswith('Unnamed')]
                use_df.drop(drop_col, axis=1, inplace=True)
                # Set col data type for merges
                use_df['EntityID'] = use_df['EntityID'].map(lambda x: x).astype(str)

                # remove rows for species outside of region; all species  loaded with when over in a region is 0

                if c_group != 'no adjustment':
                    # drop extra rows where there is no overlap for the state boundary - usage only/
                    use_df = use_df.loc[(use_df['State msq adjusted by PCT'] != 0)]
                # get list of distance columns
                distance_cols = [c for c in use_df.columns.values.tolist() if c.startswith("VALUE")]
                # set data type for distance cols to numeric
                use_df.ix[:, distance_cols] = use_df.ix[:, distance_cols].apply(pd.to_numeric)
                # mergers the current species table to the working use table for all species
                current_length = len(merge_nl48)
                merge_nl48 = pd.concat([merge_nl48, use_df], axis=0)
                # confirms the merge loaded correctly - concat is faster than merger, but see concat can load blank
                # rows if the different tables don't align properly
                check_merge = merge_nl48 .dropna(subset=['VALUE_0'])
                new_length = len(check_merge)
                if new_length != current_length + len(use_df):
                    print('Merge did not complete correctly for csv {0} check the column headers and blank rows'.format(csv))
                    sys.exit()

        merge_nl48.drop_duplicates(inplace=True)  # drops duplicate if a species was loaded twice; this can happen
        # when a previous NL48 get loaded with individual states found in conus they are removed; this happens if
        # no overlap occurred for all species and use
        # Set data type for filter to integer
        merge_nl48['STATEFP'] = merge_nl48['STATEFP'].map(lambda x: x).astype(int)
        # filters to just the NL48 states
        merge_filter = merge_nl48[merge_nl48['STATEFP'].isin([2, 15, 60, 66, 69, 72, 78])]  # code for NL48 states
        # export table
        merge_filter.to_csv(outpath_use + os.sep + nam_merg)
        print("Table can be found at {0}".format(outpath_use + os.sep + nam_merg))


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# create output locations
create_directory(out_folder)
create_directory(out_folder + os.sep + 'ParentTables')

# loops over PCT groups results
for group in pct_groups:   # add pct values as needed suffixes 'no adjustment',
    print group
    # create out directory
    create_directory(out_folder + os.sep + 'ParentTables' + os.sep + group)
    in_location_group = in_location + os.sep + group
    # get list of csvz
    list_csv = os.listdir(in_location_group)
    if group == 'no adjustment':
        out_path_use = out_folder + os.sep + 'ParentTables' + os.sep + group + os.sep + 'noadjust'
        create_directory(out_path_use)
        list_csv = [csv for csv in list_csv if csv.endswith('.csv')]
        c_list = [v for v in list_csv if v.endswith('_noadjust.csv')]
        # roll up all species in the CONUS by use; ie one table per use for all species
        loop_use(c_list, out_path_use, group, '', "_noadjust.csv")

        # get a list of NL48 folders and loops over uses then  rolls up tables so there is one use for all species by
        # region and across NL48 regions
        list_csv_out = os.listdir(out_path_use)
        c_nl48list = [v for v in list_csv_out if v.endswith("_noadjust.csv")]
        c_nl48list = [v for v in c_nl48list if not v.startswith('CONUS')]
        nl_48_regions_uses = []
        for t in c_nl48list:
            t = t.replace("_" + group + '.csv', '')
            for region_c in nl_48_regions:
                if t.split("_")[0] in nl_48_regions:
                    t = t.replace(t.split("_")[0]+"_", '')
                    if t not in nl_48_regions_uses:
                        nl_48_regions_uses.append(t)
        # roll up all species in the nl48 by use; ie one table per use for all species
        loop_use_nl48(c_nl48list, out_path_use, group, '',  "_" + group + '.csv', nl_48_regions_uses)

    else:
        for value in suffixes:
            out_path_use = out_folder + os.sep + 'ParentTables' + os.sep + group + os.sep + value
            create_directory(out_path_use)
            # get a list of table for suffix group
            c_list = [v for v in list_csv if v.endswith(value + "_" + group + '.csv')]
            # roll up all species in the CONUS by use; ie one table per use for all species
            loop_use(c_list, out_path_use, group, value, "_" + value + "_" + group + '.csv')
            # get a list of NL48 folders and loops over uses then  rolls up tables so there is one use for all species
            # by region and across NL48 regions
            list_csv_out = os.listdir(out_path_use)
            c_nl48list = [v for v in list_csv_out if v.endswith(value + "_" + group + '.csv')]
            c_nl48list = [v for v in c_nl48list if not v.startswith('CONUS')]

            nl_48_regions_uses = []  # list of nl48 uses
            for t in c_nl48list:
                t = t.replace("_" + value + "_" + group + '.csv', '')
                for region_c in nl_48_regions:
                    if t.split("_")[0] in nl_48_regions:
                        t = t.replace(t.split("_")[0]+"_", '')
                        if t not in nl_48_regions_uses:
                            nl_48_regions_uses.append(t)
            # roll up all species in the nl48 by use; ie one table per use for all species
            loop_use_nl48(c_nl48list, out_path_use, group, value, "_" + value + "_" + group + '.csv',
                          nl_48_regions_uses)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
