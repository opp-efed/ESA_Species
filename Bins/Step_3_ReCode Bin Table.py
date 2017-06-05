# Author: J. Connolly: updated 5/31/2017
# ############## ASSUMPTIONS
# Maintaining huc specific assignments, typically this happens for the marine bins when a species is found in coastal
# and land-locked hucs
# Bin columns ['Terrestrial Bin', 'Bin 1', 'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6', 'Bin 7', 'Bin 8', 'Bin 9',
# Bin 10'] are continuous
# Column order has not changed since Step 1
import os
import pandas as pd
import datetime
import sys

# #############  User input variables
# location where out table will be saved - INPUT Source user
table_folder = 'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Script_Check_20170531'

# INPUT SOURCES UpdatedBins_[date].csv  from Step_1_UpdateBinTable_RangeUpdate if update is just for the range file or
# Current_Bins_tobeLoadDB_[date].csv from Step_2_UpdateBinTable_BinAssignments if updated is based on both range and
# feedback from the Services
wide_bin_coded_name = 'UpdatedBins_20170605.csv'
entity_id_col_c = 'EntityID'  # column header species EntityID wide_bin_coded
huc2_col_c = 'HUC_2'  # column header species HUC_2 wide_bin_coded
# column headers for bins wide_bin_coded  - INPUT SOURCE- User
bin_col = ['Terrestrial Bin', 'Bin 1', 'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6', 'Bin 7', 'Bin 8', 'Bin 9', 'Bin 10']

# Note extinct species will be included in the output if a bin assignment applies- filters output to current species
current_master_nm = 'MasterListESA_Feb2017_20170410.csv'
entity_id_col_m = 'EntityID'
# Dictionary of bin track code to shorthand description
# NOTE: Everything that started with 13 is a code that cannot exists in land-locked HUCS, so it is change to No; but
# that assignment could be meaning for other HUCs. Change all keys that start with 13 if changing we do not want to
# maintain huc specific assignments. See assumptions
# ##INPUT SOURCE - User -keys are integers, values are strings
DB_code_Dict = {0: 'NAN',
                1: 'No',
                2: 'Yes',
                3: 'Yes/R',
                4: 'R',
                5: 'Dummy Bin',
                6: 'Yes',
                7: 'Indirect only- Marine host',
                8: 'Yes- FH-Obligate',
                9: 'Yes- FH-Generalist',
                10: 'Yes- FH-Specialist',
                11: 'Yes- FH-Unknown',
                12: 'Food item',
                13: 'No',
                28: 'Yes/Yes- FH-Obligate',
                29: 'Yes/ Yes-Fish Host- Generalist',
                137: 'No',
                210: 'Yes/Yes-Fish Host- Specialist',
                211: 'Yes/Yes- Fish Host- Unknown',
                412: 'Reassigned-Food item',
                312: 'Food item/Reassigned-Food item',
                612: 'Food item',
                1312: 'No',
                132: 'No',
                136: 'No',
                1328: 'No',
                1329: 'No',
                13210: 'No',
                13211: 'No'}

reassigned = [3, 4, 412, 312]  # list of coded bin values that represent a reassigned bin

# The species entityID is the tracking number by population across all ESA tools.  This number is pulled from the FWS
# TESS database. NMFS species not entered in the TESS database are given a place holder tracker that started with NMFS.
# Updated to this identifier are rare but **MUST** be taken into account at the **START** of all updates in order to
# make sure all files and species information are linked correctly.- INPUT SOURCE- User
entityid_updated = {'NMFS88': '9432',
                    'NMFS180': '11353',
                    'NMFS181': '11355',
                    '5623': '11356',
                    'NMFS22': '10377'}
# ############# Static input variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
archived_location = table_folder + os.sep + 'Archived'  # scratch workspace
os.mkdir(archived_location) if not os.path.exists(archived_location) else None
wide_bin_coded = archived_location + os.sep + wide_bin_coded_name

out_name = 'Recode_BinTable_as_of_' + date + '.csv'
outfile = table_folder + os.sep + out_name

out_name_unfiltered = 'Recode_BinTable_as_of_unfiltered' + date + '.csv'
outfile_unfiltered = archived_location + os.sep + out_name_unfiltered
current_master = archived_location + os.sep + current_master_nm


def load_data(current, master_sp):
    # VARS: current: current wide bin table in use; master_sp- current master species list
    # DESCRIPTION: removes columns without headers from all data frames; sets entity id col as str in all tables;
    # updates entity id as needed; with a secondary check using .replace to make sure bin codes load as integers.
    # Verifies all hard coded columns are found in tables. Try/Excepts makes sure we have a complete archive of data
    # used for update,and intermediate tables.
    # RETURN: data frames of inputs tables; KEY col headers standardize; entity ids updated when needed; and current
    # list of species to filter table at end
    try:
        current_df = pd.read_csv(current)
    except IOError:
        print('\nYou must move the current wide table to Archived folder for this update')
        sys.exit()

    [current_df.drop(z, axis=1, inplace=True) for z in current_df.columns.values.tolist() if z.startswith('Unnamed')]
    current_df[str(entity_id_col_c)] = current_df[str(entity_id_col_c)].astype(str)
    [current_df[str(entity_id_col_c)].replace(z, entityid_updated[z], inplace=True) for z in entityid_updated.keys()]
    final_cols = current_df.columns.values.tolist()
    if 'Spe_HUC' not in final_cols:
        current_df['Spe_HUC'] = current_df[entity_id_col_c].astype(str) + "_" + current_df[huc2_col_c].astype(str)
        final_cols = current_df.columns.values.tolist()

    current_df = current_df.reindex(columns=final_cols)
    current_df[bin_col] = current_df[bin_col].apply(pd.to_numeric, errors='ignore')
    current_df[bin_col].replace({.0: ''})  # if a number was load as a float will strip the decimal place

    try:
        sp_df = pd.read_csv(master_sp)
    except IOError:
        print('\nYou must move the master species table to Archived folder for this update')
        sys.exit()
    [sp_df.drop(z, axis=1, inplace=True) for z in sp_df.columns.values.tolist() if z.startswith('Unnamed')]
    sp_df[str(entity_id_col_c)] = sp_df[str(entity_id_col_m)].astype(str)
    list_current_species = sp_df[str(entity_id_col_m)].values.tolist()

    return current_df, final_cols, list_current_species


def check_reassigned_bin(row, in_df):
    # VARS: row: row of data being updated (apply function to flag species with a reassigned bin code) df: working_df
    # with bin codes
    # DESCRIPTION: Flags species as true or false if they have a reassigned bin values, based on reassigned list;
    # Try/Except capture an index error when making update
    # RETURN: updated values for column being updated by apply function

    bool_reassign = 'FALSE'
    ent_huc = row['Spe_HUC']
    lookup_huc_bins = in_df.loc[in_df['Spe_HUC'] == ent_huc]
    lookup_huc_bins = lookup_huc_bins.loc[:, bin_col]
    try:
        lookup_huc_bins = lookup_huc_bins.values.tolist()[0]
        for z in lookup_huc_bins:
            if z in reassigned:
                bool_reassign = 'TRUE'
            else:
                pass

        return bool_reassign
    except IndexError:
        return 'Check'


def check_multi_huc(row, in_df):
    # VARS: row: row of data being updated (apply function to flag species with a reassigned bin code) df: working_df
    # with bin codes
    # DESCRIPTION: Flags species as Y  if they are found in multiple HUC2s Try/Except capture an index error when
    # making update
    # RETURN: updated values for column being updated by apply function
    entid = row[str(entity_id_col_c)]

    filter_df = in_df.loc[in_df[str(entity_id_col_c)] == entid]
    if len(filter_df) > 1:
        return 'Y'
    elif len(filter_df) == 1:
        return 'N'
    else:
        'Check'


def check_sur_huc(row):

    # VARS: row: row of data being updated (apply function to flag species with a reassigned bin code) df: working_df
    # with bin codes
    # DESCRIPTION: Flags species as TRUE/FALSE if we need to use a surrogate HUC2; all HUC2s that start with 22 (Pacific
    # Islands) use Hawaii (21) as the surrogate huc2
    # Try/Except capture an index error when making update
    # RETURN: updated values for column being updated by apply function
    huc_2 = row[str(huc2_col_c)]
    huc_2 = int(huc_2[:2])
    if huc_2 >= 22:
        return 'TRUE'
    elif huc_2 <= 21:
        return 'FALSE'
    else:
        return 'Check'


def flag_dd_species(row):
    # VARS: row: row of data being updated in working df
    # DESCRIPTION: Flags species, Yes/No, as a DD based on bin assignment
    # Try/Except catches any species still without a bin assignment and does not flag it as a DD species
    # Logic for assignment: If a species is assigned or reassigned as a surrogate to Bins 2,3, or 4 they are in the
    # analysis. If the species is a mammal or bird and has a food item in Bin 2,3, or 4, they are also in the analysis.
    # If the species is a mammal or bird and has food items only found in marine bin that were reassigned to Bin 2,3 or
    # 4, they are not in the analysis. If a species is only assigned to Bins 5, 6, or 7, they are not in the analysis.
    # If the species is a mammal or bird and has a food item only in 5, 6, or 7,they are also not in the analysis.
    # RETURN: updated values for column being updated by apply function

    try:
        if row['Bin 2'].startswith('Yes'):
            return 'Yes'
        elif row['Bin 3'].startswith('Yes'):
            return 'Yes'
        elif row['Bin 4'].startswith('Yes'):
            return 'Yes'
        elif row['Bin 2'].startswith('Food item'):
            return 'Yes'
        elif row['Bin 3'].startswith('Food item'):
            return 'Yes'
        elif row['Bin 4'].startswith('Food item'):
            return 'Yes'
        else:
            return 'No'
    except AttributeError:
        return 'No Bin Assignment'

# Time tracker
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Step 1: load data from current bin tables, splits species info and database info into their own data frame using index
# location of bin cols.  To split the df this way it is assumed that cols are in the correct order in the current bin
# table.  Done by index rather than hard code this way so we change the supporting columns without impacting the code.

df_bins, df_bins_col, list_sp = load_data(wide_bin_coded, current_master)

supporting_col_index = df_bins_col.index(bin_col[0])
trailing_col_index = df_bins_col.index(bin_col[len(bin_col) - 1]) + 1
df_bins['Reassigned'] = df_bins.apply(lambda row: check_reassigned_bin(row, df_bins), axis=1)


# Step 2: Break coded bin table into three parts, species info (leading_col), bin info (bins) and trailing_col (database
# info for tracking updates to assignment
leading_col = df_bins.iloc[:, :supporting_col_index]
bins = df_bins.iloc[:, supporting_col_index:trailing_col_index]
bins.fillna(0, inplace=True)
trailing_col = df_bins.iloc[:, trailing_col_index:]

# Step 3:Update of of the bins codes to shorthand description based on DB_code_Dict dictionary
keys = DB_code_Dict.keys()
for i in keys:
    value = (DB_code_Dict[i])
    bins = bins.replace({i: value})
    print "Replaced {0} to {1}".format(i, value)

# Step 4: Merge the species info, bins and database info back into one df
df_out = pd.concat([leading_col, bins], axis=1)
df_out = pd.concat([df_out, trailing_col], axis=1)

# Step 5: Populated the MultiHUC and sur_HUC columns based on species/huc combos in working_df; all HUC2 that start with
# 22 (pacific islands) use 21 (HI) has surrogate, Populated updated blank updated columns to no because any species
# without a values in the Updated column was not updated
df_final = pd.DataFrame(df_out)
df_final['Multi HUC'] = df_final.apply(lambda row: check_multi_huc(row, df_final), axis=1)
df_final['sur_huc'] = df_final.apply(lambda row: check_sur_huc(row), axis=1)
df_final.loc[df_final['sur_huc'] == 'TRUE', 'HUC_2'] = '21'

df_final['Updated'].fillna('No', inplace=True) if 'Updated' in df_final.columns.values.tolist() else None

# Step 6: Drop columns added during processing, and drop any duplicate row in data frame
[df_final.drop(v, axis=1, inplace=True) for v in ['Spe_HUC']]
df_final.drop_duplicates(inplace=True)

# Step 7: Flag Downstream dilution species
df_final['DD_Species'] = df_final.apply(lambda row: flag_dd_species(row), axis=1)

# Step 8:Filters the list to the species on the current master. Exports filter data frame to csv in root folder, export
# unfiltered table to archived folder
df = df_final.loc[df_final[str(entity_id_col_c)].isin(list_sp)]
df_final.to_csv(outfile_unfiltered)
df.to_csv(outfile)
print '\nCheck output table for errors in the reassigned, multi huc, sur huc and dd species columns'

# Elapsed time
print "Script completed in: {0}".format(datetime.datetime.now() - start_time)
