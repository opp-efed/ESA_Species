# Author: J. Connolly: updated 6/12/2017, 6/1/2017
# ##############General notes
# All hard coded columns are purposefully, these are new columns used for tracking
# Before running this make sure WoE have not been changes since they were updated during Step 1 or 2
# Output: LongBins_PP_filter_AB_[date].csv goes to [C.Peck] to be incorporated into the post-processor
# In Step_7_long_to_wide; there is another check for duplicates; of duplicates are found these tables should be updated

# ############## ASSUMPTIONS
# Col order has not changed since step 1
# Long bin table is used a master for entity id and huc2 columns names
# Master huc12/ab split is used for huc12 columns names
# Col header in long bin table should be used out output header for final data frame
# Woe groups have not changes since they were updated in Step 1 or Step 2 NOTE can add loop in to also make update to
# WoE groups

import pandas as pd
import os
import datetime
import sys

# #############  User input variables
# location where out table will be saved - INPUT Source user
table_folder = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Script_Check_20170627'

# LongBins_unfiltered_AB_[date].csv from Step_5_A_B_split; file should already be located at the
# path table_folder
in_table_nm = 'LongBins_unfiltered_AB_20170627.csv'
entity_id_col_c = 'EntityID'
huc2_col_c = 'HUC_2'

# Active species master list with supporting information
# Note extinct species will be included in the output table
# Place master species list into the archive folder for tracking found at table_folder
current_master_nm = r'MasterListESA_Feb2017_20170410.csv'
entity_id_col_m = 'EntityID'
lead_agency_col = 'Lead Agency'

# ############# Static input variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
archived_location = table_folder + os.sep + 'Archived'  # scratch workspace
os.mkdir(archived_location) if not os.path.exists(archived_location) else None
long_bins = archived_location + os.sep + in_table_nm
# used to make a wide version of the A/B splits; should be the same as long_bins unless species info is updated
df_unfiltered_outfile = table_folder + os.sep + 'LongBins_unfiltered_AB_' + str(date) + '.csv'
# Used as input for post-processor
df_filtered_outfile = table_folder + os.sep + 'LongBins_PP_filter_AB_' + str(date) + '.csv'
current_master = archived_location + os.sep + current_master_nm
duplicate_assignments = archived_location + os.sep + 'DuplicateBinAssignment_' + str(date) + '.csv'


def update_bin_info(bins_df, current_species):
    # VARS: current:bins_df: bin_df: data frame of active bin table; bins_df: data frame of master species list
    # DESCRIPTION: Checks for conflicting bin assigns for the same species in a given huc2 but looking for duplicates on
    # unique identifier ['Spe_HUC_bin']. If duplicates exist user will be prompted to address them. Filters bin data
    # frame down to just the species on the current_species data frame and then a second data frame of just species
    # found in bins 2-10 used for the post processor. Both are in long format
    # RETURN: a data frame will all bin assignments for species on current_species list; a second data frame filtered to
    # species that occur in bins 2-10
    bins_df.drop_duplicates(inplace=True)
    bins_df['Spe_HUC'] = bins_df[entity_id_col_c].astype(str) + "_" + bins_df[huc2_col_c].astype(str)  # unique ID
    check_dup_bin = bins_df.loc[:, ['Spe_HUC', 'A_Bins', 'Value', 'Updated']]
    check_dup_bin.drop_duplicates(inplace=True)
    check_dup_bin['Spe_HUC_bin'] = check_dup_bin['Spe_HUC'].astype(str) + "_" + (check_dup_bin['A_Bins']).astype(str)
    dup = check_dup_bin.set_index('Spe_HUC_bin').index.get_duplicates()

    if len(dup) != 0:
        print dup
        print check_dup_bin.loc[check_dup_bin['Spe_HUC_bin'].isin(dup)]
        print '\n Duplicates values for a species in a huc in need to be addressed before moving forward \n ' \
              'See file: {0}'.format(duplicate_assignments)
        check_dup_bin.loc[check_dup_bin['Spe_HUC_bin'].isin(dup), ['Updated']] = 'Duplicate Assignment'
        check_dup_bin.to_csv(duplicate_assignments)
        sys.exit()
    else:
        current_species_bins = bins_df.loc[bins_df[entity_id_col_m].isin(current_species)]
        drop_value_no = current_species_bins.loc[~current_species_bins['Value'].isin(['No'])]
        drop_value_no = drop_value_no.loc[~drop_value_no['A_Bins'].isin(['Bin 1'])]

    return drop_value_no, current_species_bins


def set_common_cols(bin_df, sp_df):
    # VARS: bin_df: data frame of active bin table; sp_df: data frame of master species list
    # DESCRIPTION: Checks with user to see which columns from the master species list should be included on the updated
    # bin table
    # RETURN: list of columns to be included

    sp_df_cols = sp_df.columns.values.tolist()
    bin_df_cols = bin_df.columns.values.tolist()
    common_cols = [j for j in sp_df_cols if j in bin_df_cols]

    poss_answer = ['Yes', 'No']
    ask_q = True
    while ask_q:
        for p in common_cols:
            if p == '':
                common_cols.remove(p)
        user_input = raw_input(
            'Are these are the columns that should be updated from master species table? {0}: Yes or No: '.format(
                common_cols))
        if user_input not in poss_answer:
            print 'This is not a valid answer: type Yes or No'
        elif user_input == 'Yes':
            break
        else:
            additional_cols = raw_input('Which additional columns should be included - {0}: '.format(sp_df_cols))
            if type(additional_cols) is str:
                additional_cols = additional_cols.split(",")
                additional_cols = [j.replace("' ", "") for j in additional_cols]
                additional_cols = [j.replace("'", "") for j in additional_cols]
                additional_cols = [j.lstrip() for j in additional_cols]
                common_cols.extend(additional_cols)
    return common_cols


def check_final_col_order(cur_cols, updated_cols):
    # VARS: current:cur_col: order of cols in current wide bin table, updated_cols: columns identified by use to include
    # in output
    # DESCRIPTION: Set the desired order for the columns in the output table
    # RETURN: columns headers to be used as reindex for the output table

    for r in updated_cols:
        if r not in cur_cols:
            index_location = raw_input(
                'What is the index position where this column should be inserted- based 0 {0}: '.format(r))
            cur_cols.insert(int(index_location), r)

    poss_answer = ['Yes', 'No']
    ask_q = True
    while ask_q:
        user_input = raw_input('Is this the order you would like the columns to be {0}: Yes or No: '.format(cur_cols))
        if user_input not in poss_answer:
            print 'This is not a valid answer'
        elif user_input == 'Yes':

            break
        else:
            cur_cols = raw_input('Please enter the order of columns comma sep str ')
    if type(cur_cols) is str:
        cur_cols = cur_cols.split(",")
        cur_cols = [j.replace("' ", "") for j in cur_cols]
        cur_cols = [j.replace("'", "") for j in cur_cols]
        cur_cols = [j.lstrip() for j in cur_cols]
    return cur_cols


def update_columns(current_col, updated_col, list_of_col):
    # VARS: current:current_col: value in current list, updated_col: value it should be updated to, list_of_col: list of
    # values
    # DESCRIPTION: Standardize column headers using the current_bin_table header values as the master
    # RETURN: Update list of value used to update column headers

    if current_col != updated_col:
        loc = list_of_col.index(str(current_col))
        list_of_col.remove(str(current_col))
        list_of_col.insert(loc, str(updated_col))
        return list_of_col
    else:
        return list_of_col


def load_data(current, master_sp):
    # VARS: current: current long bin table in use; removed: master_sp current species master list
    # DESCRIPTION: removes columns without headers from all data frames; sets entity id col as str in all tables;
    #  generated list of col headers from in table. Try/Excepts makes sure we have a complete archive of data used for
    # update, and intermediate tables.
    # RETURN: data frames of inputs tables; col header from in bin table

    try:
        c_df = pd.read_csv(current, dtype=object)
    except IOError:
        print('\nYou must move the current bin table to Archived folder for this update')
        sys.exit()
    [c_df.drop(m, axis=1, inplace=True) for m in c_df.columns.values.tolist() if m.startswith('Unnamed')]
    c_df[str(entity_id_col_c)] = c_df[str(entity_id_col_c)].astype(str)
    c_cols = c_df.columns.values.tolist()
    c_cols = update_columns(str(entity_id_col_c), str(entity_id_col_m), c_cols)
    c_df.columns = c_cols

    wide_cols = c_df.columns.values.tolist()

    try:
        sp_df = pd.read_csv(master_sp, dtype=object)
    except IOError:
        print('\nYou must move the in table bin table to table folder')
        sys.exit()
    return c_df, sp_df, wide_cols


# Time tracker
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Step 1: Load data from current bin tables  Sets the columns from the species tables that should be included in the
# output bin tables.
long_bin_df, df_sp, out_cols = load_data(long_bins, current_master)

cols_to_update = set_common_cols(long_bin_df, df_sp)
df_spe_info = df_sp[cols_to_update]

# Step 2: Removes old species info from bin data frame then merges the new species info to the bin data frame and runs
# reindex on out cols
#
out_col_bin = [v for v in out_cols if v == str(entity_id_col_m) or v not in cols_to_update]
df_bins_no_sp_info = long_bin_df.reindex(columns=out_col_bin)

# Left outer join produces a complete set of records from Table A, with the matching records (where available) in Table
# B. If there is no match, the right side will contain null.
df_final = pd.merge(df_bins_no_sp_info, df_spe_info, on=str(entity_id_col_m), how='left')

out_col = check_final_col_order(out_cols, cols_to_update)
df_final = df_final.reindex(columns=out_col)

# Step 3: Updated lead agency code to agency abbreviations; add in flags for new species, species without a range, and
# species removed from master list
df_final.loc[df_final[lead_agency_col] >= '2', lead_agency_col] = 'NMFS'
df_final.loc[df_final[lead_agency_col] == '1', lead_agency_col] = 'USFWS'

# Step 4: Filters working data frame to a filter and unfiltered version. Unfiltered includes all bins and all values,
# filter includes just A_Bins 2-10 and bins where species are located.  Filtered version used as input for post-processor
df_filtered, df_unfiltered = update_bin_info(df_final, (df_spe_info[entity_id_col_m].values.tolist()))

# Step 5: Exports data frame to csv
# used to make a wide version of the A/B splits; should be the same as long_bins unless species info updated
# LongBins_unfiltered_AB_[date].csv
# LongBins_PP_filter_AB_[date].csv # Filters to 'yes' just bins 2-10; Used as input for post-processor

df_filtered.to_csv(df_filtered_outfile, encoding='utf-8')
df_unfiltered.to_csv(df_unfiltered_outfile, encoding='utf-8')

# Elapsed time
print "Script completed in: {0}".format(datetime.datetime.now() - start_time)
