# Author: J. Connolly: updated 5/30/2017
# ##NOTES: Before running this update table must be codes to match the current bin database codes; add in the attachID
# ## for records updated during this update
# ## This step can be skipped if using collapsed species bin assignment as surrogate for new spe/huc2 combos; or if
# ## are no updates HUC assignments or new species


# ############## ASSUMPTIONS
# Species bin assignment found in the in_update_table have code to active bin codes used
#  New Species/HUC combos are represented in the species range
# Final columns headers are the same as those in current_wide
# current_wide has cols in the desired order
import pandas as pd
import datetime
import os
import sys

# #############  User input variables
# location where out table will be saved
table_folder = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Script_Check_20170627'

# Active species master list with supporting information
# Note extinct species will be included in the output table
# Place master species list into the archive folder for tracking found at table_folder
current_master_nm = 'MasterListESA_Feb2017_20170410.csv'
entity_id_col_m = 'EntityID'

# place update table into the archive folder for tracking found at table_folder -INPUT SOURCES SERVICES
update_table_name = 'Fishes_ManualUpdate_Jan2017.csv'
entity_id_col_a = 'EntityID'  # column header species EntityID in_update_table
huc2_col_a = 'HUC_2'  # column header species HUC_2 in_update_table

# Current wide table located in the archive folder for tracking found at table_folder
# INPUT SOURCES UpdatedBins_[date].csv from Step_1_UpdateBinTable_RangeUpdate
current_wide_name = 'UpdatedBins_20170627.csv'
entity_id_col_c = 'EntityID'  # column header species EntityID current_wide
huc2_col_c = 'HUC_2'  # column header species HUC_2 current_wide

# column headers bins current_wide and in_update_table, include AttachID used to track when assignment was update- INPUT
#  SOURCE- User
bin_col = ['Terrestrial Bin', 'Bin 1', 'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6', 'Bin 7', 'Bin 8', 'Bin 9', 'Bin 10',
           'AttachID']
# The species entityID is the tracking number by population across all ESA tools.  This number is pulled from the FWS
# TESS database. NMFS species not entered in the TESS database are given a place holder tracker that started with NMFS.
# Updates to this identifier are rare but **MUST** be taken into account at the **START** of all updates in order to
# make sure all files and species information are linked correctly.- INPUT SOURCE- User
entityid_updated = {'NMFS88': '9432', 'NMFS180': '11353', 'NMFS181': '11355', '5623': '11356', 'NMFS22': '10377'}

# ############# Static input variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
archived_location = table_folder + os.sep + 'Archived'  # scratch workspace
os.mkdir(archived_location) if not os.path.exists(archived_location) else None
in_update_table = archived_location + os.sep + update_table_name
current_wide = archived_location + os.sep + current_wide_name
outfile = table_folder + os.sep + "Current_Bins_tobeLoadDB_" + str(date) + '.csv'
current_master = archived_location + os.sep + current_master_nm
new_assignments_outfile = archived_location + os.sep + "New_SpHUC2_Combos_" + str(date) + '.csv'


def set_common_cols(bin_df, sp_df):
    # VARS: current:master_entid: Species identifier master species list; bin_entid: Species identifier active bin table
    # bin_df: data frame of active bin table; sp_df: data frame of master species list
    # DESCRIPTION: Checks with user to see which columns from the master species list should be included on the updated
    # bin table
    # RETURN: list of columns to be included

    sp_df_cols = sp_df.columns.values.tolist()
    bin_df_cols = bin_df.columns.values.tolist()
    common_cols = [j for j in sp_df_cols if j in bin_df_cols]

    poss_answer = ['Yes', 'No']
    ask_q = True
    while ask_q:
        user_input = raw_input(
            'Are these are the columns that should be updated from master species table?? {0}: Yes or No: '.format(
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


def update_columns(current_col, updated_col, list_of_col):
    # VARS: current:current_col: value in current list, updated_col: value it should be updated to, list_of_col: list of
    # values
    # DESCRIPTION: Standardize column headers terminology across tables based on user input variables.
    # RETURN: Update list of values to be used as column headers for data frames

    if current_col != updated_col:
        loc = list_of_col.index(str(current_col))
        list_of_col.remove(str(current_col))
        list_of_col.insert(loc, str(updated_col))
        return list_of_col
    else:
        return list_of_col


def load_data(current, added, master_sp):
    # VARS: current: current bin table in use; removed: list of huc 2 being removed by species; added: list of huc2
    # being added by species
    # DESCRIPTION: removes columns without headers from all data frames; sets entity id col as str in all tables;
    # updates entity id as needed; generated list of col headers all data frames;['Spe_HUC'] added to all tables as a
    # unique identifier common across tables, species id col header standardize to master list, huc 2 identifier
    # standardize to bin table; these are the final output columns; other tables col headers are updated to match
    # current_bin_table.  Verifies all hard coded columns are found in tables. Try/Excepts makes sure we have a complete
    # archive of data used for update, and intermediate tables
    # RETURN: data frames of inputs tables; KEY col headers standardize; entity ids updated when needed
    try:
        current_df = pd.read_csv(current)

    except IOError:
        print('\nYou must move the current bin table to Archived folder for this update')
        sys.exit()

    [current_df.drop(z, axis=1, inplace=True) for z in current_df.columns.values.tolist() if z.startswith('Unnamed')]
    current_df[str(entity_id_col_c)] = current_df[str(entity_id_col_c)].astype(str)
    final_cols = current_df.columns.values.tolist()
    final_cols.append('Updated') if 'Updated' not in final_cols else None

    current_df = current_df.reindex(columns=final_cols)
    [current_df[str(entity_id_col_c)].replace(z, entityid_updated[z], inplace=True) for z in entityid_updated.keys()]
    current_df['Spe_HUC'] = current_df[str(entity_id_col_c)].astype(str) + "_" + current_df[str(huc2_col_c)].astype(str)
    final_cols = update_columns(str(entity_id_col_c), str(entity_id_col_m), final_cols)
    current_df.columns = final_cols
    final_cols = current_df.columns.values.tolist()

    poss_answer = ['Yes', 'No']
    ask_q = True
    while ask_q:
        user_input = raw_input('Is this the order you would like the columns to be {0}: Yes or No: '.format(final_cols))
        if user_input not in poss_answer:
            print 'This is not a valid answer'
        elif user_input == 'Yes':
            break
        else:
            final_cols = raw_input('Please enter the order of columns comma sep str ')
    if type(final_cols) is str:
        final_cols = final_cols.split(",")
        final_cols = [j.replace("' ", "") for j in final_cols]
        final_cols = [j.replace("'", "") for j in final_cols]
        final_cols = [j.lstrip() for j in final_cols]

    try:
        add_df = pd.read_csv(added)
    except IOError:
        print('\nYou must move the update table to Archived folder for this update')
        sys.exit()

    [add_df.drop(a, axis=1, inplace=True) for a in add_df.columns.values.tolist() if a.startswith('Unnamed')]
    add_df[str(entity_id_col_a)] = add_df[str(entity_id_col_a)].astype(str)
    [add_df[str(entity_id_col_a)].replace(z, entityid_updated[z], inplace=True) for z in entityid_updated.keys()]
    add_df['Spe_HUC'] = add_df[str(entity_id_col_a)].astype(str) + "_" + add_df[str(huc2_col_a)].astype(str)
    add_cols = add_df.columns.values.tolist()
    add_cols = update_columns(str(entity_id_col_a), str(entity_id_col_m), add_cols)
    add_cols = update_columns(str(huc2_col_a), str(huc2_col_c), add_cols)
    add_df.columns = add_cols
    add_cols.append('Updated') if 'Updated' not in add_cols else None
    add_df = add_df.reindex(columns=add_cols)

    try:
        sp_df = pd.read_csv(master_sp)
    except IOError:
        print('\nYou must move the master species table to Archived folder for this update')
        sys.exit()
    return current_df, add_df, final_cols, sp_df


def update_species_assignments(working_df, added_df):
    # VARS: working_df: df that is being updated; added_df: df of huc 2 being added by species
    # DESCRIPTION: Update bin assignments in the current_wide df to match what was received during the update.
    # if/else will catch a species/huc combo not included on update request, that was generated from range, length bin
    # value ==0.  NOTE this is an assumption; all new species/huc combos should be represent in
    # range files receive,range files are already locked when this is run
    # RETURN: working bin data frame
    spe_huc_updates = added_df['Spe_HUC'].values.tolist()
    out_cols = working_df.columns.values.tolist()
    new_assignments = pd.DataFrame(columns=working_df.columns.values.tolist())
    for y in spe_huc_updates:
        if len(working_df.loc[working_df['Spe_HUC'] == y, bin_col]) != 0:
            new_bin_values = added_df.loc[added_df['Spe_HUC'] == y, bin_col]
            working_df.loc[working_df['Spe_HUC'] == y, bin_col] = new_bin_values.values.tolist()
            working_df.loc[working_df['Spe_HUC'] == y, ['Updated']] = 'Yes'

        else:
            print 'Adding a species/HUC2 combo for {0} Check why this happened'.format(y)
            new_spe_huc = added_df.loc[added_df['Spe_HUC'] == y].copy()  # makes a copy in a new df based on criteria
            new_spe_huc.loc[new_spe_huc['Spe_HUC'] == y, ['Updated']] = 'Yes'
            new_assignments = pd.concat([new_assignments, new_spe_huc], axis=0)

    new_assignments = new_assignments.reindex(columns=out_cols)
    working_df = working_df.reindex(columns=out_cols)
    new_assignments.to_csv(new_assignments_outfile)
    updated_df = pd.concat([working_df, new_assignments], axis=0)

    return updated_df


# Time tracker
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Step 1: load data from current bin tables, and tables used for update
df_current, df_add, final_col, df_sp = load_data(current_wide, in_update_table, current_master)
cols_to_update = set_common_cols(df_add, df_sp)
spe_info_df = df_sp[cols_to_update]

df_add = df_add.reindex(columns=df_current.columns.values.tolist())

# Step 2: Add updated assignment for species/huc2 combos found on the in_update_table
df_out = update_species_assignments(df_current, df_add)
df_final = pd.DataFrame(df_out)

# Step 3: removes old species information and appends species information from current master list
[df_final.drop(v, axis=1, inplace=True) for v in df_final.columns.values.tolist() if
 v in spe_info_df.columns.values.tolist() and v != entity_id_col_m]

# Step 4: Append current species info based on user input left join

# Left outer join produces a complete set of records from Table A, with the matching records (where available) in Table
# B. If there is no match, the right side will contain null.
df_final = pd.merge(df_final, spe_info_df, on=entity_id_col_m, how='left')
df_final = df_final.reindex(columns=df_current.columns.values.tolist())

# Step 3: Exports data frame to csv
# Current_Bins_tobeLoadDB_[date].csv  # Working table with all huc and manual bin assignment updates to be archived
df_final.to_csv(outfile, encoding='utf-8')

# Elapsed time
print "Elapse time {0}".format(datetime.datetime.now() - start_time)
