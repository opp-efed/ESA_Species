import pandas as pd
import datetime
import os
import sys

# ##NOTES: Before running this update table must be codes to match the current bin database codes; add in the attachID
# ## for records updated during this update
# ## This step can be skipped if using collapsed species bin assignment as surrogate for new spe/huc2 combos; this will
# ## not account for new species

# #############  User input variables
# location where out table will be saved
table_folder = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Script_Check_20170531'

# place update table into the archive folder for tracking found at table_folder -INPUT SOURCES SERVICES
update_table_name = 'Test_binUpdate_20170530.csv'
entity_id_col_a = 'EntityID'  # column header species EntityID in_update_table
huc2_col_a = 'HUC_2'  # column header species HUC_2 in_update_table

# place current wide table into the archive folder for tracking found at table_folder
# INPUT SOURCES UpdatedBins_[date].csv  from Step_1_UpdateBinTable_RangeUpdate
current_wide_name = 'Current_Assignments_20170420.csv'
entity_id_col_c = 'ENTITYID'  # column header species EntityID current_wide
huc2_col_c = 'HUC_2'  # column header species HUC_2 current_wide

# column headers bins current_wide and in_update_table, include AttachID used to track when assignment was update- INPUT
#  SOURCE- User
bin_col = ['Terrestrial Bin', 'Bin 1', 'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6', 'Bin 7', 'Bin 8', 'Bin 9', 'Bin 10',
           'AttachID']
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
os.mkdir(archived_location)if not os.path.exists(archived_location) else None
in_update_table = archived_location + os.sep + update_table_name
current_wide = archived_location + os.sep + current_wide_name

# ############## ASSUMPTIONS
# Species bin assignment found in the in_update_table have been manually recode to match the current bins DB bin codes
# New Species/HUC combos are represented in the species range
# Final columns headers are the same as those in current_wide


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


def load_data(current, added):
    # VARS: current: current bin table in use; removed: list of huc 2 being removed by species; added: list of huc2
    # being added by species
    # DESCRIPTION: removes columns without headers from all data frames; sets entity id col as str in all tables;
    # updates entity id as needed; generated list of col headers all data frames;['Spe_HUC'] added to all tables as a
    # unique identifier common across tables col headers from current_wide used as master; these are the final
    # output columns; other tables col headers are updated to match current_bin_table. Verifies all hard coded columns
    # are found in tables. Try/Excepts makes sure we have a complete archive of data used for update, and intermediate
    # tables
    # RETURN: data frames of inputs tables; KEY col headers standardize; entity ids updated when needed
    try:
        current_df = pd.read_csv(current)
    except IOError:
        print('\nYou must move the current bin table to Archived folder for this update')
        sys.exit()

    [current_df.drop(v, axis=1, inplace=True) for v in current_df.columns.values.tolist() if v.startswith('Unnamed')]
    current_df[str(entity_id_col_c)] = current_df[str(entity_id_col_c)].astype(str)
    final_cols = current_df.columns.values.tolist()
    final_cols.append('Updated') if 'Updated' not in final_cols else None
    current_df = current_df.reindex(columns=final_cols)

    [current_df[str(entity_id_col_c)].replace(z, entityid_updated[z], inplace=True) for z in entityid_updated.keys()]
    current_df['Spe_HUC'] = current_df[str(entity_id_col_c)].astype(str) + "_" + current_df[str(huc2_col_c)].astype(str)
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
    add_cols = update_columns(str(entity_id_col_a), str(entity_id_col_c), add_cols)
    add_cols = update_columns(str(huc2_col_a), str(huc2_col_c), add_cols)
    add_df.columns = add_cols
    add_cols.append('Updated') if 'Updated' not in add_cols else None
    add_df = add_df.reindex(columns=add_cols)


    return current_df, add_df, final_cols


def update_species_assignments(working_df, added_df):
    # VARS: working_df: df that is being updated; added_df: df of huc 2 being added by species
    # DESCRIPTION: Update bin assignments in the current_wide df to match what was received during the update.
    # if/else will catch a species/huc combo not included on update request, length bin value ==0, that was generated
    # from range. NOTE this is an assumption; all new species/huc combos should be represent in range files receive,
    # range files are already locked when this is run
    # RETURN: working bin data frame
    spe_huc_updates = added_df['Spe_HUC'].values.tolist()
    out_cols = working_df.columns.values.tolist()
    print out_cols
    new_assignments = pd.DataFrame(columns=working_df.columns.values.tolist())
    for v in spe_huc_updates:
        if len(working_df.loc[working_df['Spe_HUC'] == v, bin_col]) != 0:
            new_bin_values = added_df.loc[added_df['Spe_HUC'] == v, bin_col]
            working_df.loc[working_df['Spe_HUC'] == v, bin_col] = new_bin_values.values.tolist()
            working_df.loc[working_df['Spe_HUC'] == v, ['Updated']] = 'Yes'

        else:
            print 'Adding a species/HUC2 combo for {0}'.format(v)
            new_spe_huc = added_df.loc[added_df['Spe_HUC'] == v].copy()  # makes a copy in a new df based on criteria
            new_spe_huc.loc[new_spe_huc['Spe_HUC'] == v, ['Updated']] = 'Yes'
            new_assignments = pd.concat([new_assignments, new_spe_huc], axis=0)

    new_assignments = new_assignments.reindex(columns=out_cols)
    working_df = working_df.reindex(columns=out_cols)
    new_assignments.to_csv(archived_location + os.sep + "New_SpHUC2_Combos_" + str(date) + '.csv')
    updated_df = pd.concat([working_df, new_assignments], axis=0)

    return updated_df

# Time tracker
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Step 1: load data from current bin tables, and tables used for update
df_current, df_add, final_col = load_data(current_wide, in_update_table)

# Step 2: Add updated assignment for species/huc2 combos found on the in_update_table
df_out = update_species_assignments(df_current, df_add)
df_final = pd.DataFrame(df_out)

# Step 3: Exports data frame to csv
df_final.to_csv(archived_location + os.sep + "Current_Bins_tobeLoadDB_" + str(date) + '.csv')

# Elapsed time
print "Elapse time {0}".format(datetime.datetime.now() - start_time)
