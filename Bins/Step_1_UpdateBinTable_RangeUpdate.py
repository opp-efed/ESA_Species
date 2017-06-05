# Author: J. Connolly: updated 5/30/2017

# ############## ASSUMPTIONS
# Final columns headers are the same as those in current_bin_table
# Columns are in the correct order ie, species info cols, bins, database info columns
# HUC 15 should be considered land-locked because it is land-locked within US jurisdiction
# Coded bins values are hierarchical, a higher number trumps a lower number when collapsing
#       ## NOTE if collapsing across all HUCs for a species the values found in the bin_code_update will need to changed
#       ## to yes in Step_2_ReCode Bin Table

import pandas as pd
import os
import datetime
import sys

# #############  User input variables
table_folder = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Script_Check_20170531'
# Collapse bin assignments so all HUC2s have the same values -INPUT SOURCE - User- True or False
# ## NOTE - if collapse_new_HUCS_only is False and collapse_huc is True HUC2 specific will be lost for the whole species
# ## NOTE - if both are true new HUC2 will be assigned bins seen across all other HUC2s
collapse_huc = True
collapse_new_HUCS_only = True  # if true collapse_huc must be true

# Note extinct species will be included in the output if a bin assignment applies
current_master_nm = 'MasterListESA_Feb2017_20170410.csv'
entity_id_col_m = 'EntityID'
lead_agency_col = 'Lead Agency'
# WoE group crosswalk used in post-processor
woe_csv_nm = r'species woe taxa.csv'
entity_id_col_woe = 'EntityID'
esa_woe_group_col = 'ESA Group'

# Bin table actively being use - INPUT SOURCE - Bin Database
# Place current bin into the archive folder for tracking found at table_folder
current_bin_table_name = 'Current_Assignments_20170420.csv'
entity_id_col_c = 'EntityID'  # column header species EntityID current_bin_table
huc2_col_c = 'HUC_2'  # column header species HUC_2 current_bin_table
# column headers bins current_bin_table
bin_col = ['Terrestrial Bin', 'Bin 1', 'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6', 'Bin 7', 'Bin 8', 'Bin 9', 'Bin 10']

# HUC2s removed from species range based on updated GIS files
# INPUT SOURCE - output table 'removed_composite.csv' from HUC2 assignment script
# Place removed_huc_table into the archive folder for tracking found at table_folder
removed_huc_table_name = 'removed_composite_JC.csv'
entity_id_col_r = 'Entity_id'  # column header species EntityID removed_huc_table
huc2_col_r = 'Huc_2'  # column header species HUC_2 removed_huc_table

# HUC2s added from species range based on updated GIS files
# INPUT SOURCE - output table 'added_composite.csv' from HUC2 assignment script
# Place removed_huc_table into the archive folder for tracking found at table_folder
add_huc_table_name = 'added_composite_JC.csv'
entity_id_col_a = 'Entity_id'  # column header species EntityID add_huc_table
huc2_col_a = 'Huc_2'  # column header species HUC_2 add_huc_table

# The species entityID is the tracking number by population across all ESA tools.  This number is pulled from the FWS
# TESS database. NMFS species not entered in the TESS database are given a place holder tracker that started with NMFS.
# Updated to this identifier are rare but **MUST** be taken into account at the **START** of all updates in order to
# make sure all files and species information are linked correctly.- INPUT SOURCE- User
entityid_updated = {'NMFS88': '9432',
                    'NMFS180': '11353',
                    'NMFS181': '11355',
                    '5623': '11356',
                    'NMFS22': '10377'}

# Dictionary of updates to bin tracking codes
# If bin code starts with 13, this is represents a marine bin where the species occurs for coastal hucs. These bins do
# do not occur in the land-locked HUC2s
# ##INPUT SOURCE - User -keys are string, values are integers
bin_code_update = {
    '7': 137,
    '12': 1312,
    '2': 132,
    '6': 136,
    '28': 1328,
    '29': 1329,
    '210': 13210,
    '211': 13211}

# ############# Static input variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

land_locked_hucs = ['4', '5', '6', '7', '9', '10', '11', '14', '15', '16']  # HUC15 is land-lock within the US
marine_bins = ['8', '9', '10']
archived_location = table_folder + os.sep + 'Archived'  # scratch workspace
os.mkdir(archived_location) if not os.path.exists(archived_location) else None

current_bin_table = archived_location + os.sep + current_bin_table_name
removed_huc_table = archived_location + os.sep + removed_huc_table_name
add_huc_table = archived_location + os.sep + add_huc_table_name
current_master = archived_location + os.sep + current_master_nm
woe_csv = archived_location + os.sep + woe_csv_nm
wide_woe = archived_location + os.sep + "WideWoeGroups_" + str(date) + '.csv'
outfile = archived_location + os.sep + "UpdatedBins_" + str(date) + '.csv'


def check_col_order(current_cols, updated_cols):
    # VARS: current:current_col: order of cols in current wide table, sp info columns that are being added
    # DESCRIPTION: Standardize column headers using the master species table and bin tables, adding in any new columns
    # header
    # RETURN: columns headers to be used as reindex for the output table
    for r in updated_cols:
        if r not in current_cols:
            index_location = raw_input(
                'What is the index position where this column should be inserted- based 0 {0}: '.format(r))
            current_cols.insert(int(index_location), r)
    return current_cols


def set_common_cols(master_entid, bin_entid, bin_df, sp_df):
    if master_entid != bin_entid:
        print ('EntityID column headers in the species master list, {0}, and the current bin table, {1}'.format
               (entity_id_col_m, entity_id_col_c))
        print('Verify all species info columns you wish to be updates have the same column header in both tables.')
    else:
        sp_df_cols = sp_df.columns.values.tolist()
        bin_df_cols = bin_df.columns.values.tolist()
        common_cols = [j for j in sp_df_cols if j in bin_df_cols]

        poss_answer = ['Yes', 'No']
        ask_q = True
        while ask_q:
            user_input = raw_input(
                'Are these are the species columns that should be updated{0}: Yes or No: '.format(common_cols))
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
    # DESCRIPTION: Standardize column headers using the master species table and  current_bin_table header values as the
    # master
    # RETURN: Update list of value used to update column headers

    if current_col != updated_col:
        loc = list_of_col.index(str(current_col))
        list_of_col.remove(str(current_col))
        list_of_col.insert(loc, str(updated_col))
        return list_of_col
    else:
        return list_of_col


def add_groupby_columns(row, df):
    entid = row[entity_id_col_m]
    filter_df = df.loc[df[entity_id_col_m] == entid]

    if len(filter_df) == 2:
        df.loc[df[entity_id_col_m] == entid, 'Woe_Group'] = ['WoE_group_1', 'WoE_group_2']
    elif len(filter_df) == 1:
        df.loc[df[entity_id_col_m] == entid, 'Woe_Group'] = ['WoE_group_1']
    elif len(filter_df) == 3:
        df.loc[df[entity_id_col_m] == entid, 'Woe_Group'] = ['WoE_group_1', 'WoE_group_2', 'WoE_group_3']


def load_data(current, removed, added, master_sp, woe_table):
    # VARS: current: current bin table in use; removed: list of huc 2 being removed by species; added: list of huc2
    # being added by species; master_sp current species master list; woe_table: woe group crosswalk from post-processor
    # DESCRIPTION: removes columns without headers from all data frames; sets entity id col as str in all tables;
    # updates entity id as needed; generated list of col headers all data frames;['Spe_HUC'] added to all tables as a
    # unique identifier common across tables col headers from current_bin_table used as master; these are the final
    # output columns; other tables col headers are updated to match current_bin_table. Verifies hard coded columns are
    # found in tables. Try/Excepts makes sure we have a complete archive of data used for update, and intermediate
    # tables.
    # RETURN: data frames of inputs tables; KEY col headers standardize; entity ids updated when needed

    try:
        c_df = pd.read_csv(current)
    except IOError:
        print('\nYou must move the current bin table to Archived folder for this update')
        sys.exit()

    [c_df.drop(m, axis=1, inplace=True) for m in c_df.columns.values.tolist() if m.startswith('Unnamed')]
    c_df[str(entity_id_col_c)] = c_df[str(entity_id_col_c)].astype(str)
    c_cols = c_df.columns.values.tolist()
    c_cols = update_columns(str(entity_id_col_c), str(entity_id_col_m), c_cols)
    c_df.columns = c_cols
    c_cols.append('Updated') if 'Updated' not in c_cols else None
    c_df = c_df.reindex(columns=c_cols)

    wide_cols = c_df.columns.values.tolist()
    bin_loc_start = wide_cols.index((bin_col[0]))
    # use to index list of bin values; add 1 because when sub-setting output does not include value in the last index
    # position but everything in front of it
    bin_loc_end = (wide_cols.index((bin_col[len(bin_col) - 1]))) + 1
    [c_df[str(entity_id_col_c)].replace(z, entityid_updated[z], inplace=True) for z in entityid_updated.keys()]
    c_df['Spe_HUC'] = c_df[str(entity_id_col_c)].astype(str) + "_" + c_df[str(huc2_col_c)].astype(str)

    try:
        remove_df = pd.read_csv(removed)
    except IOError:
        print('\nYou must move the removed huc table to Archived folder for this update')
        sys.exit()

    [remove_df.drop(z, axis=1, inplace=True) for z in remove_df.columns.values.tolist() if z.startswith('Unnamed')]
    remove_df[str(entity_id_col_r)] = remove_df[str(entity_id_col_r)].astype(str)
    [remove_df[str(entity_id_col_r)].replace(z, entityid_updated[z], inplace=True) for z in entityid_updated.keys()]
    remove_df['Spe_HUC'] = remove_df[str(entity_id_col_r)].astype(str) + "_" + remove_df[str(huc2_col_r)].astype(str)
    removed_cols = remove_df.columns.values.tolist()
    removed_cols = update_columns(str(entity_id_col_r), str(entity_id_col_m), removed_cols)
    removed_cols = update_columns(str(huc2_col_r), str(huc2_col_c), removed_cols)
    remove_df.columns = removed_cols

    try:
        add_df = pd.read_csv(added)
    except IOError:
        print('\nYou must move the added huc table to Archived folder for this update')
        sys.exit()

    [add_df.drop(a, axis=1, inplace=True) for a in add_df.columns.values.tolist() if a.startswith('Unnamed')]
    add_df[str(entity_id_col_a)] = add_df[str(entity_id_col_a)].astype(str)
    [add_df[str(entity_id_col_a)].replace(z, entityid_updated[z], inplace=True) for z in entityid_updated.keys()]
    add_df['Spe_HUC'] = add_df[str(entity_id_col_a)].astype(str) + "_" + add_df[str(huc2_col_a)].astype(str)
    add_cols = add_df.columns.values.tolist()
    add_cols = update_columns(str(entity_id_col_a), str(entity_id_col_m), add_cols)
    add_cols = update_columns(str(huc2_col_a), str(huc2_col_c), add_cols)
    add_df.columns = add_cols

    try:
        sp_df = pd.read_csv(master_sp)
    except IOError:
        print('\nYou must move the master species table to Archived folder for this update')
        sys.exit()
    try:
        woe_df = pd.read_csv(woe_table)
        woe_df[str(entity_id_col_woe)] = woe_df[str(entity_id_col_woe)].astype(str)
        woe_cols = woe_df.columns.values.tolist()
        woe_cols = update_columns(str(entity_id_col_woe), str(entity_id_col_m), woe_cols)
        woe_df.columns = woe_cols
    except IOError:
        print('\nYou must move the woe group crosswalk table to Archived folder for this update')
        sys.exit()

    return c_df, remove_df, add_df, sp_df, bin_loc_start, bin_loc_end, woe_df


def archived_removed_huc(working_df, removed_df):
    # VARS: working_df: df that is being updated; removed_df: df of huc 2 being removed by species
    # DESCRIPTION: removes rows values from working def based on the species/huc2 combinations found in the removed_df;
    # uses the common columns ['Spe_HUC'] added when data is loaded to filter table. Rows found in both removed and
    # working are archived, only found in working returned as the working_df. All removed values are save to csv located
    # at the path indicated by archived_location
    # RETURN: working bin data frame

    removed_entries = working_df.loc[working_df['Spe_HUC'].isin(removed_df['Spe_HUC'])]
    removed_entries.to_csv(archived_location + os.sep + "ArchivedBinEntries_" + str(date) + '.csv')
    working_df = working_df.loc[~working_df['Spe_HUC'].isin(removed_df['Spe_HUC'])]
    return working_df


def add_huc_assignments(row, look_df, add_df):
    # VARS: row: row of data being updated (apply function in added_hucs) look_df: working_df use to look up the current
    # bins for the species; add_df: df of huc 2 being add by species
    # DESCRIPTION: For species with a new huc2, checks current bin assignments to see if they are different based on the
    # huc 2 and adds comment ['Updated']; noting they are different. Allows for tracking updates over time and serves as
    # a flag for species with bin assignments that are collapsed.
    # RETURN: working bin data frame

    entid = str(row['Spe_HUC'])

    if entid in add_df['Spe_HUC'].values.tolist():
        lookup_huc_bins = look_df.loc[
            look_df[str(entity_id_col_c)] == entid, bin_col]
        lookup_huc_bins.drop_duplicates(inplace=True)
        if len(lookup_huc_bins) != 1:
            return 'New HUC added'
        elif len(lookup_huc_bins) == 0:
            return 'Yes'
    else:
        return 'No'


def added_hucs(working_df, added_df):
    # VARS: working_df: df that is being updated; added_df: df of huc 2 being added by species

    # DESCRIPTION: adds rows values from working def based on the species/huc2 combinations found in the added_df;
    # Filters working df to just the species with a new huc2 the uses the common column ['Spe_HUC'] added when data is
    # loaded to filter table to identify which huc2s are new and therefore do not have a bin assignment. Rows with empty
    # bin assignments added to the working df for these new huc2s. ['Updated'] column added that flags species found in
    # the add_df and have current bin assignments that change across huc2. See apply function add_huc_assignments.

    # RETURN: working bin data frame

    all_columns = working_df.columns.values.tolist()
    look_up_bins_df = working_df.loc[working_df[str(entity_id_col_c)].isin(added_df[str(entity_id_col_c)])]
    missing_from_bin_table = added_df.loc[~added_df['Spe_HUC'].isin(working_df['Spe_HUC'])]

    missing_df = missing_from_bin_table[[str(entity_id_col_c), str(huc2_col_c), 'Spe_HUC']]
    missing_df = missing_df.reindex(columns=all_columns)
    concat_df = pd.concat([working_df, missing_df], axis=0)

    updated_df = pd.DataFrame(concat_df)  # makes a copy of df with all values included based on defined indexing
    updated_df['Updated'] = updated_df.apply(lambda row: add_huc_assignments(row, look_up_bins_df, added_df), axis=1)
    updated_df.to_csv(archived_location + os.sep + "WorkingTable_w_updates_" + str(date) + '.csv')
    missing_df.to_csv(archived_location + os.sep + "Species_w_NewHUC2_" + str(date) + '.csv')

    return updated_df


def collapse_species(working_df, added_df, start_index, end_index):
    # VARS: working_df: df that is being updated; added_df: df of huc 2 being added by species
    # DESCRIPTION: Collapses species bin assignments so that there is one assignment for the species that includes all
    # bins in the different huc2. This species assignment is applied either to just the new HUCs that do not have a bin
    # assignment or all huc2 for the species based on the user inputs collapse_huc and collapse_new_HUCS_only. The
    # single species bin assignment is generated by loading the assignment for the first huc2 into a list based on the
    # index position of the bin columns, then comparing the values for each of the following huc2 retain max value as
    # the master.  These master assignment for the species is then applied to the huc2s based on the user input
    # collapse_huc and collapse_new_HUCS_only.
    # RETURN: working bin data frame

    ent_list = list(set(working_df[entity_id_col_c].astype(str).values.tolist()))
    if collapse_new_HUCS_only:
        list_added_species = list(set(added_df[entity_id_col_c].astype(str).values.tolist()))
    else:
        list_added_species = ent_list

    for k in ent_list:

        if k not in list_added_species:
            pass
        else:
            ent = str(k)
            lookup_huc_bins = working_df.loc[working_df[str(entity_id_col_c)] == ent]
            spe_huc = lookup_huc_bins['Spe_HUC'].values.tolist()
            list_spe_huc = lookup_huc_bins.values.tolist()
            count_huc = len(list_spe_huc)
            try:
                starting_values = map(int, list_spe_huc[0][int(start_index):int(end_index)])
            except ValueError:  # this catches HUCs with blank assignments can't convert float nan to int
                starting_values = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

            counter = 1  # counter set to 1 because index position 0 is used as the starting values
            huc_specific_assignments = False

            while counter < count_huc:
                try:
                    current_bins = map(int, list_spe_huc[counter][int(start_index):int(end_index)])
                except ValueError:  # this catches HUCs with blank assignments can't convert float nan to int
                    current_bins = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

                for x in current_bins:
                    index_pos = current_bins.index(x)
                    out_value = starting_values[index_pos]
                    if out_value == x:
                        pass
                    elif out_value != x:
                        update_value = max(out_value, x)  # coded bin values are hierarchical retain max val
                        starting_values[index_pos] = update_value
                        huc_specific_assignments = True

                counter += 1

                if huc_specific_assignments and collapse_new_HUCS_only:
                    for t in spe_huc:
                        if t in added_df['Spe_HUC'].values.tolist():
                            working_df.loc[working_df['Spe_HUC'] == t, ['Updated']] = 'Yes, bins for new huc ' \
                                                                                      'based previously assigned ' \
                                                                                      'hucs with huc specific bin ' \
                                                                                      'assignments'
                            working_df.loc[working_df['Spe_HUC'] == t, bin_col] = starting_values
                        else:
                            pass
                elif huc_specific_assignments and not collapse_new_HUCS_only:
                    for t in spe_huc:
                        working_df.loc[working_df['Spe_HUC'] == t, ['Updated']] = 'Yes, all hucs collapsed to a' \
                                                                                  ' single assignments, original ' \
                                                                                  'bin assignments for huc had huc' \
                                                                                  ' specific bin'
                        working_df.loc[working_df['Spe_HUC'] == t, bin_col] = starting_values
                elif not huc_specific_assignments and collapse_new_HUCS_only:
                    for t in spe_huc:
                        if t in added_df['Spe_HUC'].values.tolist():
                            working_df.loc[working_df['Spe_HUC'] == t, ['Updated']] = 'Yes, bins for new huc based' \
                                                                                      ' previously assigned hucs ' \
                                                                                      'without huc specific bin' \
                                                                                      ' assignments'
                            working_df.loc[working_df['Spe_HUC'] == t, bin_col] = starting_values
                        else:
                            pass

                elif not huc_specific_assignments and not collapse_new_HUCS_only:
                    for t in spe_huc:
                        working_df.loc[working_df['Spe_HUC'] == t, ['Updated']] = 'All HUCs have the same bin' \
                                                                                  ' assignments, did not collapse'
                else:
                    for t in spe_huc:
                        working_df.loc[working_df['Spe_HUC'] == t, ['Updated']] = 'All HUCs have the same bin' \
                                                                                  ' assignments, did not collapse'

    return working_df


def check_land_locked_hucs(working_df):
    # VARS: working_df: df that is being updated
    # DESCRIPTION: Check the marine bin assignments for species found in the land locked hucs to make sure they are
    # these huc are set to the values in the bin_code_update
    # RETURN: working bin data frame

    code_update = bin_code_update.keys()
    for p in marine_bins:
        column = 'Bin ' + p
        for k in code_update:
            update_value = bin_code_update[k]
            working_df.loc[
                working_df[str(huc2_col_c)].isin(land_locked_hucs) & (working_df[str(column)] == int(k)), column] = int(
                update_value)

    return working_df

def check_final_col_order (col_order):

    poss_answer = ['Yes', 'No']
    ask_q = True
    while ask_q:
        user_input = raw_input('Is this the order you would like the columns to be {0}: Yes or No: '.format(col_order))
        if user_input not in poss_answer:
            print 'This is not a valid answer'
        elif user_input == 'Yes':
            break
        else:
            col_order = raw_input('Please enter the order of columns comma sep str ')
    if type(col_order) is str:
        col_order = col_order.split(",")
        col_order = [j.replace("' ", "") for j in col_order]
        col_order= [j.replace("'", "") for j in col_order]
        col_order = [j.lstrip() for j in col_order]
    return col_order

# Time tracker
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Step 1: load data from current bin tables, and tables used for update
df_current, df_remove, df_add, df_sp, bin_start_index, bin_end_index, df_woe = load_data(current_bin_table,
                                                                                         removed_huc_table,
                                                                                         add_huc_table, current_master,
                                                                                         woe_csv)
cols_to_update = set_common_cols(entity_id_col_m, entity_id_col_c, df_current, df_sp)


# Step 2: Make species info data frames from master, used to update species info, and woe group crosswalk from post
# processor, append current woe groups to current species info, save a wide version of the woe groups to archive, add
# woe groups to the column headers for the out table
df_spe_info = df_sp[cols_to_update]
woe_col = df_woe.columns.values.tolist()
df_woe = df_woe.reindex(columns=woe_col.append('Woe_Group'))
df_woe.apply(lambda row: add_groupby_columns(row, df_woe), axis=1)
pivot = (df_woe.pivot(index=entity_id_col_m, columns='Woe_Group', values=esa_woe_group_col)).reset_index()
pivot.to_csv(wide_woe)
df_spe_info = pd.merge(df_spe_info, pivot, on=entity_id_col_m, how='left')
cols_to_update.extend(['WoE_group_1', 'WoE_group_2', 'WoE_group_3'])
out_col = check_col_order(df_current.columns.values.tolist(), cols_to_update)

# Step 3: Add new HUCs based on species range update and make archived of intermediate tables
add_w_bin = added_hucs(df_current, df_add)

# Step 4: Removed HUCs based on species range update and make archived of intermediate tables
df_final = archived_removed_huc(add_w_bin, df_remove)
df_land = pd.DataFrame(df_final)  # make a copy of the df based on the applied indexing to this point

# Step 5: Updated bins assignments in land locked hucs based on bin_code_update dictionary
df_out = check_land_locked_hucs(df_land)

# Step 6: Collapse bins assignment across all hucs into a single assignment for a species, when this occurs, and what
# species/huc 2 combos are updated to the single species assignment is set by collapse_huc, collapse_new_HUCS_only

if collapse_huc:
    df_collapse = pd.DataFrame(df_out)
    del df_final
    df_final = collapse_species(df_collapse, df_add, bin_start_index, bin_end_index)

# Step 7: Runs a final check to make sure all entity id have been updated based on entityid_updated dictionary; Pulls
# replace the species information in the output table with the current information found in the species master list and
# woe group crosswalk; reindex col order to match user input; and updated lead agency codes to NMFS or USFWS
updated_entity_ids = entityid_updated.keys()
[df_out[str(entity_id_col_c)].replace(i, entityid_updated[i], inplace=True) for i in updated_entity_ids]
df_out_col = df_out.columns.values.tolist()

df_out_col_bin = [v for v in df_out_col if v == str(entity_id_col_m) or v not in cols_to_update]
df_bins_no_sp_info = df_out[df_out_col_bin]

df_final = pd.merge(df_bins_no_sp_info, df_spe_info, on=str(entity_id_col_m), how='left')

print df_final
out_col = check_final_col_order(out_col)
df_final = df_final.reindex(columns=out_col)

df_final.loc[df_final[lead_agency_col] >= 2, lead_agency_col] = 'NMFS'
df_final.loc[df_final[lead_agency_col] == 1, lead_agency_col] = 'USFWS'

# Step 8: Exports data frame to csv
df_final.to_csv(outfile)

# Elapsed time
end_script = datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)
