# Author: J. Connolly: updated 5/30/2017
# ##############General notes
# All hard coded columns are purposefully, these are new columns used for tracking and grouping
#
# When collapsing bin assignments, all current assignment are considered and used to generated a single species
# assignment. You will lose huc specific assignments. Typically this is only done for new huc 2 where we do not have an
# assignment. Relies on the assumption that higher coded values should trump lower coded values.
#     collapse_huc = True
#     collapse_new_HUCS_only = True
#
# If you opt to collapse across all HUCs for a species you will violate the assumption that higher values trump lower
# values. As a result you will **need to updated the DB_code_Dict in Step_3_ReCode Bin Table** so that the values
# starting with 13 are 'Yes' and not 'No'. These are huc specific assignment for land locked hucs, species can only be
# in these bins in coastal HUC2s.

#  ############## ASSUMPTIONS
# Final columns headers related to bin information are the same as those in current_bin_table
# Bin input table columns are in the correct order ie, species info cols, bins, database info columns
# HUC-2 15 should be considered land-locked because it is land-locked within US jurisdiction
# ***Coded bins values are hierarchical, a higher number trumps a lower number when collapsing***
#       ## NOTE if collapsing across all HUCs for a species DB_code_Dict will need to be updated in
#          Step_3_ReCode Bin Table

import pandas as pd
import os
import datetime
import sys

# #############  User input variables
# location where out table will be saved
table_folder = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Script_Check_20170627'
# Boolean variables to generates a bin assignment for the species based on all current assignments across HUCS
# If only collapse_huc is True HUC2 specific bin assignment will be lost for the whole species
# If both are true collapsed species bin assignment would only be applied to an added HUC2
# Both False bin assignments for new hucs will be blank
collapse_huc = True
collapse_new_HUCS_only = True  # if true collapse_huc must be true

# Active species master list with supporting information
# Note extinct species will be included in the output table
# Place master species list into the archive folder for tracking found at table_folder
current_master_nm = 'MasterListESA_Feb2017_20170410.csv'
entity_id_col_m = 'EntityID'
lead_agency_col = 'Lead Agency'
# WoE group crosswalk used in post-processor
# Place WoE group crosswalk into the archive folder for tracking found at table_folder
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
removed_huc_table_name = 'removed_Fall2016.csv'
entity_id_col_r = 'Entity_id'  # column header species EntityID removed_huc_table
huc2_col_r = 'Huc_2'  # column header species HUC_2 removed_huc_table

# HUC2s added from species range based on updated GIS files
# INPUT SOURCE - output table 'added_composite.csv' from HUC2 assignment script
# Place removed_huc_table into the archive folder for tracking found at table_folder
add_huc_table_name = 'added_Fall2016.csv'
entity_id_col_a = 'Entity_id'  # column header species EntityID add_huc_table
huc2_col_a = 'Huc_2'  # column header species HUC_2 add_huc_table

# The species entityID is the tracking number by population across all ESA tools.  This number is pulled from the FWS
# TESS database. NMFS species not entered in the TESS database are given a place holder tracker that started with NMFS.
# Updates to this identifier are rare but **MUST** be taken into account at the **START** of all updates in order to
# make sure all files and species information are linked correctly.- INPUT SOURCE- User
entityid_updated = {'NMFS88': '9432', 'NMFS180': '11353', 'NMFS181': '11355', '5623': '11356', 'NMFS22': '10377'}

# Dictionary of updates to bin tracking codes
# If bin code starts with 13, this is represents a marine bin where the species occurs for coastal hucs. These bins do
# do not occur in the land-locked HUC2s
# ##INPUT SOURCE - User -keys are string, values are integers
bin_code_update = {'7': 137, '12': 1312, '2': 132, '6': 136, '28': 1328, '29': 1329, '210': 13210, '211': 13211}

# ############# Static input variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

land_locked_hucs = ['4', '5', '6', '7', '9', '10', '11', '14', '15', '16']  # HU2-15 is land-lock within the US
marine_bins = ['8', '9', '10']  # marine bins not directly modelled
archived_location = table_folder + os.sep + 'Archived'  # scratch workspace
os.mkdir(archived_location) if not os.path.exists(archived_location) else None

current_bin_table = archived_location + os.sep + current_bin_table_name
removed_huc_table = archived_location + os.sep + removed_huc_table_name
add_huc_table = archived_location + os.sep + add_huc_table_name
current_master = archived_location + os.sep + current_master_nm
woe_csv = archived_location + os.sep + woe_csv_nm
archived_file = archived_location + os.sep + "DroppedSpecies_" + str(date) + '.csv'  # Species removed from master list
no_bins_file = archived_location + os.sep + "Species_w_NewHUC2_" + str(date) + '.csv'  # Species/HUC w/o bin assignment
removed_hucs = archived_location + os.sep + "RemovedHUC2_" + str(date) + '.csv'  # HUC2 no longer found in species range
wide_woe = archived_location + os.sep + "WideWoeGroups_" + str(date) + '.csv'  # Wide format of WoE group crosswalk
outfile = archived_location + os.sep + "UpdatedBins_" + str(date) + '.csv'  # working bin tables with updates


def load_data(current, removed, added, master_sp, woe_table):
    # VARS: current: current bin table in use; removed: list of huc 2 being removed by species; added: list of huc2
    # being added by species; master_sp current species master list; woe_table: woe group crosswalk from post-processor
    # DESCRIPTION: removes columns without headers from all data frames; sets entity id col as str in all tables;
    # updates entity ids as needed; generated list of col headers all data frames; EntityID col head standardize to
    # match the master species data frame and HUC2 col header standardized to match the active bin data frame;
    # ['Spe_HUC'] (entityid_huc) added to all tables as a unique identifier common across tables. Try/Excepts makes sure
    # we have a complete archive of data used for update, and intermediate tables.
    # RETURN: data frames of inputs tables; KEY col headers standardize; entity ids updated when needed

    try:
        c_df = pd.read_csv(current)
    except IOError:
        print('\nYou must move the current bin table to Archived folder for this update')
        sys.exit()

    [c_df.drop(m, axis=1, inplace=True) for m in c_df.columns.values.tolist() if m.startswith('Unnamed')]
    c_df[str(entity_id_col_c)] = c_df[str(entity_id_col_c)].astype(str)
    c_cols = c_df.columns.values.tolist()
    c_cols = update_columns_header(str(entity_id_col_c), str(entity_id_col_m), c_cols)
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
    removed_cols = update_columns_header(str(entity_id_col_r), str(entity_id_col_m), removed_cols)
    removed_cols = update_columns_header(str(huc2_col_r), str(huc2_col_c), removed_cols)
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
    add_cols = update_columns_header(str(entity_id_col_a), str(entity_id_col_m), add_cols)
    add_cols = update_columns_header(str(huc2_col_a), str(huc2_col_c), add_cols)
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
        woe_cols = update_columns_header(str(entity_id_col_woe), str(entity_id_col_m), woe_cols)
        woe_df.columns = woe_cols
    except IOError:
        print('\nYou must move the woe group crosswalk table to Archived folder for this update')
        sys.exit()

    return c_df, remove_df, add_df, sp_df, bin_loc_start, bin_loc_end, woe_df


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


def update_columns_header(current_col, updated_col, list_of_col):
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


def add_groupby_columns(row, df):
    # VARS: row: row: identifier used for grouping (species entityid, df: working df with bin codes
    # DESCRIPTION:  For each entityid in WoE df, filters the WoE df to include all occurrences of the entityID,
    # flags row as WoE_group_1, _2 or _3 depending on the number of WoE groups the species occurs in. This sets up the
    # data frame allowing a pivot to convert format to wide format ie each entityID is a single row with multiple
    # columns if the species is in multiple groups. (Input table is long format, each row is a unique species WoE group
    # combo but the same entity ID can be found on multiple rows.)
    # RETURN: None groups applied directly in df

    filter_df = df.loc[df[entity_id_col_m] == row]

    if len(filter_df) == 2:
        df.loc[df[entity_id_col_m] == row, 'Woe_Group'] = ['WoE_group_1', 'WoE_group_2']
    elif len(filter_df) == 1:
        df.loc[df[entity_id_col_m] == row, 'Woe_Group'] = ['WoE_group_1']
    elif len(filter_df) == 3:
        df.loc[df[entity_id_col_m] == row, 'Woe_Group'] = ['WoE_group_1', 'WoE_group_2', 'WoE_group_3']


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


def updates_hucs(working_df, added_df, removed_df, spe_df):
    # VARS: working_df: df that is being updated; added_df: df of huc 2 being added by species, removed_df: df of huc 2
    # being removed by species, spe_df: data frame will all current species info
    # DESCRIPTION: Filters working df to just species with a new huc2 using the common column ['Spe_HUC'] new hucs will
    # not have a bin assignment. Saves table of just the new hucs with blank bin assignments; Filters out species/huc
    # combos using ['Spe_HUC'] that are no longer in species range; Saves archive of removed hucs
    # RETURN: working bin data frame

    all_columns = working_df.columns.values.tolist()
    missing_from_bin_table = added_df.loc[~added_df['Spe_HUC'].isin(working_df['Spe_HUC'])]

    missing_df = missing_from_bin_table[[str(entity_id_col_c), str(huc2_col_c), 'Spe_HUC']]
    missing_df = missing_df.reindex(columns=all_columns)
    concat_df = pd.concat([working_df, missing_df], axis=0)

    updated_df = pd.DataFrame(concat_df)  # makes a copy of df with all values included based on defined indexing
    missing_df = pd.merge(missing_df, spe_df, on=str(entity_id_col_m), how='outer')
    missing_df.to_csv(no_bins_file)

    removed_entries = working_df.loc[working_df['Spe_HUC'].isin(removed_df['Spe_HUC'])]
    removed_entries.to_csv(removed_hucs)
    updated_df = updated_df.loc[~updated_df['Spe_HUC'].isin(removed_df['Spe_HUC'])]
    return updated_df


def collapse_species(working_df, added_df, start_index, end_index):
    # VARS: working_df: df that is being updated; added_df: df of huc 2 being added by species
    # DESCRIPTION: Generates species bin assignment based on previous assignments across hucs. This species assignment
    # is applied either to just the new HUCs that do not have a bin assignment or replaces all bin assignments across
    # huc2s for the species based on the user inputs collapse_huc and collapse_new_HUCS_only. The single bin assignment
    # is generated by loading the assignment for the first huc2 into a list based on the index positions of the bin
    # columns, then comparing the values for each of the following huc2s and retaining max value seen for each bin as
    # the master.  These single collapsed bin assignment for the species is applied to specific huc2s based on the user
    # input for boolean variable [collapse_huc] and [collapse_new_HUCS_only.]
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
                        working_df.loc[working_df['Spe_HUC'] == t, ['Updated']] = 'New HUC add - Collapsed, ' \
                                                                                  'huc specific bin assignments'
                        working_df.loc[working_df['Spe_HUC'] == t, bin_col] = starting_values
                    else:
                        pass
            elif huc_specific_assignments and not collapse_new_HUCS_only:
                working_df.loc[working_df['Spe_HUC'].isin(spe_huc), ['Updated']] = 'All hucs collapsed, included huc ' \
                                                                              'specific bin assignments'
                working_df.loc[working_df['Spe_HUC'].isin(spe_huc), bin_col] = starting_values

            elif starting_values == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]:
                for t in spe_huc:
                    if t in added_df['Spe_HUC'].values.tolist():
                        working_df.loc[working_df['Spe_HUC'] == t, ['Updated']] = 'New Species/No previous assignment'

                        working_df.loc[working_df['Spe_HUC'] == t, bin_col] = ['', '', '', '', '', '', '', '', '', '',
                                                                               '']
                    else:
                        pass
            elif not huc_specific_assignments and collapse_new_HUCS_only:
                for t in spe_huc:
                    if t in added_df['Spe_HUC'].values.tolist():
                        working_df.loc[working_df['Spe_HUC'] == t, ['Updated']] = 'New HUC add - Collapsed, No ' \
                                                                                  'huc specific bin assignments'
                        working_df.loc[working_df['Spe_HUC'] == t, bin_col] = starting_values
                    else:
                        pass

            elif not huc_specific_assignments and not collapse_new_HUCS_only:
                working_df.loc[working_df['Spe_HUC'].isin(spe_huc), ['Updated']] = 'All hucs collapsed, no huc ' \
                                                                                   'specific bin assignments'

            else:

                working_df.loc[working_df['Spe_HUC'].isin(spe_huc), ['Updated']] = 'All hucs collapsed, no huc ' \
                                                                              'specific bin assignments'

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


# Time tracker
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Step 1: Load data from current bin tables, tables used to update hucs and species info. Sets the columns from the
# species tables that should be included in the output bin tables.
df_current, df_remove, df_add, df_sp, bin_start_index, bin_end_index, df_woe = load_data(current_bin_table,
                                                                                         removed_huc_table,
                                                                                         add_huc_table, current_master,
                                                                                         woe_csv)
cols_to_update = set_common_cols(df_current, df_sp)
df_spe_info = df_sp[cols_to_update]

# Step 2: Make species data frames from woe group crosswalk from post processor, converts the woe group crosswalk from
# long format to wide by adding a grouping category to the new [WoE_group] column for each entity id. Appends woe group
# in wide format to species info df. Saves a wide version of the woe groups to archive folder.
# WideWoeGroups_[date].csv - WoE group crosswalk in ide format
woe_col = df_woe.columns.values.tolist()
df_woe = df_woe.reindex(columns=woe_col.append('Woe_Group'))
[add_groupby_columns(species, df_woe) for species in df_woe[entity_id_col_m].values.tolist()]
pivot = (df_woe.pivot(index=entity_id_col_m, columns='Woe_Group', values=esa_woe_group_col)).reset_index()
pivot.to_csv(wide_woe)

# Left outer join produces a complete set of records from Table A, with the matching records (where available) in Table
# B. If there is no match, the right side will contain null."
df_spe_info = pd.merge(df_spe_info, pivot, on=entity_id_col_m, how='left')
cols_to_update.extend(['WoE_group_1', 'WoE_group_2', 'WoE_group_3'])

# Step 3: Adds new HUCs and removes dropped HUCs based on species range update. Make archived of intermediate tables
# Species_w_NewHUC2_[date].csv - all blank bin assignments; new species and species with new HUCs
# RemovedHUC2_[date].csv - bins assignment for huc 2 dropped from species range

df_updated = updates_hucs(df_current, df_add, df_remove, df_spe_info)
df_land = pd.DataFrame(df_updated)  # make a copy of the df

# Step 4: Updated bins assignments in land locked hucs based on bin_code_update dictionary
df_final = check_land_locked_hucs(df_land)
df_final = pd.DataFrame(df_final)  # makes a copy of the df

# Step 5: Collapse bins assignment for a species across all hucs into a single assignment for a species, applies bin
# assignment to specific hucs based on the user input variables collapse_huc, collapse_new_HUCS_only

if collapse_huc:
    df_collapse = pd.DataFrame(df_final)
    del df_final
    df_final = collapse_species(df_collapse, df_add, bin_start_index, bin_end_index)

# Step 6: Runs a final check on entity ids, removes old species info from bin data frame then merges the new species
# info to the bin data frame.

updated_entity_ids = entityid_updated.keys()
[df_final[str(entity_id_col_c)].replace(i, entityid_updated[i], inplace=True) for i in updated_entity_ids]

df_final = pd.DataFrame(df_final)  # makes a copy of the df
df_out_col = df_final.columns.values.tolist()
df_out_col_bin = [v for v in df_out_col if v == str(entity_id_col_m) or v not in cols_to_update]
df_bins = df_final[df_out_col_bin]

# Full outer join produces the set of all records in Table A and Table B, with matching records from both sides where
# available. If there is no match, the missing side will contain null.
df_final = pd.merge(df_spe_info, df_bins, on=str(entity_id_col_m), how='outer')
out_col = check_final_col_order(df_current.columns.values.tolist(), cols_to_update)
df_final = df_final.reindex(columns=out_col)

# Step 7: Updated lead agency code to agency abbreviations; add in flags for new species, species without a range, and
# species removed from master list

df_final.loc[df_final[lead_agency_col] >= 2, lead_agency_col] = 'NMFS'
df_final.loc[df_final[lead_agency_col] == 1, lead_agency_col] = 'USFWS'
df_final[huc2_col_c].fillna('No Range file', inplace=True)
df_final.loc[df_final[huc2_col_c] == 'No Range file', ['Updated']] = 'No Range file'
df_archived = df_current.loc[~df_current[str(entity_id_col_c)].isin(df_spe_info[str(entity_id_col_c)])]
for s in df_archived[str(entity_id_col_c)].values.tolist():
    ent_index = out_col.index(entity_id_col_m)
    col_flag_removed = out_col[ent_index + 1]
    df_final.loc[df_final[str(entity_id_col_m)] == s, [col_flag_removed]] = 'Species removed from master list- see ' \
                                                                            'DroppedSpecies table for species name'
# Step 8: Exports data frame to csv
# UpdatedBins_[date].csv'  # working bin tables with updates
# DroppedSpecies_[date].csv  # Species removed from master list

df_final.to_csv(outfile, encoding='utf-8')
df_archived.to_csv(archived_file, encoding='utf-8')
# Elapsed time
end_script = datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)
