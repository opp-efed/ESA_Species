import pandas as pd
import os
import datetime

apply_current_bins_to_new_huc = True
archived_location = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\UpdatedToDB_20170419\Archived'

current_bin_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\UpdatedToDB_20170419\Current_Assignments_20170420.csv'
removed_huc_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Update_Fall2016\updatedHUCs\removed_composite.csv'
add_huc_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Update_Fall2016\updatedHUCs\added_composite_JC.csv'

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

# TODO check for incorrect assignments land locked HUCs= 4,5,6,7,9,10,11,14,16
def load_data(current, removed, added):  # Load tables and add in entid_HUC ID

    current_df = pd.read_csv(current)
    [current_df.drop(v, axis=1, inplace=True) for v in current_df.columns.values.tolist() if v.startswith('Unnamed')]
    final_cols = current_df.columns.values.tolist()
    remove_df = pd.read_csv(removed)
    [remove_df.drop(v, axis=1, inplace=True) for v in remove_df.columns.values.tolist() if v.startswith('Unnamed')]
    add_df = pd.read_csv(added)
    [add_df.drop(v, axis=1, inplace=True) for v in add_df.columns.values.tolist() if v.startswith('Unnamed')]
    current_df['ENTITYID'] = current_df['ENTITYID'].astype(str)
    current_df['Spe_HUC'] = current_df['ENTITYID'].astype(str) + "_" + current_df['HUC_2'].astype(str)
    remove_df['Spe_HUC'] = remove_df['Entity_id'].astype(str) + "_" + remove_df['Huc_2'].astype(str)
    add_df['Spe_HUC'] = add_df['Entity_id'].astype(str) + "_" + add_df['Huc_2'].astype(str)
    add_df.columns = ['ENTITYID', 'HUC_2', 'Spe_HUC']
    remove_df.columns = ['ENTITYID', 'HUC_2', 'Spe_HUC']
    return current_df, remove_df, add_df, final_cols


def archived_removed_huc(working_df, removed_df):
    removed_entries = working_df[working_df['Spe_HUC'].isin(removed_df['Spe_HUC']) == True]
    removed_entries.to_csv(archived_location + os.sep + "ArchivedBinEntries_" + str(date) + '.csv')
    working_df = working_df[working_df['Spe_HUC'].isin(removed_df['Spe_HUC']) == False]
    return working_df


def add_huc_assignments(row, look_df, add_df, working_df):
    entid = str(row['ENTITYID'])

    if entid in add_df['ENTITYID'].values.tolist():
        lookup_huc_bins = look_df.loc[
            look_df['ENTITYID'] == entid, ['Bin 1', 'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6', 'Bin 7', 'Bin 8',
                                           'Bin 9', 'Bin 10']]
        lookup_huc_bins.drop_duplicates(inplace=True)
        if len(lookup_huc_bins) != 1:
            return 'HUC assignments are different'
            print 'test'
            pass  # add in dynamic collapseing if apply_curret_bin is true
        elif len(lookup_huc_bins) == 0:
            return 'Yes'
            pass
        else:
            if apply_current_bins_to_new_huc:
                bins_other_hucs = lookup_huc_bins.iloc[0, :].values.tolist()
                working_df.loc[
                    working_df['ENTITYID'] == entid, ['Bin 1', 'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6', 'Bin 7',
                                                      'Bin 8', 'Bin 9', 'Bin 10']] = bins_other_hucs
                return 'Yes'
            else:
                return 'Assignmnet for different HUC available'
                pass


    else:
        return 'No'


def added_hucs(working_df, added_df):
    working_df['ENTITYID'] = working_df['ENTITYID'].astype(str)
    all_columns = working_df.columns.values.tolist()
    added_df['ENTITYID'] = added_df['ENTITYID'].astype(str)

    look_up_bins_df = working_df[working_df['ENTITYID'].isin(added_df['ENTITYID']) == True]
    missing_from_bin_table = added_df[added_df['Spe_HUC'].isin(working_df['Spe_HUC']) == False]

    missing_df = missing_from_bin_table[['ENTITYID', 'HUC_2', 'Spe_HUC']]
    missing_df = missing_df.reindex(columns=all_columns)
    working_df = pd.concat([working_df, missing_df], axis=0)

    working_df['Updated'] = working_df.apply(
        lambda row: add_huc_assignments(row, look_up_bins_df, added_df, working_df), axis=1)
    working_df.to_csv(archived_location + os.sep + "WorkingTable_w_updates_" + str(date) + '.csv')
    return working_df


def collaspse_species(working_df):
    out_collapses = pd.DataFrame()

    #working_df['EntityID'] = working_df['ENTITYID'].astype(str)

    working_df['Spe_HUC'] = working_df['ENTITYID'].astype(str) + ", " + working_df['HUC_2'].astype(str)
    dups = working_df.set_index('Spe_HUC').index.get_duplicates()
    duplicates = working_df[working_df['Spe_HUC'].isin(dups) == True]
    print duplicates.iloc[0, 4]

    clean = working_df[working_df['Spe_HUC'].isin(dups) == False]

    row_count = len(duplicates)
    counter = 0

    while counter < row_count:
        current_entid = duplicates.iloc[counter, 4]
        print current_entid

        working_bin_assignments = duplicates[duplicates['EntityID'].isin([current_entid]) == True]
        print working_bin_assignments

        col_header = working_bin_assignments.columns.values.tolist()

        new_row = working_bin_assignments.iloc[0, :].values.tolist()
        new_row = pd.DataFrame(new_row)
        new_row = new_row.T
        new_row.columns = col_header

        col_count = len(working_bin_assignments.columns.values.tolist())

        row_count_2 = len(working_bin_assignments)
        counter_col = 7
        counter_row = 0
        while counter_row < row_count_2:
            while counter_col < col_count:
                bin_assign = working_bin_assignments.iloc[counter_row, counter_col]
                check_bin = new_row.iloc[counter_row, counter_col]
                if check_bin >= bin_assign:
                    counter_col += 1
                else:
                    new_row.iloc[counter_row, counter_col] = bin_assign
                    counter_col += 1
            counter_row += 1

        new_row_df = new_row

        out_collapses = pd.concat([out_collapses, new_row_df], axis=0)
        counter += 1
    out_collapses.columns = col_header
    out_collapses.drop_duplicates(inplace=True)
    out_collapses.to_csv(archived_location + os.sep + "Collapsed_" + str(date) + '.csv')
    out_complete = pd.concat([clean, out_collapses], axis=0)

    return out_complete


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

df_current, df_remove, df_add, final_col = load_data(current_bin_table, removed_huc_table, add_huc_table)
add_w_bin = added_hucs(df_current, df_add)

df_working = archived_removed_huc(add_w_bin, df_remove)
df_working = df_working.reindex(columns=final_col)
#

# df_final = collaspse_species(df_with_added)
# df_final = df_final.reindex(columns=final_col)
#
df_working.to_csv(archived_location + os.sep + "UpdatedBins_" + str(date) + '.csv')
#
end_script = datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)
