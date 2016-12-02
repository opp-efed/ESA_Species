import pandas as pd
import os
import datetime

archived_location = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\Update_Fall2016\ArchivedData'
date = 20161115
current_bin_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\Update_Fall2016\BinTable_asof_20161115.csv'
removed_huc_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\Update_Fall2016\updatedHUCs\removed_composite.csv'
add_huc_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\Update_Fall2016\updatedHUCs\added_composite_JC.csv'


def load_data(current, removed, added):
    current_df = pd.read_csv(current)
    final_cols = current_df.columns.values.tolist()
    remove_df = pd.read_csv(removed)
    add_df = pd.read_csv(added)
    current_df['ENTITYID'] = current_df['ENTITYID'].astype(str)
    current_df['Spe_HUC'] = current_df['ENTITYID'].astype(str) + ", " + current_df['HUC_2'].astype(str)
    remove_df['Spe_HUC'] = remove_df['Entity_id'].astype(str) + ", " + remove_df['Huc_2'].astype(str)
    add_df['Spe_HUC'] = add_df['Entity_id'].astype(str) + ", " + add_df['Huc_2'].astype(str)
    return current_df, remove_df, add_df, final_cols


def archived_removed_huc(working_df, removed_df):
    removed_entries = working_df[working_df['Spe_HUC'].isin(removed_df['Spe_HUC']) == True]
    removed_entries.to_csv(archived_location + os.sep + "ArchivedBinEntries_" + str(date) + '.csv')
    working_df = working_df[working_df['Spe_HUC'].isin(removed_df['Spe_HUC']) == False]
    return working_df


def added_hucs(working_df, added_df):
    missing_bins = False
    out_add_w_bin = pd.DataFrame()

    working_df['EntityID'] = working_df['ENTITYID'].astype(str)
    added_df['Entity_id'] = added_df['Entity_id'].astype(str)

    look_up_bins_df = working_df[working_df['EntityID'].isin(added_df['Entity_id']) == True]
    missing_from_bin_table = added_df[added_df['Entity_id'].isin(look_up_bins_df['ENTITYID']) == False]

    look_up_bins_df.to_csv(archived_location + os.sep + "MasBinEntries_" + str(date) + '.csv')
    missing_from_bin_table.to_csv(archived_location + os.sep + "MissBinEntries_" + str(date) + '.csv')

    row_count = len(added_df)
    counter = 0

    while counter < row_count:
        current_entid = str(added_df.ix[counter, 'Entity_id'])
        huc_2 = str(added_df.ix[counter, 'Huc_2'])
        working_bin_assignments = look_up_bins_df[look_up_bins_df['EntityID'].isin([current_entid]) == True]
        col_header = working_bin_assignments.columns.values.tolist()
        if len(working_bin_assignments) == 0:
            missing_bins = True
            counter += 1
            continue
        elif len(working_bin_assignments) == 1:
            new_row = working_bin_assignments.iloc[0, :].values.tolist()
            new_row = pd.DataFrame(new_row)
            new_row = new_row.T
            new_row.columns = col_header
            new_row.ix[0, 'HUC_2'] = huc_2
            new_row_df = new_row
            out_add_w_bin = pd.concat([out_add_w_bin, new_row_df], axis=0)
            counter += 1
        else:
            # print working_bin_assignments
            new_row = working_bin_assignments.iloc[0, :].values.tolist()
            new_row = pd.DataFrame(new_row)
            new_row = new_row.T
            new_row.columns = col_header
            new_row.ix[0, 'HUC_2'] = huc_2

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

            out_add_w_bin = pd.concat([out_add_w_bin, new_row_df], axis=0)
            counter += 1
    out_add_w_bin.columns = col_header
    out_add_w_bin.to_csv(archived_location + os.sep + "NewBinEntries_" + str(date) + '.csv')
    if missing_bins is True:
        print 'Check for missing bin assignments at {0}'.format(
            archived_location + os.sep + "MissBinEntries_" + str(date) + '.csv')
    return out_add_w_bin


def collaspse_species(working_df):

    out_collapses = pd.DataFrame()

    working_df['EntityID'] = working_df['ENTITYID'].astype(str)

    working_df['Spe_HUC'] = working_df['ENTITYID'].astype(str) + ", " + working_df['HUC_2'].astype(str)
    dups = working_df.set_index('Spe_HUC').index.get_duplicates()
    duplicates = working_df[working_df['Spe_HUC'].isin(dups)==True]
    print duplicates.iloc[0,4]

    clean =working_df[working_df['Spe_HUC'].isin(dups)==False]


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
    out_complete = pd.concat([clean,out_collapses],axis=0)


    return out_complete

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

df_current, df_remove, df_add, final_col = load_data(current_bin_table, removed_huc_table, add_huc_table)
add_w_bin = added_hucs(df_current, df_add)
add_w_bin = add_w_bin.reindex(columns=final_col)

df_working = archived_removed_huc(df_current, df_remove)
df_working = df_working.reindex(columns=final_col)

df_with_added = pd.concat([df_working, add_w_bin], axis=0)
df_final = collaspse_species(df_with_added)
df_final = df_final.reindex(columns=final_col)

df_final.to_csv(archived_location + os.sep + "UpdatedBins_" + str(date) + '.csv')

end_script = datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)
