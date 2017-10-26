import datetime
import numpy as np
import pandas as pd

# #############  User input variables
# location where out table will be saved - INPUT Source user

old_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Script_Check_20170606\Archived' \
            r'\Current_Assignments_20170420.csv'
entity_id_col_o = 'EntityID'  # column header species EntityID  old bin table
huc2_col_o = 'HUC_2'  # column header species HUC_2 old bin table
bin_col_o = ['Terrestrial Bin', 'Bin 1', 'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6', 'Bin 7', 'Bin 8', 'Bin 9',
             'Bin 10']

new_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Script_Check_20170606\Archived' \
            r'\UpdatedBins_20170613.csv'
entity_id_col_c = 'EntityID'  # column header species EntityID new bin table
huc2_col_c = 'HUC_2'  # column header species HUC_2 new bin table
bin_col_c = ['Terrestrial Bin', 'Bin 1', 'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6', 'Bin 7', 'Bin 8', 'Bin 9',
             'Bin 10']

out_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Script_Check_20170606\Changes_20170420_20170613.csv'


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


def load_data(old_bins, new_bins):
    # VARS: current: current wide bin table in use; master_sp- current master species list
    # DESCRIPTION: removes columns without headers from all data frames; sets entity id col as str in all tables;
    # updates entity id as needed; with a secondary check using .replace to make sure bin codes load as integers.
    # Verifies all hard coded columns are found in tables. Try/Excepts makes sure we have a complete archive of data
    # used for update,and intermediate tables.
    # RETURN: data frames of inputs tables; KEY col headers standardize; entity ids updated when needed; and current
    # list of species to filter table at end

    new_df = pd.read_csv(new_bins)
    new_df[str(entity_id_col_c)] = new_df[str(entity_id_col_c)].astype(str)
    [new_df.drop(z, axis=1, inplace=True) for z in new_df.columns.values.tolist() if z.startswith('Unnamed')]
    new_df[bin_col_c] = new_df[bin_col_c].apply(pd.to_numeric, errors='ignore')
    new_df[bin_col_c].replace({.0: ''})  # if a number was load as a float will strip the decimal place
    new_df['Spe_HUC'] = new_df[str(entity_id_col_c)].astype(str) + "_" + new_df[str(huc2_col_c)].astype(str)
    new_cols = new_df.columns.tolist()

    old_df = pd.read_csv(old_bins)
    [old_df.drop(z, axis=1, inplace=True) for z in old_df.columns.values.tolist() if z.startswith('Unnamed')]
    old_df[bin_col_o] = old_df[bin_col_o].apply(pd.to_numeric, errors='ignore')
    old_df[bin_col_o].replace({.0: ''})  # if a number was load as a float will strip the decimal place
    old_df[str(entity_id_col_o)] = old_df[str(entity_id_col_o)].astype(str)
    old_df_col = old_df.columns.values.tolist()
    old_df_col = update_columns_header(str(entity_id_col_o), str(entity_id_col_c), old_df_col)
    old_df_col = update_columns_header(str(huc2_col_o), str(huc2_col_c), old_df_col)
    old_df.columns = old_df_col
    old_df['Spe_HUC'] = old_df[str(entity_id_col_c)].astype(str) + "_" + old_df[str(huc2_col_c)].astype(str)

    common_cols_new_old = [v for v in new_cols if v in old_df_col]
    list_spe_huc = new_df['Spe_HUC'].values.tolist()
    return new_df, old_df, common_cols_new_old, new_cols, list_spe_huc


def update_change_df(new_df, old_df, changes_df, cols_to_check, current_spe_huc):
    entid = current_spe_huc.split("_")[0]
    huc_2 = current_spe_huc.split("_")[1]

    changes_df.loc[changes_df['Spe_HUC'] == current_spe_huc, entity_id_col_c] = entid
    changes_df.loc[changes_df['Spe_HUC'] == current_spe_huc, huc2_col_c] = huc_2

    for col in cols_to_check:
        if col == entity_id_col_c or col == huc2_col_c:
            pass
        else:
            new_value = (new_df.loc[new_df['Spe_HUC'] == current_spe_huc, [col]])
            old_value = (old_df.loc[old_df['Spe_HUC'] == current_spe_huc, [col]])
            try:
                if new_value.iloc[0, 0] == old_value.iloc[0, 0]:

                    changes_df.loc[changes_df['Spe_HUC'] == current_spe_huc, [col]] = 'False'
                else:
                    changes_df.loc[changes_df['Spe_HUC'] == current_spe_huc, [col]] = 'True'
            except IndexError:
                changes_df.loc[changes_df['Spe_HUC'] == current_spe_huc, [col]] = 'True'


# Time tracker
start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

df_new_bins, df_old_bins, common_cols, current_cols, spe_huc_list = load_data(old_table, new_table)

df_changes = pd.DataFrame(columns=current_cols)
df_changes['Spe_HUC'] = spe_huc_list

for spe_huc in spe_huc_list:
    if spe_huc_list.index(spe_huc) in np.arange(0, (len(spe_huc_list) + 1), 25):
        print 'Completed: {0} remaining: {1}'.format(spe_huc_list.index(spe_huc),
                                                     (len(spe_huc_list) - (spe_huc_list.index(spe_huc))))
    update_change_df(df_new_bins, df_old_bins, df_changes, common_cols, spe_huc)
df_changes.to_csv(out_table)

# Elapsed time
print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
