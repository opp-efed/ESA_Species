import pandas as pd
import os
import datetime

archived_location = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\UpdatedToDB_20170419\Archived'
entityid_updated = {'NMFS88': '9432',
                    'NMFS180': '11353',
                    'NMFS181': '11355',
                    '5623': '11356',
                    'NMFS22': '10377'}

current_bin_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\UpdatedToDB_20170419\Current_Assignments_20170420.csv'
removed_huc_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Update_Fall2016\updatedHUCs\removed_composite.csv'
add_huc_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Update_Fall2016\updatedHUCs\added_composite_JC.csv'
land_locked_hucs = ['4', '5', '6', '7', '9', '10', '11', '14', '15', '16']  # HUC15 is land-lock within the US
marine_bins = ['8', '9', '10']
# collapse so species has same bin across all HUCs or apply assignments across all HUCS for new HUC
# CHECK: to collapsing across all HUCS will mean overwriting HUC 2 specific assignment

collapse_huc = True # collaspse_species function
collapse_new_HUCS_only = True # collaspse_species function

huc_2_index = 7
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

DBcodeDict = {
    '7': 137,
    '12': 1312,
    '2': 132,
    '6': 136,
    '28': 1328,
    '29': 1329,
    '210': 13210,
    '211': 13211}


# Bin assignments that can exists in land-locked but what to track the meaning assignment the number 13, 'No,
# is concatenated to the other code

def load_data(current, removed, added):  # Load tables and add in entid_HUC ID
    current_df = pd.read_csv(current)
    [current_df.drop(v, axis=1, inplace=True) for v in current_df.columns.values.tolist() if v.startswith('Unnamed')]
    final_cols = current_df.columns.values.tolist()
    current_df['ENTITYID'] = current_df['ENTITYID'].astype(str)

    updated_entity_ids = entityid_updated.keys()
    [current_df['ENTITYID'].replace(i, entityid_updated[i], inplace=True) for i in updated_entity_ids]
    current_df['Spe_HUC'] = current_df['ENTITYID'].astype(str) + "_" + current_df['HUC_2'].astype(str)

    remove_df = pd.read_csv(removed)
    [remove_df.drop(z, axis=1, inplace=True) for z in remove_df.columns.values.tolist() if z.startswith('Unnamed')]
    remove_df['Spe_HUC'] = remove_df['Entity_id'].astype(str) + "_" + remove_df['Huc_2'].astype(str)
    remove_df.columns = ['ENTITYID', 'HUC_2', 'Spe_HUC']

    add_df = pd.read_csv(added)

    [add_df.drop(a, axis=1, inplace=True) for a in add_df.columns.values.tolist() if a.startswith('Unnamed')]
    add_df['Spe_HUC'] = add_df['Entity_id'].astype(str) + "_" + add_df['Huc_2'].astype(str)
    add_df.columns = ['ENTITYID', 'HUC_2', 'Spe_HUC']

    return current_df, remove_df, add_df, final_cols


def archived_removed_huc(working_df, removed_df):
    removed_entries = working_df.loc[working_df['Spe_HUC'].isin(removed_df['Spe_HUC'])]
    removed_entries.to_csv(archived_location + os.sep + "ArchivedBinEntries_" + str(date) + '.csv')
    working_df = working_df.loc[~working_df['Spe_HUC'].isin(removed_df['Spe_HUC'])]
    return working_df


def add_huc_assignments(row, look_df, add_df):
    entid = str(row['ENTITYID'])

    if entid in add_df['ENTITYID'].values.tolist():
        lookup_huc_bins = look_df.loc[
            look_df['ENTITYID'] == entid, ['Bin 1', 'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6', 'Bin 7', 'Bin 8',
                                           'Bin 9', 'Bin 10']]
        lookup_huc_bins.drop_duplicates(inplace=True)
        if len(lookup_huc_bins) != 1:
            return 'HUC assignments are different'

            pass  # add in dynamic collapseing if apply_curret_bin is true
        elif len(lookup_huc_bins) == 0:
            return 'Yes'
            pass

    else:
        return 'No'


def added_hucs(working_df, added_df):
    working_df['ENTITYID'] = working_df['ENTITYID'].astype(str)
    all_columns = working_df.columns.values.tolist()
    added_df['ENTITYID'] = added_df['ENTITYID'].astype(str)

    look_up_bins_df = working_df.loc[working_df['ENTITYID'].isin(added_df['ENTITYID'])]
    missing_from_bin_table = added_df.loc[~added_df['Spe_HUC'].isin(working_df['Spe_HUC'])]

    missing_df = missing_from_bin_table[['ENTITYID', 'HUC_2', 'Spe_HUC']]
    missing_df = missing_df.reindex(columns=all_columns)
    working_df = pd.concat([working_df, missing_df], axis=0)

    working_df['Updated'] = working_df.apply(
        lambda row: add_huc_assignments(row, look_up_bins_df, added_df), axis=1)
    working_df.to_csv(archived_location + os.sep + "WorkingTable_w_updates_" + str(date) + '.csv')

    return working_df


def collaspse_species(working_df, added_df):
    ent_list = list(set(working_df['ENTITYID'].astype(str).values.tolist()))
    if collapse_new_HUCS_only:
        list_added_species = list(set(added_df['ENTITYID'].astype(str).values.tolist()))
    else:
        list_added_species = ent_list

    for v in ent_list:
        if collapse_new_HUCS_only:
            if v not in list_added_species:
                pass
            else:
                ent = str(v)
                lookup_huc_bins = working_df.loc[working_df['ENTITYID'] == ent]
                spe_huc = lookup_huc_bins['Spe_HUC'].values.tolist()
                list_spe_huc = lookup_huc_bins.values.tolist()
                count_huc = len(list_spe_huc)
                starting_values = map(int,list_spe_huc[0][9:19])
                counter = 1
                huc_specific_assignments = False
                while counter < count_huc:
                    try:
                        current_bins = map(int, list_spe_huc[counter][9:19])
                        for i in current_bins:
                            index_pos = current_bins.index(i)
                            out_value = starting_values[index_pos]
                            if out_value == i:
                                pass
                            elif out_value != i:
                                print i, out_value, ent
                                update_value = max(out_value, i)
                                starting_values[index_pos] = update_value
                                huc_specific_assignments = True #TODO Get thing into the table


                        counter += 1
                    except ValueError:
                        counter+=1
                        pass

                if collapse_new_HUCS_only:
                    huc_2 = added_df.loc[added_df['ENTITYID'] == v]
                    spe_huc = huc_2['Spe_HUC'].values.tolist()

                for t in spe_huc:
                    working_df.loc[working_df['Spe_HUC'] == t, ['Bin 1', 'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6',
                                                                'Bin 7', 'Bin 8', 'Bin 9', 'Bin 10']] = starting_values


    return working_df


def check_land_locked_hucs(working_df):
    code_update = DBcodeDict.keys()
    for v in marine_bins:
        column = 'Bin ' + v
        for k in code_update:
            update_value = DBcodeDict[k]
            working_df.loc[
                working_df['HUC_2'].isin(land_locked_hucs) & (working_df[str(column)] == int(k)), column] = int(
                update_value)

    return working_df


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

df_current, df_remove, df_add, final_col = load_data(current_bin_table, removed_huc_table, add_huc_table)
add_w_bin = added_hucs(df_current, df_add)
df_final = archived_removed_huc(add_w_bin, df_remove)

if collapse_huc:  # collapse so species has same bin across all HUCs or apply assignments across all HUCS for new HUC
    df_collapse = df_final.loc[:, :].copy()
    del df_final
    df_final = collaspse_species(df_collapse, df_add)

df_land = df_final.loc[:, :].copy()
df_out = check_land_locked_hucs(df_land)

updated_entity_ids = entityid_updated.keys()
[df_out['ENTITYID'].replace(i, entityid_updated[i], inplace=True) for i in updated_entity_ids]

df_out.to_csv(archived_location + os.sep + "UpdatedBins_" + str(date) + '.csv')

end_script = datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)
