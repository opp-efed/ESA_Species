import pandas as pd
import os
import datetime

# TODO not updated value when they don't match, see if this error is impact the other collaspe script


archived_location = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\UpdatedToDB_20170419\Archived'
entityid_updated = {'NMFS88': '9432',
                    'NMFS180': '11353',
                    'NMFS181': '11355',
                    '5623': '11356',
                    'NMFS22': '10377'}

current_bin_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\UpdatedToDB_20170419\Archived\Fishes_Bins_notcollapsed.csv'

land_locked_hucs = ['4', '5', '6', '7', '9', '10', '11', '14', '15', '16']  # HUC15 is land-lock within the US
marine_bins = ['8', '9', '10']
# collapse so species has same bin across all HUCs or apply assignments across all HUCS for new HUC
# CHECK: to collapsing across all HUCS will mean overwriting HUC 2 specific assignment


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

def load_data(current):  # Load tables and add in entid_HUC ID
    current_df = pd.read_csv(current)
    [current_df.drop(v, axis=1, inplace=True) for v in current_df.columns.values.tolist() if v.startswith('Unnamed')]
    final_cols = current_df.columns.values.tolist()
    current_df['ENTITYID'] = current_df['ENTITYID'].astype(str)

    updated_entity_ids = entityid_updated.keys()
    [current_df['ENTITYID'].replace(i, entityid_updated[i], inplace=True) for i in updated_entity_ids]
    current_df['Spe_HUC'] = current_df['ENTITYID'].astype(str) + "_" + current_df['HUC_2'].astype(str)

    return current_df,final_cols


def collaspse_species(working_df,):
    ent_list = list(set(working_df['Spe_HUC'].astype(str).values.tolist()))


    for v in ent_list:
        ent = str(v)
        lookup_huc_bins = working_df.loc[working_df['Spe_HUC'] == ent]
        spe_huc = lookup_huc_bins['Spe_HUC'].values.tolist()
        list_spe_huc = lookup_huc_bins.values.tolist()
        count_huc = len(list_spe_huc)
        starting_values = list_spe_huc[0][9:19]
        counter = 1

        while counter < count_huc:
            current_bins = list_spe_huc[counter][9:19]
            for i in current_bins:
                index_pos = current_bins.index(i)
                out_value = starting_values[index_pos]
                print i, out_value, ent
                if out_value == i:
                    pass
                elif out_value > i:
                    starting_values[index_pos] = out_value
            counter += 1

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

df_current, final_col = load_data(current_bin_table)
df_final = collaspse_species(df_current)

df_land = df_final.loc[:, :].copy()
df_out = check_land_locked_hucs(df_land)

updated_entity_ids = entityid_updated.keys()
[df_out['ENTITYID'].replace(i, entityid_updated[i], inplace=True) for i in updated_entity_ids]

df_out.to_csv(archived_location + os.sep + "Fishes_UpdatedBins_" + str(date) + '.csv')

end_script = datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)
