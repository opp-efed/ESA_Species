import pandas as pd
import os
import datetime

# TODO get final output of columns correct
in_split = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\AquModeling\SpatialJoins\Summarized_spatialJoins\AllHUC_a_b.csv'
huc_12_cross = r'L:\Workspace\ESA_Species\Range\HUC12\AllSpe\HUC12\GDB\R_AllSpe_FWS_NMFS_ByHUC12_20170328.txt'
long_bins = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\UpdatedToDB_20170419\Archived\LongBins_unfilter_20170504.csv'
final_cols = ['lead_agency', 'Group', 'HUC_2', 'comname', 'EntityID', 'sciname', 'status_text',
              'Reassigned', 'Bins_reassigned', 'sur_huc', 'AttachID', 'Bins', 'Value', 'WoE_group_1', 'WoE_group_2',
              'WoE_group_3']

huc_split = ['10', '11', '12', '15', '16', '17', '18', '19', '20']

split_df = pd.read_csv(in_split)
[split_df.drop(v, axis=1, inplace=True) for v in split_df.columns.values.tolist() if v.startswith('Unnamed')]

df_cross = pd.read_csv(huc_12_cross)
[df_cross.drop(v, axis=1, inplace=True) for v in df_cross.columns.values.tolist() if v.startswith('Unnamed')]

df_long = pd.read_csv(long_bins)
df_long.drop('Unnamed: 0', axis=1, inplace=True)
df_long['EntityID'] = df_long['EntityID'].map(lambda x: x).astype(str)
df_long['HUC_2'] = df_long['HUC_2'].map(lambda x: x).astype(str)
df_cross.columns = ['EntityID', 'HUC_12']

out_location = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\UpdatedToDB_20170419\Archived'
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

HUC2_Dict = {'10a': '10',
             '10b': '10',
             '11a': '11',
             '11b': '11',
             '12a': '12',
             '12b': '12',
             '15a': '15',
             '15b': '15',
             '16a': '16',
             '16b': '16',
             '17a': '17',
             '17b': '17',
             '18a': '18',
             '18b': '18'
             }


def a_b_huc12(df_split):
    huc_a_b_split = {}
    df_split['HUC_12'] = df_split['HUC_12'].map(lambda x: str(x))
    df_split['HUC_12'] = df_split['HUC_12'].map(lambda x: '0' + x if len(x) == 11 else x).astype(str)
    df_split['HUC_02'] = (df_split['HUC_12'].map(lambda x: x[:2]))

    df_split['A_B'] = df_split['HUC_02'] + df_split['HUC_Split']
    grouped = df_split.groupby(['A_B'])
    huc_2_splits = grouped.groups.keys()
    for z in huc_2_splits:
        huc_2_df = grouped.get_group(z)
        huc_a_b_split[z] = huc_2_df

    # a small number of HUC12 are flagged as HUC2 9 due to borders, these need to be removed for the
    # purposes this assignment; there is not a/b split in HUC9
    huc_a_b_split.pop('09a', None)
    return huc_a_b_split


def assign_a_b(row):
    huc_2 = str(row['HUC_2'])
    a_b = str(row['HUC_Split_Value'])
    if a_b == 'No':
        return huc_2
    else:
        new_val = huc_2 + a_b
        return new_val


def species_a_b(huc_2_a_b_dict, species_list, cross_dict, long_df):
    huc_split_l48 = ['10', '11', '12', '15', '16', '17', '18']

    long_df['EntityID'] = long_df['EntityID'].map(lambda x: x).astype(str)
    long_df['HUC_2'] = long_df['HUC_2'].map(lambda x: x).astype(str)

    l48_split_df = long_df.loc[long_df['HUC_2'].isin(huc_split_l48)]
    cols = l48_split_df.columns.values.tolist()
    cols.extend(['A', 'B'])
    l48_df = l48_split_df.reindex(columns=final_cols)
    list_a_b_hucs = huc_2_a_b_dict.keys()

    for species in species_list:
        list_spe_huc = (cross_dict[species])
        try:
            list_spe_huc = list_spe_huc[0].split(",")
        except AttributeError:
            list_spe_huc = []
        for a_b in list_a_b_hucs:
            a_or_b = a_b[-1:].capitalize()
            huc2 = HUC2_Dict[a_b]
            a_b_df = huc_2_a_b_dict[a_b]
            a_b_df_filter = (a_b_df.loc[a_b_df['HUC_2'] == a_b])
            list_spe_huc_filter = [huc12 for huc12 in list_spe_huc if huc12[:2] == huc2]
            if len(a_b_df_filter.loc[a_b_df_filter['HUC_12'].isin(list_spe_huc_filter)]) != 0:
                l48_df.loc[(l48_df['EntityID'] == species) & (l48_df['HUC_2'] == huc2), a_or_b] = a_or_b.lower()
            else:
                pass

    df = pd.melt(l48_df,
                 id_vars=['Lead_Agency', 'ENTITYID', 'HUC_2', 'Multi HUC', 'Reassigned', 'sur_huc', 'Bins_reassigned',
                          'AttachID', 'Bins', 'Value', 'EntityID', 'lead_agency', 'Group', 'comname', 'sciname',
                          'status_text', 'WoE_group_1', 'WoE_group_2', 'WoE_group_3'],
                 value_vars=['A', 'B'],
                 var_name='HUC_Split', value_name='HUC_Split_Value')

    df['HUC_Split_Value'].fillna('No', inplace=True)
    df['HUC_2'] = df.apply(lambda row: assign_a_b(row), axis=1)
    df = df.loc[~df['HUC_2'].isin(huc_split_l48)]

    return list_a_b_hucs, df


def split_long_df(long_df):
    nl48_huc_split = ['19', '20']

    nl48_split_df = long_df.loc[long_df['HUC_2'].isin(nl48_huc_split)]
    cols = nl48_split_df.columns.values.tolist()
    cols.extend(['A', 'B'])
    nl48_df = nl48_split_df.reindex(columns=cols)

    nl48_df['A'].fillna('a', inplace=True)
    nl48_df['B'].fillna('b', inplace=True)

    df = pd.melt(nl48_split_df,
                 id_vars=['Lead_Agency', 'ENTITYID', 'HUC_2', 'Multi HUC', 'Reassigned', 'sur_huc', 'Bins_reassigned',
                          'AttachID', 'Bins', 'Value', 'EntityID', 'lead_agency', 'Group', 'comname', 'sciname',
                          'status_text', 'WoE_group_1', 'WoE_group_2', 'WoE_group_3'],
                 value_vars=['A', 'B'],
                 var_name='HUC_Split', value_name='HUC_Split_Value')

    df.to_csv(out_location + os.sep + 'NL_Working_AB_long_' + str(date) + '.csv')
    df['HUC_Split_Value'].fillna('No', inplace=True)
    df.to_csv(out_location + os.sep + 'NL_Working_AB_long_' + str(date) + '.csv')
    df['HUC_2'] = df.apply(lambda row: assign_a_b(row), axis=1)

    return df


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

out_df = pd.DataFrame(columns=df_long.columns.values.tolist())

dict_a_b_split = a_b_huc12(split_df)
huc_12_crosswalk_dict = df_cross.set_index('EntityID').T.to_dict('list')
list_species = huc_12_crosswalk_dict.keys()

hucs_a_b, working_df = species_a_b(dict_a_b_split, list_species, huc_12_crosswalk_dict, df_long)

working_df.to_csv(out_location + os.sep + 'Working_L48_AB_' + str(date) + '.csv')
df_working_nl48 = split_long_df(df_long)

df_working_nl48.to_csv(out_location + os.sep + 'Working_NL48_AB_' + str(date) + '.csv')

l48_nl48_df = working_df.append(df_working_nl48)

reindex_cols = df_long.columns.values.tolist()
l48_nl48_df = l48_nl48_df.reindex(columns=final_cols)

non_split_hucs = df_long.loc[~df_long['HUC_2'].isin(huc_split)]

df_final = non_split_hucs.append(l48_nl48_df)
df_final = df_final.reindex(columns=final_cols)

df_final.drop_duplicates(inplace=True)

df_final.to_csv(out_location + os.sep + 'LongBins_unfilter_AB_' + str(date) + '.csv')
end_script = datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)
