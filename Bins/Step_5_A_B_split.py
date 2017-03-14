import pandas as pd
import os
import datetime

in_split = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\AquModeling\SpatialJoins\Summarized_spatialJoins\AllHUC_a_b.csv'
huc_12_cross = r'L:\Workspace\ESA_Species\Range\HUC12\SpInBins\HUC12\GDB\R_SpInBins_FWS_NMFS_wide_HUC12_Cross_ForA_BSplit_bins.txt'
long_bins = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Updates_Feb2017\LongBins_unfilter_20170209.csv'
final_cols = ['lead_agency', 'Group', 'HUC_2', 'comname', 'EntityID', 'sciname', 'status_text',
              'Reassigned', 'Bins_reassigned', 'AttachID', 'Bins', 'Value', 'WoE_group_1', 'WoE_group_2']

land_locked_hucs = ['10a', '10b', '11a', '11b', '15a', '15b', '16a', '16b', '14', '9', '4', '5', '6', '7']
marine_bins = ['Bin 10', 'Bin 8', 'Bin 9']
df_split = pd.read_csv(in_split)
df_cross = pd.read_csv(huc_12_cross)
df_long = pd.read_csv(long_bins)
df_long.drop('Unnamed: 0', axis=1, inplace=True)
df_long['EntityID'] = df_long['EntityID'].map(lambda x: x).astype(str)
df_long['HUC_2'] = df_long['HUC_2'].map(lambda x: x).astype(str)
df_cross.columns = ['EntityID', 'HUC_12']

out_location = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Updates_Feb2017'
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
    for v in huc_2_splits:
        huc_2_df = grouped.get_group(v)
        huc_a_b_split[v] = huc_2_df
    return huc_a_b_split


def huc12_dictionary(cross_df):
    huc_cross = {}
    row_count = len(cross_df)
    counter = 0
    while counter < row_count:
        entid = cross_df.iloc[counter, 0]
        huc12s = cross_df.iloc[counter, 1]
        #print huc12s
        #huc12s = huc12s.replace(']', '')
        #huc12s = huc12s.replace(')', '')
        #huc12s = huc12s.split(',')
        huc_cross[str(entid)] = huc12s

        counter += 1
    return huc_cross


def species_a_b(huc_2_a_b_dict, species_list, cross_dict, long_df, out_df):
    long_df['EntityID'] = long_df['EntityID'].map(lambda x: x).astype(str)
    long_df['HUC_2'] = long_df['HUC_2'].map(lambda x: x).astype(str)
    #
    list_a_b_hucs = huc_2_a_b_dict.keys()
    list_a_b_hucs.remove('09a')


    for species in species_list:
        print species
        list_spe_huc = (cross_dict[species])
        try:
            list_spe_huc = list_spe_huc.split(",")
            #print list_spe_huc
            for a_b in list_a_b_hucs:
                if a_b == '09a':
                    continue
                huc2 = HUC2_Dict[a_b]
                a_b_df = huc_2_a_b_dict[a_b]

                a_b_df_filter = (a_b_df[a_b_df['HUC_2'] == a_b])
                # for huc12 in list_spe_huc:
                #     print huc12
                #     print huc12[:2]
                #     print huc2
                list_spe_huc_filter = [huc12 for huc12 in list_spe_huc if huc12[:2] == huc2]
                if species == '9432':
                    print list_spe_huc
                    print list_spe_huc_filter
                #print list_spe_huc_filter

                if len(a_b_df_filter[a_b_df_filter['HUC_12'].isin(list_spe_huc_filter) == True]) != 0:
                    long_df_filtered = long_df[(long_df['EntityID'] == species)]
                    if species == '9432':
                        print long_df_filtered
                    long_df_filtered =long_df_filtered[(long_df_filtered['HUC_2'] == huc2)]
                    if species == '9432':
                        print long_df_filtered

                    if species == '9432':
                        print species
                        print huc2
                        print long_df_filtered

                    long_df_filtered.loc[:, ('HUC_2')] = a_b
                    if species == '9432':
                        print long_df_filtered

                    out_df = pd.concat([out_df, long_df_filtered], axis=0)
                    #if species == '9432':
                        # print long_df

        except:
            pass

    print out_df
    return list_a_b_hucs, out_df


def split_long_df(long_df, out_df):
    hucs_w_split = ['18', '15', '12', '11', '10', '17', '16']
    nl48_huc_split = ['19', '20']

    list_cols = long_df.columns.values.tolist()
    huc2_index = list_cols.index('HUC_2')

    row_count = len(long_df)
    counter = 0

    while counter < row_count:
        current_row = long_df.iloc[counter, :].values.tolist()
        huc_2 = str(long_df.ix[counter, 'HUC_2'])
        #print huc_2
        if huc_2 not in hucs_w_split:
            if huc_2 in nl48_huc_split:
                current_a = current_row
                current_a[huc2_index] = str(huc_2) + 'a'
                current_row_a_df = pd.DataFrame(current_a)
                current_row_a_df = current_row_a_df.T
                current_row_a_df.columns = long_df.columns.values.tolist()
                out_df = pd.concat([out_df, current_row_a_df], axis=0)

                current_b = current_row
                current_b[huc2_index] = str(huc_2) + 'b'
                current_row_b_df = pd.DataFrame(current_b)
                current_row_b_df = current_row_b_df.T
                current_row_b_df.columns = long_df.columns.values.tolist()

                out_df = pd.concat([out_df, current_row_b_df], axis=0)
            else:
                current_row_df = pd.DataFrame(current_row)
                current_row_df = current_row_df.T
                current_row_df.columns = long_df.columns.values.tolist()

                out_df = pd.concat([out_df, current_row_df], axis=0)
        else:
            counter += 1
            continue

        counter += 1

    # print out_df
    return out_df


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

out_df = pd.DataFrame(columns=df_long.columns.values.tolist())
dict_a_b_split = a_b_huc12(df_split)
huc_12_crosswalk_dict = huc12_dictionary(df_cross)
list_species = huc_12_crosswalk_dict.keys()

hucs_a_b, working_df = species_a_b(dict_a_b_split, list_species, huc_12_crosswalk_dict, df_long, out_df)
working_df.to_csv(out_location + os.sep + 'Working_LongBins_unfilter_AB_' + str(date) + '.csv')
df_final = split_long_df(df_long, working_df)
df_final.to_csv(out_location + os.sep + 'Pre_Reindex_LongBins_unfilter_AB_' + str(date) + '.csv')
df_final = df_final.reindex(columns=final_cols)
df_final.drop_duplicates(inplace=True)

# TOOD figure out a way to drop the marine bins from the land locked huc - poss do-able with groupby or an apply function
# df_final_filtered = df_final[(df_final['HUC_2'].isin(land_locked_hucs) == True) & (df_final['Bins'].isin(marine_bins) ==True )]
df_final.to_csv(out_location + os.sep + 'LongBins_unfilter_AB_' + str(date) + '.csv')

end_script = datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)
