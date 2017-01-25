import pandas as pd
import os
import datetime

# TODO move the update of species information and species that were removed to the REcode so it is updated in wide formated then here just fileter
current_master = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Update_Jan2016_FishesError\MasterList_BinUpdated_20170106.csv'
long_bins = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Update_Jan2016_FishesError\LongBins_20170110.csv'
woe_csv = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Update_Jan2016_FishesError\species woe taxa.csv'
col_included = ['lead_agency', 'Group', 'comname', 'EntityID', 'sciname', 'status_text']
col_index_bins = {'lead_agency': 1, 'Group': 2, 'comname': 3, 'EntityID': 4, 'sciname': 5, 'status_text': 6}

removed_entity_ids = ['10', '36', '157', '158', '289', '304', '397', '1353', '1538', '2065', '2299', '2381', '2691',
                      '3145', '3748', '3778', '3899', '4110', '4120', '4579', '4648', '5018', '5089', '5318', '5386',
                      '5666', '5819', '5966', '6334', '6416', '6464', '6765', '7659', '7745', '7753', '8391', '8741',
                      '9337', '9966', '10221', '10291', '10309', '10321', '10377', '10482', 'NMFS136', 'NMFS158',
                      'NMFS182', 'NMFS52', 'NMFS94', 'NMFS138', 'NMFS176', 'NMFS177', 'NMFS179', '16', '19', '23', '26',
                      '64', '68', '77', '78', '91', '93', '100', '105', '109', '122', '141', '191', '1302', '1953',
                      '6345', '9433', '9435', '9437', '9445', '9447', '9451', '9455', '9463', '9481', '10582',
                      ]
out_location = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Update_Jan2016_FishesError'
date = 20170106


def extract_species_info(master_in_table, col_from_master):
    if master_in_table.split('.')[1] == 'csv':
        master_list_df = pd.read_csv(master_in_table)
    else:
        master_list_df = pd.read_excel(master_in_table)

    master_list_df['EntityID'] = master_list_df['EntityID'].astype(str)
    # Extracts on the columns set by user into a df to be used in the script
    sp_info_df = pd.DataFrame(master_list_df, columns=col_from_master)
    # Extracts a list of species groups for looking across gdbs
    group_df = master_list_df['Group']
    unq_groups = group_df.drop_duplicates()
    del master_list_df
    sp_info_df.reindex(columns=col_included)
    return sp_info_df, unq_groups


def update_species_info(bin_table, spe_inf_df, woe_df, removed):
    long_bin_df = pd.read_csv(bin_table)
    try:
        long_bin_df('Unnamed:0', axis=1, inplace=True)
    except:
        pass

    long_bin_df['EntityID'] = long_bin_df['EntityID'].astype(str)
    list_cols = long_bin_df.columns.values.tolist()
    entid_col_index = list_cols.index('EntityID')


    list_entid = spe_inf_df['EntityID'].values.tolist()

    row_count = len(long_bin_df)
    row = 0
    updated = []

    while row < row_count:
        print row, row_count
        entid = str(long_bin_df.iloc[row, entid_col_index])

        if entid in removed:
            pass
        else:
            try:
                index_entid = list_entid.index(entid)
                current_sp = spe_inf_df[spe_inf_df['EntityID'].isin([entid]) == True]
                for col in col_included:
                    bin_col = int(col_index_bins[col])
                    value_bin = long_bin_df.iloc[row, bin_col]
                    value_master = current_sp.loc[index_entid, str(col)]
                    if value_master != value_bin:
                        long_bin_df.iloc[row, bin_col] = value_master
                        value = str(entid) + "_" + str(col) + "_" + str(value_bin) + "_" + str(value_master)
                        updated.append(value)

                huc_2 = long_bin_df.iloc[row, 0]

                if huc_2.startswith('22'):
                    long_bin_df.iloc[row, 0] = 20
                    long_bin_df.iloc[row, 10] = 'TRUE'
                else:
                    pass

                woe_groups_df = woe_df[woe_df['EntityID'].isin([entid]) == True]

                if len(woe_groups_df) == 0:
                    pass
                elif len(woe_groups_df) == 1:
                    woe_group_a = woe_groups_df.iloc[0, 3]  # 14
                    long_bin_df.iloc[row, 14] = woe_group_a
                else:
                    woe_group_a = woe_groups_df.iloc[0, 3]  # 14
                    woe_group_b = woe_groups_df.iloc[1, 3]
                    long_bin_df.iloc[row, 14] = woe_group_a
                    long_bin_df.iloc[row, 15] = woe_group_b


            except:
                pass
        row += 1

    # print
    try:
        long_bin_df('Unnamed:0', axis=1, inplace=True)
    except:
        pass
    print long_bin_df
    return long_bin_df, updated,


def update_bin_info(bins_df, removed_species_list):
    removed_species_bins = bins_df[bins_df['EntityID'].isin(removed_species_list) == True]
    bins_drop_removed = bins_df[bins_df['EntityID'].isin(removed_species_list) == False]
    drop_value_no = bins_drop_removed[bins_drop_removed['Value'].isin(['No']) == False]
    drop_value_no = drop_value_no[drop_value_no['Bins'].isin(['Bin 1']) == False]

    return drop_value_no, removed_species_bins, bins_drop_removed


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
df_woe = pd.read_csv(woe_csv, dtype=object)
df_woe['EntityID'] = df_woe['EntityID'].astype(str)


spe_info, sp_groups = extract_species_info(current_master, col_included)
df_bin, updated_info = update_species_info(long_bins, spe_info, df_woe, removed_entity_ids)
print df_bin
df_filtered, df_removed_master, df_unfiltered = update_bin_info(df_bin, removed_entity_ids)

updated_species_info = pd.DataFrame(updated_info)

df_filtered.to_csv(out_location + os.sep + 'LongBins_filter_' + str(date) + '.csv')
df_unfiltered.to_csv(out_location + os.sep + 'LongBins_unfilter_' + str(date) + '.csv')
df_removed_master.to_csv(out_location + os.sep + 'RemovedFromMaster_' + str(date) + '.csv')
updated_species_info.to_csv(out_location + os.sep + 'UpdatedInfo_' + str(date) + '.csv')
end_script = datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)
