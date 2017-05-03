import pandas as pd
import os
import datetime

# TODO move the update of species information and species that were removed to the REcode so it is updated in wide formated then here just fileter
out_location = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\UpdatedToDB_20170419\Archived'
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

long_bins = out_location + os.sep + 'LongBins_20170503.csv'
woe_csv = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Update_Jan2016_FishesError\species woe taxa.csv'
col_included = ['lead_agency', 'Group', 'comname', 'EntityID', 'sciname', 'status_text']
col_index_bins = {'Lead_Agency': 1, 'Group': 2, 'COMNAME': 3, 'ENTITYID': 4, 'SCINAME': 5, 'STATUS_TEXT': 6}

# Note extinct species will be included in the output if a bin assignment applies
current_master = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\Creation\April2017\MasterListESA_Feb2017_20170410.csv'


def extract_species_info(master_in_table, col_from_master):
    if master_in_table.split('.')[1] == 'csv':
        master_list_df = pd.read_csv(master_in_table)
    else:
        master_list_df = pd.read_excel(master_in_table)

    master_list_df['EntityID'] = master_list_df['EntityID'].astype(str)
    [master_list_df.drop(t, axis=1, inplace=True) for t in master_list_df.columns.values.tolist() if
     t.startswith('Unnamed')]
    # Extracts on the columns set by user into a df to be used in the script
    sp_info_df = pd.DataFrame(master_list_df, columns=col_from_master)
    sp_info_df = sp_info_df.reindex(columns=col_included)
    del master_list_df
    return sp_info_df


def update_bin_info(bins_df, current_species):
    removed_species_bins = bins_df.loc[~bins_df['EntityID'].isin(current_species)]
    current_species_bins = bins_df.loc[bins_df['EntityID'].isin(current_species)]
    drop_value_no = current_species_bins.loc[~current_species_bins['Value'].isin(['No'])]
    drop_value_no = drop_value_no.loc[~drop_value_no['Bins'].isin(['Bin 1'])]

    return drop_value_no, removed_species_bins, current_species_bins


def add_groupby_columns(row, df):
    entid = row['EntityID']
    filter_df = df.loc[df['EntityID'] == entid]

    if len(filter_df) == 2:
        df.loc[df['EntityID'] == entid, 'Woe_Group'] = ['WoE_group_1', 'WoE_group_2']
    elif len(filter_df) == 1:
        df.loc[df['EntityID'] == entid, 'Woe_Group'] = 'WoE_group_1'
    elif len(filter_df) == 3:
        df.loc[df['EntityID'] == entid, 'Woe_Group'] = ['WoE_group_1', 'WoE_group_2', 'WoE_group_3']


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
df_woe = pd.read_csv(woe_csv, dtype=object)
df_woe['EntityID'] = df_woe['EntityID'].astype(str)
df_woe = df_woe.reindex(columns=['EntityID', 'Taxa', 'ESA Group', 'Woe_Group'])

df_woe.apply(lambda row: add_groupby_columns(row, df_woe), axis=1)
pivot = (df_woe.pivot(index='EntityID', columns='Woe_Group', values='ESA Group')).reset_index()

spe_info = extract_species_info(current_master, col_included)
spe_info = pd.merge(spe_info, pivot, on='EntityID', how='left')

long_bin_df = pd.read_csv(long_bins)
[long_bin_df.drop(v, axis=1, inplace=True) for v in long_bin_df.columns.values.tolist() if v.startswith('Unnamed')]
long_bin_df['EntityID'] = long_bin_df['ENTITYID'].astype(str)

print long_bin_df.loc[long_bin_df['EntityID'] == '11356']

df_bin = pd.merge(long_bin_df, spe_info, on='EntityID', how='left')
df_bin.loc[df_bin['lead_agency'] >= 2, 'lead_agency'] = 'NMFS'
df_bin.loc[df_bin['lead_agency'] == 1, 'lead_agency'] = 'USFWS'

df_filtered, df_removed_master, df_unfiltered = update_bin_info(df_bin, (spe_info['EntityID'].values.tolist()))

df_filtered.to_csv(out_location + os.sep + 'LongBins_filter_' + str(date) + '.csv')
df_unfiltered.to_csv(out_location + os.sep + 'LongBins_unfilter_' + str(date) + '.csv')
df_removed_master.to_csv(out_location + os.sep + 'RemovedFromMaster_' + str(date) + '.csv')

end_script = datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)
