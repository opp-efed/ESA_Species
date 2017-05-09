import pandas as pd
import datetime

in_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\UpdatedToDB_20170419\Archived\LongBins_unfilter_AB_20170504.csv'
out_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\UpdatedToDB_20170419\Archived\WideBins_unfilter_AB_20170504.csv'

final_cols = ['lead_agency', 'Group', 'HUC_2', 'comname', 'EntityID', 'sciname', 'status_text', 'Multi HUC',
              'Reassigned', 'Bins_reassigned', 'sur_huc', 'AttachID', 'WoE_group_1', 'WoE_group_2', 'Bin 1',
              'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6', 'Bin 7', 'Bin 8', 'Bin 9', 'Bin 10']

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
in_df = pd.read_csv(in_table, dtype=object)

in_df['Spe_HUC'] = in_df['EntityID'].astype(str) + ", " + in_df['HUC_2'].astype(str)
in_df['Spe_HUC_bin'] = in_df['EntityID'].astype(str) + ", " + in_df['HUC_2'] + in_df['Bins'].astype(str)

species_info = in_df.loc[:, ['lead_agency', 'Group', 'HUC_2', 'comname', 'EntityID', 'sciname', 'status_text',
                             'Multi HUC', 'Reassigned','Bins_reassigned', 'sur_huc', 'AttachID',
                             'WoE_group_1', 'WoE_group_2', 'Spe_HUC']]
in_df.drop('EntityID', axis=1, inplace=True)
in_df.drop('HUC_2', axis=1, inplace=True)
in_df.drop_duplicates(inplace=True)

dups = in_df.set_index('Spe_HUC_bin').index.get_duplicates()
if len(dups) != 0:
    print dups
    print in_df.loc[in_df['Spe_HUC'].isin(dups)]

reindex = in_df.reindex(columns=['Spe_HUC', 'Bins', 'Value'])
in_df.drop_duplicates(inplace=True)

wide_df = in_df.pivot('Spe_HUC', 'Bins', 'Value')
wide_df.reset_index(inplace=True)

wide_df = pd.merge(species_info, wide_df, on='Spe_HUC', how='left')
wide_df.drop_duplicates(inplace=True)
df_final = wide_df.reindex(columns=final_cols)

df_final.to_csv(out_table)

end_script = datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)
