import pandas as pd
import datetime
in_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Update_Jan2016_FishesError\LongBins_unfilter_AB_20170110.csv'
out_table = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Update_Jan2016_FishesError\WideBins_unfilter_20170110_b.csv'


final_cols =['lead_agency', 'Group', 'HUC_2', 'comname', 'EntityID', 'sciname', 'status_text', 'Multi HUC',
             'Reassigned', 'sur_huc',  'AttachID', 'WoE_group_1', 'WoE_group_2', 'Bin 1',
              'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6', 'Bin 7', 'Bin 8', 'Bin 9','Bin 10']



start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
in_df = pd.read_csv(in_table)

in_df['Spe_HUC'] = in_df['EntityID'].astype(str) + ", " + in_df['HUC_2'].astype(str)
in_df['Spe_HUC_bin'] = in_df['EntityID'].astype(str) + ", " + in_df['HUC_2']+ in_df['Bins'].astype(str)


species_info = in_df.loc[:,['lead_agency', 'Group', 'HUC_2', 'comname', 'EntityID', 'sciname', 'status_text',
                            'Multi HUC', 'Reassigned', 'sur_huc', 'AttachID',
                            'WoE_group_1', 'WoE_group_2', 'Spe_HUC']]
in_df.drop('EntityID',axis=1,inplace=True)
in_df.drop('HUC_2',axis=1,inplace=True)

in_df.drop_duplicates(inplace=True)
dups = in_df.set_index('Spe_HUC_bin').index.get_duplicates()
print dups
print in_df[in_df['Spe_HUC'].isin(dups)==True]

reindex =in_df.reindex(columns = ['Spe_HUC','Bins','Value'])


in_df.drop_duplicates(inplace=True)
wide_df = in_df.pivot('Spe_HUC','Bins','Value')



wide_df.reset_index( inplace=True)

wide_df = pd.merge(species_info , wide_df, on='Spe_HUC', how='left')
print wide_df.columns.values.tolist()
print wide_df.columns.values.tolist()
df_final = wide_df.reindex(columns=final_cols)

df_final.to_csv(out_table)

end_script = datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)
