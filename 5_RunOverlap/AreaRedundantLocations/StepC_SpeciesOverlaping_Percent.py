import pandas as pd
import os
import datetime


in_table = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\Overlaping_species\output' \
           r'\Sp_array_final_20170731.csv'
acres_table = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tables\R_Acres_by_region_20170131_n.csv'
out_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\Overlaping_species\output'

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

in_count_df = pd.read_csv(in_table)
in_acres_df = pd.read_csv(acres_table)

[in_count_df.drop(t, axis=1, inplace=True) for t in in_count_df.columns.values.tolist() if t.startswith('Unnamed')]
[in_acres_df.drop(t, axis=1, inplace=True) for t in in_acres_df.columns.values.tolist() if t.startswith('Unnamed')]

in_count_df['EntityID'] = in_count_df['EntityID'].map(lambda x: x).astype(str)
working_entityid = in_count_df['EntityID'].values.tolist()

acres_df = in_acres_df.ix[:, ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Range_Filename', 'Acres_L48']]
acres_df['Acres_L48'].fillna(0, inplace=True)
acres_df['EntityID'] = acres_df['EntityID'].map(lambda x: x).astype(str)
acres_df['Acres_L48'] = acres_df['Acres_L48'].map(lambda x: x).astype(int).map(lambda x: x).astype(int)
acres_df = acres_df.loc[acres_df['EntityID'].isin(working_entityid)]

zero_acres = acres_df.loc[acres_df['Acres_L48'] == 0]
acres_df = acres_df.loc[acres_df['Acres_L48'] != 0]
acres_df = acres_df.sort_values(by='EntityID')

in_count_df = in_count_df.loc[~in_count_df['EntityID'].isin(zero_acres['EntityID'].values.tolist())]
working_entityid = in_count_df['EntityID'].values.tolist()

in_count_df = in_count_df.T.reset_index()
in_count_df.columns = in_count_df.iloc[0]
in_count_df = in_count_df.reindex(in_count_df.index.drop(0))
in_count_df = in_count_df.loc[in_count_df['EntityID'].isin(working_entityid)]
in_count_df = in_count_df.sort_values(by='EntityID')

range_acres_float = acres_df['Acres_L48'].map(lambda x: x).astype(int).map(lambda x: x).astype(int)
val_col = in_count_df.columns.values.tolist()
val_col.remove('EntityID')

in_count_df.ix[:, val_col] *= 900
in_count_df.to_csv(out_location + os.sep + 'Overlapping_species_msq.csv')
in_count_df.ix[:, val_col] *= 0.000247
in_count_df.to_csv(out_location + os.sep + 'Overlapping_species_acres.csv')

list_acres = range_acres_float.values.tolist()
se = pd.Series(list_acres)
in_count_df['Acres'] = se.values

in_count_df.ix[:, val_col] = in_count_df.ix[:, val_col].div(in_count_df.Acres, axis=0)
in_count_df.ix[:, val_col] *= 100

in_count_df.to_csv(out_location + os.sep + 'Overlapping_species_percent_filtered_' + date + '.csv')

acres_df['EntityID'] = acres_df['EntityID'].map(lambda x: x).astype(str)
in_count_df['EntityID'] = in_count_df['EntityID'].map(lambda x: x).astype(str)
print acres_df
final_df = pd.merge(in_count_df, acres_df, on='EntityID', how='left')
final_df.to_csv(out_location + os.sep + 'Overlapping_species_final_' + date + '.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
