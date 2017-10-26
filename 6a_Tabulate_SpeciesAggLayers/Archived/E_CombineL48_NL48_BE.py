import pandas as pd
import datetime
import os

in_table_L48 = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\FinalTables_Range\BETables\R_L48_BE_SprayDriftIntervals_20170207.csv'
in_table_NL48 = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\FinalTables_Range\BETables\R_NL48_BE_SprayDriftIntervals_20170209.csv'
outlocation = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\FinalTables_Range\BETables'

regions = ['AK', 'GU', 'HI', 'AS', 'PR', 'VI', 'CNMI','CONUS','AS'] #Range
master_list = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_June2016_20170117.xlsx'

master_col = ['EntityID', 'Group', 'comname', 'sciname','status_text','pop_abbrev', 'Des_CH', 'CH_GIS',
              'Source of Call final BE-Range', 'WoE Summary Group', 'Source of Call final BE-Critical Habitat',
              'Critical_Habitat_','Migratory', 'Migratory_']

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

out_csv = outlocation + os.sep + 'R_AllUses_BE_' + date + '.csv'

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# master_list_df =  pd.read_csv(master_list)
master_list_df = pd.read_excel(master_list)
master_list_df = master_list_df.ix[:, master_col]
master_list_df['EntityID'] = master_list_df['EntityID'].map(lambda x: x).astype(str)
print master_list_df

in_df_L48 = pd.read_csv(in_table_L48, dtype=object)
in_df_L48['EntityID'] = in_df_L48['EntityID'].map(lambda x: x).astype(str)
in_df_L48_col = in_df_L48.columns.values.tolist()
col_reindex = ['EntityID']

for col in in_df_L48_col:
    region = col.split("_")[0]
    if col in col_reindex:
        pass
    elif region in regions:
        col_reindex.append(col)
    else:
        in_df_L48.drop(col, axis=1, inplace=True)
        print 'dropped: ' + col
in_df_L48.columns = col_reindex

col_reindex = ['EntityID']
in_df_NL48 = pd.read_csv(in_table_NL48, dtype=object)
in_df_NL48['EntityID'] = in_df_NL48['EntityID'].map(lambda x: x).astype(str)
in_df_NL48_col = in_df_NL48.columns.values.tolist()
col_reindex = ['EntityID']
for col in in_df_NL48_col:
    region = col.split("_")[0]
    if col in col_reindex:
        pass
    elif region in regions:
        col_reindex.append(col)
    else:
        in_df_NL48.drop(col, axis=1, inplace=True)
        print 'dropped: ' + col
in_df_NL48.columns = col_reindex

out_df = pd.merge(master_list_df, in_df_L48, on='EntityID', how='inner')
out_df = pd.merge(out_df, in_df_NL48, on='EntityID', how='inner')
print out_df
out_df.to_csv(out_csv)
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
