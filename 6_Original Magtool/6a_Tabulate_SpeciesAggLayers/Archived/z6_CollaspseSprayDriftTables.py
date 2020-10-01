import pandas as pd
import datetime
import os
from natsort import natsorted

# NOTE DO WE NEED THIS???

# NOTE this as not been cleaned up to incorportate new file struture due to decisions that need to be made
# TODO to may uses being added to develop and cultivated (OSD and Non Cultivated) check for others
# TODO GOLF COURSE is differetn on NL48 and L48 tables golfcourse and golf course
# TODO need to decided what to do with AG
# TODO figure out why to put these in order based on interval so it can be reindexed
in_table_L48 = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\Agg_layers\R_MagTool_SprayDrift_WholeRange_20170508_clean.csv'
sp_index_cols = 11
in_table_NL48 = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\NL48\Range\R_NL48_SprayInterval_20170508_WholeRange_clean.csv'
outlocation = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\FinalTables_Range\BETables'

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

out_csv = outlocation + os.sep + 'R_Collapsed_Drift_' + date + '.csv'
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

L48_df = pd.read_csv(in_table_L48, )
L48_df['EntityID'] = L48_df['EntityID'].astype(str)
NL48_df = pd.read_csv(in_table_NL48)
NL48_df['EntityID'] = NL48_df['EntityID'].astype(str)

full_df = pd.merge(L48_df, NL48_df, on='EntityID', how='outer')
[full_df.drop(t, axis=1, inplace=True) for t in full_df.columns.values.tolist() if t.startswith('Unnamed')]
sp_info_df = L48_df.iloc[:, 1:sp_index_cols]
reindex_col = sp_info_df.columns.values.tolist()
columns_full = full_df.columns.values.tolist()

list_uses = (
    list(set([p.split("_")[1] + "_" + p.split("_")[2] if len(p.split("_")) == 3 else 'nan' for p in columns_full])))
nat_sort_cols= natsorted(list_uses)
reindex_col.extend(nat_sort_cols)
collapsed_dict = {}
for k in list_uses:
    values = []
    for i in columns_full:
        if i.endswith(k):
            values.append(i)
    collapsed_dict[k] = values

collapsed_df = pd.DataFrame(data=sp_info_df)

for use in list_uses:
    binned_col = list(collapsed_dict[use])
    binned_df = full_df[binned_col]
    use_results_df = binned_df.apply(pd.to_numeric, errors='coerce')
    collapsed_df[(str(use))] = use_results_df.sum(axis=1)

final_df = pd.concat([sp_info_df, collapsed_df], axis=1)


final_df = collapsed_df.reindex(columns=reindex_col)
final_df = final_df.fillna(0)

final_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)


