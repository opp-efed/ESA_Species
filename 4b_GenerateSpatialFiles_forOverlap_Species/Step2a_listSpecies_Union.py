import arcpy
import pandas as pd
import os
import datetime

in_location = 'C:\Users\JConno02\Documents\Projects\ESA\UnionFiles_Winter2018\Range\R_SpGroup_Union_final_20180110.gdb'
file_suffix_clean = '_Union_Final_20180110'
out_csv = r'C:\Users\JConno02\Documents\Projects\ESA\UnionFiles_Winter2018\Range\R_Species_included_inUnion' + \
          file_suffix_clean + '.csv'

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

out_df = pd.DataFrame(index=(list(range(0, 1000))))
arcpy.env.workspace = in_location
fc_list = arcpy.ListFeatureClasses()

for fc in fc_list:
    group = fc.split("_")[1]
    att_array = arcpy.da.TableToNumPyArray((in_location + os.sep + fc), ['ZoneSpecies'])
    att_df = pd.DataFrame(data=att_array)
    att_df['ZoneSpecies'] = att_df['ZoneSpecies'].apply(
        lambda x: x.replace('[', '').replace(']', '').replace('u', '').replace(' ', '').replace("'", ""))
    spl = att_df['ZoneSpecies'].str.split(',', expand=True)
    out_list = []
    for col in spl.columns.values.tolist():
        out_list.extend(spl[col].values.tolist())
    group_spe = list(set(out_list))

    count = len(group_spe)
    remaining = [None] * (1000 - count)  # all columns need to 1000 rows - makes additional rows with value none
    merge_list = group_spe + remaining
    series_sp = pd.Series(merge_list)
    out_df[group] = series_sp.values

out_df.to_csv(out_csv)
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
