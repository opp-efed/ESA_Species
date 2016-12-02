import datetime
import os

import arcpy
import numpy as np
import pandas as pd

csvFolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\Agg_layers\Ag\Range'
outFolder= 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\Transposed\Agg_layers\Ag\Range'

interval_step = 30
max_dis = 1501
bins = np.arange((0 - interval_step), max_dis, interval_step)

def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()


out_folders = os.listdir(csvFolder)
for folder in out_folders:
    use = folder.replace('_euc','')

    out_folder= outFolder +os.sep + folder

    createdirectory(out_folder)

    list_csv = os.listdir(csvFolder+os.sep+folder)

    list_csv = [csv for csv in list_csv if csv.endswith( str(folder) +'.csv')]


    for csv in list_csv:
        out_csv = out_folder +os.sep+ csv
        #if not os.path.exists(out_csv):
        print csv

        in_csv = csvFolder+os.sep+folder + os.sep + csv
        in_df = pd.read_csv(in_csv)

        in_df.drop('OID', axis=1, inplace=True)
        in_df.drop('TableID', axis=1, inplace=True)

        in_df['LABEL'] = in_df['LABEL'].astype(str)
        in_df['LABEL'] = in_df['LABEL'].map(lambda x: x.replace(',', '')).astype(long)

        binned_df = in_df.groupby(pd.cut(in_df['LABEL'], bins)).sum()  # breaks out into binned intervals
        group_df_by_zone_sum = binned_df.transpose()  # transposes so it is Zones by interval and not interval by zone

        group_df_by_zone_sum = group_df_by_zone_sum.ix[1:]  # removed the summed interval row that is added when binned

        cols = bins.tolist()
        cols.remove((0 - interval_step))
        outcol = []

        for i in cols:
            col = use + "_" + str(i)
            outcol.append(col)

        group_df_by_zone_sum.columns = outcol
        print group_df_by_zone_sum

        group_df_by_zone_sum['ZoneID'] = group_df_by_zone_sum.index
        group_df_by_zone_sum['ZoneID'] = group_df_by_zone_sum['ZoneID'].map(lambda x: x.replace('VALUE_', '')).astype(str)
        print group_df_by_zone_sum


        # transposed_df.columns = out_cols
        group_df_by_zone_sum.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
