import os
import datetime
import pandas as pd

import arcpy
from arcpy.sa import *

# Title- runs Zonal Histogram for all sp union file against each use

in_HUC_base = r'L:\NHDPlusV2'

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

list_dir = os.listdir(in_HUC_base)
list_HUC2 = [huc2 for huc2 in list_dir if huc2.startswith('NHDPlus')]

acres_df = pd.DataFrame()

for value in list_HUC2:
    print value

    HUC12_path = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\acres_sum\outfc' + os.sep + value + '_WBD.shp'
    array = arcpy.da.TableToNumPyArray(HUC12_path, ['HUC_12', 'Acres_prj'])

    df = pd.DataFrame(array)
    # dups = df.set_index('HUC_12').index.get_duplicates()

    att_sum = df.groupby(by=['HUC_12'])['Acres_prj'].sum()
    att_df = att_sum.to_frame()
    att_df.reset_index(level=0, inplace=True)
    att_df.columns = ['HUC_12','Acres']

    out_csv = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\acres_sum\{0}'.format(value+'.csv')

    att_df.to_csv(out_csv)

    acres_df = pd.concat([acres_df,att_df], axis =0)


final_df = acres_df.drop_duplicates()
print final_df
outdf = pd.DataFrame()
final_df.columns = ['HUC_12','Acres']
outdf['HUC12'] = final_df ['HUC_12'].map(lambda x: x).astype(str)
outdf['Acres'] = final_df ['Acres'].map(lambda x: x).astype(float)
outdf['HUC12'] = outdf['HUC12'].map(lambda x: '0'+x if len(x)==11 else x).astype(str)
outdf['HUC02'] = outdf['HUC12'].map(lambda x: x[:2])
outdf.to_csv(r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\acres_sum\AllHUC_acres.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
