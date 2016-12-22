import os
import datetime
import pandas as pd

import arcpy
from arcpy.sa import *

# Title- runs Zonal Histogram for all sp union file against each use

in_HUC_base = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\NHD_acresHUC2\acres_sum\outfc'

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

arcpy.env.workspace = in_HUC_base
fc_list = arcpy.ListFeatureClasses()
all_acres = pd.DataFrame(columns=['HUC12', 'Acres_prj'])
for fc in fc_list:
    in_HUC12_shp = in_HUC_base + os.sep + fc
    att_array = arcpy.da.TableToNumPyArray(in_HUC12_shp, ['HUC_12', 'Acres_prj'])
    att_df = pd.DataFrame(data=att_array)
    att_df['HUC_12'] = att_df['HUC_12'].astype(str)
    att_df['HUC_12'] = att_df['HUC_12'].map(lambda x: '0'+x if len(x) == 11 else x).astype(str)
    # att_sum = att_df.groupby(by=['HUC_12'])['Acres_prj'].sum()
    att_sum = att_df.groupby(by=['HUC_12']).sum()
    # att_df = att_sum.to_frame()
    att_sum.reset_index(level=0, inplace=True)
    att_sum['HUC12'] = att_sum['HUC_12'].astype(str)
    att_sum.drop(labels=['HUC_12'], axis=1, inplace=True)
    att_sum.sort_values(['HUC12'])
    all_acres = pd.concat([all_acres, att_sum], axis=0)

all_acres = all_acres.groupby(by=['HUC12']).sum()
all_acres.reset_index(level=0, inplace=True)

final_df = all_acres.drop_duplicates()

print final_df
outdf = pd.DataFrame()
final_df.columns = ['HUC_12','Acres']
outdf['HUC12'] = final_df ['HUC_12'].map(lambda x: x).astype(str)
outdf['Acres'] = final_df ['Acres'].map(lambda x: x).astype(float)
outdf['HUC12'] = outdf['HUC12'].map(lambda x: '0'+x if len(x)==11 else x).astype(str)
outdf['HUC02'] = outdf['HUC12'].map(lambda x: x[:2])
outdf.to_csv(r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tables\AllHUC_acres.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
