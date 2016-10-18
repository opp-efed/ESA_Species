import os
import datetime
import pandas as pd

import arcpy
from arcpy.sa import *

# Title- runs Zonal Histogram for all sp union file against each use

in_HUC_base = 'L:\NHDPlusV2'

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

list_dir = os.listdir(in_HUC_base)
list_HUC2 = [huc2 for huc2 in list_dir if huc2.startswith('NHDPlus')]

acres_df = pd.DataFrame(columns=['HUC_12', 'ACRES'])
acres_df_false = pd.DataFrame(columns=['HUC_12', 'ACRES'])

for value in list_HUC2:
    print value
    HUC12_path = in_HUC_base + os.sep + value + os.sep + 'WBDSnapshot\WBD\WBD_Subwatershed.shp'


    array = arcpy.da.TableToNumPyArray(HUC12_path, ['HUC_12', 'ACRES'])

    df = pd.DataFrame(array)
    dups = df.set_index('HUC_12').index.get_duplicates()

    att_sum = df.groupby(by=['HUC_12'])['ACRES'].sum()
    out_csv = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\acres_sum\{0}'.format(value+'.csv')
    att_sum .to_csv(out_csv)
    print att_sum
    #acres_df = pd.concat([acres_df, df])

#acres_df.to_csv('L:\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Tables\HUC12\AllHUCs.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
