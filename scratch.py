import arcpy
import pandas as pd
import os

in_fc = r'L:\ESA\UnionFiles_Winter2018\NHDFiles\L48\NHDCatchments_20180122_20180122_interCnty.gdb\HUC12'
out_gdb = r'L:\ESA\UnionFiles_Winter2018\NHDFiles\L48\HUC_12byHUC2.gdb'

array = arcpy.da.TableToNumPyArray(in_fc , ['HUC_2'])
sp_zone_df = pd.DataFrame(data=array, dtype=object)
huc_2 = set(list(sp_zone_df['HUC_2'].values.tolist()))

arcpy.MakeFeatureLayer_management(in_fc, "HUC48_lyr")
for v in huc_2:
    whereclause = "HUC_2 = '%s'" % v
    print whereclause
    # Makes a feature layer that will only include current entid using whereclause
    arcpy.MakeFeatureLayer_management("HUC48_lyr", "lyr", whereclause)
    arcpy.CopyFeatures_management("lyr", out_gdb + os.sep + "HUC2_"+ str(v))
    arcpy.Delete_management("lyr")
