import os

import arcpy
arcpy.env.workspace = r"G:\\Temp"

shplocation= r"G:\\Species\\GIS\WebApp\\Fishnet375000\\Fishnet_375000_shp"
fc= "G:\\Fishnest_Co-Occur\\Co-Occur.gdb\\fishnet_375000_ProjectWGSb"



#arcpy.MakeFeatureLayer_management(fc, "fc_lyr")
sc = arcpy.da.SearchCursor(fc, ["netID"])

for row in sc:
    arcpy.Delete_management("lyr")
    id = row[0]
    print str(id)
    extent = shplocation+ os.sep + str(id)+ "_fc"
    netID = "netID"
    whereclause = '"' + netID + '" = ' + str(id)
    #whereclause = "netID = '%s'" % (id)
    arcpy.MakeFeatureLayer_management(fc, "lyr", whereclause)
    arcpy.CopyFeatures_management("lyr",extent)

