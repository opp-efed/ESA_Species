# Name: PolygonToRaster_Ex_02.py
# Description: Converts polygon features to a raster dataset.

# Import system modules
import os
import datetime

import arcpy

__author__ = 'JConno02'

inlocation = 'J:\Workspace\ESA_Species\FinalBE_ForCoOccur\Union_Range.gdb'
outlocation = 'J:\Workspace\ESA_Species\FinalBE_ForCoOccur\Raster_UnionRange_20160705.gdb'
snapRaster = r"J:\Workspace\UseSites\CONUS_Ag_151103.gdb\CONUS_Corn"

start_script = datetime.datetime.now()
print "Started at {0}".format(start_script)


def polygon_to_raster(infc, outlocation, snapRaster, inlocation):
    start_conversion = datetime.datetime.now()
    # set environmental variables
    arcpy.env.workspace = outlocation
    arcpy.Delete_management("snap")
    arcpy.MakeRasterLayer_management(snapRaster, "snap")
    arcpy.env.snapRaster = "snap"

    # Set local variables
    inFeatures = inlocation + os.sep + infc

    valField = "Zone"
    outRaster = outlocation + os.sep + infc
    assignmentType = "CELL_CENTER"
    cellSize = snapRaster

    print inFeatures
    arcpy.Delete_management("fc_lyr")
    arcpy.MakeFeatureLayer_management(inFeatures, "fc_lyr")


    # Execute PolygonToRaster
    if not arcpy.Exists(outRaster):
        arcpy.PolygonToRaster_conversion("fc_lyr", valField, outRaster, assignmentType, "NONE", cellSize)
        print "\nCompleted conversion of {0} in {1}".format(outRaster, (datetime.datetime.now() - start_conversion))

    else:
        print "\nAlready completed conversion {0}".format(outRaster)
    arcpy.Delete_management("snap")


if inlocation[-3:] == 'gdb':
    arcpy.env.workspace = inlocation
    fclist = arcpy.ListFeatureClasses()
    for fc in fclist:
        polygon_to_raster(fc, outlocation, snapRaster, inlocation)
else:
    list_ws = os.listdir(inlocation)
    print list_ws
    for v in list_ws:
        if v[-3:] == 'gdb':
            ingdb = inlocation + os.sep + v
            arcpy.env.workspace = ingdb
            fclist = arcpy.ListFeatureClasses()
            for fc in fclist:
                print fc
                polygon_to_raster(fc, outlocation, snapRaster, inlocation)
        else:
            continue

print "Completed in {0}".format(datetime.datetime.now() - start_script)
