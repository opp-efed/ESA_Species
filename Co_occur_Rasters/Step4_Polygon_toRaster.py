import os
import datetime

import arcpy

# Title- converts all polygons in the inlocation to raster to be used in zonal historgram

# in and out location
inlocation = 'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\Range\R_Clipped_UnionRange_20160907.gdb'
outlocation = 'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\Range\Clipped_MaxArea.gdb'

# snap raster for convesion
snapRaster = r"J:\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.gdb\cultmask_2015_NAD83"


# ###Functions
# Create a new GDB
def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


# polygon to raster conversion
def polygon_to_raster(in_fc, out_location, snap_raster, in_location):
    print in_fc
    start_conversion = datetime.datetime.now()
    # set environmental variables
    arcpy.env.workspace = outlocation
    arcpy.Delete_management("snap")
    arcpy.MakeRasterLayer_management(snap_raster, "snap")
    arcpy.env.snapRaster = "snap"

    # Set local variables
    inFeatures = in_location + os.sep + in_fc

    valField = "OBJECTID"
    outRaster = out_location + os.sep + in_fc
    # variable that sets raster type; using max area so no matter how small the range a cell is generated
    assignmentType = "MAXIMUM_AREA"
    cellSize = snapRaster

    # print inFeatures
    arcpy.Delete_management("fc_lyr")
    arcpy.MakeFeatureLayer_management(inFeatures, "fc_lyr")

    # Execute PolygonToRaster
    if arcpy.Exists(outRaster):
        print "Already completed conversion {0} \n".format(outRaster)

    else:
        print "Start conversion"
        try:
            arcpy.PolygonToRaster_conversion("fc_lyr", valField, outRaster, assignmentType, "NONE", cellSize)
            print "Completed conversion of {0} in {1}\n".format(outRaster, (datetime.datetime.now() - start_conversion))
        except Exception as error:
            print(error.args[0])
            arcpy.Delete_management(outRaster)

    arcpy.Delete_management("snap")

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

folder, gdb = os.path.split(outlocation)
create_gdb(folder, gdb, outlocation)

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
            in_gdb = inlocation + os.sep + v
            arcpy.env.workspace = in_gdb
            fc_list = arcpy.ListFeatureClasses()
            for fc in fc_list:
                print fc
                polygon_to_raster(fc, outlocation, snapRaster, inlocation)
        else:
            continue

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
