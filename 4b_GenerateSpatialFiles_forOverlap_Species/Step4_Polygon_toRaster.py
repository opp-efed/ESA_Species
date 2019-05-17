import os
import datetime

import arcpy

# Title- converts all polygons in the inlocation to raster to be used in zonal historgram
# This is done as recommend by ESRI to have greater control oer the conversion

# http://desktop.arcgis.com/en/arcmap/10.3/tools/spatial-analyst-toolbox/tabulate-area.htm
# It is generally recommended to only use rasters as the zone and class inputs. If your inputs are feature, consider
# converting them to rasters first with the To Raster conversion tools. This offers you greater control over the
# vector-to-raster conversion, helping to ensure you consistently get the expected results.


inlocation = 'L:\Workspace\StreamLine\ESA\UnionFiles_Winter2018\CriticalHabitat\CH_Clipped_Union_CntyInter_HUC2Inter_20180612.gdb'
outlocation = 'L:\Workspace\StreamLine\ESA\UnionFiles_Winter2018\CriticalHabitat\CH_Raster_Clipped_Union_CntyInter_HUC2Inter_20180612.gdb'

# #ZoneID (species only), InterID (species and political boundaries) or HUCID (species political boundaries and HUC2s)
id_field = 'HUCID'  #  ZoneID, InterID or HUCID


skip_group =[]
# snap raster for conversion must be a NAD 83 geographic project with 30 meter cells; default cell is very large
snapRaster = r"F:\UseSite_ESA\Spring 2017\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.gdb\cultmask_2015_NAD83"
#

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

    valField = id_field
    outRaster = out_location + os.sep + in_fc
    # variable that sets raster type; using max area or Max combined so no matter how
    # small the range a cell is generated - max combine will include area both different but give it one value max area
    # will only keeo the area with the most
    assignmentType = "MAXIMUM_COMBINED_AREA"

    # print inFeatures
    arcpy.Delete_management("fc_lyr")
    arcpy.MakeFeatureLayer_management(inFeatures, "fc_lyr")

    # Execute PolygonToRaster
    if arcpy.Exists(outRaster):
        print "Already completed conversion {0} \n".format(outRaster)

    else:
        print "Start conversion"
        try:
            arcpy.PolygonToRaster_conversion("fc_lyr", valField, outRaster, assignmentType, "ZoneID", "snap")
            print "Completed conversion of {0} in {1}\n".format(outRaster, (datetime.datetime.now() - start_conversion))
        except Exception as error:
            print(error.args[0])
            arcpy.Delete_management(outRaster)

    arcpy.Delete_management("snap")

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

folder, gdb = os.path.split(outlocation)
if not os.path.exists(outlocation):
    create_gdb(folder, gdb, outlocation)

if inlocation[-3:] == 'gdb':
    arcpy.env.workspace = inlocation
    fclist = arcpy.ListFeatureClasses()
    for fc in fclist:
        sp_group = fc.split("_")[1]

        if sp_group in skip_group:
            continue
        else:
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
                sp_group = fc.split("_")[1]
                print sp_group
                if sp_group in skip_group:
                    continue
                else:
                    polygon_to_raster(fc, outlocation, snapRaster, inlocation)
        else:
            continue

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
