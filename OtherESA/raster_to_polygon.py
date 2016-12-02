import arcpy
import os
from arcpy.sa import *


in_location =r'L:\NewFWS_RangesStep_20161017\FinalRaster'
outBase = 'L:\NewFWS_RangesStep_20161017\FinalShapes\FinalFeatureClasses.gdb'

arcpy.env.workspace = in_location
rast_list = arcpy.ListRasters()
print rast_list

for raster in rast_list:
    print 'Starting: {0}'.format(raster)
    out_fc = outBase + os.sep + raster
    if not arcpy.Exists(out_fc):
        arcpy.MakeRasterLayer_management(Raster(raster), "in_raster")
        # arcpy.RasterToPolygon_conversion("in_raster", out_fc, "NO_SIMPLIFY","VALUE")
        arcpy.RasterToPolygon_conversion("in_raster", out_fc, "SIMPLIFY","VALUE")
        print 'Saved: {0}'.format(out_fc)
