# Name: EucDistance_Ex_02.py
# Description: Calculates for each cell the Euclidean distance to the nearest source.
# Requirements: Spatial Analyst Extension

# Import system modules
import arcpy
from arcpy.sa import *
import os
import datetime

# L:\Workspace\UseSites\ByProject\hold_2.gdb

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
inraster_gdb = r'L:\Workspace\UseSites\ByProject\AK_UseLayer.gdb'
arcpy.env.workspace = inraster_gdb
arcpy.env.Scratchworkspace = r'L:\Workspace\UseSites\ByProject\hold_2.gdb'
rast_list = ['WGS_1984_Albers_AK_FederalLands_AK']
# rast_list = arcpy.ListRasters()
# rast_list = [raster for raster in rast_list  if not raster.startswith('z')]
# rast_list =[raster for raster in rast_list if raster.split("_")[1] =='2015']
outtemp = r'L:\Workspace\UseSites\ByProject\hold_2.gdb'
outbase = inraster_gdb
arcpy.CheckOutExtension("Spatial")

for raster in rast_list:
    arcpy.env.workspace = inraster_gdb
    in_raster = inraster_gdb + os.sep + raster
    print in_raster
    start_loop = datetime.datetime.now()

    print '\nStarting: {0}'.format(raster)
    temp_raster = outtemp + os.sep + os.sep + raster + '_euc'
    region = str(raster.split("_")[0])
    # out_raster = outbase+ os.sep + region "_UseLayer.gdb" + os.sep + raster +'_euc'
    out_raster = outbase + os.sep + raster + '_euc'
    if arcpy.Exists(out_raster):
        arcpy.Delete_management(temp_raster)
        continue
    if not arcpy.Exists(temp_raster):
        where = "Value = 1"
        arcpy.MakeRasterLayer_management(Raster(in_raster), "in_raster", where)
        arcpy.gp.EucDistance_sa("in_raster", temp_raster, "1500", "in_raster")

    if not arcpy.Exists(out_raster):
        arcpy.env.workspace = inraster_gdb
        print out_raster
        outraster = Int(Raster(temp_raster))
        outraster.save(out_raster)
        print 'Saved: {0}'.format(out_raster)
        arcpy.Delete_management(temp_raster)
    end = datetime.datetime.now()

    print "Elapse time {0}".format(end - start_loop)

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_time)
