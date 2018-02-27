import os
import datetime
from arcpy.sa import *
import arcpy
import pandas as pd

# Title - Re-projects union raster into projection by region
# in and out location

inGDB = 'L:\NewFWS_RangesStep_20161017\FinalRaster_STD_Name'
outGDB = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_GAP_pilotSpecies.gdb'
midGBD = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\scratch.gdb'
snap_raster_location = r'L:\Workspace\UseSites\ByProject\CONUS_UseLayer.gdb\Albers_Conical_Equal_Area_CONUS_Carbaryl_UseFootprint_160824_euc'

# projection folder
prjFolder = "L:\projections\FinalBE"
coordFile = 'Albers_Conical_Equal_Area.prj'
prj_name = 'Albers_Conical_Equal_Area'


def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


# loop through use raster and projections into final regional project
def raster_project(prj_current, inraster, in_gdb, prj_folder, out_folder):
    start_raster = datetime.datetime.now()
    print "\n"
    print inraster

    in_raster = Raster(in_gdb + os.sep + str(inraster))
    out_gdb = out_folder

    snap_raster = snap_raster_location
    arcpy.Delete_management("snap")
    arcpy.MakeRasterLayer_management(snap_raster, "snap")
    arcpy.env.snapRaster = "snap"
    print str(snap_raster)

    # extract snap raster for region

    # location prj files
    wgs_coord_file = prj_folder + os.sep + 'WGS_1984.prj'
    prj_file_path = prj_folder + os.sep + prj_current

    # extract spatial information from prj files
    dsc_wgs = arcpy.Describe(wgs_coord_file)
    wgs_coord_sys = dsc_wgs.spatialReference

    dsc_prj = arcpy.Describe(prj_file_path)
    prj_sr = dsc_prj.spatialReference
    prj_datum = prj_sr.GCS.datumName

    try:
        if prj_datum == "D_WGS_1984":
            arcpy.Delete_management("inital_r_lyr")
            print in_raster
            arcpy.MakeRasterLayer_management(in_raster, "inital_r_lyr")

            raster_other_geo = str(inraster) + "_WGS84"
            out_other_raster = midGBD + os.sep + raster_other_geo
            print out_other_raster

            prj_raster_name = raster_other_geo + "_" + prj_name
            prj_raster = out_gdb + os.sep + prj_raster_name

            if not arcpy.Exists(out_other_raster):
                print 'Projecting {0} into {1}'.format(inraster, 'WGS 1984')
                arcpy.ProjectRaster_management("inital_r_lyr", out_other_raster, wgs_coord_sys)

            if not arcpy.Exists(prj_raster):
                arcpy.Delete_management("WGS_lyr")
                arcpy.MakeRasterLayer_management(out_other_raster, "WGS_lyr", "#", snap_raster, '#')
                print 'Projecting {0} into {1}'.format(inraster, prj_name)

                arcpy.ProjectRaster_management("WGS_lyr", prj_raster, "snap", 'NEAREST', "30")

            else:
                print str(prj_raster_name) + " already exists"

        else:

            print in_raster
            prj_raster_name = str(inraster) + "_" + prj_name
            prj_raster = out_gdb + os.sep + prj_raster_name

            if not arcpy.Exists(prj_raster):
                arcpy.Delete_management("inital_r_lyr")
                arcpy.MakeRasterLayer_management(in_raster, "inital_r_lyr", "#", snap_raster, '#')
                print 'Projecting {0} into {1}'.format(inraster, prj_name)
                arcpy.ProjectRaster_management("inital_r_lyr", prj_raster, "snap", 'NEAREST', "30")

            else:
                print str(prj_raster) + " already exists"

        print 'Completed projection of {0} in: {1}'.format(prj_name, (datetime.datetime.now() - start_raster))
    except Exception as error:
        print 'Error in loop'
        print(error.args[0])


arcpy.env.overwriteOutput = True  # ## Change this to False if you don't want GDB to be overwritten
arcpy.env.scratchWorkspace = ""

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
arcpy.env.workspace = inGDB
raster_list = arcpy.ListRasters()
print raster_list

for raster in raster_list:
    try:
        raster_project(coordFile, raster, inGDB, prjFolder, outGDB)
    except Exception as error:
        print(error.args[0])

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
