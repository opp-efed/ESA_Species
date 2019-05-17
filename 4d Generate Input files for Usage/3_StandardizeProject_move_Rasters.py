import os
import pandas as pd
import datetime
from arcpy.sa import *
import arcpy

# Title - Re-projects union raster into projection by region
# in and out location

# Excute this step if the habitat or elevation files have been updated
in_directory = 'D:\Workspace\Elevation\Habitat Zip\Habitat.gdb'  # ## folder or gdb
out_directory = 'D:\Workspace\UseSites\ByProjection'  # ## folder with regional gdbs
midGDB = r'D:\Workspace\UseSites\scratch.gdb'  # ## scratch workspace

# projection folder - check all values in Final Projection columns have a corresponding file with the same name
prjFolder = r'D:\One_drive_old_computer_20180214\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive' \
            r'\Projects\ESA\_ExternalDrive\projections\FinalBE'

# snap raster must be in desired projection with the desired cell size

regions = ['HI', 'AS', 'CNMI', 'GU', 'PR', 'VI', 'AK', 'CONUS']

Region_Dict = {'CONUS': 'Albers_Conical_Equal_Area.prj',
               'HI': 'NAD_1983_UTM_Zone_4N.prj',
               'AK': 'WGS_1984_Albers.prj',
               'AS': 'WGS_1984_UTM_Zone_2S.prj',
               'CNMI': 'WGS_1984_UTM_Zone_55N.prj',
               'GU': 'WGS_1984_UTM_Zone_55N.prj',
               'PR': 'Albers_Conical_Equal_Area.prj',
               'VI': 'WGS_1984_UTM_Zone_20N.prj'}

SnapRaster_Dict = {
    'CONUS': r'D:\Workspace\UseSites\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_cultmask_2016',
    'HI': r'D:\Workspace\UseSites\ByProjection\SnapRasters.gdb\NAD_1983_UTM_Zone_4N_HI_Ag',
    'AK': r'D:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_Albers_AK_Ag',
    'AS': r'D:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_2S_AS_Ag',
    'CNMI': r'D:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_CNMI_Ag',
    'GU': r'D:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_GU_Ag_30',
    'PR': r'D:\Workspace\UseSites\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_PR_Ag',
    'VI': r'D:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_20N_VI_Ag_30'}


def create_gdb(out_folder, out_name, outpath):  # Create a new GDB
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


# loop through use raster and projections into final regional project
def raster_project(inraster, prj_file, prj_folder, snap, region, outraster, cellsize):
    start_raster = datetime.datetime.now()

    in_raster = Raster(inraster)

    arcpy.Delete_management("snap")
    arcpy.MakeRasterLayer_management(snap, "snap")
    arcpy.env.snapRaster = "snap"



    # location prj files
    wgs_coord_file = prj_folder + os.sep + 'WGS_1984.prj'
    nad83_coord_file = prj_folder + os.sep + 'NAD_1983.prj'
    prj_file_path = prj_folder + os.sep + prj_file

    current_raster_dsc = arcpy.Describe(in_raster)
    current_sr = current_raster_dsc.spatialReference
    current_datum = current_sr.GCS.datumName
    ORGprj = current_sr.name

    # extract spatial information from prj files
    dsc_wgs = arcpy.Describe(wgs_coord_file)
    wgs_coord_sys = dsc_wgs.spatialReference

    dsc_nad83 = arcpy.Describe(nad83_coord_file)
    nad83_coord_sys = dsc_nad83.spatialReference

    dsc_prj = arcpy.Describe(prj_file_path)
    prj_sr = dsc_prj.spatialReference
    prj_datum = prj_sr.GCS.datumName
    prj_name = prj_file.replace('.prj', '')
    OUTprj = prj_sr.name

    print ORGprj, OUTprj

    # No re-projection - raster is copied and file name is updated
    if ORGprj == OUTprj:
        if not arcpy.Exists(outraster):
            print 'Copying {0}'.format(inraster)
            print inraster, outraster
            arcpy.CopyRaster_management(inraster, outraster)
        else:
            print str(outraster) + " already exists"
    # Check Datum then re-projects

    elif prj_datum == "D_WGS_1984":
        if current_datum != "D_WGS_1984":
            if not arcpy.Exists(outraster):
                print 'Projecting {0} into {1}'.format(inraster, prj_name)
                arcpy.ProjectRaster_management(in_raster, outraster, prj_sr, 'NEAREST', cellsize,
                                               "NAD_1983_To_WGS_1984_1")
            else:
                print str(outraster) + " already exists"
        else:
            if not arcpy.Exists(outraster):
                print 'Projecting {0} into {1}'.format(inraster, prj_name)
                arcpy.ProjectRaster_management(in_raster, outraster, prj_sr, 'NEAREST', cellsize)
            else:
                print str(outraster) + " already exists"
    else:
        if prj_datum == "D_North_American_1983":
            if current_datum != "D_North_American_1983":
                print 'Projecting {0} into {1}'.format(inraster, prj_name)
                arcpy.ProjectRaster_management(in_raster, outraster, prj_sr, 'NEAREST', cellsize,
                                               "NAD_1983_To_WGS_1984_1")
            else:
                if not arcpy.Exists(outraster):
                    print 'Projecting {0} into {1}'.format(inraster, prj_name)
                    arcpy.ProjectRaster_management(in_raster, outraster, prj_sr, 'NEAREST', cellsize)
                else:
                    print str(outraster) + " already exists"

    print 'Completed projection in: {0}\n'.format((datetime.datetime.now() - start_raster))


arcpy.env.overwriteOutput = True  # ## Change this to False if you don't want GDB to be overwritten
arcpy.env.scratchWorkspace = ""

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

if not os.path.exists(midGDB):
    create_gdb(os.path.dirname(midGDB), os.path.basename(midGDB), midGDB)

arcpy.env.workspace = in_directory
list_raster = arcpy.ListRasters()
print list_raster
for v in list_raster:
    region = [i for i in v.split('_') if i in regions]
    if len (region) == 0:
        continue
    else:
        region = region[0]
    region_projection = Region_Dict[region]
    snap_raster = SnapRaster_Dict[region]
    out_raster = out_directory + os.sep + region + "_HabitatElevation.gdb" + os.sep + region_projection.replace('.prj',
                                                                                                                '') + "_" + v
    print 'Working on {0}'.format(out_raster)
    if os.path.exists(out_raster):
        continue
    else:
        path, tail = os.path.split(out_raster)
        if not os.path.exists(path):
            create_gdb(os.path.dirname(path), os.path.basename(path), path)

        try:
            raster_project(v, region_projection, prjFolder, snap_raster, region, out_raster, '30')
        except Exception as error:
            print(error.args[0])

end = datetime.datetime.now()
print "\nEnd Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
