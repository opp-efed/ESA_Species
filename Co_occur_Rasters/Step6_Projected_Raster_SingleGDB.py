import os
import datetime
from arcpy.sa import *
import arcpy
# Title - Re-projects union raster into projection by region
# in and out location
inGDB = 'H:\Workspace\UseSites\ActionAreas\AK_AA.gdb'
outfolder = 'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject'

midGBD = 'C:\Users\Admin\Documents\Jen\Workspace\UseSites\scratch.gdb'

# projection folder
prjFolder = "H:\Workspace\projections\FinalBE"
# Dictionary of all projections needed for raster and the snap raster
# snap raster must be in desired projection with the desired cell size
# TODO UPDATE SNAP RASTER
RegionalProjection_Dict = {
    'CONUS': r'C:\Users\Admin\Documents\Jen\Workspace\UseSites\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.gdb\cultmask_2015',
    'HI': r'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject\HI_UseLayer.gdb\NAD_1983_UTM_Zone__4N_HI_VegetablesGroundFruit_euc',
    'AK': r'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject\AK_UseLayer.gdb\WGS_1984_Albers_AK_Developed_euc',
    'AS': r'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject\AS_UseLayer.gdb\WGS_1984_UTM_Zone__2S_AS_OSD_euc',
    'CNMI': r'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject\CNMI_UseLayer.gdb\WGS_1984_UTM_Zone_55N_CNMI_Developed_euc',
    'GU': r'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject\GU_UseLayer.gdb\WGS_1984_UTM_Zone_55N_GU_Ag_euc',
    'PR': r'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject\PR_UseLayer.gdb\NAD_1983_StatePlane_Puerto_Rico_Virgin_Isl_FIPS_5200_PR_Ag_euc',
    'VI': r'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject\VI_UseLayer.gdb\WGS_1984_UTM_Zone_20N_VI_Ag_euc'
}

# Had to shorted the file name fo the PR prj file in order to me file path charater limits

Region_Dict = {'CONUS': 'Albers_Conical_Equal_Area.prj',
               'HI': 'NAD_1983_UTM_Zone__4N.prj',
               'AK': 'WGS_1984_Albers.prj',
               'AS': 'WGS_1984_UTM_Zone__2S.prj',
               'CNMI': 'WGS_1984_UTM_Zone_55N.prj',
               'GU': 'WGS_1984_UTM_Zone_55N.prj',
               'PR': 'NAD_1983_StatePlane_Puerto_Rico_Virgin_Isl_FIPS_5200.prj',
               'VI': 'WGS_1984_UTM_Zone_20N.prj',
               'Howland_Baker_Jarvis': 'NAD_1983_Albers.prj',
               'Johnston': 'NAD_1983_Albers.prj',
               'Laysan': 'NAD_1983_Albers.prj',
               'Mona': 'NAD_1983_Albers.prj',
               'Necker': 'NAD_1983_Albers.prj',
               'Nihoa': 'NAD_1983_Albers.prj',
               'NorthwesternHI': 'NAD_1983_Albers.prj',
               'Palmyra_Kingman': 'NAD_1983_Albers.prj',
               'Wake': 'NAD_1983_Albers.prj'
               }

outGDBdict = {'CONUS': 'Conus_UseLayer.gdb',
              'HI': 'HI_UseLayer.gdb',
              'AK': 'AK_UseLayer.gdb',
              'AS': 'AS_UseLayer.gdb',
              'CNMI': 'CNMI_UseLayer.gdb',
              'GU': 'GU_UseLayer.gdb',
              'PR': 'PR_UseLayer.gdb',
              'VI': 'VI_UseLayer.gdb',
              'Howland': 'Howland_Baker_Jarvis_UseLayer.gdb',
              'Johnston': 'Johnston_UseLayer.gdb',
              'Laysan': 'Laysan_UseLayer.gdb',
              'Mona': 'Mona_UseLayer.gdb',
              'Necker': 'Necker_UseLayer.gdb',
              'Nihoa': 'Nihoa_UseLayer.gdb',
              'NorthwesternHI': 'NortherwesternHI_UseLayer.gdb',
              'Palmyra': 'Palmyra_Kingman_UseLayer.gdb',
              'Wake': 'Wake_UseLayer.gdb'}


# Create a new GDB
def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


# loop through use raster and projections into final regional project
def raster_project(inraster, in_gdb, prj_folder, region_dict, out_gdb_dict, out_folder):
    start_raster = datetime.datetime.now()
    print "\n"
    print inraster
    in_raster = Raster(in_gdb + os.sep + str(inraster))
    region = (raster.split('_'))[0]
    out_gdb_name = out_gdb_dict[region]
    out_gdb = out_folder + os.sep + out_gdb_name
    create_gdb(out_folder, out_gdb_name, out_gdb)
    prj_file = region_dict[region]

    snap_raster = Raster(RegionalProjection_Dict[region])
    arcpy.Delete_management("snap")
    arcpy.MakeRasterLayer_management(snap_raster, "snap")
    arcpy.env.snapRaster = "snap"
    use_extent = snap_raster.extent
    arcpy.env.extent = use_extent

    # extract snap raster for region

    # location prj files
    wgs_coord_file = prj_folder + os.sep + 'WGS_1984.prj'
    nad83_coord_file = prj_folder + os.sep + 'NAD_1983.prj'
    prj_file_path = prj_folder + os.sep + prj_file
    print prj_file_path

    current_raster_dsc = arcpy.Describe(in_raster)
    current_sr = current_raster_dsc.spatialReference
    current_datum = current_sr.GCS.datumName

    # extract spatial information from prj files
    dsc_wgs = arcpy.Describe(wgs_coord_file)
    wgs_coord_sys = dsc_wgs.spatialReference

    dsc_nad83 = arcpy.Describe(nad83_coord_file)
    nad83_coord_sys = dsc_nad83.spatialReference

    dsc_prj = arcpy.Describe(prj_file_path)
    prj_sr = dsc_prj.spatialReference
    prj_datum = prj_sr.GCS.datumName
    prj_name = prj_file.replace('.prj', '')

    arcpy.MakeRasterLayer_management


    if prj_sr == current_sr:
        prj_raster_name = prj_name + "_" + str(inraster)
        prj_raster = out_gdb + os.sep + prj_raster_name
        if not arcpy.Exists(prj_raster):
                print 'Copying {0}'.format(inraster)
                arcpy.CopyRaster_management(in_raster, prj_raster)
        else:
            print str(prj_raster) + " already exists"

    elif prj_datum == "D_WGS_1984":
        if current_datum != "D_WGS_1984":
            raster_other_geo = str(inraster) + "_WGS84"
            out_other_raster = midGBD + os.sep + raster_other_geo

            prj_raster_name = prj_name + "_" + raster_other_geo
            prj_raster = out_gdb + os.sep + prj_raster_name

            if not arcpy.Exists(out_other_raster):
                print 'Projecting {0} into {1}from '.format(inraster, 'WGS 1984')
                arcpy.ProjectRaster_management(in_raster, out_other_raster, wgs_coord_sys)

            if not arcpy.Exists(prj_raster):
                print 'Projecting {0} into {1}'.format(inraster, prj_name)
                arcpy.ProjectRaster_management(out_other_raster, prj_raster,snap_raster)

            else:
                print str(prj_raster_name) + " already exists"
        else:
            prj_raster_name = prj_name + "_" + str(inraster)
            prj_raster = out_gdb + os.sep + prj_raster_name

            if not arcpy.Exists(prj_raster):
                print 'Projecting {0} into {1}'.format(inraster, prj_name)
                arcpy.ProjectRaster_management(in_raster, prj_raster, snap_raster)

            else:
                print str(prj_raster) + " already exists"

    else:
        if prj_datum == "D_North_American_1983":
            if current_datum != "D_North_American_1983":
                raster_other_geo = str(inraster) + "_NAD83"
                out_other_raster = midGBD + os.sep + raster_other_geo

                prj_raster_name = prj_name + "_" + raster_other_geo
                prj_raster = out_gdb + os.sep + prj_raster_name

                if not arcpy.Exists(out_other_raster):
                    print 'Projecting {0} into {1}'.format(inraster, 'NAD83')
                    arcpy.ProjectRaster_management(in_raster, out_other_raster, nad83_coord_sys)

                if not arcpy.Exists(prj_raster):
                    print 'Projecting {0} into {1}'.format(inraster, prj_name)
                    arcpy.ProjectRaster_management(out_other_raster, prj_raster, snap_raster)

                else:
                    print str(prj_raster_name) + " already exists"
            else:
                prj_raster_name = prj_name + "_" + str(inraster)
                prj_raster = out_gdb + os.sep + prj_raster_name

                if not arcpy.Exists(prj_raster):
                    print 'Projecting {0} into {1}'.format(inraster, prj_name)
                    arcpy.ProjectRaster_management(in_raster, prj_raster, snap_raster)

                else:
                    print str(prj_raster) + " already exists"


    print 'Completed projection of {0} in: {1}'.format(prj_name, (datetime.datetime.now() - start_raster))


arcpy.env.overwriteOutput = True  # ## Change this to False if you don't want GDB to be overwritten
arcpy.env.scratchWorkspace = ""

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

arcpy.env.workspace = inGDB
raster_list = arcpy.ListRasters()
for raster in raster_list:

    split_name = raster.split("_")
    if split_name[(len(split_name)-1)]!= 'euc':
        continue
    try:
        raster_project(raster, inGDB, prjFolder, Region_Dict, outGDBdict, outfolder)
    except Exception as error:
            print(error.args[0])

end = datetime.datetime.now()
print "\nEnd Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
