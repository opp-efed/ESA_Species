import os
import datetime
from arcpy.sa import *
import arcpy
# Title - Re-projects union raster into projection by region
# in and out location
inGDB = 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb'
outfolder = 'J:\Workspace\UseSites\ByProject'

midGBD = 'J:\Workspace\UseSites\scratch.gdb'

# projection folder
prjFolder = "J:\Workspace\projections\FinalBE"
# Dictionary of all projections needed for raster and the snap raster
# snap raster must be in desired projection with the desired cell size
# TODO UPDATE SNAP RASTER
RegionalProjection_Dict = {
    'CONUS': r'J:\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.gdb\cultmask_2015',
    'HI': r'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'AK': r'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\AK_Developed_euc',
    'AS': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\AS_OSD_euc',
    'CNMI': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\CNMI_OSD_euc',
    'GU': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\GU_Developed_euc',
    'PR': 'J:\Workspace\EDM_2015\Euclidean\NonCONUS_Ag_euc_151109.gdb\PR_OtherCrops_euc',
    'VI': 'J:\Workspace\EDM_2015\Euclidean\NonCONUS_Ag_euc_151109.gdb\VI_Ag_euc',
    'Howland': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Johnston': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Laysan': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Mona': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Necker': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Nihoa': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'NorthwesternHI': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Palmyra': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Wake': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc'
}

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
                arcpy.ProjectRaster_management(out_other_raster, prj_raster, prj_file_path)

            else:
                print str(prj_raster_name) + " already exists"
        else:
            prj_raster_name = prj_name + "_" + str(inraster)
            prj_raster = out_gdb + os.sep + prj_raster_name

            if not arcpy.Exists(prj_raster):
                print 'Projecting {0} into {1}'.format(inraster, prj_name)
                arcpy.ProjectRaster_management(in_raster, prj_raster, prj_file_path)

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
                    arcpy.ProjectRaster_management(out_other_raster, prj_raster, prj_file_path)

                else:
                    print str(prj_raster_name) + " already exists"
            else:
                prj_raster_name = prj_name + "_" + str(inraster)
                prj_raster = out_gdb + os.sep + prj_raster_name

                if not arcpy.Exists(prj_raster):
                    print 'Projecting {0} into {1}'.format(inraster, prj_name)
                    arcpy.ProjectRaster_management(in_raster, prj_raster, prj_file_path)

                else:
                    print str(prj_raster) + " already exists"
        else:
            if current_sr == prj_sr:
                prj_raster_name = prj_name + "_" + str(inraster)
                prj_raster = out_gdb + os.sep + prj_raster_name

                if not arcpy.Exists(prj_raster):
                    print 'Projecting {0} into {1}'.format(inraster, prj_name)
                    arcpy.ProjectRaster_management(in_raster, prj_raster, prj_file_path)

                else:
                    print str(prj_raster) + " already exists"
            else:
                print "Could not project {0} into {1}".format(inraster, prj_name)
    print 'Completed projection of {0} in: {1}'.format(prj_name, (datetime.datetime.now() - start_raster))


arcpy.env.overwriteOutput = True  # ## Change this to False if you don't want GDB to be overwritten
arcpy.env.scratchWorkspace = ""

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

arcpy.env.workspace = inGDB
raster_list = arcpy.ListRasters()
for raster in raster_list:
    try:
        raster_project(raster, inGDB, prjFolder, Region_Dict, outGDBdict, outfolder)
    except Exception as error:
            print(error.args[0])

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
