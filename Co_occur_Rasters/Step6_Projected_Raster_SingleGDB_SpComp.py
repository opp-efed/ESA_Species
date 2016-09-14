import os
import datetime
from arcpy.sa import *
import arcpy
# Title - Re-projects union raster into projection by region
# in and out location
inGDB = 'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\Range\Clipped_MaxArea.gdb'
outfolder = 'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\Range\SpCompRaster_byProjection'

# inGDB = 'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\Critical Habitat\Clipped_MaxArea.gdb'
# outfolder = 'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\Critical Habitat\CH_SpCompRaster_byProjection'

midGBD = 'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\Critical Habitat\scratch.gdb'

# projection folder
prjFolder = "J:\Workspace\projections\FinalBE"
# Dictionary of all projections needed for raster and the snap raster
# snap raster must be in desired projection with the desired cell size
# TODO UPDATE SNAP RASTER
RegionalProjection_Dict = {
    'Albers_Conical_Equal_Area.prj': r'J:\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.gdb\cultmask_2015',
    'NAD 1983 UTM Zone  4N.prj': r'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'WGS_1984_Albers.prj': r'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\AK_Developed_euc',
    'WGS 1984 UTM Zone  2S.prj': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\AS_OSD_euc',
    'WGS 1984 UTM Zone 55N.prj': 'J:\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\GU_Developed_euc',
    'NAD 1983 StatePlane Puerto Rico Virgin Isl FIPS 5200 (Meters).prj': 'J:\Workspace\EDM_2015\Euclidean\NonCONUS_Ag_euc_151109.gdb\PR_OtherCrops_euc',
    'NAD_1983_Albers.prj': 'J:\Workspace\EDM_2015\Euclidean\NonCONUS_Ag_euc_151109.gdb\VI_Ag_euc',
}

Region_list = ['Albers_Conical_Equal_Area.prj',
               'NAD_1983_UTM_Zone__4N.prj',
               'WGS_1984_Albers.prj',
               'WGS_1984_UTM_Zone__2S.prj',
               'WGS_1984_UTM_Zone_55N.prj',
               'NAD_1983_StatePlane_Puerto_Rico_Virgin_Isl_FIPS_5200.prj',
               'WGS_1984_UTM_Zone_20N.prj',
               'NAD_1983_Albers.prj'

               ]


# Create a new GDB
def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


# loop through use raster and projections into final regional project
def raster_project(prj_current, in_gdb, prj_folder, out_folder):
    arcpy.env.workspace = in_gdb
    raster_list = arcpy.ListRasters()

    for inraster in raster_list:
        start_raster = datetime.datetime.now()
        print "\n"
        print inraster
        in_raster = Raster(in_gdb + os.sep + str(inraster))
        prj_name = prj_current.replace('.prj', '')
        out_gdb_name = prj.replace('.prj', '.gdb')
        out_gdb_name = out_gdb_name.replace(" ", "_")
        out_gdb = out_folder + os.sep + out_gdb_name
        create_gdb(out_folder, out_gdb_name, out_gdb)

        snap_raster = Raster(RegionalProjection_Dict[prj_current])
        arcpy.Delete_management("snap")
        arcpy.MakeRasterLayer_management(snap_raster, "snap")
        arcpy.env.snapRaster = "snap"
        print snap_raster
        use_extent = snap_raster.extent
        arcpy.env.extent = use_extent

        # extract snap raster for region

        # location prj files
        wgs_coord_file = prj_folder + os.sep + 'WGS_1984.prj'
        prj_file_path = prj_folder + os.sep + prj_current
        print prj_file_path

        # extract spatial information from prj files
        dsc_wgs = arcpy.Describe(wgs_coord_file)
        wgs_coord_sys = dsc_wgs.spatialReference

        dsc_prj = arcpy.Describe(prj_file_path)
        prj_sr = dsc_prj.spatialReference
        prj_datum = prj_sr.GCS.datumName

        print prj_datum
        try:

            if prj_datum == "D_WGS_1984":

                raster_other_geo = str(inraster) + "_WGS84"
                out_other_raster = midGBD + os.sep + raster_other_geo

                prj_raster_name = raster_other_geo + "_" + prj_name
                prj_raster = out_gdb + os.sep + prj_raster_name

                if not arcpy.Exists(out_other_raster):
                    print 'Projecting {0} into {1}'.format(inraster, 'WGS 1984')
                    arcpy.ProjectRaster_management(in_raster, out_other_raster, wgs_coord_sys)

                if not arcpy.Exists(prj_raster):
                    print 'Projecting {0} into {1}'.format(inraster, prj_name)
                    arcpy.ProjectRaster_management(out_other_raster, prj_raster, prj_file_path)

                else:
                    print str(prj_raster_name) + " already exists"

            else:
                prj_raster_name = str(inraster)+ "_" + prj_name
                prj_raster = out_gdb + os.sep + prj_raster_name

                if not arcpy.Exists(prj_raster):
                    print 'Projecting {0} into {1}'.format(inraster, prj_name)
                    arcpy.ProjectRaster_management(in_raster, prj_raster, prj_file_path)

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

for prj in Region_list:
    try:
        raster_project(prj, inGDB, prjFolder, outfolder)
    except Exception as error:
            print(error.args[0])


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
