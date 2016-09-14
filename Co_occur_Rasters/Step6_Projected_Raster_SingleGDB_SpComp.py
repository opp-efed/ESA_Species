import os
import datetime
from arcpy.sa import *
import arcpy
import pandas as pd
# Title - Re-projects union raster into projection by region
# in and out location
inGDB = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\Clipped_MaxArea.gdb'
outfolder = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\SpCompRaster_byProjection'
regional_acres_table ='C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Tables\R_ConvertedAcres_SqMiles_1.5625E-03_byregion20160910.csv'
# inGDB = 'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\Critical Habitat\Clipped_MaxArea.gdb'
# outfolder = 'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\Critical Habitat\CH_SpCompRaster_byProjection'

midGBD = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\scratch.gdb'

# projection folder
prjFolder = "C:\Users\Admin\Documents\Jen\Workspace\projections\FinalBE"
# Dictionary of all projections needed for raster and the snap raster
# snap raster must be in desired projection with the desired cell size
# TODO UPDATE SNAP RASTER
RegionalProjection_Dict = {
    'CONUS': r'C:\Users\Admin\Documents\Jen\Workspace\UseSites\Cultivated_Layer\2015_Cultivated_Layer.gdb\cultmask_2015',
    'HI': r'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'AK': r'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\AK_Developed_euc',
    'AS': 'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\AS_OSD_euc',
    'CNMI': 'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\CNMI_OSD_euc',
    'GU': 'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\GU_Developed_euc',
    'PR': 'JC:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonCONUS_Ag_euc_151109.gdb\PR_OtherCrops_euc',
    'VI': 'JC:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonCONUS_Ag_euc_151109.gdb\VI_Ag_euc',
    'Howland': 'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Johnston': 'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Laysan': 'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Mona': 'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Necker': 'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Nihoa': 'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'NorthwesternHI': 'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Palmyra': 'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc',
    'Wake': 'C:\Users\Admin\Documents\Jen\Workspace\EDM_2015\Euclidean\NonAg_euc_151103.gdb\HI_ManagedForests_euc'
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




# Create a new GDB
def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


# loop through use raster and projections into final regional project
def raster_project(prj_current,inraster,in_gdb, prj_folder, out_folder,c_region):
    start_raster = datetime.datetime.now()
    print "\n"
    print inraster
    in_raster = Raster(in_gdb + os.sep + str(inraster))
    prj_name = prj_current.replace('.prj', '')
    out_gdb_name = prj_current.replace('.prj', '.gdb')
    out_gdb_name = out_gdb_name.replace(" ", "_")
    out_gdb = out_folder + os.sep + out_gdb_name
    create_gdb(out_folder, out_gdb_name, out_gdb)

    snap_raster = Raster(RegionalProjection_Dict[c_region])
    arcpy.Delete_management("snap")
    arcpy.MakeRasterLayer_management(snap_raster, "snap")
    arcpy.env.snapRaster = "snap"
    print str(snap_raster)
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
    print prj_datum +'Dataum'

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
            prj_raster_name = str(inraster) + "_" + prj_name
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

def sp_group_in_region(regional_table,c_region ):
    check_extention = (regional_table.split('.'))[1]
    if check_extention == 'xlsx':
        regional_sp_df = pd.read_excel(regional_table)
    else:
        regional_sp_df= pd.read_csv(regional_table)
    region_df = regional_sp_df[['Group',('Acres_'+ str(c_region))]]
    region_df  = region_df.fillna(-88888)  # fills in nan to be filtered out
    filter_region= region_df[region_df[('Acres_'+ str(c_region))].isin([-88888]) == False]
    region_group_df= filter_region['Group']
    region_group_df= region_group_df.drop_duplicates()
    region_list= sorted(region_group_df.values.tolist())
    return region_list


arcpy.env.overwriteOutput = True  # ## Change this to False if you don't want GDB to be overwritten
arcpy.env.scratchWorkspace = ""

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
list_regions = sorted(RegionalProjection_Dict.keys())
arcpy.env.workspace = inGDB
raster_list = arcpy.ListRasters()
for region in list_regions:
    print region
    sp_group_region = sp_group_in_region(regional_acres_table,region)
    print sp_group_region
    regional_prj = Region_Dict[region]
    for raster in raster_list:
        try:
            raster_project(regional_prj, raster,inGDB, prjFolder, outfolder, region)


        except Exception as error:
            print(error.args[0])


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
