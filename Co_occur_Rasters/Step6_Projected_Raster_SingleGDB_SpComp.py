import os
import datetime
from arcpy.sa import *
import arcpy
import pandas as pd

# Title - Re-projects union raster into projection by region
# in and out location

inGDB = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\Clipped_MaxArea.gdb'
outfolder = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\SpCompRaster_byProjection\New Folder'
regional_acres_table = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Tables\R_ConvertedAcres_SqMiles_1.5625E-03_byregion20160910.csv'
midGBD = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\scratch.gdb'

# inGDB = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\Clipped_MaxArea.gdb'
# outfolder = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\CH_SpCompRaster_byProjection'
# regional_acres_table = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Tables\CH_ConvertedAcres_SqMiles_1.5625E-03_byregion20160910.csv'
# midGBD = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\scratch.gdb'

# projection folder
prjFolder = "C:\Users\Admin\Documents\Jen\Workspace\projections\FinalBE"
# Dictionary of all projections needed for raster and the snap raster
# snap raster must be in desired projection with the desired cell size

skip_region = ['CONUS','HI','AK','AS','CNMI','PR','VI']

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
               'PR': 'StatePlane_Puerto_Rico.prj',
               'VI': 'WGS_1984_UTM_Zone_20N.prj'
               }
# Regions outside of use extent
# 'Howland_Baker_Jarvis': 'NAD_1983_Albers.prj',
# 'Johnston': 'NAD_1983_Albers.prj',
# 'Laysan': 'NAD_1983_Albers.prj',
# 'Mona': 'NAD_1983_Albers.prj',
# 'Necker': 'NAD_1983_Albers.prj',
# 'Nihoa': 'NAD_1983_Albers.prj',
# 'NorthwesternHI': 'NAD_1983_Albers.prj',
# 'Palmyra_Kingman': 'NAD_1983_Albers.prj',
# 'Wake': 'NAD_1983_Albers.prj'
# Create a new GDB
def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


# loop through use raster and projections into final regional project
def raster_project(prj_current, inraster, in_gdb, prj_folder, out_folder, c_region, ):
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
            arcpy.MakeRasterLayer_management(in_raster, "inital_r_lyr")

            raster_other_geo = str(inraster) + "_WGS84"
            out_other_raster = midGBD + os.sep + raster_other_geo

            prj_raster_name = raster_other_geo + "_" + prj_name
            prj_raster = out_gdb + os.sep + prj_raster_name

            if not arcpy.Exists(out_other_raster):
                print 'Projecting {0} into {1}'.format(inraster, 'WGS 1984')
                arcpy.ProjectRaster_management("inital_r_lyr", out_other_raster, wgs_coord_sys)

            if not arcpy.Exists(prj_raster):
                arcpy.Delete_management("WGS_lyr")
                arcpy.MakeRasterLayer_management(out_other_raster, "WGS_lyr", "#", snap_raster, '#')
                print 'Projecting {0} into {1}'.format(inraster, prj_name)
                arcpy.ProjectRaster_management("WGS_lyr", prj_raster, snap_raster)

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
                arcpy.ProjectRaster_management("inital_r_lyr", prj_raster, snap_raster)

            else:
                print str(prj_raster) + " already exists"

        print 'Completed projection of {0} in: {1}'.format(prj_name, (datetime.datetime.now() - start_raster))
    except Exception as error:
        print 'Error in loop'
        print(error.args[0])


def sp_group_in_region(regional_table, c_region):
    check_extention = (regional_table.split('.'))[1]
    if check_extention == 'xlsx':
        regional_sp_df = pd.read_excel(regional_table)
    else:
        regional_sp_df = pd.read_csv(regional_table)
    region_df = regional_sp_df[['Group', ('Acres_' + str(c_region))]]
    region_df = region_df.fillna(-88888)  # fills in nan to be filtered out
    filter_region = region_df[region_df[('Acres_' + str(c_region))].isin([-88888]) == False]
    region_group_df = filter_region['Group']
    region_group_df = region_group_df.drop_duplicates()
    region_list = sorted(region_group_df.values.tolist())
    return region_list


arcpy.env.overwriteOutput = True  # ## Change this to False if you don't want GDB to be overwritten
arcpy.env.scratchWorkspace = ""

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
list_regions = sorted(RegionalProjection_Dict.keys())
arcpy.env.workspace = inGDB
raster_list = arcpy.ListRasters()
for region in list_regions:
    if region in skip_region:
        continue
    else:
        print region
        sp_group_region = sp_group_in_region(regional_acres_table, region)
        final_sp_group_region = []
        for v in sp_group_region:
            split_group = v.split(" ")
            final_sp_group_region.append(split_group[0])


        print final_sp_group_region
        regional_prj = Region_Dict[region]
        for raster in raster_list:
            sp_group = (raster.split)("_")[1]
            if sp_group not in final_sp_group_region:
                continue
            else:
                try:
                    raster_project(regional_prj, raster, inGDB, prjFolder, outfolder, region)
                except Exception as error:
                    print(error.args[0])

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
