import os
import datetime
from arcpy.sa import *
import arcpy
import pandas as pd

# Title - Re-projects union raster into projection by region
# in and out location

# inGDB = 'C:\Users\JConno02\Documents\Projects\ESA\UnionFiles_Winter2018\CriticalHabitat\CH_Raster_Clipped_Union_20180110.gdb'
# outfolder = r'C:\Users\JConno02\Documents\Projects\ESA\UnionFiles_Winter2018\CriticalHabitat\SpCompRaster_byProjection'
# regional_acres_table = 'C:\Users\JConno02\Documents\Projects\ESA\CompositeFiles_Winter2018\CH_Acres_by_region_20180110.csv'
# midGBD = r'C:\Users\JConno02\Documents\Projects\ESA\UnionFiles_Winter2018\CriticalHabitat\temp.gdb'

inGDB = 'C:\Users\JConno02\Documents\Projects\ESA\UnionFiles_Winter2018\Range\R_Raster_Clipped_Union_20180110.gdb'
outfolder = r'C:\Users\JConno02\Documents\Projects\ESA\UnionFiles_Winter2018\Range\SpCompRaster_byProjection'
regional_acres_table = 'C:\Users\JConno02\Documents\Projects\ESA\CompositeFiles_Winter2018\R_Acres_by_region_20180110.csv'
midGBD = r'C:\Users\JConno02\Documents\Projects\ESA\UnionFiles_Winter2018\Range\temp.gdb'


skip_group = []
#Amphibians', 'Arachnids', 'Birds', 'Clams', 'Conifers', 'Corals', 'Crustaceans', 'Ferns', 'Flowering', 'Insects',
# 'Lichens', 'Mammals','Snails'
# projection folder
prjFolder = "C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA" \
            "\_ExternalDrive\projections\FinalBE"
# Dictionary of all projections needed for raster and the snap raster
# snap raster must be in desired projection with the desired cell size
#
skip_region = []

RegionalProjection_Dict = {
    'CONUS': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_cultmask_2016',
    'HI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\NAD_1983_UTM_Zone_4N_HI_Ag',
    'AK': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_Albers_AK_Ag',
    'AS': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_2S_AS_Ag',
    'CNMI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_CNMI_Ag',
    'GU': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_GU_Ag_30',
    'PR': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_PR_Ag',
    'VI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_20N_VI_Ag_30'}

# Had to shorted the file name fo the PR prj file in order to me file path charater limits
# TODO can the snap raster be used as the spatial reference for the projection?
Region_Dict = {'CONUS': 'Albers_Conical_Equal_Area.prj',
               'HI': 'NAD_1983_UTM_Zone__4N.prj',
               'AK': 'WGS_1984_Albers.prj',
               'AS': 'WGS_1984_UTM_Zone__2S.prj',
               'CNMI': 'WGS_1984_UTM_Zone_55N.prj',
               'GU': 'WGS_1984_UTM_Zone_55N.prj',
               'PR': 'Albers_Conical_Equal_Area.prj',
               'VI': 'WGS_1984_UTM_Zone_20N.prj',
               }


# Create a new GDB
def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


# loop through use raster and projections into final regional project
def raster_project(prj_current, inraster, in_gdb, prj_folder, out_folder, c_region):
    start_raster = datetime.datetime.now()
    print "\n"
    print inraster

    in_raster = Raster(in_gdb + os.sep + str(inraster))

    prj_name = prj_current.replace('.prj', '')
    out_gdb_name = prj_current.replace('.prj', '.gdb')
    out_gdb_name = c_region + "_" + out_gdb_name.replace(" ", "_")
    out_gdb = out_folder + os.sep + out_gdb_name
    create_gdb(out_folder, out_gdb_name, out_gdb)

    snap_raster = Raster(RegionalProjection_Dict[c_region])
    arcpy.Delete_management("snap")
    arcpy.MakeRasterLayer_management(snap_raster, "snap")
    arcpy.env.snapRaster = "snap"
    print str(snap_raster)

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

        print 'Completed projection of {0} in: {1}\n'.format(prj_name, (datetime.datetime.now() - start_raster))
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
print raster_list
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
            if sp_group in skip_group:
                continue

            elif sp_group not in final_sp_group_region:
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
