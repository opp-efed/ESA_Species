import os
import datetime
from arcpy.sa import *
import arcpy
import pandas as pd

# Title - Re-projects union raster into projection by region - if geographic conversion needs to be done (NAD to WGS_
# NAD_1983_To_WGS_1984_1" is used
# in and out location

# LOCATION OF RASTER UNION COMPOSITES BY SPECIES GROUP
inGDB = 'F:\Union Composites_ESA\Spring 2018\Range\R_Raster_Clipped_Union_CntyInter_HUC2ABInter_20180612.gdb'
outfolder = r'F:\Union Composites_ESA\Spring 2018\Range\test'
regional_acres_table = r'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
                       r'\Endangered Species Pilot Assessments - OverlapTables\R_Acres_Pixels_20180428.csv'

# 'r'C:\Users\JConno02\Environmental Protection Agency (EPA)
# \Endangered Species Pilot Assessments - OverlapTableA\R_Acres_Pixels_20180428.csv'


# 'CONUS', 'AK', 'AS',
# 'PR','AK', 'HI','AS','CNMI','GU','VI'
# ToDO see why PR species are causing python to quit if extent is set; to avoid remove extent limit line 84, 85, the
# when output will be all species but it will be limited to just PR when the next step of raster to the format
skip_region = ['AK', 'AS', 'CONUS']

# 'Amphibians', 'Birds', 'Clams', 'Conifers', 'Crustaceans', 'Ferns', 'Fishes', 'Insects', 'Lichens', 'Mammals', 'Reptiles', 'Snails']
skip_group = []

# projection folder
# prjFolder = r'L:\Workspace\StreamLine\projections\FinalBE'
prjFolder = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ED_results\projections'

# Also use as snap rasters when projecting with will set the extent of the output projected raster to just the region
# RegionalProjection_Dict = {
#     'CONUS': r'L:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_cultmask_2016',
#     'HI': r'L:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\NAD_1983_UTM_Zone_4N_HI_Ag',
#     'AK': r'L:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\WGS_1984_Albers_AK_Ag',
#     'AS': r'L:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_2S_AS_Ag',
#     'CNMI': r'L:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_CNMI_Ag',
#     'GU': r'L:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_GU_Ag_30',
#     'PR': r'L:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_PR_Ag',
#     'VI': r'L:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_20N_VI_Ag_30'}

RegionalProjection_Dict = {
    'CONUS': r'F:\UseSite_ESA\Feb 2019\SnapRasters.gdb\Albers_Conical_Equal_Area_cultmask_2016',
    'HI': r'F:\UseSite_ESA\Feb 2019\SnapRasters.gdb\NAD_1983_UTM_Zone_4N_HI_Ag',
    'AK': r'F:\UseSite_ESA\Feb 2019\SnapRasters.gdb\WGS_1984_Albers_AK_Ag',
    'AS': r'F:\UseSite_ESA\Feb 2019\SnapRasters.gdb\WGS_1984_UTM_Zone_2S_AS_Ag',
    'CNMI': r'F:\UseSite_ESA\Feb 2019\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_CNMI_Ag',
    'GU': r'F:\UseSite_ESA\Feb 2019\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_GU_Ag_30',
    'PR': r'F:\UseSite_ESA\Feb 2019\SnapRasters.gdb\Albers_Conical_Equal_Area_PR_Ag',
    'VI': r'F:\UseSite_ESA\Feb 2019\SnapRasters.gdb\WGS_1984_UTM_Zone_20N_VI_Ag_30'}

# Had to shorted the file name fo the PR prj file in order to me file path charater limits
# TODO can the snap raster be used as the spatial reference for the projection?
Region_Dict = {'CONUS': 'Albers_Conical_Equal_Area.prj',
               'HI': 'NAD_1983_UTM_Zone_4N.prj',
               'AK': 'WGS_1984_Albers.prj',
               'AS': 'WGS_1984_UTM_Zone_2S.prj',
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
    # Resets arcpy environment variables to default
    arcpy.ResetEnvironments()
    print "\n"
    print inraster

    # in_raster = Raster(in_gdb + os.sep + str(inraster))
    in_raster = in_gdb + os.sep + str(inraster)

    prj_name = prj_current.replace('.prj', '')
    out_gdb_name = prj_current.replace('.prj', '.gdb')
    out_gdb_name = c_region + "_" + out_gdb_name.replace(" ", "_")
    out_gdb = out_folder + os.sep + out_gdb_name
    create_gdb(out_folder, out_gdb_name, out_gdb)

    snap_raster = Raster(RegionalProjection_Dict[c_region])
    arcpy.Delete_management("snap")
    print RegionalProjection_Dict[c_region]
    arcpy.MakeRasterLayer_management(snap_raster, "snap", "#", "#", "#")
    arcpy.env.snapRaster = "snap"
    print str(snap_raster)
    # Set the processing extent to be equal to the use layer; only species within the extent will be
    # included in the output species file; applied when the we make the raster layers
    myExtent = snap_raster.extent

    # location prj files
    prj_file_path = prj_folder + os.sep + prj_current

    # extract spatial information from prj files

    dsc_prj = arcpy.Describe(prj_file_path)
    prj_sr = dsc_prj.spatialReference
    prj_datum = prj_sr.GCS.datumName
    print in_raster
    prj_raster_name = str(inraster) + "_" + prj_name  # regional species raster
    prj_raster = out_gdb + os.sep + prj_raster_name  # complete output path for regional species raster
    print prj_raster
    try:
        if prj_datum == "D_WGS_1984":  # # indicates the file needs a geographic tranformation from  NAD 83 to WGS 84
            if not arcpy.Exists(prj_raster):

                arcpy.MakeRasterLayer_management(in_raster, "inital_r_lyr", "#", myExtent, "#")
                print 'Projecting {0} into {1}'.format(inraster, prj_name)
                # "NAD_1983_To_WGS_1984_1" is a geographic transformation used to go from NAD_1983 to WGS 84 for the US
                # TODO check to see if different transformation would make more sense WGS_1984_(ITRF00)_To_NAD_1983
                arcpy.ProjectRaster_management("inital_r_lyr", prj_raster, prj_sr, "NEAREST", "30",
                                               "NAD_1983_To_WGS_1984_1")
                arcpy.Delete_management("inital_r_lyr")

            else:
                print str(prj_raster) + " already exists"

        else:

            if not arcpy.Exists(prj_raster):
                print in_gdb + os.sep + str(inraster)
                arcpy.MakeRasterLayer_management(in_raster, "inital_r_lyr", "#", myExtent, "#")
                print 'Projecting {0} into {1}'.format(inraster, prj_name)
                arcpy.ProjectRaster_management("inital_r_lyr", prj_raster, prj_sr, 'NEAREST', "30")
                arcpy.Delete_management("inital_r_lyr")

            else:
                print str(prj_raster) + " already exists"
        print 'Completed loop of {0} in: {1}\n'.format(prj_name, (datetime.datetime.now() - start_raster))

    except Exception as error:
        print 'Error in loop'
        print(error.args[0])


# Identifies the species groups found in each the region; only the species files for the groups in the regions need to
# be projected
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
raster_list = [u'R_Insects_Union_Final_20180110_ClippedRegions_20180110_20180619',
               u'R_Lichens_Union_Final_20180110_ClippedRegions_20180110_20180619',
               u'R_Mammals_Union_Final_20180110_ClippedRegions_20180110_20180619',
               u'R_Reptiles_Union_Final_20180110_ClippedRegions_20180110_20180619',
               u'R_Amphibians_Union_Final_20180110_ClippedRegions_20180110_20180619',
               u'R_Birds_Union_Final_20180110_ClippedRegions_20180110_20180619',
               u'R_Clams_Union_Final_20180110_ClippedRegions_20180110_20180619',
               u'R_Conifers_and_Cycads_Union_Final_20180110_ClippedRegions_20180110_20180619',
               u'R_Crustaceans_Union_Final_20180110_ClippedRegions_20180110_20180619',
               u'R_Ferns_and_Allies_Union_Final_20180110_ClippedRegions_20180110_20180619',
               u'R_Fishes_Union_Final_20180110_ClippedRegions_20180110_20180619',
               u'R_Snails_Union_Final_20180110_ClippedRegions_20180110_20180619',
               u'R_Flowering_Plants_Union_Final_20180110_ClippedRegions_20180110_20180619']
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

        print final_sp_group_region  # list of species groups found in region; only include first work if there is a
        # "_" in the species group name ie Flowering_Plants is loaded as Flowering
        regional_prj = Region_Dict[region]

        for raster in raster_list:
            sp_group = (raster.split)("_")[1]

            if sp_group in skip_group:
                continue
            elif sp_group not in final_sp_group_region:
                continue
            else:
                # try:

                raster_project(regional_prj, raster, inGDB, prjFolder, outfolder, region)
            # except Exception as error:
            #     print(error.args[0])

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
