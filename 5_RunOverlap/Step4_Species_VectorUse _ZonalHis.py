import os
import datetime
import pandas as pd
import arcpy
from arcpy.sa import *


# Title- Co-occur analysis for vector uses
# ##User input variables
# # location of use site to runt
# in_location_use =r'L:\Workspace\UseSites\CDL_Reclass\CDL_reclass_2010_2015.gdb'
#TODO is the best way to clip or to extrct by maske the area within the zone raster; add a col to raster that
# applies a zero to ID vector use  then run zone histogram
# TODO can you generate euclidean distance from vector file
inlocation_species =r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\SpCompRaster_byProjection\Grids_byProjection\Albers_Conical_Equal_Area'
Range = True
temp_file = "temp_table4"
region = "CONUS"

use_location_base = 'L:\Workspace\UseSites\ByProject'
use_location = use_location_base + os.sep + str(region) + "_UseLayer.gdb"

#use_list =[ u'Albers_Conical_Equal_Area_CONUS_TribalLands_euc_final']
arcpy.env.workspace = use_location
use_list = (arcpy.ListFeatureClasses())


if Range:

    out_results = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\test'
else:

    out_results = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\L48\Agg_layers\NonAg\CriticalHabitat'


current_use = 1
count_use = len(use_list)
print 'Uses that will be run {0}'.format(use_list)




# recursively checks workspaces found within the inFileLocation and makes list of all feature class
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


# Create a new GDB
def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")

def vector_overlap(zone_file, usepath, ):
    print "Run to " + str(runID)
    arcpy.env.overwriteOutput = True
    start_loop = datetime.datetime.now()
    out_array = arcpy.da.TableToNumPyArray(zone_file, ['ZoneID'])
    att_df = pd.DataFrame(data = out_array)
    att_df = att_df.reindex(columns=['ZoneID', 'CoOccur_Acres'])
    with arcpy.da.SearchCursor(zone_file, ('ZoneID')) as clipper:
        for rcrd in clipper:
            ent = rcrd[0]
            where = "ZoneID = '%s'" % ent
            MakeRasterLayer_management(Raster(zone_file,"zone",where)
            arcpy.MakeFeatureLayer_management(usepath, "use")
            with arcpy.da.SearchCursor(usepath, ('intervalID')) as interval_loop:


            env.workspace = outpath
            in_features = usepath
            clip_features = lyr
            out_feature_class = outpath + os.sep + lyr
            xy_tolerance = ""
            print "Clipping"
            arcpy.Clip_analysis(in_features, "zone", out_feature_class, xy_tolerance)
            arcpy.AddField_management(out_feature_class, "Acres", "DOUBLE", "#", "#", "#", "#", "NULLABLE",
                                      "NON_REQUIRED", "#")
            print "Calculating Acres"
            arcpy.CalculateField_management(out_feature_class, "Acres", "!shape.area@acres!", "PYTHON_9.3", "#")
            with arcpy.da.SearchCursor(out_feature_class, ("Acres")) as cursor:
                total_acres = 0
                for row in cursor:
                    acres = row[0]
                    total_acres = total_acres + acres

            arcpy.AddMessage("Data transfer...")
            with arcpy.da.UpdateCursor(fc, ("EntityID", "CoOccur_Acres")) as cursor:
                for row in cursor:
                    if row[0] != ent:
                        continue
                    else:
                        row[1] = total_acres

                        cursor.updateRow(row)
                del row, cursor
    print "Run to " + str(runID)

    outFC = outpath_final + os.sep + runID
    desc = arcpy.Describe(fc)
    filepath = desc.catalogPath
    print outFC
    if not arcpy.Exists(outFC):
        arcpy.Copy_management(filepath, outFC)
        print "Exported: " + str(outFC)

    print outFC_use
    if not arcpy.Exists(outFC_use):
        arcpy.Copy_management(filepath, outFC_use)
        print "Exported: " + str(outFC_use)

    with arcpy.da.UpdateCursor(fc, ("CoOccur_Acres")) as cursor:
        for row in cursor:
            if row[0] > -2:
                row[0] = None
                cursor.updateRow(row)
        del row, cursor

    del use
    print "Loop completed in: {0}".format(datetime.datetime.now() - start_loop)

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

count = 0
if inlocation_species[-3:] != 'gdb':
    path, gdb = os.path.split(inlocation_species)
    arcpy.env.workspace = inlocation_species
    count_sp = len(arcpy.ListRasters())
    count = 0
    list_raster = (arcpy.ListRasters())
    for raster_in in list_raster:
        count += 1
        in_sp = inlocation_species + os.sep + raster_in

        print raster_in
        raster_file = Raster(in_sp)
        print "\nWorking on uses for {0} species file {1} of {2}".format(raster_in, count, count_sp)
        # loops through all use raster to be included
        for use_nm in use_list:
            run_id = str(raster_in) + '_' + str(use_nm)
            use_path = use_location + os.sep + use_nm
            print 'Starting use layer {0}, use {1} of {2}'.format(use_path, current_use, count_use)







end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)


# import os
# import datetime
# import pandas as pd
# import arcpy
# from arcpy.sa import *
#
#
# # Title- runs Zonal Histogram against specified use sites to the HUC12 file bu HUC2; Generates raw results
#
# # TODO generate list of raster in the in_location_use gdb to be used as the input for the use_included list
#
# # ##User input variables
# # # location of use site to runt
# # in_location_use =r'L:\Workspace\UseSites\CDL_Reclass\CDL_reclass_2010_2015.gdb'
# inlocation_species =r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\SpCompRaster_byProjection\Grids_byProjection\Albers_Conical_Equal_Area'
# Range = True
# temp_file = "temp_table4"
# region = "CONUS"
#
# use_location_base = 'L:\Workspace\UseSites\ByProject'
# in_location_use= use_location_base + os.sep + str(region) + "_UseLayer.gdb"
#
# #use_list =[ u'Albers_Conical_Equal_Area_CONUS_TribalLands_euc_final']
# arcpy.env.workspace = in_location_use
# use_list = (arcpy.ListFeatureClasses())
#
#
# if Range:
#
#     out_results = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\test'
# else:
#
#     out_results = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\L48\Agg_layers\NonAg\CriticalHabitat'
#
#
# current_use = 1
# count_use = len(use_list)
# print 'Uses that will be run {0}'.format(use_list)
#
#
# symbology_dict = {
#     'CONUS': ['L:\Workspace\UseSites\ByProject\SymblogyLayers\Albers_Conical_Equal_Area_CONUS_CDL_1015_100x2_euc.lyr',
#               r'L:\Workspace\UseSites\ByProject\SymblogyLayers\CDL_2013_rec.lyr'],
#     'HI': r'L:\Workspace\UseSites\ByProject\SymblogyLayers\NAD_1983_UTM_Zone__4N_HI_Ag_euc.lyr',
#     'AK': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_Albers_AK_CattleEarTag_euc.lyr',
#     'AS': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_UTM_Zone__2S_AS_Ag_euc.lyr',
#     'CNMI': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_UTM_Zone_55N_CNMI_Ag_euc.lyr',
#     'GU': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_UTM_Zone_55N_GU_Ag_euc.lyr',
#     'PR': 'L:\Workspace\UseSites\ByProject\SymblogyLayers'
#           '\NAD_1983_StatePlane_Puerto_Rico_Virgin_Isl_FIPS_5200_PR_Ag_euc.lyr',
#     'VI': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_UTM_Zone_20N_VI_Ag_euc.lyr'}
#
#
# def zone(zone_sp, zone_raster, temp_table, ):
#     start_zone = datetime.datetime.now()
#     arcpy.CreateTable_management("in_memory", temp_table)
#     temp = "in_memory" + os.sep + temp_table
#     arcpy.env.overwriteOutput = True
#     arcpy.gp.ZonalHistogram_sa(zone_sp, "HUC_12", zone_raster, temp)
#     print "Completed Zonal Histogram"
#     return temp, start_zone
#
#
# # Run Zonal Histogram
# def zonal_hist(in_zone_data, in_value_raster, set_raster_symbol, use_name, results_folder, huc12, temp_table, region_c):
#
#     # out paths
#     break_use = use_path.split("_")
#     break_bool = False
#     use_nm_folder = region_c  # starting point that will be used for use_nm_folder
#
#     for v in break_use:
#         if v != region_c:
#             pass
#         else:
#             break_bool = True
#         if break_bool:
#             if v == region_c:
#                 continue
#             else:
#                 use_nm_folder = use_nm_folder + "_" + v
#
#     # parse out information needed for file names
#     huc_12_value = huc12.replace('NHDPlus', '_')
#     run_id = use_nm_folder + huc_12_value
#
#     csv = results_folder + os.sep + run_id + '.csv'
#     if os.path.exists(csv):
#         print ("Already completed run for {0}".format(run_id))
#     elif not os.path.exists(csv):
#         print ("Running Statistics...for species group {0} and raster {1}".format(huc_12_value, use_name))
#         arcpy.CheckOutExtension("Spatial")
#         arcpy.Delete_management("rd_lyr")
#         arcpy.Delete_management("HUC12_lyr")
#         arcpy.MakeFeatureLayer_management(in_zone_data, "HUC12_lyr")
#         arcpy.MakeRasterLayer_management(Raster(in_value_raster), "rd_lyr")
#         arcpy.ApplySymbologyFromLayer_management("rd_lyr", set_raster_symbol)
#         temp_return, zone_time = zone("HUC12_lyr", "rd_lyr", temp_table)
#
#         list_fields = [f.name for f in arcpy.ListFields(temp_return)]
#         att_array = arcpy.da.TableToNumPyArray(temp_return, list_fields)
#         att_df = pd.DataFrame(data=att_array)
#         att_df['LABEL'] = att_df['LABEL'].map(lambda x: x).astype(str)
#         att_df.to_csv(csv)
#         print 'Final file can be found at {0}'.format(csv)
#         # arcpy.TableToTable_conversion(temp_return, outpath_final, dbf)
#         arcpy.Delete_management("in_memory\\temp_table")
#
#     print "Completed in {0}".format((datetime.datetime.now() - zone_time))
#
#
# start_time = datetime.datetime.now()
# print "Start Time: " + start_time.ctime()
#
# count = 0
# if inlocation_species[-3:] != 'gdb':
#     path, gdb = os.path.split(inlocation_species)
#     arcpy.env.workspace = inlocation_species
#     count_sp = len(arcpy.ListRasters())
#     count = 0
#     list_raster = (arcpy.ListRasters())
#     for raster_in in list_raster:
#         count += 1
#         in_sp = inlocation_species + os.sep + raster_in
#
#         print raster_in
#         raster_file = Raster(in_sp)
#         print "\nWorking on uses for {0} species file {1} of {2}".format(raster_in, count, count_sp)
#         for use_nm in use_list:  # loops through all use raster to be included
#
#     use_path = in_location_use + os.sep + use_nm
#     count = 1
#     split_use_nm = use_nm.split("_")
#     symbology_flag = split_use_nm[(len(split_use_nm) - 1)]
#     if symbology_flag == 'euc':
#         symbologyLayer = symbology_list[0]
#     else:
#         symbologyLayer = symbology_list[1]
#     for value in list_HUC2:  # loop all NHD folders for each use
#         HUC12_path = in_HUC_base + os.sep + value + os.sep + tail
#         out_folder = out_results + os.sep + use_nm
#         if not os.path.exists(out_folder):
#             os.mkdir(out_folder)
#
#         print "\nWorking on uses for {0} HUC file {1} of {2}".format(value, count, len(list_HUC2))
#         try:
#
#             zonal_hist(HUC12_path, use_path, symbologyLayer, use_nm, out_folder, value, temp_file, region)
#         except Exception as error:
#             print(error.args[0])
#             print "Failed on {0} with use {1}".format(value, use_nm)
#         count += 1
#     current_use += 1
#
# end = datetime.datetime.now()
# print "End Time: " + end.ctime()
# elapsed = end - start_time
# print "Elapsed  Time: " + str(elapsed)
