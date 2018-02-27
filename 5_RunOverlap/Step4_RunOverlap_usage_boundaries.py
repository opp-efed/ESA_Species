import os
import datetime
import pandas as pd
import arcpy
from arcpy.sa import *


# Title- Runs overlap using Zonal Histogram for all FEATURE to RASTER analyses including:
#           1) NHD HUC12s features to aggregated layers, AA, Ag and NonAG
#           2) NHD HUC12s features to non euc distance individual years of the CDL

# ##User input variables


# sub-directory folder where shapefile
in_sum_file = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              r'\_ExternalDrive\_CurrentSpeciesSpatialFiles\Boundaries.gdb\Counties_all_overlap_albers'
region = 'CONUS'
temp_file = 'table_1'

# # location of use site to runt
use_location = r"L:\Workspace\UseSites\CDL_Reclass\161031\CDL_Reclass_1015_161031.gdb"
use_location = 'L:\Workspace\_MovedOneDrive\UseSites\ByProject\Diaz_Moasics\CONUS_UseLayer.gdb'
arcpy.env.workspace = use_location

use_list = (arcpy.ListRasters())

use_list =[u'Albers_Conical_Equal_Area_CONUS_Nurseries_euc', u'Albers_Conical_Equal_Area_CONUS_Cultivated_2015_euc',
          u'Albers_Conical_Equal_Area_CONUS_CDL_1015_60x2_euc',
          u'Albers_Conical_Equal_Area_CONUS_CDL_1015_70x2_euc', u'Albers_Conical_Equal_Area_CONUS_Diazinon_euc']  # runs specified layers in use location

# location of results
out_results = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              r'\_ExternalDrive\_CurrentResults\Results_PolBoundaries\Agg_layers'


# STATIC Variables
# Symbology layer so that the unique values can be applied to use layer before running zonal stats

symbology_dict = {"CONUS": r"L:\Workspace\_MovedOneDrive\UseSites\ByProject\Diaz_Moasics\Sym_Layers"
                           r"\Albers_Conical_Equal_Area_CONUS_CDL_1015_60x2_euc.lyr"}

snap_raster_dict = {
    'CONUS': r"L:\Workspace\UseSites\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.img"}


def zone(zone_lyr, raster_lyr, temp_table, snap, zone_header):
    # Set Snap Raster environment
    arcpy.env.snapRaster = Raster(snap)
    start_zone = datetime.datetime.now()
    arcpy.CreateTable_management("in_memory", temp_table)
    temp = "in_memory" + os.sep + temp_table
    arcpy.env.overwriteOutput = True
    arcpy.gp.ZonalHistogram_sa(zone_lyr, zone_header, raster_lyr, temp)
    print "Completed Zonal Histogram"

    return temp, start_zone


def zonal_hist(in_zone_data, in_value_raster, set_raster_symbol, use_name, results_folder, temp_table, region_c, snap, cnt):
    break_use = use_path.split("_")
    break_bool = False
    use_nm_folder = region_c  # starting point that will be used for use_nm_folder

    for v in break_use:
        if v != region_c:
            pass
        else:
            break_bool = True
        if break_bool:
            if v == region_c:
                continue
            else:
                use_nm_folder = use_nm_folder + "_" + v

    out_use_folder = results_folder + os.sep + use_nm_folder
    create_directory(out_use_folder)

    if not os.path.exists(out_folder):
            os.mkdir(out_folder)

    # parse out information needed for file names

    for zone_title in ["STATEFP","GEOID"]:
        if zone_title.startswith("STATE"):
            run_id = use_nm_folder + "_State"
        else:
            run_id = use_nm_folder + "_County"

        create_directory(out_use_folder)
        csv = out_use_folder + os.sep + run_id + '.csv'
        if os.path.exists(csv):
            print ("Already completed run for {0}".format(run_id))
        elif not os.path.exists(csv):
            print ("\nRunning Statistics...for {0} and raster {1}".format(zone_title, use_name))
            arcpy.CheckOutExtension("Spatial")
            arcpy.Delete_management("rd_lyr")
            arcpy.Delete_management("pol_bnd_lyr")

            arcpy.MakeFeatureLayer_management(in_zone_data, "pol_bnd_lyr")
            arcpy.MakeRasterLayer_management(Raster(in_value_raster), "rd_lyr")
            arcpy.ApplySymbologyFromLayer_management("rd_lyr", set_raster_symbol)
            table = temp_table + str(cnt)
            arcpy.Delete_management("in_memory" +os.sep +table)
            temp_return, zone_time = zone("pol_bnd_lyr", "rd_lyr", table, snap, zone_title)
            cnt +=1

            list_fields = [f.name for f in arcpy.ListFields(temp_return)]
            att_array = arcpy.da.TableToNumPyArray(temp_return, list_fields)
            att_df = pd.DataFrame(data=att_array)
            att_df['LABEL'] = att_df['LABEL'].map(lambda x: x).astype(str)
            att_df.to_csv(csv)
            print 'Final file can be found at {0}'.format(csv)
            arcpy.Delete_management("in_memory" +os.sep +table)

            print "Completed in {0}".format((datetime.datetime.now() - zone_time))


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
create_directory(out_results)
# generates list of all HUC2 with NHD data to be used for file structure

# Generates list of use raster that will be included based on use_included list
arcpy.env.workspace = use_location

# all rasters in in_location_use gdb
list_raster_use = (arcpy.ListRasters())
# filters list to those in the use_included list - user input
list_raster_use = [raster for raster in list_raster_use if raster in use_list]
count = len(list_raster_use)

print list_raster_use

for use_nm in list_raster_use:  # loops through all use raster to be included
    out_folder = out_results
    use_path = use_location + os.sep + use_nm
    split_use_nm = use_nm.split("_")

    symbologyLayer = symbology_dict[region]
    snap_raster = snap_raster_dict[region]



    # try:
    zonal_hist(in_sum_file, use_path, symbologyLayer, use_nm, out_folder, temp_file, region, snap_raster, count)
    count += 1
    # except Exception as error:
        # print(error.args[0])
        # print "Failed on use {0}".format( use_nm)



end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
