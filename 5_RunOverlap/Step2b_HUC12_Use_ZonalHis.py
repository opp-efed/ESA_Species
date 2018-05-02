import os
import datetime
import pandas as pd
import arcpy
from arcpy.sa import *


# Title- Runs overlap using Zonal Histogram for all FEATURE to RASTER analyses including:
#           1) NHD HUC12s features to aggregated layers, AA, Ag and NonAG
#           2) NHD HUC12s features to non euc distance individual years of the CDL

# ##User input variables

# Location of spatial library od NHDPluse
# in_HUC_base = 'L:\NHDPlusV2'
# # sub-directory folder where shapefile
# tail = 'WBDSnapshot\WBD\WBD_Subwatershed.shp'
# region = 'CONUS'
# temp_file = 'table_1'

in_HUC_base = 'L:\ESA\UnionFiles_Winter2018\NHDFiles\L48\HUC_12byHUC2.gdb'
# # sub-directory folder where shapefile
# tail = 'WBDSnapshot\WBD\WBD_Subwatershed.shp'
region = 'CONUS'
temp_file = 'table_8'

# # location of use site to run
use_location = r'L:\Workspace\UseSites\ByProjection' + os.sep + region +'_UseLayers.gdb'
# use_location = r'L:\Workspace\UseSites\ByProjection' + os.sep + region +'_Yearly.gdb'


arcpy.env.workspace = use_location
use_list = (arcpy.ListRasters())

# location of results
out_results = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              r'\_ED_results\Results\L48\HUC_2'


# STATIC Variables
# Symbology layer so that the unique values can be applied to use layer before running zonal stats
symbology_dict = {"CONUS" : ['L:\Workspace\UseSites\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_CDL_1016_110x2_euc.lyr',
              r'L:\Workspace\UseSites\ByProjection\Symbol_Layers'
              r'\Albers_Conical_Equal_Area_CDL_2010_rec.lyr']}

snap_raster_dict = {
    'CONUS': r'L:\Workspace\UseSites\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.img'}


if os.path.basename(out_results) != 'HUC12':
    out_results = out_results +os.sep+ 'HUC12'

def zone(zone_lyr, raster_lyr, temp_table, snap):
    # fclist_field = [f.name for f in arcpy.ListFields(zone_lyr) if not f.required]
    # print fclist_field
    # Set Snap Raster environment
    arcpy.env.snapRaster = Raster(snap)
    start_zone = datetime.datetime.now()
    arcpy.CreateTable_management("in_memory", temp_table)
    temp = "in_memory" + os.sep + temp_table
    arcpy.env.overwriteOutput = True
    arcpy.gp.ZonalHistogram_sa(zone_lyr, "huc_fips", raster_lyr, temp)
    print "Completed Zonal Histogram"
    return temp, start_zone


def zonal_hist(in_zone_data, in_value_raster, set_raster_symbol, use_name, results_folder, huc12, temp_table, region_c, snap):

    # out paths
    break_use = os.path.basename(use_path).split("_")
    break_bool = False
    use_nm_folder = region_c  # starting point that will be used for use_nm_folder

    for v in break_use:  # SEE TODO
        if v != region and v != 'CDL':
            # 'Area' and v != 'AK' and v != '2S'and v != '55N' and v != 'Area' and v != '4N' and v != '20N':
            pass
        else:
            break_bool = True
        if break_bool:
            if v == region_c:
                pass
            else:
                use_nm_folder = use_nm_folder + "_" + v

    use_nm_folder = use_nm_folder.split(".")[0]

    out_folder = results_folder + os.sep + use_nm_folder
    if not os.path.exists(out_folder):
            os.mkdir(out_folder)

    # parse out information needed for file names
    # huc_12_value = huc12.replace('NHDPlus', '_')
    huc_12_value = huc12
    run_id = use_nm_folder +"_" +huc_12_value
    csv = out_folder + os.sep + run_id + '.csv'
    print csv
    if os.path.exists(csv):
        print ("Already completed run for {0}".format(run_id))
    elif not os.path.exists(csv):
        print ("Running Statistics...for HUC2 {0} and raster {1}".format(huc_12_value, use_name))
        arcpy.CheckOutExtension("Spatial")
        arcpy.Delete_management("rd_lyr")
        arcpy.Delete_management("HUC12_lyr")
        arcpy.MakeFeatureLayer_management(in_zone_data, "HUC12_lyr")
        arcpy.MakeRasterLayer_management(Raster(in_value_raster), "rd_lyr")
        arcpy.ApplySymbologyFromLayer_management("rd_lyr", set_raster_symbol)
        temp_return, zone_time = zone("HUC12_lyr", "rd_lyr", temp_table, snap)

        list_fields = [f.name for f in arcpy.ListFields(temp_return)]
        att_array = arcpy.da.TableToNumPyArray(temp_return, list_fields)
        att_df = pd.DataFrame(data=att_array)
        att_df['LABEL'] = att_df['LABEL'].map(lambda x: x).astype(str)
        att_df.to_csv(csv)
        print 'Final file can be found at {0}'.format(csv)
        # arcpy.TableToTable_conversion(temp_return, outpath_final, dbf)
        arcpy.Delete_management("in_memory\\temp_table")

        print "Completed in {0}".format((datetime.datetime.now() - zone_time))


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


arcpy.env.workspace = in_HUC_base
fc_list = (arcpy.ListFeatureClasses())


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
create_directory(out_results)
# generates list of all HUC2 with NHD data to be used for file structure
list_dir = os.listdir(in_HUC_base)
list_HUC2 = [huc2 for huc2 in list_dir if huc2.startswith('NHDPlus')]

# Generates list of use raster that will be included based on use_included list
arcpy.env.workspace = use_location
current_use = 1
# all rasters in in_location_use gdb
list_raster_use = (arcpy.ListRasters())
# filters list to those in the use_included list - user input
list_raster_use = [raster for raster in list_raster_use if raster in use_list]
count_use = len(list_raster_use)
print list_raster_use

for use_nm in list_raster_use:  # loops through all use raster to be included
    out_folder = out_results
    use_path = use_location + os.sep + use_nm
    count = 1
    split_use_nm = use_nm.split("_")
    symbology_flag = split_use_nm[(len(split_use_nm) - 1)]
    if region != 'CONUS':
            symbologyLayer = symbology_dict[region]
            snap_raster = snap_raster_dict[region]
    else:
        if symbology_flag == 'euc':
            symbologyLayer = symbology_dict[region][0]
            snap_raster = snap_raster_dict[region]

            out_folder= out_folder + os.sep + 'Agg_Layers'
            create_directory(out_folder)
        else:
            symbologyLayer = symbology_dict[region][1]
            snap_raster = snap_raster_dict[region]

            out_folder = out_folder + os.sep + 'Indiv_Year_raw'
            create_directory(out_folder)
    # for value in list_HUC2:  # loop all NHD folders for each use
    for value in fc_list:
            # HUC12_path = in_HUC_base + os.sep + value + os.sep + tail
            HUC12_path = in_HUC_base + os.sep + value


            print "\nWorking on uses for {0} HUC file {1} of {2}".format(value, count, len(list_HUC2))
            # try:

            zonal_hist(HUC12_path, use_path, symbologyLayer, use_nm, out_folder, value, temp_file, region, snap_raster)
            # except Exception as error:
            #     print(error.args[0])
            #     print "Failed on {0} with use {1}".format(value, use_nm)
            count += 1
    current_use += 1

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
