import os
import datetime
import pandas as pd
import arcpy
from arcpy.sa import *

# Title- Runs overlap using Tabulate Area for political boundaries need for usage:
# TODO when run clean up inputs to match the other script in this tool - to make it more streamlined

# ##User input variables


# sub-directory folder where shapefile
in_sum_file = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive' \
              r'\Projects\ESA\_ExternalDrive\_CurrentSpeciesSpatialFiles\Boundaries.gdb\Counties_all_overlap'
region = 'CONUS'
temp_file = 'table3'
run_group = 'UseLayers'

# # location of use site to runt
# use_location = r"L:\Workspace\UseSites\CDL_Reclass\161031\CDL_Reclass_1015_161031.gdb"

use_location = 'L:\Workspace\StreamLine\ByProjection' + os.sep + str(region) + "_" + run_group + ".gdb"


arcpy.env.workspace = use_location

use_list = [u'Albers_Conical_Equal_Area_CDL_1016_100x2_euc', u'Albers_Conical_Equal_Area_CDL_1016_70x2_euc',
            u'Albers_Conical_Equal_Area_CDL_1016_71x2_euc', u'Albers_Conical_Equal_Area_CDL_1016_40x2_euc',
            u'Albers_Conical_Equal_Area_CDL_1016_10x2_euc', u'Albers_Conical_Equal_Area_CDL_1016_80x2_euc',
            u'Albers_Conical_Equal_Area_CDL_1016_72x2_euc', u'Albers_Conical_Equal_Area_CDL_1016_20x2_euc',
            u'Albers_Conical_Equal_Area_CDL_1016_90x2_euc', u'Albers_Conical_Equal_Area_CDL_1016_60x2_euc',
            u'Albers_Conical_Equal_Area_CDL_1016_30x2_euc', u'Albers_Conical_Equal_Area_CDL_1016_110_euc',
            u'Albers_Conical_Equal_Area_CONUS_Methomyl_CONUS_bermudagrass2_euc',
            u'Albers_Conical_Equal_Area_CONUS_carbaryl_171227d_AA_ag_euc',
            u'Albers_Conical_Equal_Area_CONUS_methomyl_citrus_171227_euc',
            u'Albers_Conical_Equal_Area_CONUS_Methomyl_alleycropping2_euc',
            u'Albers_Conical_Equal_Area_CONUS_methomyl_wheat_171227_euc',
            u'Albers_Conical_Equal_Area_CONUS_methomyl_171227_AA_ag_euc']  # runs specified layers in use location

if len(use_list) ==  0:
    use_list = (arcpy.ListRasters())  # run all rasters in the input gdb

# location of results
out_results = r'L:\Workspace\StreamLine\ESA\Results_Usage\PolBoundaries\Agg_layers'

# STATIC Variables
# Symbology layer so that the unique values can be applied to use layer before running zonal stats

symbology_dict = {'CONUS': [
    r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_CDL_1016_110x2_euc.lyr',
    r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_OnOff_X7072_171227.lyr',
    r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_CDL_2010_rec.lyr'],
    'HI': [r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\NAD_1983_UTM_Zone_4N_HI_Ag_euc.lyr',
           r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\NAD_1983_UTM_Zone_4N_CCAP_HI_6.lyr'],
    'AK': [r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\WGS_1984_Albers_AK_Ag_euc.lyr',
           r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\WGS_1984_Albers_AK_NLCD_2011_81.lyr'],
    'AS': [r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_2S_AS_Ag_euc.lyr',
           r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_2S_CCAP_AS_6.lyr'],
    'CNMI': [r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_55N_CNMI_Ag_euc.lyr',
             r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_55N_CCAP_CNMI_6.lyr'],
    'GU': [r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_55N_GU_Ag_euc.lyr',
           r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_55N_CCAP_GU_6_30.lyr'],
    'PR': [r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_PR_Ag_euc.lyr',
           r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_PR_NLCD_81.lyr'],
    'VI': [r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_20N_VI_Ag_euc.lyr',
           r'L:\Workspace\StreamLine\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_20N_CCAP_VI_6_30.lyr']}

snap_raster_dict = {'CONUS': r'L:\Workspace\StreamLine\ByProjection\SnapRasters.gdb'
                             r'\Albers_Conical_Equal_Area_cultmask_2016',
                    'HI': r'L:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\NAD_1983_UTM_Zone_4N_HI_Ag',
                    'AK': r'L:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\WGS_1984_Albers_AK_Ag',
                    'AS': r'L:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_2S_AS_Ag',
                    'CNMI': r'L:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_CNMI_Ag',
                    'GU': r'L:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_GU_Ag_30',
                    'PR': r'L:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_PR_Ag',
                    'VI': r'L:\Workspace\StreamLine\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_20N_VI_Ag_30'}

def zone(zone_lyr, raster_lyr, temp_table, snap, zone_headers):
    # Set Snap Raster environment and set extent
    arcpy.env.snapRaster = Raster(snap)
    my_extent = Raster(snap).extent
    arcpy.env.extent = my_extent

    start_zone = datetime.datetime.now()
    arcpy.Delete_management("in_memory" + os.sep + temp_table)
    arcpy.CreateTable_management("in_memory", temp_table)
    temp = "in_memory" + os.sep + temp_table
    arcpy.env.overwriteOutput = True
    TabulateArea(zone_lyr, zone_headers, raster_lyr, "Value", temp)

    print "Completed Tabulate Area"

    return temp, start_zone


def zonal_hist(in_zone_data, in_value_raster, set_raster_symbol, use_name, results_folder, temp_table, region_c, snap,
               cnt):
    # out paths
    break_use = os.path.basename(use_path).split("_")
    break_bool = False
    use_nm_folder = region_c  # starting point that will be used for use_nm_folder

    for v in break_use:  # SEE TODO
        if v != region_c and v != 'CDL':
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
    print use_nm_folder


    out_use_folder = results_folder + os.sep + use_nm_folder
    create_directory(out_use_folder)

    if not os.path.exists(out_folder):
        os.mkdir(out_folder)

    # parse out information needed for file names

    for zone_title in ["GEOID","STATEFP"]:
        if zone_title.startswith("STATE"):
            run_id = use_nm_folder + "_State"
        else:
            run_id = use_nm_folder + "_County"

        create_directory(out_use_folder)
        csv = out_use_folder + os.sep + run_id + '.csv'
        print csv
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
            arcpy.Delete_management("in_memory" + os.sep + table)
            temp_return, zone_time = zone("pol_bnd_lyr", "rd_lyr", table, snap, zone_title)
            cnt += 1

            list_fields = [f.name for f in arcpy.ListFields(temp_return)]
            att_array = arcpy.da.TableToNumPyArray(temp_return, list_fields)
            att_df = pd.DataFrame(data=att_array)
            #att_df['VALUE'] = att_df['VALUE'].map(lambda x: x).astype(str)
            att_df.to_csv(csv)
            print 'Final file can be found at {0}'.format(csv)
            arcpy.Delete_management(temp_return)

            print "Completed in {0}".format((datetime.datetime.now() - zone_time))


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
create_directory(os.path.dirname(out_results))
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

    symbologyLayer = symbology_dict[str(region)][0]
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
