import os
import datetime
import pandas as pd

import arcpy
from arcpy.sa import *

# Title- Runs overlap using Zonal Histogram for all RASTER to RASTER analyses including:
#           1) NHD catchment rasters to aggregated layers, AA, Ag and NonAG using featureID as the values

# Static variables are updated once per update; user input variables update each  run

# #### User input variables
# Species files and region
# TODO load in use name from input table
in_location_nhd = r'L:\ESA\UnionFiles_Winter2018\NHDFiles\L48\NHD_usage_ByProjection\Raster_NHDCatchments_20180122_20180122_interCnty'
split_by = 2
temp_file = "temp_table1"
region = "CONUS"

# #### Use layer location

# use_location = r'L:\Workspace\UseSites\ByProjection' + os.sep + region +'_UseLayers.gdb'
use_location = r'L:\Workspace\UseSites\ByProjection' + os.sep + region +'_Yearly.gdb'


arcpy.env.workspace = use_location

# Manual sub-set layers to be run; user is able to split out layers to complete run faster by splitting run into several
# instances
use_list = (arcpy.ListRasters())  # run all layers in use location

# use_list = [u'Albers_Conical_Equal_Area_methomyl_171227_AA_euc']  # runs specified layers in use location

# ################Static variables
out_results = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ED_results\Usage_Results'
# out_results = r'C:\Users\Admin\Documents\Jen\Workspace\StreamLine\Usage_Results'
find_file_type = in_location_nhd.split(os.sep)

if os.path.basename(use_location).startswith('CONUS'):
        out_results = out_results + os.sep + 'L48' + os.sep + 'NHD'
else:
        out_results = out_results + os.sep + 'NL48' + os.sep + 'NHD'

# Each region is a list pos 0 euc distance, pos 1 on/off, pos 3 Yearly (CONUS only
symbology_dict = {
    'CONUS': ['L:\Workspace\UseSites\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_CDL_1016_110x2_euc.lyr',
              r'L:\Workspace\UseSites\ByProjection\Symbol_Layers'
              r'\Albers_Conical_Equal_Area_OnOff_X7072_171227.lyr',
              r'L:\Workspace\UseSites\ByProjection\Symbol_Layers'
              r'\Albers_Conical_Equal_Area_CDL_2010_rec.lyr'],

    'HI': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\NAD_1983_UTM_Zone_4N_HI_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\NAD_1983_UTM_Zone_4N_CCAP_HI_6.lyr'],

    'AK': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_Albers_AK_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_Albers_AK_NLCD_2011_81.lyr'],

    'AS': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_2S_AS_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_2S_CCAP_AS_6.lyr'],

    'CNMI':[r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_55N_CNMI_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_55N_CCAP_CNMI_6.lyr'],

    'GU': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_55N_GU_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_55N_CCAP_GU_6_30.lyr'],

    'PR': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_PR_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_PR_NLCD_81.lyr'],

    'VI': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_20N_VI_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_20N_CCAP_VI_6_30.lyr']}


snap_raster_dict = {'CONUS': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb'
                             r'\Albers_Conical_Equal_Area_cultmask_2016',
                    'HI': 'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb'
                          '\NAD_1983_UTM_Zone_4N_HI_Ag',
                    'AK': 'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb'
                          '\WGS_1984_Albers_AK_Ag',
                    'AS': 'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb'
                          '\WGS_1984_UTM_Zone_2S_AS_Ag',
                    'CNMI': 'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb'
                            '\WGS_1984_UTM_Zone_55N_CNMI_Ag',
                    'GU': 'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb'
                          '\WGS_1984_UTM_Zone_55N_GU_Ag_30',
                    'PR': r'L:\Workspace\UseSites\ByProjection'
                          r'\SnapRasters.gdb\Albers_Conical_Equal_Area_PR_Ag',
                    'VI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb'
                          r'\WGS_1984_UTM_Zone_20N_VI_Ag_30'}

current_use = 1
arcpy.env.workspace = use_location
list_raster_use = (arcpy.ListRasters())

list_raster_use = [raster for raster in list_raster_use if raster in use_list]
count_use = len(list_raster_use)
print list_raster_use


# ################Functions


def zone(zone_lyr, raster_lyr, temp_table, snap, extent_use_layer):
    # Set Snap Raster environment
    arcpy.env.snapRaster = Raster(snap)
    arcpy.env.extent = zone_lyr
    start_zone = datetime.datetime.now()
    arcpy.CreateTable_management("in_memory", temp_table)
    temp = "in_memory" + os.sep + temp_table
    arcpy.env.overwriteOutput = True
    arcpy.gp.ZonalHistogram_sa(zone_lyr, "Value", raster_lyr, temp)
    print "Completed Zonal Histogram"

    return temp, start_zone


def zonal_hist(in_zone, in_value_raster, set_raster_symbology, region_c, use_name, temp_table, final_folder, snap, count_l):
    # In paths
    path_fc, in_file = os.path.split(in_zone)
    file_name  = in_file.split("_")[0]

    break_use = os.path.basename(use_path).split("_")
    break_bool = False
    use_nm_folder = region_c  # starting point that will be used for use_nm_folder

    for v in break_use:  # SEE TODO
        if v != 'Area' and v != 'AK' and v != '2S'and v != '55N' and v != 'Area' and v != '4N' and v != '20N':
            pass
        else:
            break_bool = True
        if break_bool:
            if v == region_c or v == '2S'or v == '55N' or v == 'Area' or v == '4N'or v == '20N' or v == 'Area':
                continue
            else:
                use_nm_folder = use_nm_folder + "_" + v

    use_nm_folder = use_nm_folder.split(".")[0]


    if not os.path.exists(final_folder + os.sep + use_nm_folder):
        os.mkdir(final_folder + os.sep + use_nm_folder)

    out_tables = final_folder + os.sep + use_nm_folder
    run_id = file_name + "_" + use_nm_folder +"_"+str(count_l)
    out_path_final = out_tables

    csv = run_id + '.csv'
    if os.path.exists(out_path_final + os.sep + csv):
        print ("Already completed run for {0}".format(run_id))

    elif not os.path.exists(out_path_final + os.sep + csv):
        print ("Running Statistics...for nhd file {0} and raster {1}".format(file_name, use_name))
        arcpy.CheckOutExtension("Spatial")

        arcpy.MakeRasterLayer_management(Raster(in_zone), "zone")
        arcpy.MakeRasterLayer_management(Raster(in_value_raster), "rd_lyr")
        arcpy.ApplySymbologyFromLayer_management("rd_lyr", set_raster_symbology)
        temp_return, zone_time = zone("zone", "rd_lyr", temp_table, snap, in_value_raster)

        list_fields = [f.name for f in arcpy.ListFields(temp_return)]
        att_array = arcpy.da.TableToNumPyArray((temp_return), list_fields)
        att_df = pd.DataFrame(data=att_array)
        att_df['LABEL'] = att_df['LABEL'].map(lambda x: x).astype(str)
        att_df = att_df.T
        att_df.to_csv(out_path_final + os.sep + csv)
        print 'Final file can be found at {0}'.format(out_path_final + os.sep + csv)
        print "Completed in {0}\n".format((datetime.datetime.now() - zone_time))


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
create_directory(os.path.dirname(os.path.dirname(out_results)))
create_directory(os.path.dirname(out_results))
create_directory(out_results)
arcpy.env.workspace = in_location_nhd
list_raster = (arcpy.ListRasters())
count_sp = len(arcpy.ListRasters())
count = 0


for raster_in in list_raster:
    count += 1
    in_sp = in_location_nhd + os.sep + raster_in
    raster_array = arcpy.da.TableToNumPyArray(in_sp, ['VALUE', 'COUNT'])
    raster_zone_df = pd.DataFrame(data=raster_array, dtype=object)
    max = raster_zone_df['VALUE'].max()
    split = max/split_by


    arcpy.MakeRasterLayer_management ((in_sp), "raster_a", "VALUE > " + str(split))
    arcpy.MakeRasterLayer_management ((in_sp), "raster_b", "VALUE <= " + str(split))
    for raster_layer in ["raster_a", "raster_b"]:
        count_list =1
        print "\nWorking on uses for {0} NHD file {1} of {2}".format(raster_in, count, count_sp)
        for use_nm in list_raster_use:
            out_folder = out_results
            if region != 'CONUS':
                snap_raster = snap_raster_dict[region]
                if os.path.basename(use_location).endswith('UseLayers.gdb'):
                    symbologyLayer = symbology_dict[region][0]
                    print symbologyLayer
                    out_folder = out_folder + os.sep + 'Agg_Layers'
                    create_directory(out_folder)
                elif os.path.basename(use_location).endswith('OnOffField.gdb'):
                    symbologyLayer = symbology_dict[region][1]
                    out_folder = out_folder + os.sep + 'OnOffField'
                    create_directory(out_folder)
            else:
                snap_raster = snap_raster_dict[region]

                if os.path.basename(use_location).endswith('UseLayers.gdb'):
                    symbologyLayer = symbology_dict[region][0]
                    out_folder = out_folder + os.sep + 'Agg_Layers'
                    create_directory(out_folder)
                elif os.path.basename(use_location).endswith('OnOffField.gdb'):
                    symbologyLayer = symbology_dict[region][1]
                    out_folder = out_folder + os.sep + 'OnOffField'
                    create_directory(out_folder)
                elif os.path.basename(use_location).endswith('Yearly.gdb'):
                    symbologyLayer = symbology_dict[region][2]
                    out_folder = out_folder + os.sep + 'Indiv_Year_raw'
                    create_directory(out_folder)

            use_path = use_location + os.sep + use_nm
            print 'Starting use layer {0}, use {1} of {2}'.format(use_path, current_use, count_use)
            #try
            zonal_hist(in_sp, use_path, symbologyLayer, region, use_nm, temp_file, out_folder, snap_raster, count_list)
            #except Exception as error:
            #     print(error.args[0])
            #     print "Failed on {0} with use {1}".format(raster_in, use_nm)
            current_use += 1
        current_use = 1

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
