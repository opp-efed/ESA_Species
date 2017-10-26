import os
import datetime
import pandas as pd

import arcpy
from arcpy.sa import *

# Title- Runs overlap using Zonal Histogram for all RASTER to RASTER analyses including:
#           1) Species zone rasters to aggregated layers, AA, Ag and NonAG
#           2) Species zone raster to non euc distance individual years of the CDL
#           3) Pilot GAP species to aggregated layers and non euc distance individual years -
#                TODO may be able to 3) Pilot GAP to do a last minute update to an individual species range

# Static variables are updated once per update; user input variables update each  run

# #### User input variables
# Species files and region
in_location_species = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\SpCompRaster_byProjection\Grids_byProjection\Albers_Conical_Equal_Area'
temp_file = "temp_table"
region = "CONUS"

# #### Use layer location
# Running aggregate layers
use_location_base = 'L:\Workspace\UseSites\ByProject'
use_location = use_location_base + os.sep + str(region) + "_UseLayer.gdb"

# Running individual years
# use_location = r"L:\Workspace\UseSites\CDL_Reclass\161031\CDL_Reclass_1015_161031.gdb"

arcpy.env.workspace = use_location
# print use_location

# Manual sub-set layers to be run; user is able to split out layers to complete run faster by splitting run into several
# instances
use_list = (arcpy.ListRasters())  # run all layers in use location
# use_list =[ u'Albers_Conical_Equal_Area_CONUS_TribalLands_euc_final'] # runs specified layers in use location

# ################Static variables
out_results = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\test_gap'
find_file_type = in_location_species.split(os.sep)
if 'Range' in find_file_type:
    Range = True
    out_results = out_results + os.sep + 'Range'
else:
    Range = False
    out_results = out_results + os.sep + 'CriticalHabitat'

symbology_dict = {
    'CONUS': ['L:\Workspace\UseSites\ByProject\SymblogyLayers\Albers_Conical_Equal_Area_CONUS_CDL_1015_100x2_euc.lyr',
              r'L:\Workspace\UseSites\ByProject\SymblogyLayers\CDL_2013_rec.lyr'],
    'HI': r'L:\Workspace\UseSites\ByProject\SymblogyLayers\NAD_1983_UTM_Zone__4N_HI_Ag_euc.lyr',
    'AK': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_Albers_AK_CattleEarTag_euc.lyr',
    'AS': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_UTM_Zone__2S_AS_Ag_euc.lyr',
    'CNMI': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_UTM_Zone_55N_CNMI_Ag_euc.lyr',
    'GU': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_UTM_Zone_55N_GU_Ag_euc.lyr',
    'PR': 'L:\Workspace\UseSites\ByProject\SymblogyLayers'
          '\NAD_1983_StatePlane_Puerto_Rico_Virgin_Isl_FIPS_5200_PR_Ag_euc.lyr',
    'VI': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_UTM_Zone_20N_VI_Ag_euc.lyr'}

# TODO update region snap to binary Ag layers
snap_raster_dict = {
    'CONUS': r'L:\Workspace\UseSites\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.img',
    'HI': r'L:\Workspace\UseSites\ByProject\HI_UseLayer.gdb\NAD_1983_UTM_Zone__4N_HI_Ag_euc',
    'AK': 'L:\Workspace\UseSites\ByProject\AK_UseLayer.gdb\WGS_1984_Albers_AK_Ag_euc',
    'AS': 'L:\Workspace\UseSites\ByProject\AS_UseLayer.gdb\WGS_1984_UTM_Zone__2S_AS_Ag_euc',
    'CNMI': 'L:\Workspace\UseSites\ByProject\CNMI_UseLayer.gdb\WGS_1984_UTM_Zone_55N_CNMI_Ag_euc',
    'GU': 'L:\Workspace\UseSites\ByProject\GU_UseLayer.gdb\WGS_1984_UTM_Zone_55N_GU_Ag_euc',
    'PR': r'L:\Workspace\UseSites\ByProject\PR_UseLayer.gdb'
          r'\NAD_1983_StatePlane_Puerto_Rico_Virgin_Isl_FIPS_5200_PR_Ag_euc',
    'VI': 'L:\Workspace\UseSites\ByProject\VI_UseLayer.gdb\WGS_1984_UTM_Zone_20N_VI_Ag_euc',}

current_use = 1
arcpy.env.workspace = use_location
list_raster_use = (arcpy.ListRasters())

list_raster_use = [raster for raster in list_raster_use if raster in use_list]
count_use = len(list_raster_use)
print list_raster_use

# ################Functions


def zone(zone_lyr, raster_lyr, temp_table, snap):
    # Set Snap Raster environment
    arcpy.env.snapRaster = Raster(snap)
    start_zone = datetime.datetime.now()
    arcpy.CreateTable_management("in_memory", temp_table)
    temp = "in_memory" + os.sep + temp_table
    arcpy.env.overwriteOutput = True
    arcpy.gp.ZonalHistogram_sa(zone_lyr, "Value", raster_lyr, temp)
    print "Completed Zonal Histogram"

    return temp, start_zone


def zonal_hist(in_zone, in_value_raster, set_raster_symbology, region_c, use_name, temp_table, final_folder, snap):
    # In paths
    path_fc, in_species = os.path.split(in_zone)
    sp_group = in_species.split("_")[1]

    # out paths
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

    use_nm_folder = use_nm_folder.split(".")[0]
    print use_nm_folder

    if not os.path.exists(final_folder+ os.sep + use_nm_folder):
        os.mkdir(final_folder + os.sep + use_nm_folder)

    out_tables = final_folder + os.sep + use_nm_folder
    run_id = in_species + "_" + use_nm_folder
    out_path_final = out_tables

    csv = run_id + '.csv'


    if os.path.exists(out_path_final + os.sep + csv):
        print ("Already completed run for {0}".format(run_id))

    elif not os.path.exists(out_path_final + os.sep + csv):
        print ("Running Statistics...for species group {0} and raster {1}".format(sp_group, use_name))
        arcpy.CheckOutExtension("Spatial")

        arcpy.MakeRasterLayer_management(Raster(in_zone), "zone")
        arcpy.MakeRasterLayer_management(Raster(in_value_raster), "rd_lyr")
        arcpy.ApplySymbologyFromLayer_management("rd_lyr", set_raster_symbology)
        temp_return, zone_time = zone("zone", "rd_lyr", temp_table, snap)

        list_fields = [f.name for f in arcpy.ListFields(temp_return)]
        att_array = arcpy.da.TableToNumPyArray((temp_return), list_fields)
        att_df = pd.DataFrame(data=att_array)
        att_df['LABEL'] = att_df['LABEL'].map(lambda x: x).astype(str)
        att_df.to_csv(out_path_final + os.sep + csv)
        print 'Final file can be found at {0}'.format(out_path_final + os.sep + csv)
        print "Completed in {0}\n".format((datetime.datetime.now() - zone_time))


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_directory(out_results)
arcpy.env.workspace = in_location_species
count_sp = len(arcpy.ListRasters())
count = 0
list_raster = (arcpy.ListRasters())

for raster_in in list_raster:
    count += 1
    in_sp = in_location_species + os.sep + raster_in

    print raster_in
    raster_file = Raster(in_sp)
    print "\nWorking on uses for {0} species file {1} of {2}".format(raster_in, count, count_sp)
    for use_nm in list_raster_use:
        out_folder = out_results
        if region != 'CONUS':
            symbologyLayer = symbology_dict[region]
            snap_raster = snap_raster_dict[region]
        else:
            split_use_nm = use_nm.split("_")
            symbology_flag = split_use_nm[(len(split_use_nm) - 1)]
            if symbology_flag == 'euc':
                symbologyLayer = symbology_dict[region][0]
                snap_raster = snap_raster_dict[region]
                if raster_in.split('_')[len(raster_in.split("_")) -1] == 'gap' or raster_in.split('_')[len(raster_in.split("_")) -1] == 'GAP':
                    out_folder = out_results + os.sep + 'PilotGAP'
                    create_directory(out_folder)
                out_folder = out_folder + os.sep + 'Agg_Layers'
                create_directory(out_folder)
            else:
                symbologyLayer = symbology_dict[region][1]
                snap_raster = snap_raster_dict[region]
                if raster_in.split('_')[len(raster_in.split("_")) -1] == 'gap' or raster_in.split('_')[len(raster_in.split("_")) -1] == 'GAP':
                    out_folder = out_results + os.sep + 'PilotGAP'
                    create_directory(out_folder)
                out_folder = out_folder + os.sep + 'Indiv_Year_raw'
                create_directory(out_folder)

        use_path = use_location + os.sep + use_nm
        print 'Starting use layer {0}, use {1} of {2}'.format(use_path, current_use, count_use)
        try:
            zonal_hist(in_sp, use_path, symbologyLayer, region, use_nm, temp_file, out_folder, snap_raster)
        except Exception as error:
            print(error.args[0])
            print "Failed on {0} with use {1}".format(raster_in, use_nm)
        current_use += 1
    current_use = 1


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
