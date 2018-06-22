import os
import datetime
import pandas as pd

import arcpy
from arcpy.sa import *

# Title- Runs overlap using Zonal Histogram for all RASTER to RASTER analyses including:
#           1) Species zone rasters to aggregated layers, AA, Ag and NonAG
#           2) Species zone raster to non euc distance individual years of the CDL
#           3) Species zone raster to on/off field
#           4) HUCID zone to incorporate usage, habitat, elevation and HUC breaks into overlap tables
#           Archived
#               3) Pilot GAP species to aggregated layers and non euc distance individual years -
#           TODO Set up a way to update single species

# Assumptions
#   1) folder with species composite must start with region abb
#   2) temp file name should not use the same temp file name when running multiple instances at the same time
#   3) Snap raster and symbology dictionaries have been updated for current use layers

# #### User input variables

# Update once then remains static to set file structure
use_location_base = 'L:\Workspace\UseSites\ByProjection'
out_results = r'L:\ESA\Results_Usage'


# Update for each run - species base only updaed when switching from Range or CriticalHabitat in the path
in_location_species_base = r'L:\ESA\UnionFiles_Winter2018\CriticalHabitat\SpComp_UsageHUCAB_byProjection\Grid_byProjections_Combined'
#in_location_species_base = r'L:\ESA\UnionFiles_Winter2018\Range\SpComp_UsageHUCAB_byProjection_2\Grid_byProjections_Combined'



in_location_species_folder = 'CONUS_Albers_Conical_Equal_Area'
# Range
# AK_WGS_1984_Albers
# VI_WGS_1984_UTM_Zone_20N

# HI_NAD_1983_UTM_Zone_4N ~~~
# PR_Albers_Conical_Equal_Area
# CONUS_Albers_Conical_Equal_Area

# CH
# CONUS_Albers_Conical_Equal_Area
# VI_WGS_1984_UTM_Zone_20N
temp_file = "temp_table9"  # Should not use the same temp file name when running multiple instances at the same time
run_group = 'UseLayers'  # UseLayers, Yearly, OnOffField

# Manually sub-set layers to be run: complete region run faster by splitting run into several instances

use_list = ['Albers_Conical_Equal_Area_CONUS_carbaryl_171227d_AA_ag_euc',
             'Albers_Conical_Equal_Area_CONUS_ManagedForests_xmas_180307_euc',
             'Albers_Conical_Equal_Area_CONUS_methomyl_171227_AA_ag_euc',
             'Albers_Conical_Equal_Area_CONUS_Ndev_ROW_180306_euc',
             'Albers_Conical_Equal_Area_CONUS_carbaryl_180410_AA_nonAg_euc',]

# 'Albers_Conical_Equal_Area_CONUS_carbaryl_171227d_AA_ag_euc',
#             'Albers_Conical_Equal_Area_CONUS_ManagedForests_xmas_180307_euc',
#             'Albers_Conical_Equal_Area_CONUS_methomyl_171227_AA_ag_euc',
#             'Albers_Conical_Equal_Area_CONUS_Ndev_ROW_180306_euc',
#             'Albers_Conical_Equal_Area_CONUS_carbaryl_180410_AA_nonAg_euc',


# [u'Albers_Conical_Equal_Area_CDL_1016_100x2_euc', u'Albers_Conical_Equal_Area_CDL_1016_70x2_euc',
#  u'Albers_Conical_Equal_Area_CDL_1016_71x2_euc', u'Albers_Conical_Equal_Area_CDL_1016_40x2_euc',
#  u'Albers_Conical_Equal_Area_CDL_1016_10x2_euc',
#
#
# u'Albers_Conical_Equal_Area_CDL_1016_80x2_euc', u'Albers_Conical_Equal_Area_CDL_1016_72x2_euc',
#  u'Albers_Conical_Equal_Area_CDL_1016_20x2_euc', u'Albers_Conical_Equal_Area_CDL_1016_90x2_euc',
#  u'Albers_Conical_Equal_Area_CDL_1016_60x2_euc',
#
# u'Albers_Conical_Equal_Area_CDL_1016_30x2_euc', u'Albers_Conical_Equal_Area_CONUS_OSD_euc',
#  u'Albers_Conical_Equal_Area_CONUS_Developed_euc', u'Albers_Conical_Equal_Area_CONUS_FederalLands_euc',
#  u'Albers_Conical_Equal_Area_CONUS_methomyl_171227_AA_euc',
#
# u'Albers_Conical_Equal_Area_CONUS_carbaryl_180410_AA_euc',
#  u'Albers_Conical_Equal_Area_CONUS_Methomyl_CONUS_bermudagrass2_euc',
#  u'Albers_Conical_Equal_Area_CONUS_methomyl_citrus_171227_euc',
#  u'Albers_Conical_Equal_Area_CONUS_Methomyl_alleycropping2_euc',
#  u'Albers_Conical_Equal_Area_CONUS_methomyl_wheat_171227_euc', u'Albers_Conical_Equal_Area_CDL_1016_110_euc']

# ################Static variables
arcpy.CheckOutExtension("Spatial")
in_location_species = in_location_species_base + os.sep + in_location_species_folder
region = os.path.basename(in_location_species).split("_")[0]  # folder with species composite must start with region abb
use_location = use_location_base + os.sep + str(region) + "_" + run_group + ".gdb"
arcpy.env.workspace = use_location
if len(use_list) == 0:
    use_list = (arcpy.ListRasters())  # run all layers in use location
print use_list

count_use = len(use_list)
find_file_type = in_location_species.split(os.sep)

if 'Range' in find_file_type:
    Range = True
    if os.path.basename(use_location).startswith('CONUS'):
        out_results = out_results + os.sep + 'L48' + os.sep + 'Range'
    else:
        out_results = out_results + os.sep + 'NL48' + os.sep + 'Range'

else:
    Range = False
    if os.path.basename(use_location).startswith('CONUS'):
        out_results = out_results + os.sep + 'L48' + os.sep + 'CriticalHabitat'

    else:
        out_results = out_results + os.sep + 'NL48' + os.sep + 'CriticalHabitat'


# Each region is a list pos 0 euc distance, pos 1 on/off, pos 2 Yearly (CONUS only)
symbology_dict = {'CONUS': [
    r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_CDL_1016_110x2_euc.lyr',
    r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_OnOff_X7072_171227.lyr',
    r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_CDL_2010_rec.lyr'],
    'HI': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\NAD_1983_UTM_Zone_4N_HI_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\NAD_1983_UTM_Zone_4N_CCAP_HI_6.lyr'],
    'AK': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_Albers_AK_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_Albers_AK_NLCD_2011_81.lyr'],
    'AS': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_2S_AS_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_2S_CCAP_AS_6.lyr'],
    'CNMI': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_55N_CNMI_Ag_euc.lyr',
             r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_55N_CCAP_CNMI_6.lyr'],
    'GU': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_55N_GU_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_55N_CCAP_GU_6_30.lyr'],
    'PR': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_PR_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_PR_NLCD_81.lyr'],
    'VI': [r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_20N_VI_Ag_euc.lyr',
           r'L:\Workspace\UseSites\ByProjection\Symbol_Layers\WGS_1984_UTM_Zone_20N_CCAP_VI_6_30.lyr']}

snap_raster_dict = {'CONUS': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb'
                             r'\Albers_Conical_Equal_Area_cultmask_2016',
                    'HI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\NAD_1983_UTM_Zone_4N_HI_Ag',
                    'AK': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_Albers_AK_Ag',
                    'AS': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_2S_AS_Ag',
                    'CNMI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_CNMI_Ag',
                    'GU': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_GU_Ag_30',
                    'PR': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_PR_Ag',
                    'VI': r'L:\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_20N_VI_Ag_30'}

current_use = 1

# ################Functions


def zone(zone_lyr, raster_lyr, temp_table, snap):
    # Set Snap Raster environment and set extent
    arcpy.env.snapRaster = Raster(snap)
    my_extent = Raster(snap).extent
    arcpy.env.extent = my_extent

    start_zone = datetime.datetime.now()
    arcpy.Delete_management("in_memory" + os.sep + temp_table)
    arcpy.CreateTable_management("in_memory", temp_table)
    temp = "in_memory" + os.sep + temp_table
    arcpy.env.overwriteOutput = True
    TabulateArea(zone_lyr, "Value", raster_lyr, "Value", temp)

    print "Completed Tabulate Area"

    return temp, start_zone


def zonal_hist(in_zone, in_value_raster, set_raster_symbology, region_c, use_name, temp_table, final_folder, snap):
    # In paths
    path_fc, in_species = os.path.split(in_zone)
    sp_group = in_species.split("_")[1]

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

    if not os.path.exists(final_folder + os.sep + use_nm_folder):
        os.mkdir(final_folder + os.sep + use_nm_folder)

    out_tables = final_folder + os.sep + use_nm_folder
    run_id = in_species + "_" + use_nm_folder
    out_path_final = out_tables

    csv = run_id + '.csv'

    if os.path.exists(out_path_final + os.sep + csv):
        print ("Already completed run for {0}".format(run_id))

    elif not os.path.exists(out_path_final + os.sep + csv):
        print ("Running Statistics...for species group {0} and raster {1}".format(sp_group, use_name))
        # arcpy.CheckOutExtension("Spatial")

        arcpy.MakeRasterLayer_management(Raster(in_zone), "zone")

        arcpy.MakeRasterLayer_management(Raster(in_value_raster), "rd_lyr")

        arcpy.ApplySymbologyFromLayer_management("rd_lyr", set_raster_symbology)
        temp_return, zone_time = zone("zone", "rd_lyr", temp_table, snap)

        list_fields = [f.name for f in arcpy.ListFields(temp_return)]
        att_array = arcpy.da.TableToNumPyArray(temp_return, list_fields)
        att_df = pd.DataFrame(data=att_array)
        att_df['VALUE'] = att_df['VALUE'].map(lambda x: x).astype(str)
        att_df.to_csv(out_path_final + os.sep + csv)
        print 'Final file can be found at {0}'.format(out_path_final + os.sep + csv)
        print "Completed in {0}\n".format((datetime.datetime.now() - zone_time))


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_directory(os.path.dirname(out_results))
print out_results
create_directory(out_results)
arcpy.env.workspace = in_location_species
count_sp = len(arcpy.ListRasters())
count = 0
list_raster = (arcpy.ListRasters())
# list_raster = [v for v in list_raster if v.split('_')[0] =='r']
print list_raster

for raster_in in list_raster:
    count += 1
    in_sp = in_location_species + os.sep + raster_in
    print raster_in
    raster_file = Raster(in_sp)
    print "\nWorking on uses for {0} species file {1} of {2}".format(raster_in, count, count_sp)
    for use_nm in use_list:
        out_folder = out_results
        if region != 'CONUS':
            snap_raster = snap_raster_dict[str(region)]
            if use_location.endswith('UseLayers.gdb'):
                symbologyLayer = symbology_dict[str(region)][0]
                out_folder = out_folder + os.sep + 'Agg_Layers'
                create_directory(out_folder)
            elif use_location.endswith('OnOffField.gdb'):
                symbologyLayer = symbology_dict[str(region)][1]
                out_folder = out_folder + os.sep + 'OnOffField'
                create_directory(out_folder)
        else:
            snap_raster = snap_raster_dict[str(region)]
            if use_location.endswith('UseLayers.gdb'):
                symbologyLayer = symbology_dict[str(region)][0]
                out_folder = out_folder + os.sep + 'Agg_Layers'
                create_directory(out_folder)
            elif use_location.endswith('OnOffField.gdb'):
                symbologyLayer = symbology_dict[str(region)][1]
                out_folder = out_folder + os.sep + 'OnOffField'
                create_directory(out_folder)
            elif use_location.endswith('Yearly.gdb'):
                symbologyLayer = symbology_dict[str(region)][2]
                out_folder = out_folder + os.sep + 'Indiv_Year_raw'
                create_directory(out_folder)
        use_path = use_location + os.sep + use_nm
        print 'Starting use layer {0}, use {1} of {2}'.format(use_path, current_use, count_use)
        # try:  # uncomment try/expect loop if runs need to be done quickly: be sure to check if something fail and why
        zonal_hist(in_sp, use_path, symbologyLayer, region, use_nm, temp_file, out_folder, snap_raster)
        # except Exception as error:
        #     print(error.args[0])
        #     print "Failed on {0} with use {1}".format(raster_in, use_nm)
        current_use += 1
    current_use = 1

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
