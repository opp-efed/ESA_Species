import datetime
import os

import arcpy
import pandas as pd
from arcpy.sa import *

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# Title- Runs overlap using Tabulate Area for all RASTER to RASTER analyses including:
#           1) Species zone rasters to aggregated layers, AA, Ag and NonAG
#           2) Species zone raster to non euc distance individual years of the CDL (currently on hold/not used)
#           3) Species zone raster to on/off field (currently on hold/not used)
#           4) HUCID zone to incorporate usage, habitat, elevation and HUC breaks into overlap tables
#           Archived
#               5) Pilot GAP species to aggregated layers and non euc distance individual years -
#           TODO Set up a way to update single species

# Assumptions
#   1) folder with species unioned composite must start with region abb
#   2) variable temp_file should not use file name when running multiple instances at the same time
#   3) Snap raster dictionaries have been updated for current use layers

# #### User input variables
# location of use layer library
use_location_base = r'D:\ByProjection'
# Results - location - there should be one folder for each run type, no adjustment, usage only, and habitat/elevation

# out_results = r'D:\Results_HUCAB'
out_results = r'D:\Results_Habitat'
# Species union input files - as ESRI Grids;range or crithab

# in_location_species_base = r'D:\Species\UnionFile_Spring2020\Range\SpComp_UsageHUCAB_byProjection\Grids_byProjection'
in_location_species_base = r'D:\Species\UnionFile_Spring2020\Range\SpComp_UsageHUCAB_byProjection\Grid_byProjections_Combined'

# species groups to skip; run multiple instances to speed up processing time or species group is complete
# format for this variable is a list, must use the species file names printed when script run
# see line 183 print (list_raster)
skip_species = []
# [u'r_amphib',  u'r_ferns',  u'r_insect', u'r_rept
# regional species composite folder to be run
in_location_species_folder = 'CNMI_WGS_1984_UTM_Zone_55N'
# name used to temp table
temp_file = "temp_table"  # Should not use the same temp file name when running multiple instances at the same time
# use layer run group - name of regional GDB
run_group = 'UseLayers'  # UseLayers, Yearly, OnOffField - NOTE - Currently only using UseLayers

# Manually sub-set layers to be run: complete region run faster by splitting run into several instances
# format for this variable is a list, must use the use file names printed when script runs
# see line 162 print use_list
uselist = []

# snap rasters by region for zones
snap_raster_dict = {
    'CONUS': r'D:\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_cultmask_2016',
    'HI': r'D:\ByProjection\SnapRasters.gdb\NAD_1983_UTM_Zone_4N_HI_Ag',
    'AK': r'D:\ByProjection\SnapRasters.gdb\WGS_1984_Albers_AK_Ag',
    'AS': r'D:\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_2S_AS_Ag',
    'CNMI': r'D:\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_CNMI_Ag',
    'GU': r'D:\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_GU_Ag_30',
    'PR': r'D:\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_PR_Ag',
    'VI': r'D:\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_20N_VI_Ag_30'}


# ################Functions


def zone(zone_lyr, raster_lyr, temp_table, snap):
    # Set Snap Raster environment and sets processing extent from snap raster
    arcpy.env.snapRaster = Raster(snap)
    my_extent = Raster(snap).extent
    arcpy.env.extent = my_extent

    start_zone = datetime.datetime.now()  # start time for zone tool
    # deletes ESRI table in memory if it exists
    if arcpy.Exists("in_memory" + os.sep + temp_table):
        arcpy.Delete_management("in_memory" + os.sep + temp_table)
    # creates ESRI table in memory to store output
    arcpy.CreateTable_management("in_memory", temp_table)
    temp = "in_memory" + os.sep + temp_table
    arcpy.env.overwriteOutput = True  # overwrite output if needed
    # runs zonal tool - generates raster layers
    TabulateArea(Raster(zone_lyr), "VALUE", Raster(raster_lyr), "VALUE", temp)
    print ("   Completed Tabulate Area")
    return temp, start_zone  # return output table and time zonal tool began


def zonal_hist(sp_path, in_value_raster, region_c, use_name, temp_table, final_folder, snap, usepath):
    # In paths
    path_fc, in_species = os.path.split(sp_path)
    sp_group = in_species.split("_")[1]

    # creates out paths; generates out path for zonal table based on use name
    break_use = os.path.basename(usepath).split("_")
    break_bool = False
    use_nm_folder = region_c  # starting point that will be used for use_nm_folder
    # set out folder bases on use name by splitting the use name and extracting the important pieces
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
    # creates folder for output based on use name
    if not os.path.exists(final_folder + os.sep + use_nm_folder):
        os.mkdir(final_folder + os.sep + use_nm_folder)

    out_tables = final_folder + os.sep + use_nm_folder  # final out path
    run_id = in_species + "_" + use_nm_folder  # run_id for tracking
    out_path_final = out_tables  # final path
    csv = run_id + '.csv'  # out csv name
    if os.path.exists(out_path_final + os.sep + csv):  # checks for an existing output; use/species run previously
        print ("   Already completed run for {0}: {1}".format(run_id, out_path_final + os.sep + csv))
    elif not os.path.exists(out_path_final + os.sep + csv):
        print ("   Running Statistics...for species group {0} and raster {1}".format(sp_group, use_name))
        # arcpy.CheckOutExtension("Spatial")
        start_species = datetime.datetime.now()  # start time for species
        print ("   loaded species in  {0}".format((datetime.datetime.now() - start_species)))
        # runs zonal tool
        temp_return, zone_time = zone(sp_path, in_value_raster, temp_table, snap)
        # exports the temp ESRI table save in memory output from zonal toll to csv
        list_fields = [f.name for f in arcpy.ListFields(temp_return)]  # get list of fields
        att_array = arcpy.da.TableToNumPyArray(temp_return, list_fields)  # exports table to array
        del list_fields  # deletes field list
        arcpy.Delete_management(temp_return)  # delete temp table in memory after saving - frees up memory
        att_df = pd.DataFrame(data=att_array)  # saves array as pandas df
        del att_array  # deletes temp array
        att_df['VALUE'] = att_df['VALUE'].map(lambda x: x).astype(str)  # sets value column from output as str
        att_df.to_csv(out_path_final + os.sep + csv)  # save df as csv
        print ('   Final file can be found at {0}'.format(out_path_final + os.sep + csv))  # print for tracking
        print ("   Completed in {0}\n".format((datetime.datetime.now() - zone_time)))  # elapse time print
        del att_df  # deletes df after the table is save


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


def main(out_res, in_spe, use_list, spe_folder):
    start_time = datetime.datetime.now()
    print "Start Time: " + start_time.ctime()
    arcpy.CheckOutExtension("Spatial")  # checks out licenses
    in_location_species = in_spe + os.sep + spe_folder  # full in path to species
    region = os.path.basename(in_location_species).split("_")[0]  # folder with species composite starts with region abb
    use_location = use_location_base + os.sep + str(region) + "_" + run_group + ".gdb"  # full path to in use layers

    # gets a list of use layers
    arcpy.env.workspace = use_location
    if len(use_list) == 0:  # use list can be overridden by user but setting list above
        use_list = (arcpy.ListRasters())  # run all layers in use location
    print use_list  # print use layers to be run for tracking
    count_use = len(use_list)  # gets count of use layers for tracking
    current_use = 1

    find_file_type = in_location_species.split(os.sep)  # breaks range or critical habitat by splitting the path
    # sets output path as range or critical habitat and L48 or NL48
    if 'Range' in find_file_type:
        if os.path.basename(use_location).startswith('CONUS'):
            out_res = out_res + os.sep + 'L48' + os.sep + 'Range'
        else:
            out_res = out_res + os.sep + 'NL48' + os.sep + 'Range'
    else:
        if os.path.basename(use_location).startswith('CONUS'):
            out_res = out_res + os.sep + 'L48' + os.sep + 'CriticalHabitat'

        else:
            out_res = out_res + os.sep + 'NL48' + os.sep + 'CriticalHabitat'
    # set up output folders if they don't exists
    create_directory(os.path.dirname(os.path.dirname(out_res)))
    create_directory(os.path.dirname(out_res))
    create_directory(out_res)
    # get a list of the input species files based on path set by user
    arcpy.env.workspace = in_location_species
    list_raster = (arcpy.ListRasters())
    # print list_raster
    #  raster that start with t- temp raster that was saved during a previous run - removed from list
    list_raster = [v for v in list_raster if v.split('_')[0] != 't']
    count_sp = len(list_raster)  # get a count of species for tracking
    print (list_raster)  # prints all species rasters for tracking
    # loops over all use layers
    for use_nm in use_list:
        start_use = datetime.datetime.now()  # start for use
        out_folder = out_res  # set output location
        use_path = use_location + os.sep + use_nm  # set input of use layer
        # based on region and which input use gdb is set by user; pulls snap rasters and sets final out folders
        if region != 'CONUS':
            snap_raster = snap_raster_dict[str(region)]
            if use_location.endswith('UseLayers.gdb'):
                out_folder = out_folder + os.sep + 'Agg_Layers'
                create_directory(out_folder)
            elif use_location.endswith('OnOffField.gdb'):
                out_folder = out_folder + os.sep + 'OnOffField'
                create_directory(out_folder)
        else:
            snap_raster = snap_raster_dict[str(region)]
            if use_location.endswith('UseLayers.gdb'):
                out_folder = out_folder + os.sep + 'Agg_Layers'
                create_directory(out_folder)
            elif use_location.endswith('OnOffField.gdb'):
                out_folder = out_folder + os.sep + 'OnOffField'
                create_directory(out_folder)
            elif use_location.endswith('Yearly.gdb'):
                out_folder = out_folder + os.sep + 'Indiv_Year_raw'
                create_directory(out_folder)
        # print statements for tracking
        print ('\nStarting use layer {0}, use {1} of {2}'.format(use_path, current_use, count_use))
        print ("loaded use in  {0}".format((datetime.datetime.now() - start_use)))
        print ('In location will be: {0}'.format(in_location_species))
        print ('Out location will be: {0}'.format(out_folder))
        print ('Snap raster used will be: {0}'.format(snap_raster))
        count = 1  # counter for species
        for raster_in in list_raster:  # loops of species files
            if raster_in in skip_species:  # bypasses species if in the skip list -used to run multiple instances
                continue
            else:
                # print statement for tracking
                print ("   \nWorking on {0} species file {1} of {2}".format(raster_in, count, count_sp))
                count += 1  # adds one to species count for next run
                in_sp = in_location_species + os.sep + raster_in  # full path to current species file
                try:  # try/expect loop if runs need to be done quickly: be sure to check if something fail and why
                    zonal_hist(in_sp, use_path, region, use_nm, temp_file, out_folder, snap_raster, use_path)
                except Exception as error:
                    print(error.args[0])
                    print "Failed on {0} with use {1}".format(raster_in, use_nm)
        # print statement with elapse time for all species for a use
        print "Completed use in {0}\n".format((datetime.datetime.now() - start_use))
        current_use += 1  # adds one to use counter for the next loop

    end = datetime.datetime.now()
    print "End Time: " + end.ctime()
    elapsed = end - start_time
    print "Elapsed  Time: " + str(elapsed)


main(out_results, in_location_species_base, uselist, in_location_species_folder)
