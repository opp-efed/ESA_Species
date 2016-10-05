import os
import datetime
import functions

import arcpy
from arcpy.sa import *

# Title- runs Zonal Histogram for all sp union file against each use

# in folder with many gdbs or a single gdb
# inlocation_species = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\CH_SpCompRaster_byProjection\Albers_Conical_Equal_Area'

# 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\SpCompRaster_byProjection\Albers_Conical_Equal_Area'
inlocation_species = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\CH_SpCompRaster_byProjection\Albers_Conical_Equal_Area'
#
    #'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\CH_SpCompRaster_byProjection\Albers_Conical_Equal_Area'
#
region = 'CONUS'

Range = False
temp_file = "temp_table2"

# set to a no zero number to skip x raster in the inlocation
start_file = 0
# raster must be set to unique values as symbology to run raster histogram

# Use sites

use_location_base = 'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject'
if Range:
    out_results = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\results'

else:
    out_results = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\results'

symbology_dict = {
    'CONUS': 'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject\SymblogyLayers\Albers_Conical_Equal_Area_CONUS_CattleEarTag_euc.lyr',
    'HI': 'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject\SymblogyLayers\NAD_1983_UTM_Zone__4N_HI_Ag_euc.lyr',
    'AK': 'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_Albers_AK_Ag_euc.lyr',
    'AS': 'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_UTM_Zone__2S_AS_Ag_euc.lyr',
    'CNMI': 'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_UTM_Zone_55N_CNMI_Ag_euc.lyr',
    'GU': 'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_UTM_Zone_55N_GU_Ag_euc.lyr',
    'PR': 'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject\SymblogyLayers\NAD_1983_StatePlane_Puerto_Rico_Virgin_Isl_FIPS_5200_PR_Ag_euc.lyr',
    'VI': 'C:\Users\Admin\Documents\Jen\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_UTM_Zone_20N_VI_Ag_euc.lyr'}

symbologyLayer = symbology_dict[region]
use_location = use_location_base + os.sep + str(region) + "_UseLayer.gdb"
print use_location
arcpy.env.workspace = use_location
count_use = len(arcpy.ListRasters())
current_use = 1
list_raster_use = (arcpy.ListRasters())
print list_raster_use


# recursively looks for all raster in workspace
def rasters_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for raster in arcpy.ListRasters():
        yield (raster)
    for ws in arcpy.ListWorkspaces():
        for raster in rasters_in_workspace(ws):
            yield raster


# Create a new GDB
def create_gdb(out_folder, out_name, out_path):
    if not arcpy.Exists(out_path):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


def zone(zone, raster, temp_table, extent):
    start_zone = datetime.datetime.now()
    arcpy.env.extent= extent
    arcpy.CreateTable_management("in_memory", temp_table)
    # temp = "in_memory\\temp_table"
    temp = "in_memory" + os.sep + temp_table

    arcpy.env.overwriteOutput = True

    arcpy.gp.ZonalHistogram_sa(zone, "Value", raster, temp)
    try:

        arcpy.AddField_management(temp, 'TableID', "TEXT", "", "", "100", "", "NULLABLE",
                                  "NON_REQUIRED", "")

        arcpy.CalculateField_management(temp, "TableID", "!OBJECTID!", "PYTHON_9.3", "")
    except:
        pass
    print "Completed Zonal Histogram"

    return temp, start_zone


# loops runs zonal histogram for union files
def ZonalHist(inZoneData, inValueRaster, set_raster_symbology, region_c, use_nm, temp_table,extent):

    # In paths
    path_fc, in_species = os.path.split(inZoneData)
    sp_group = in_species.split("_")[1]

    # out paths
    break_use = use_path.split("_")
    break_bool = False
    use_nm_folder = region_c

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

    if not os.path.exists(out_results + os.sep + use_nm_folder):
        os.mkdir(out_results + os.sep + use_nm_folder)

    out_tables = out_results + os.sep + use_nm_folder
    runID = in_species + "_" + use_nm_folder
    outpath_final = out_tables
    csv = runID + '.csv'

    if os.path.exists(outpath_final + os.sep + csv):
        print ("Already completed run for {0}".format(runID))
    else:
        print ("Running Statistics...for species group {0} and raster {1}".format(sp_group, use_nm))
        arcpy.CheckOutExtension("Spatial")

        arcpy.MakeRasterLayer_management(Raster(inZoneData), "zone")
        arcpy.MakeRasterLayer_management(Raster(inValueRaster), "rd_lyr")
        arcpy.ApplySymbologyFromLayer_management("rd_lyr", set_raster_symbology)
        temp_return, start_time = zone("zone", "rd_lyr", temp_table,extent)

        arcpy.TableToTable_conversion(temp_return, outpath_final, csv)
        dbf = csv.replace('csv', 'dbf')
        arcpy.TableToTable_conversion(temp_return, outpath_final, dbf)
        print 'Final file can be found at {0}'.format(outpath_final + os.sep + csv)
        print "Completed in {0}\n".format((datetime.datetime.now() - start_time))


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
        if count < start_file:
            print in_sp
            continue
        else:
            print raster_in
            raster_file = Raster(in_sp)
            sp_extent = raster_file.extent
            extent = "{0} {1} {2} {3}".format(str(sp_extent.XMin), str(sp_extent.YMin),str(sp_extent.XMax),str(sp_extent.YMax))
            print extent
            print "\nWorking on uses for {0} species file {1} of {2}".format(raster_in, count, count_sp)
            for use_nm in list_raster_use:
                use_path = use_location + os.sep + use_nm
                print 'Starting use layer {0}, use {1} of {2}'.format(use_path,current_use, count_use)
                try:
                    ZonalHist(in_sp, use_path, symbologyLayer, region, use_nm, temp_file,extent)
                except Exception as error:
                    print(error.args[0])
                    print "Failed on {0} with use {1}".format(raster_in, use_nm)
                current_use += 1
            current_use =1

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
