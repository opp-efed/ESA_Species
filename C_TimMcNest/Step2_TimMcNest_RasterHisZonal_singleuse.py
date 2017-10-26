import os
import datetime
import pandas as pd

import arcpy
from arcpy.sa import *

# Title- runs Zonal Histogram for all sp union file against each use
# TODO add snap layer to zonal histogram
# in folder with many gdbs or a single gdb
inlocation_species = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TimMcNest\Clipped_GAP.gdb'

Range = True
temp_file = "temp_table1"
region = 'CONUS'

use_location = r'L:\Workspace\UseSites\CDL_Reclass\CDL_GenClass11_1015_161031_euc.gdb'
print use_location
arcpy.env.workspace = use_location

use_list = (arcpy.ListRasters())
# set to a no zero number to skip x raster in the inlocation
start_file = 0
# raster must be set to unique values as symbology to run raster histogram

if Range:
    outpath_final = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TimMcNest\Results_Fall2016\Indiv_Year_Drift\CONUS'
else:
    outpath_final =r''

symbology_dict = {
    'CONUS': r'L:\Workspace\UseSites\ByProject\SymblogyLayers\Albers_Conical_Equal_Area_CONUS_CattleEarTag_euc.lyr'
}

symbologyLayer = symbology_dict[region]

current_use = 1
arcpy.env.workspace = use_location
list_raster_use = (arcpy.ListRasters())
list_raster_use = [raster for raster in list_raster_use if raster in use_list]
count_use = len(list_raster_use)
print list_raster_use


# recursively looks for all raster in workspace

# Create a new GDB
def create_gdb(out_folder, out_name, out_path):
    if not arcpy.Exists(out_path):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


def zone(zone, raster, temp_table, outpath_final, dbf):
    start_zone = datetime.datetime.now()
    arcpy.MakeRasterLayer_management(
        r'L:\Workspace\ESA_Species\Step3\Step3_Proposal\GAP\layerfiles_use_to_change_legend_in _ArcMap\6EcolSys_landuse.lyr',
        "snap")
    arcpy.env.snapRaster = "snap"

    arcpy.CreateTable_management("in_memory", temp_table)
    temp = "in_memory" + os.sep + temp_table
    arcpy.env.overwriteOutput = True
    arcpy.gp.ZonalHistogram_sa(zone, "Value", raster, temp)
    try:
        arcpy.AddField_management(temp, 'TableID', "TEXT", "", "", "100", "", "NULLABLE", "NON_REQUIRED", "")
        arcpy.CalculateField_management(temp, "TableID", "!OBJECTID!", "PYTHON_9.3", "")
    except:
        pass
    print "Completed Zonal Histogram"

    try:
        arcpy.TableToTable_conversion(temp, outpath_final, dbf)
    except:
        pass
    list_fields = [f.name for f in arcpy.ListFields(temp)]
    att_array = arcpy.da.TableToNumPyArray((temp), list_fields)
    att_df = pd.DataFrame(data=att_array)

    return att_df, start_zone


# loops runs zonal histogram for union files
def ZonalHist(inZoneData, inValueRaster, set_raster_symbology, region_c, use_nm, temp_table):
    # In paths

    path_fc, in_species = os.path.split(inZoneData)
    sp_group = in_species.split("_")[5]

    runID = in_species + "_" + use_nm
    csv = runID + '.csv'
    dbf = csv.replace('csv', 'dbf')

    if os.path.exists(outpath_final + os.sep + csv):
        print ("Already completed run for {0}".format(runID))

    elif not os.path.exists(outpath_final + os.sep + dbf):
        print ("Running Statistics...for species {0} and raster {1}".format(sp_group, use_nm))
        arcpy.CheckOutExtension("Spatial")

        arcpy.MakeRasterLayer_management(Raster(inZoneData), "zone")
        arcpy.MakeRasterLayer_management(Raster(inValueRaster), "rd_lyr")
        arcpy.ApplySymbologyFromLayer_management("rd_lyr", set_raster_symbology)
        temp_return, start_time = zone("zone", "rd_lyr", temp_table, outpath_final, dbf)

        temp_return['LABEL'] = temp_return['LABEL'].map(lambda x: x).astype(str)
        temp_return.to_csv(outpath_final + os.sep + csv)
        print 'Final file can be found at {0}'.format(outpath_final + os.sep + csv)
        print "Completed in {0}\n".format((datetime.datetime.now() - start_time))

    elif not os.path.exists(outpath_final + os.sep + csv):
        list_fields = [f.name for f in arcpy.ListFields(outpath_final + os.sep + dbf)]
        att_array = arcpy.da.TableToNumPyArray((outpath_final + os.sep + dbf), list_fields)
        att_df = pd.DataFrame(data=att_array)
        att_df['LABEL'] = att_df['LABEL'].map(lambda x: x).astype(str)
        att_df.to_csv(outpath_final + os.sep + csv)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

count = 0

path, gdb = os.path.split(inlocation_species)

arcpy.env.workspace = inlocation_species
count_sp = len(arcpy.ListRasters())
count = 0
list_raster = (arcpy.ListRasters())


[list_raster.remove(raster) for raster in list_raster if raster.startswith('z')]
for raster_in in list_raster:
    count += 1
    in_sp = inlocation_species + os.sep + raster_in

    if count < start_file:
        print in_sp
        continue
    else:
        print raster_in
        raster_file = Raster(in_sp)

        print "\nWorking on uses for {0} species file {1} of {2}".format(raster_in, count, count_sp)
        for use_nm in list_raster_use:
            use_path = use_location + os.sep + use_nm
            print 'Starting use layer {0}, use {1} of {2}'.format(use_path, current_use, count_use)
            try:
                ZonalHist(in_sp, use_path, symbologyLayer, region, use_nm, temp_file)
            except Exception as error:
                print(error.args[0])
                print "Failed on {0} with use {1}".format(raster_in, use_nm)
            current_use += 1
        current_use = 1

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
