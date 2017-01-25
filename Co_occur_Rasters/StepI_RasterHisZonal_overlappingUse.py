import os
import datetime
import pandas as pd

import arcpy
from arcpy.sa import *

# Title- runs Zonal Histogram for all sp union file against each use
# TODO add snap layer to zonal histogram
# in folder with many gdbs or a single gdb

# r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\SpCompRaster_byProjection\Grids_byProjection\Albers_Conical_Equal_Area'
# r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\CriticalHabitat\SpCompRaster_byProjection\Grids_byProjection\Albers_Conical_Equal_Area'

inlocation_use = r'L:\Workspace\UseSites\ByProject\CONUS_UseLayer.gdb\Albers_Conical_Equal_Area_CONUS_CDL_1015_20x2_euc'

out_results = r'L:\Workspace\UseSites\ByProject\Overlapping_Use'
temp_file = "temp_table"
region = 'CONUS'
previously_run = ['zAlbers_Conical_Equal_Area_CONUS_CDL_1015_30x2_euc',
                  'Albers_Conical_Equal_Area_CONUS_CDL_1015_10x2_euc', 'Albers_Conical_Equal_Area_CONUS_Nurseries_euc',
                  'Albers_Conical_Equal_Area_CONUS_ROW_euc', 'Albers_Conical_Equal_Area_CONUS_OSD_euc']

# ['zAlbers_Conical_Equal_Area_CONUS_CDL_1015_30x2_euc','Albers_Conical_Equal_Area_CONUS_CDL_1015_10x2_euc','Albers_Conical_Equal_Area_CONUS_Nurseries_euc',
#                'Albers_Conical_Equal_Area_CONUS_ROW_euc','Albers_Conical_Equal_Area_CONUS_OSD_euc','Albers_Conical_Equal_Area_CONUS_CDL_1015_20x2_euc']
run_count = 0

symbology_dict = {
    'CONUS': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\Albers_Conical_Equal_Area_CONUS_CDL_1015_100x2_euc.lyr',
    # 'HI': r'L:\Workspace\UseSites\ByProject\SymblogyLayers\NAD_1983_UTM_Zone__4N_HI_Ag_euc.lyr',
    # 'AK': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_Albers_AK_CattleEarTag_euc.lyr',
    # 'AS': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_UTM_Zone__2S_AS_Ag_euc.lyr',
    # 'CNMI': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_UTM_Zone_55N_CNMI_Ag_euc.lyr',
    # 'GU': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_UTM_Zone_55N_GU_Ag_euc.lyr',
    # 'PR': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\NAD_1983_StatePlane_Puerto_Rico_Virgin_Isl_FIPS_5200_PR_Ag_euc.lyr',
    # 'VI': 'L:\Workspace\UseSites\ByProject\SymblogyLayers\WGS_1984_UTM_Zone_20N_VI_Ag_euc.lyr'}
}

symbologyLayer = symbology_dict[region]
path, current_use = os.path.split(inlocation_use)
use_location = path
print use_location

arcpy.env.workspace = use_location
list_raster_use = (arcpy.ListRasters())
list_raster_use.remove(current_use)
for i in previously_run:
    if i == current_use:
        pass
    else:
        list_raster_use.remove(i)
print list_raster_use
count_use = len(list_raster_use)
print list_raster_use


# Create a new GDB
def create_gdb(out_folder, out_name, out_path):
    if not arcpy.Exists(out_path):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


def zone(zone, raster, temp_table, ):
    start_zone = datetime.datetime.now()

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
def ZonalHist(inZoneData, inValueRaster, set_raster_symbology, region_c, use_nm, temp_table, current_use):
    use_nm_folder = current_use
    if not os.path.exists(out_results + os.sep + use_nm_folder):
        os.mkdir(out_results + os.sep + use_nm_folder)

    out_tables = out_results + os.sep + use_nm_folder
    runID = str(current_use) + "_" + str(use_nm)
    outpath_final = out_tables
    csv = runID + '.csv'
    dbf = csv.replace('csv', 'dbf')

    if os.path.exists(outpath_final + os.sep + csv):
        print ("Already completed run for {0}".format(runID))

    elif not os.path.exists(outpath_final + os.sep + dbf):
        print ("Running Statistics...for {0} and raster {1}".format(current_use, use_nm))
        arcpy.CheckOutExtension("Spatial")

        arcpy.MakeRasterLayer_management(Raster(inZoneData), "zone")
        arcpy.MakeRasterLayer_management(Raster(inValueRaster), "rd_lyr")
        arcpy.ApplySymbologyFromLayer_management("rd_lyr", set_raster_symbology)
        temp_return, start_time = zone("zone", "rd_lyr", temp_table)
        list_fields = [f.name for f in arcpy.ListFields(temp_return)]
        att_array = arcpy.da.TableToNumPyArray((temp_return), list_fields)
        att_df = pd.DataFrame(data=att_array)
        att_df.to_csv(outpath_final + os.sep + csv)

        # arcpy.TableToTable_conversion(temp_return, outpath_final, csv)
        arcpy.TableToTable_conversion(temp_return, outpath_final, dbf)

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

in_use = inlocation_use
raster_file = Raster(inlocation_use)

print "\nWorking on use {0}".format(current_use)
for use_nm in list_raster_use:
    use_path = use_location + os.sep + use_nm
    print 'Starting use layer {0}, use {1} of {2}'.format(use_nm, run_count, count_use)
    try:
        ZonalHist(inlocation_use, use_path, symbologyLayer, region, use_nm, temp_file, current_use)
    except Exception as error:
        print(error.args[0])
        print "Failed on {0} with use {1}".format(current_use, use_nm)
    run_count += 1
run_count = 1

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
