import os
import datetime
import pandas as pd

import arcpy
from arcpy.sa import *

# Title- runs Zonal Histogram for all sp union file against each other

in_location_spe = r'L:\ESA\UnionFiles_Winter2018\Range\SpCompRaster_byProjection\Grids_byProjection' \
                  r'\CONUS_Albers_Conical_Equal_Area\r_amphib'

out_results = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\Overlaping_species' \
              r'\overlapping_species'
temp_file = "temp_table"

previously_run = []

run_count = 0
sy_path = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\SpCompRaster_byProjection' \
          '\Grids_byProjection\Albers_Conical_Equal_Area\Symbology'

path, current_spe = os.path.split(in_location_spe)
symbologyLayer_in = sy_path + os.sep + current_spe + '.lyr'
use_location = path
arcpy.env.workspace = use_location
list_raster_use = (arcpy.ListRasters())
list_raster_use.remove(current_spe)
[list_raster_use.remove(v) for v in previously_run]
count_use = len(list_raster_use)
print list_raster_use


# Create a new GDB
def create_gdb(out_folder, out_name, out_path):
    if not arcpy.Exists(out_path):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


def zone(zone_sp, raster, temp_table, ):
    start_zone = datetime.datetime.now()
    arcpy.CreateTable_management("in_memory", temp_table)
    # temp = "in_memory\\temp_table"
    temp = "in_memory" + os.sep + temp_table
    arcpy.env.overwriteOutput = True
    arcpy.gp.ZonalHistogram_sa(zone_sp, "Value", raster, temp)
    print "Completed Zonal Histogram"
    return temp, start_zone


# loops runs zonal histogram for union files
def zonal_hist(in_zone, in_raster, set_raster_symbology, use_nm, temp_table, current_use):
    use_nm_folder = current_use
    if not os.path.exists(out_results + os.sep + use_nm_folder):
        os.mkdir(out_results + os.sep + use_nm_folder)
    symbology_loop = sy_path + os.sep + use_nm + '.lyr'
    out_tables = out_results + os.sep + use_nm_folder
    run_id = str(current_use) + "_" + str(use_nm)
    out_path_final = out_tables
    csv = run_id + '.csv'
    dbf = csv.replace('csv', 'dbf')

    if os.path.exists(out_path_final + os.sep + csv):
        print ("Already completed run for {0}".format(run_id))

    elif not os.path.exists(out_path_final + os.sep + dbf):
        print ("Running Statistics...for {0} v raster {1}".format(current_use, use_nm))
        arcpy.CheckOutExtension("Spatial")
        arcpy.MakeRasterLayer_management(Raster(in_zone), "zone")
        arcpy.ApplySymbologyFromLayer_management("zone", set_raster_symbology)
        arcpy.MakeRasterLayer_management(Raster(in_raster), "rd_lyr")
        arcpy.ApplySymbologyFromLayer_management("rd_lyr", symbology_loop)
        temp_return, start_loop = zone("zone", "rd_lyr", temp_table)

        list_fields = [f.name for f in arcpy.ListFields(temp_return)]
        att_array = arcpy.da.TableToNumPyArray(temp_return, list_fields)
        att_df = pd.DataFrame(data=att_array)
        att_df.to_csv(out_path_final + os.sep + csv)
        # arcpy.TableToTable_conversion(temp_return, out_path_final, dbf)

        print 'Final file can be found at {0}'.format(out_path_final + os.sep + csv)
        print "Completed in {0}\n".format((datetime.datetime.now() - start_time))

    elif not os.path.exists(out_path_final + os.sep + csv):
        list_fields = [f.name for f in arcpy.ListFields(out_path_final + os.sep + dbf)]
        att_array = arcpy.da.TableToNumPyArray((out_path_final + os.sep + dbf), list_fields)
        att_df = pd.DataFrame(data=att_array)
        att_df['LABEL'] = att_df['LABEL'].map(lambda x: x).astype(str)
        att_df.to_csv(out_path_final + os.sep + csv)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

in_use = in_location_spe
raster_file = Raster(in_location_spe)

for sp_nm in list_raster_use:
    use_path = use_location + os.sep + sp_nm
    print 'Starting spe layer {0} v {1},  {2} of {3}'.format(current_spe, sp_nm, run_count, count_use)
    try:
        zonal_hist(in_location_spe, use_path, symbologyLayer_in, sp_nm, temp_file, current_spe)
    except Exception as error:
        print(error.args[0])
        print "Failed on {0} with use {1}".format(current_spe, sp_nm)
    run_count += 1


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
