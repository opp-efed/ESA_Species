import os
import datetime
import pandas as pd
import arcpy
from arcpy.sa import *

# Title- runs Zonal Histogram for all sp union file against each use

# in folder with many gdbs or a single gdb

inlocation_species = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range' \
                     r'\SpCompRaster_byProjection\Grids_byProjection\Albers_Conical_Equal_Area'

region = 'CONUS'

Range = True
temp_file = "temp_table5"

if Range:
    out_results = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\L48\Indiv_Year_raw' \
                  r'\Range'

else:
    out_results = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\L48\Indiv_Year_raw' \
                  r'\CriticalHabitat'

# NOTE NOTE MAKES SURE THE SYBOLOGY LAYER HAS ALL OF THE CATEGORIES, CDL 2013 has one more than the other years
symbologyLayer = r'L:\Workspace\UseSites\ByProject\SymblogyLayers\CDL_2013_rec.lyr'
use_location = r"L:\Workspace\UseSites\CDL_Reclass\161031\CDL_Reclass_1015_161031.gdb"

arcpy.env.workspace = use_location
# use_list =[ u'Albers_Conical_Equal_Area_CONUS_CDL_2015_rec']
use_list = (arcpy.ListRasters())
list_raster_use = (arcpy.ListRasters())
list_raster_use = [raster for raster in list_raster_use if raster in use_list]
count_use = len(list_raster_use)
current_use = 1


def zone(zone_lyr, raster_lyr, temp_table, ):

    start_zone = datetime.datetime.now()
    arcpy.CreateTable_management("in_memory", temp_table)
    temp = "in_memory" + os.sep + temp_table
    arcpy.env.overwriteOutput = True
    arcpy.gp.ZonalHistogram_sa(zone_lyr, "Value", raster_lyr, temp)
    print "Completed Zonal Histogram"

    return temp, start_zone


# loops runs zonal histogram for union files
def zonal_hist(in_zone, in_value_raster, set_raster_symbology, region_c, use_name, temp_table):
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

    if not os.path.exists(out_results + os.sep + use_nm_folder):
        os.mkdir(out_results + os.sep + use_nm_folder)

    out_tables = out_results + os.sep + use_nm_folder
    run_id = in_species + "_" + use_nm_folder
    out_path_final = out_tables

    csv = run_id + '.csv'
    dbf = csv.replace('csv', 'dbf')

    if os.path.exists(out_path_final + os.sep + csv):
        print ("Already completed run for {0}".format(run_id))

    elif not os.path.exists(out_path_final + os.sep + csv):
        print ("Running Statistics...for species group {0} and raster {1}".format(sp_group, use_name))
        arcpy.CheckOutExtension("Spatial")

        arcpy.MakeRasterLayer_management(Raster(in_zone), "zone")
        arcpy.MakeRasterLayer_management(Raster(in_value_raster), "rd_lyr")
        arcpy.ApplySymbologyFromLayer_management("rd_lyr", set_raster_symbology)
        temp_return, zone_time = zone("zone", "rd_lyr", temp_table)

        list_fields = [f.name for f in arcpy.ListFields(out_path_final + os.sep + dbf)]
        att_array = arcpy.da.TableToNumPyArray((out_path_final + os.sep + dbf), list_fields)
        att_df = pd.DataFrame(data=att_array)
        att_df['LABEL'] = att_df['LABEL'].map(lambda x: x).astype(str)
        att_df.to_csv(out_path_final + os.sep + csv)
        print 'Final file can be found at {0}'.format(out_path_final + os.sep + csv)
        print "Completed in {0}\n".format((datetime.datetime.now() - zone_time))

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

arcpy.env.workspace = inlocation_species
count_sp = len(arcpy.ListRasters())
count = 0
list_raster = (arcpy.ListRasters())

for raster_in in list_raster:
    count += 1
    in_sp = inlocation_species + os.sep + raster_in
    raster_file = Raster(in_sp)
    print "\nWorking on uses for {0} species file {1} of {2}".format(raster_in, count, count_sp)
    for use_nm in list_raster_use:
        split_use_nm = use_nm.split("_")
        symbology_flag = split_use_nm[(len(split_use_nm) - 1)]
        use_path = use_location + os.sep + use_nm
        print 'Starting use layer {0}, use {1} of {2}'.format(use_path, current_use, count_use)
        try:
            zonal_hist(in_sp, use_path, symbologyLayer, region, use_nm, temp_file)
        except Exception as error:
            print(error.args[0])
            print "Failed on {0} with use {1}".format(raster_in, use_nm)
        current_use += 1
    current_use = 1


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
