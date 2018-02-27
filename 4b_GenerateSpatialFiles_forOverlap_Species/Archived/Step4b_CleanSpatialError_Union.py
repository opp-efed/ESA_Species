import os
import datetime

import arcpy

## TODO: Do we need this?
SingleRaster = True
inlocation = r'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\scratch.gdb\CH_Amphibians_Union_Final_20160811_ClippedRegions_20160816'
# Species to include or exclude depending on if use is running all sp group
skipgroup = []

# location for the final smoothed filee
out_location = r'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\CH_Smoothed_Rasters_20160819.gdb'

# Static variable

file_suffix = '_Smooth_Final_20160811'


def clean_sp_raster_files(in_ws, single_raster, skip_group, out_ws):
    arcpy.CheckOutExtension("Spatial")
    if single_raster:
        raster_list = [in_ws]
    else:
        raster_list = arcpy.ListFeatureClasses()

    for raster in raster_list:
        sp_group = raster.split("_")[1]  # split file name to extract sp group
        if sp_group in skip_group:
            continue
        else:
            if single_raster:
                path, file_name = os.path.split(raster)
                out_raster = out_ws + os.sep + file_name + file_suffix
                in_raster = raster
            else:
                in_raster = in_ws + os.sep + raster
                out_raster = out_ws + os.sep + raster + file_suffix
            if not arcpy.Exists(out_raster):
                start_process = datetime.datetime.now()
                print "\nStarting {0} at {1}".format(out_raster, (datetime.datetime.now() - start_process))
                arcpy.env.workspace = in_ws

                # Execute BoundaryClean
                try:
                    arcpy.gp.BoundaryClean_sa(in_raster, out_raster, "ASCEND", "TWO_WAY")
                    print "\nCreated output {0} in {1}".format(out_raster, (datetime.datetime.now() - start_process))

                except Exception as error:
                    print(error.args[0])
                    arcpy.Delete_management(out_raster)


            else:
                print '\nAlready union {0}'.format(out_raster)


def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


start_script = datetime.datetime.now()
print "Started at {0}".format(start_script)

folder, gdb = os.path.split(out_location)
create_gdb(folder, gdb, out_location)
# loop through gdb from in location get a list of raster unions, and run the clean boundary tool to
# smooth the boundary to address slivers
if SingleRaster:
    clean_sp_raster_files(inlocation, SingleRaster, skipgroup, out_location)

elif inlocation[-3:] == 'gdb':
    ingdb = inlocation
    clean_sp_raster_files(ingdb, SingleRaster, skipgroup, out_location)

else:
    list_ws = os.listdir(inlocation)
    print list_ws
    for v in list_ws:
        if v[-3:] == 'gdb':
            ingdb = inlocation
            clean_sp_raster_files(ingdb, SingleRaster, skipgroup, out_location)

print "Completed in {0}".format(datetime.datetime.now() - start_script)
