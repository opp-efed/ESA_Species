import os
import datetime
import pandas as pd
import arcpy
from arcpy.sa import *

# Title- runs Zonal Histogram against specified use sites to the HUC12 file bu HUC2; Generates raw results
# TODO generate list of raster in the in_location_use gdb to be used as the input for the use_included list

# ##User input variables
# # location of use site to runt
# in_location_use =r'L:\Workspace\UseSites\CDL_Reclass\CDL_reclass_2010_2015.gdb'
in_location_use = r'L:\Workspace\UseSites\ByProject\CONUS_UseLayer.gdb'

# Location of spatial library od NHDPluse
in_HUC_base = 'L:\NHDPlusV2'
# sub-directory folder where shapefile
tail = 'WBDSnapshot\WBD\WBD_Subwatershed.shp'

# location of results
# out_results = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\L48\HUC12\YearlyCDL'
out_results = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\L48\HUC12\AAs'
# Symbology layer so that the unique values can be applied to use layer before running zonal stats
# # NOTE NOTE if the symbology layer is corrupt and the default is used of stretch symbology then result will be the
# # 0-255 rather than being based on the raster value of the use layer (euc 0-1500, raw- cdl codes
symbologyLayer = r'L:\Workspace\UseSites\ByProject\SymblogyLayers' \
                 r'\Albers_Conical_Equal_Area_CONUS_Malathion_UseFootprint_woAdulticide_160815_euc.lyr'
# symbologyLayer = r'L:\Workspace\UseSites\CDL_Reclass\CDL_2014_rec.lyr'
# List of uses to include
# use_included = ['CDL_2010_rec','cdl_2011_rec','CDL_2012_rec','CDL_2013_rec','CDL_2014_rec','CDL_2015_rec']
use_included = ['Albers_Conical_Equal_Area_CONUS_Malathion_UseFootprint_160815_euc']


# Create a new GDB
def create_gdb(outfolder, out_name, out_path):
    if not arcpy.Exists(out_path):
        arcpy.CreateFileGDB_management(outfolder, out_name, "CURRENT")

# Run Zonal Histogram
def ZonalHist(inZoneData, inValueRaster, set_raster_symbology, use_nm, results_folder, HUC12, use_gdb):
    # parse out information needed for file names
    huc_12_value = HUC12.replace('NHDPlus', '_')
    run_id = use_nm + huc_12_value
    # Generate temp layers to run; Zonal histogram runs against the layers not objects
    arcpy.CheckOutExtension("Spatial")
    arcpy.Delete_management("rd_lyr")
    arcpy.MakeRasterLayer_management(Raster(inValueRaster), "rd_lyr")
    arcpy.ApplySymbologyFromLayer_management("rd_lyr", set_raster_symbology)
    arcpy.Delete_management("HUC12_lyr")
    arcpy.MakeFeatureLayer_management(inZoneData, "HUC12_lyr")
    # timer
    start_zone = datetime.datetime.now()
    # output files
    dbf_gdb = results_folder + os.sep + use_gdb + os.sep + run_id
    csv = results_folder + os.sep + run_id + '.csv'
    # runs zonal stats
    if not arcpy.Exists(dbf_gdb):
        print ("Running Statistics...for species group {0} and raster {1}".format(inZoneData, use_nm))
        print dbf_gdb
        arcpy.gp.ZonalHistogram_sa("HUC12_lyr", "HUC_12", "rd_lyr", dbf_gdb)
        print "Completed Zonal Histogram"
    # exports dbf to csv
    if not os.path.exists(csv):
        print csv
        list_fields = [f.name for f in arcpy.ListFields(dbf_gdb)]
        att_array = arcpy.da.TableToNumPyArray(dbf_gdb, list_fields)
        att_df = pd.DataFrame(data=att_array)
        att_df['LABEL'] = att_df['LABEL'].map(lambda x: x).astype(str)
        att_df.to_csv(csv)
        print 'Final file can be found at {0}'.format(csv)
    print "Completed in {0}".format((datetime.datetime.now() - start_zone))

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# generates list of all HUC2 with NHD data to be used for file structure
list_dir = os.listdir(in_HUC_base)
list_HUC2 = [huc2 for huc2 in list_dir if huc2.startswith('NHDPlus')]

# Generates list of use raster that will be included based on use_included list
arcpy.env.workspace = in_location_use
current_use = 1
# all rasters in in_location_use gdb
list_raster_use = (arcpy.ListRasters())
# filters list to those in the use_included list - user input
list_raster_use = [raster for raster in list_raster_use if raster in use_included]
count_use = len(list_raster_use)
print list_raster_use

for use_nm in list_raster_use:  # loops through all use raster to be included
    gdb = (use_nm + '.gdb')
    use_path = in_location_use + os.sep + use_nm
    count = 1
    for value in list_HUC2:  # loop all NHD folders for each use
        HUC12_path = in_HUC_base + os.sep + value + os.sep + tail
        out_folder = out_results + os.sep + value
        if not os.path.exists(out_folder):
            os.mkdir(out_folder)
        create_gdb(out_folder, gdb, out_folder + os.sep + gdb)
        arcpy.env.workspace = out_folder + os.sep + gdb
        print "\nWorking on uses for {0} HUC file {1} of {2}".format(value, count, len(list_HUC2))
        try:
            ZonalHist(HUC12_path, use_path, symbologyLayer, use_nm, out_folder, value, gdb)
        except Exception as error:
            print(error.args[0])
            print "Failed on {0} with use {1}".format(value, use_nm)
        count += 1
    current_use += 1

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
