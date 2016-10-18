import os
import datetime
import functions

import arcpy
from arcpy.sa import *

# Title- runs Zonal Histogram for all sp union file against each use

inlocation_use = r'L:\Workspace\UseSites\ByProject\Conus_UseLayer.gdb'

in_HUC_base = 'L:\NHDPlusV2'
out_results = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_results'

symbologyLayer = 'L:\Workspace\UseSites\CDL_Reclass\Albers_Conical_Equal_Area_CONUS_Developed_euc.lyr'

print inlocation_use
arcpy.env.workspace = inlocation_use
count_use = len(arcpy.ListRasters())
current_use = 1
list_raster_use = (arcpy.ListRasters())
list_raster_use = [raster for raster in list_raster_use if not raster.startswith('zAlbers')]
print list_raster_use


# Create a new GDB
def create_gdb(out_folder, out_name, out_path):
    if not arcpy.Exists(out_path):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


def ZonalHist(inZoneData, inValueRaster, set_raster_symbology, use_nm, results_folder, HUC12):
    HUC12_value = HUC12.replace('NHDPlus', '_')
    out_tables = results_folder

    runID = use_nm + HUC12_value
    outpath_final = out_tables
    csv = runID + '.csv'
    gdb = (use_nm + '.gdb')

    if os.path.exists(outpath_final + os.sep + csv):
        print ("Already completed run for {0}".format(runID))
    else:
        print ("Running Statistics...for species group {0} and raster {1}".format(inZoneData, use_nm))
        arcpy.CheckOutExtension("Spatial")

        arcpy.Delete_management("rd_lyr")
        arcpy.MakeRasterLayer_management(Raster(inValueRaster), "rd_lyr")
        arcpy.ApplySymbologyFromLayer_management("rd_lyr", set_raster_symbology)

        arcpy.Delete_management("HUC12_lyr")
        arcpy.MakeFeatureLayer_management(inZoneData, "HUC12_lyr")

        start_zone = datetime.datetime.now()

        dbf_gdb = out_folder + os.sep + gdb + os.sep + runID
        if not arcpy.Exists(dbf_gdb):
            print dbf_gdb
            arcpy.gp.ZonalHistogram_sa("HUC12_lyr", "HUC_12", "rd_lyr", dbf_gdb)
            print "Completed Zonal Histogram"
        # if not os.path.exists(outpath_final+os.sep+csv):
        #     list_fields = [f.name for f in arcpy.ListFields(dbf_gdb)]
        #     att_array = arcpy.da.TableToNumPyArray (dbf_gdb, list_fields)
        #     att_df = pd.DataFrame(data=att_array)
        #     att_df.to_csv(outpath_final+os.sep+csv)
        #     print 'Final file can be found at {0}'.format(outpath_final + os.sep + csv)
        print "Completed in {0}\n".format((datetime.datetime.now() - start_zone))


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

count_use = len(list_raster_use)
list_dir = os.listdir(in_HUC_base)
list_HUC2 = [huc2 for huc2 in list_dir if huc2.startswith('NHDPlus')]

count_HUC2 = len(list_HUC2)
current_use = 0
for use_nm in list_raster_use:

    gdb = (use_nm + '.gdb')
    use_path = inlocation_use + os.sep + use_nm
    count = 0
    for value in list_HUC2:

        HUC12_path = in_HUC_base + os.sep + value + os.sep + 'WBDSnapshot\WBD\WBD_Subwatershed.shp'
        out_folder = out_results + os.sep + value

        if not os.path.exists(out_folder):
            os.mkdir(out_folder)
        create_gdb(out_folder, gdb, out_folder + os.sep + gdb)
        arcpy.env.workspace = out_folder + os.sep + gdb
        print "\nWorking on uses for {0} HUC file {1} of {2}".format(value, count, len(list_HUC2))
        try:
            ZonalHist(HUC12_path, use_path, symbologyLayer, use_nm, out_folder, value)

        except Exception as error:
            print(error.args[0])
            print "Failed on {0} with use {1}".format(value, use_nm)

        count += 1

    current_use += 1

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
