import arcpy
import os
import datetime
from arcpy.sa import *

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# folder or gdb with projected files
in_folder = r'path\filename.gdb'
# snap raster dictionary
RegionalProjection_Dict = {
    'CONUS': r'path\Albers_Conical_Equal_Area_cultmask_2016',
    'HI': r'path\NAD_1983_UTM_Zone_4N_HI_Ag',
    'AK': r'path\WGS_1984_Albers_AK_Ag',
    'AS': r'path\WGS_1984_UTM_Zone_2S_AS_Ag',
    'CNMI': r'path\WGS_1984_UTM_Zone_55N_CNMI_Ag',
    'GU': r'path\WGS_1984_UTM_Zone_55N_GU_Ag_30',
    'PR': r'path\Albers_Conical_Equal_Area_PR_Ag',
    'VI': r'path\WGS_1984_UTM_Zone_20N_VI_Ag_30'}


def create_folder(folder):
    if not os.path.exists(folder):
        os.mkdir(folder)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()


def raster_other_format(in_gdb_path, snap_dict, region_c, c_gdb, out_path):
    arcpy.env.workspace = in_gdb_path
    raster_list = arcpy.ListRasters()
    snap_raster = Raster(snap_dict[region_c])
    arcpy.Delete_management("snap")
    arcpy.MakeRasterLayer_management(snap_dict[region_c], "snap")
    arcpy.env.snapRaster = "snap"
    myExtent = snap_raster.extent
    arcpy.env.extent = myExtent
    start_conversion = datetime.datetime.now()
    print 'Starting the conversion for {0} in {1}\n'.format(raster_list, c_gdb)
    arcpy.RasterToOtherFormat_conversion(raster_list, out_path, "GRID")


    print 'Completed conversion in: {0}\n'.format((datetime.datetime.now() - start_conversion))


if in_folder.endswith('gdb'):
    in_gdb = in_folder
    director_name, gdb = os.path.split(in_folder)
    create_folder(director_name)

    out_folder = director_name + os.sep + 'Grids_byProjection'
    create_folder(out_folder)
    out_location = out_folder + os.sep + gdb.replace('.gdb', '')
    create_folder(out_location)
    c_region = gdb.split("_")[0]
    out_location = out_folder + os.sep + gdb.replace('.gdb', '')
    raster_other_format(in_gdb, RegionalProjection_Dict, c_region, gdb, out_location)

else:
    list_gdb = os.listdir(in_folder)
    list_gdb = [gdb for gdb in list_gdb if gdb.endswith('gdb')]
    print list_gdb
    out_folder = in_folder + os.sep + 'Grids_byProjection'
    create_folder(out_folder)

    for gdb in list_gdb:
        in_gdb = in_folder + os.sep + gdb
        c_region = gdb.split("_")[0]
        out_location = out_folder + os.sep + gdb.replace('.gdb', '')
        if os.path.exists(out_location):
            continue
        create_folder(out_location)
        raster_other_format(in_gdb, RegionalProjection_Dict, c_region, gdb, out_location)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
