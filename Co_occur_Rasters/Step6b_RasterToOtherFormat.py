import arcpy
import os
import datetime

in_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\SpCompRaster_byProjection'


def create_folder(folder):
    if not os.path.exists(folder):
        os.mkdir(folder)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

out_folder = in_folder + os.sep + 'Grids_byProjection'
create_folder(out_folder)
list_gdb = os.listdir(in_folder)
list_gdb = [gdb for gdb in list_gdb if gdb.endswith('gdb')]
print list_gdb

for gdb in list_gdb:
    in_gdb = in_folder + os.sep + gdb
    out_location = out_folder + os.sep + gdb.replace('.gdb', '')
    if os.path.exists(out_location):
        continue
    create_folder(out_location)
    arcpy.env.workspace = in_gdb
    raster_list = arcpy.ListRasters()
    start_conversion = datetime.datetime.now()
    print 'Starting the conversion for {0} in {1}\n'.format(raster_list, gdb)
    arcpy.RasterToOtherFormat_conversion(raster_list, out_location, "GRID")
    print 'Completed conversion in: {0}\n'.format((datetime.datetime.now() - start_conversion))

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
