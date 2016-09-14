import arcpy
import os
import datetime
from arcpy import env
import arcpy.cartography as CA
# #Tile: Will run the simplify polygon tool on files with geometry error that cannot be correct with the repair geo tool
outgdb = r"C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\NAD83_SinglePartALL\Topo\Simplified_topoCorrected_singlepart.gdb"

ingdb = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\NAD83\\topo\Simplified_topoCorrected.gdb'


# recursively checks workspaces found within the inFileLocation and makes list of all feature class
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield (fc)


# Create a new GDB
def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)

path, gdb = os.path.split(outgdb)
create_gdb(path, gdb, outgdb)

arcpy.env.workspace = ingdb
fclist = arcpy.ListFeatureClasses()

for fc in fclist:
    infile = ingdb + os.sep + fc
    outfile = outgdb + os.sep + fc
    if not arcpy.Exists(outfile):
        print "Print trying to simplify {0}".format(fc)
        CA.SimplifyPolygon(infile, outfile, "POINT_REMOVE", "30 Meters", "0 Unknown", "RESOLVE_ERRORS",
                           "KEEP_COLLAPSED_POINTS")
    else:
        continue

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
