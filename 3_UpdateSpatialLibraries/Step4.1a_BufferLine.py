import arcpy
import os
import datetime


##TODO archive line feature class to line GDB
# Tile: Will buffer point and line features by 15 meters, this buffer distance can be adjust with the buffer_distance
# variable

# User input variable
# in and out workspaces
ingdb = r'J:\Workspace\ESA_Species\CriticalHabitat\NAD_Final\Clams.gdb'
outgdb = r"J:\Workspace\ESA_Species\CriticalHabitat\NAD_Final\Clams.gdb"
buffer_distance = "15 METERS"

# recursively checks workspaces found within the inFileLocation and makes list of all feature class
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


# Create a new GDB
def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()


folder, gdb = os.path.split(outgdb)
create_gdb(folder, gdb, outgdb)

arcpy.env.workspace = ingdb
fclist = arcpy.ListFeatureClasses()

for fc in fclist:
    infile = ingdb + os.sep + fc
    fcname = fc.split("_")
    line = str(fcname[2])
    if line == 'line':
        ent = str(fcname[1])
        outfile = outgdb + os.sep + "CH_" + ent + "_lineBuffer_20150428"
        if not arcpy.Exists(outfile):
            arcpy.Buffer_analysis(infile, outfile, buffer_distance, "FULL", "ROUND", "LIST")
            print "Print buffer {0}".format(fc)
    else:
        continue

end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
