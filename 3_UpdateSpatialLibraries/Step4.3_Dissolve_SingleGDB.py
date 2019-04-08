import os
import csv
import datetime

import arcpy

# Tile: Dissolve files identify in Step 5 so that there is a single multipart polygon for each species.

# TODo General clean up to make it more useful in this step, pull from a different dissolve script can me optimize
# for this step of the process

# Final fields to be kept
#dissolveFields = ['NAME', 'Name_sci', 'SPCode', 'VIPCode', 'FileName', 'EntityID','Status', 'Pop_Abb']
dissolveFields = ['FileName']
ingdb = r'L:\Workspace\StreamLine\Species Spatial Library\_CurrentFiles\HUC12\CH_Aquatic_HUC12.gdb'

outGDB = r'L:\Workspace\StreamLine\Species Spatial Library\_CurrentFiles\HUC12\DissolvedCH_Aquatics_HUC12.gdb'
# sp groups to skip
skipgroup = []


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

arcpy.env.overwriteOutput = True  # ## Change this to False if you don't want GDB to be overwritten
arcpy.env.scratchWorkspace = ""
start_script = datetime.datetime.now()

arcpy.env.workspace = ingdb

fcList = arcpy.ListFeatureClasses()
total = len(fcList)
outfolder, gdb =os.path.split(outGDB)
create_gdb(outfolder,gdb, outGDB)

for fc in fcList:
    print fc
    outgdb_name = outGDB
    infile = ingdb + os.sep + fc
    outfile = outGDB + os.sep + fc

    if arcpy.Exists(outfile):
        continue
    else:
        arcpy.Delete_management("temp_lyr")
        arcpy.MakeFeatureLayer_management(infile, "temp_lyr")

        arcpy.Dissolve_management("temp_lyr", outfile, dissolveFields, "", "MULTI_PART",
                                  "DISSOLVE_LINES")
        print "Dissolving {0} ".format(fc, )
        print "completed {0} {1} remaining ".format(fc, total)
        total -= 1

arcpy.env.workspace = outGDB
fcList2 = arcpy.ListFeatureClasses()
if len(fcList) == len(fcList2):
    print "\nAll  files Dissolved ".format()
else:
    print "\nCheck for missing ".format()

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
