import os
import datetime

import arcpy

# TODO This script can likely be rolled into Step 3?

# #Tile: Copy adds and populates the field filename with the file name of the fc, and the dissolve field with the
# number one- these fields are used to dissolve file into one multi part polygon for each species to be use in
# generating the composite files for the co-occur analysis

# Variables to be set by user###############################################################


# Input File Locations
InGDB = r"L:\Workspace\StreamLine\Species Spatial Library\_CurrentFiles\HUC12\CH_Aquatic_HUC12.gdb"



# Field names to be added that will be used to dissolve to a single multipart polygon (Dissolve) and the join column to
# add other attributes (Filename)

JoinFieldFC = "FileName"


# FUNCTIONS
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


# NOTE Change this to False if you don't want GDB to be overwritten
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = ""

# start clock on timing script

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()



# for each file in inGDB adds and populated the files Filename and Dissolve
for fc in fcs_in_workspace(InGDB):
    try:
        arcpy.AddField_management(fc, str(JoinFieldFC), "TEXT", "", "", "225", "", "NULLABLE", "NON_REQUIRED", "")

    except:
        print "Failed to add fields " + str(fc)


for fc in fcs_in_workspace(InGDB):
    try:
        rows = arcpy.da.UpdateCursor(fc, JoinFieldFC)
        for row in rows:
            row[0] = str(fc)
            rows.updateRow(row)
    except:
        print "Failed to pop field:" + str(fc)

# #End clock time script
end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
