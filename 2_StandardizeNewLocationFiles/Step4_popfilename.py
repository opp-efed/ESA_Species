import os
import datetime

import arcpy

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# TODO This script can likely be rolled into Step 3?

# #Tile: Copy adds and populates the field filename with the file name of the fc, and the dissolve field with the
# number one- these fields are used to dissolve file into one multi part polygon for each species to be use in
# generating the composite files for the co-occur analysis

# Variables to be set by user###############################################################

# Input File Locations from step 3
InGDB = r"L:\Workspace\StreamLine\Species Spatial Library\_CurrentFiles\No Call Species\GDB\ReNm_FWS_20190130_2020-05-12.gdb"

abb = "FWS"

# Workspace for copied and standardize files; stays static for the whole tool
ws = "L:\Workspace\StreamLine\Species Spatial Library\_CurrentFiles"
# Folder in workspace where outputs will be saved
name_dir = "No Call Species"

# in yyyymmdd received date
receivedDate = '20190130'

# Field names to be added that will be used to dissolve to a single multipart polygon (Dissolve) and the join column to
# add other attributes (Filename)

JoinFieldFC = "FileName"
Dissolve = "Dissolve"


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


# static Variables no user input needed unless changing structure of script

# import time
datelist = []
todaydate = datetime.date.today()
datelist.append(todaydate)

path_dir = ws + os.sep + str(name_dir)
outLocationCSV = path_dir + os.sep + "CSV"
OutFolderGDB = path_dir + os.sep + "GDB"

out_nameGDB = "STD_ReNmNMFS_" + str(receivedDate)

# NOTE Change this to False if you don't want GDB to be overwritten
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = ""

# start clock on timing script

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_gdb(path_dir, outLocationCSV, OutFolderGDB)

# for each file in inGDB adds and populated the files Filename and Dissolve
for fc in fcs_in_workspace(InGDB):
    try:
        arcpy.AddField_management(fc, str(JoinFieldFC), "TEXT", "", "", "225", "", "NULLABLE", "NON_REQUIRED", "")
        arcpy.AddField_management(fc, str(Dissolve), "TEXT", "", "", "", "5", "NULLABLE", "NON_REQUIRED", "")
    except:
        print "Failed to add dissolve fields " + str(fc)

for fc in fcs_in_workspace(InGDB):
    try:
        name = str(fc) + "_STD"
        namesimple = str(fc)
        # print namesimple
        JoinField = JoinFieldFC
        rows = arcpy.da.UpdateCursor(fc, JoinField)
        for row in rows:
            row[0] = namesimple
            rows.updateRow(row)
    except:
        print "filename not added: " + str(fc)
for fc in fcs_in_workspace(InGDB):
    try:
        rows = arcpy.da.UpdateCursor(fc, Dissolve)
        for row in rows:
            row[0] = "1"
            rows.updateRow(row)
    except:
        print "Failed to pop dissolve field:" + str(fc)

# #End clock time script
end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
