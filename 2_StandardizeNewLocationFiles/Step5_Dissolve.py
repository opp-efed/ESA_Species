import os
import datetime

import arcpy

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# #Tile: Dissolves files into a single  multipart polygons by species

# NOTE - script adjusted to check error with aquatics- aquatic files sometimes had topology issues especially files
# with many rows.  Set the maxrow variable as the max row cut off to flag species as aquatic and process them using
# script 5b.  If the dissolve fails the file is moved to a failed gdb to be processed separately

# Variables to be set by user

maxrow = 5000000

# Input File Locations from step 4
InGDB = r"L:\Workspace\StreamLine\Species Spatial Library\_CurrentFiles\No Call Species\GDB\ReNm_FWS_20190130_2020-05-12.gdb"

abb = "FWS"

# Workspace for copied and standardize files; stays static for the whole tool
ws = "L:\Workspace\StreamLine\Species Spatial Library\_CurrentFiles"
# Folder in workspace where outputs will be saved
name_dir = "No Call Species"

# in yyyymmdd received date
receivedDate = '20190130'

# Field Names that will be kept in the dissolve
Dissolve = "Dissolve"
JoinFieldFC = "FileName"


# ####FUNCTIONS


# recursively checks workspaces found within the inFileLocation and makes list of all feature class
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fcs in arcpy.ListFeatureClasses():
        yield (fcs)
    for wks in arcpy.ListWorkspaces():
        for fcs in fcs_in_workspace(wks):
            yield (fcs)


# creates directories to save files
def create_directory(path_directory, out_csv, out_gdb):
    if not os.path.exists(path_directory):
        os.mkdir(path_directory)
        print "created directory {0}".format(path_directory)
    if not os.path.exists(out_csv):
        os.mkdir(out_csv)
        print "created directory {0}".format(out_csv)
    if not os.path.exists(out_gdb):
        os.mkdir(out_gdb)
        print "created directory {0}".format(out_gdb)


# creates date stamped generic file
def create_flnm_timestamp(namefile, outlocation, date_list, file_extension):
    file_extension.replace('.', '')
    filename = str(namefile) + "_" + str(date_list[0]) + '.' + file_extension
    filepath = os.path.join(outlocation, filename)
    return filename, filepath


# Create a new GDB
def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


# ##################################################Static Variables no user input needed###########

# ##import time
datelist = []
todaydate = datetime.date.today()
datelist.append(todaydate)

path_dir = ws + os.sep + str(name_dir)
outLocationCSV = path_dir + os.sep + "CSV"
OutFolderGDB = path_dir + os.sep + "GDB"

out_nameGDB = "STD_ReNm" + str(abb) + str(receivedDate)

# CREATES FILE NAMES
FailedGDB = "Failed_" + str(out_nameGDB)
TopoGDB = "Topo_" + str(out_nameGDB)
OutGDB, outFilegdbpath = create_flnm_timestamp(out_nameGDB, OutFolderGDB, datelist, 'gdb')
FailGDB, outFilefailgdbpath = create_flnm_timestamp(FailedGDB, OutFolderGDB, datelist, 'gdb')
TopoGDB, outfileTopogdbpath = create_flnm_timestamp(TopoGDB, OutFolderGDB, datelist, 'gdb')

arcpy.env.workspace = outFilefailgdbpath
# NOTE Change this to False if you don't want GDB to be overwritten
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = ""
#####################################################################################################

# start clock on timing script
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_directory(path_dir, outLocationCSV, OutFolderGDB)
create_gdb(OutFolderGDB, OutGDB, outFilegdbpath)
create_gdb(OutFolderGDB, FailGDB, outFilefailgdbpath)
create_gdb(OutFolderGDB, TopoGDB, outfileTopogdbpath)

DissolveList = [Dissolve, JoinFieldFC]
print DissolveList

for fc in fcs_in_workspace(InGDB):
    try:
        print fc
        arcpy.Delete_management("fc_lyr")
        arcpy.Delete_management("outfc_lyr")
        name = str(fc) + "_STD"
        arcpy.MakeFeatureLayer_management(fc, "fc_lyr")
        outfc = outFilegdbpath + os.sep + str(name)
        topofc = outfileTopogdbpath + os.sep + str(fc)
        count = int(arcpy.GetCount_management(fc).getOutput(0))
        if arcpy.Exists(outfc):
            print str(fc) + ": Already exists"
            continue
        if arcpy.Exists(topofc):
            print str(fc) + ": Already exists"
            continue
        elif count == 1:
            try:
                arcpy.Dissolve_management("fc_lyr", outfc, DissolveList, "", "MULTI_PART", "DISSOLVE_LINES")
                print "Dissolved: " + str(fc)
            except Exception as error:
                print(error.args[0])
                if arcpy.Exists(outfc):  # If it partial file exists it is deleted
                    arcpy.Delete_management(outfc)
                outFailedFC = outFilefailgdbpath + os.sep + str(fc)
                if arcpy.Exists(outFailedFC):
                    continue
                else:
                    # outFC= arcpy.ValidateTableName(outFailedFC)
                    print "Failed Dissolve: " + str(outFailedFC)
                    outFailedFC = outFilefailgdbpath + os.sep + str(fc)
                    if arcpy.Exists(outFailedFC):
                        continue
                    else:
                        arcpy.CopyFeatures_management(fc, outFailedFC)
                        continue

        elif count > 1:
            if not arcpy.Exists(outfc):
                if count > maxrow:
                    print count
                    topofc = outfileTopogdbpath + os.sep + str(fc)
                    print "Run dissolve B"
                    print "Failed Topo post Dissolve" + str(topofc)
                    if arcpy.Exists(topofc):
                        continue
                    else:
                        arcpy.CopyFeatures_management(fc, topofc)
                        arcpy.Delete_management(outfc)
                        continue
                try:
                    if arcpy.Exists(topofc):
                        continue
                    else:
                        arcpy.Dissolve_management("fc_lyr", outfc, DissolveList, "", "MULTI_PART", "DISSOLVE_LINES")
                        print "Dissolved: " + str(fc)

                except Exception as error:
                    print(error.args[0])
                    if arcpy.Exists(outfc):  # If it partial file exists it is deleted
                        arcpy.Delete_management(outfc)
                    print count
                    topofc = outfileTopogdbpath + os.sep + str(fc)
                    if arcpy.Exists(topofc):
                        print "Run dissolve B"
                        print "Failed Topo post Dissolve" + str(topofc)
                        continue
                    else:
                        print "Run dissolve B"
                        print "Failed Topo post Dissolve" + str(topofc)
                        arcpy.CopyFeatures_management(fc, topofc)
                        arcpy.Delete_management(outfc)
                        continue
    except Exception as error:
        print(error.args[0])
        continue

arcpy.env.workspace = outfileTopogdbpath
fc_list = arcpy.ListFeatureClasses()

arcpy.env.workspace = outFilefailgdbpath
fc_list2 = arcpy.ListFeatureClasses()

# Provided feedback on files that failed
if len(fc_list2) > 0:
    print "\nDissolve did  not complete for all files"

if len(fc_list) > 0:
    print "\nTopo files need to be addressed"

# #End clock time script
end = datetime.datetime.now()
print "\nEnd Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
