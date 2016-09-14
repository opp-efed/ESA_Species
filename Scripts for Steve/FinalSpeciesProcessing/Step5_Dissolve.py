import os
import csv
import time
import datetime

import arcpy

# #Tile: Dissolves files into a single  multipart polygons by species

# NOTE - script adjusted to check error with aquatics- aquatic files sometimes had topology issues especially files with
# many rows.  Set the maxrow variable as the max row cut off to flag species as aquatic and process them using script
# 5b.  If the dissolve fails the file is moved to a failed gdb to be processed separately

# Variables to be set by user

maxrow = 10000

# Input File Locations
InGDB = r"J:\Workspace\ESA_Species\CriticalHabitat\HUC12\Fishes_HUC12.gdb"
abb = "FWS"

# workspace and folder dirctory where output will be saved
ws = "J:\Workspace\ESA_Species\CriticalHabitat"
name_dir = "HUC12"

# in yyyymmdd received date
receivedDate = '20160819'

# Field Names that will be kept in the dissolve
Dissolve = "Dissolve"
JoinFieldFC = "FileName"


# ####FUNCTIONS


# recursively checks workspaces found within the inFileLocation and makes list of all feature class
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield (fc)


# creates directories to save files
def create_directory(path_dir, outLocationCSV, OutFolderGDB):
    if not os.path.exists(path_dir):
        os.mkdir(path_dir)
        print "created directory {0}".format(path_dir)
    if not os.path.exists(outLocationCSV):
        os.mkdir(outLocationCSV)
        print "created directory {0}".format(outLocationCSV)
    if not os.path.exists(OutFolderGDB):
        os.mkdir(OutFolderGDB)
        print "created directory {0}".format(OutFolderGDB)


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


######################################################################Static Variables no user input needed###########

###import time
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
            except:
                outFailedFC = outFilefailgdbpath + os.sep + str(fc)
                if arcpy.Exists(outFailedFC):
                    continue
                else:
                    # outFC= arcpy.ValidateTableName(outFailedFC)
                    print "Failed Dissolve: " + str(outFailedFC)
                    outFailedFC = outFilefailgdbpath + os.sep + str(fc)
                    if arcpy.Exists(outFailedFC):
                        continue
                    arcpy.CopyFeatures_management(fc, outFailedFC)
                    continue
                continue
        elif count > 1:
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
            except:
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
    except:
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
