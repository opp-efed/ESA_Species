import os
import csv
import time
import datetime

import arcpy

#
# #Tile: Dissolves files that were flagged as possibly having topology issues
# into a single  multipart polygons by species

# NOTE - Run the Step B repair geometry on inGDB before running this script the first time.  If species still have
# geometry issues that are not rectified can run the Step V Simple polygon to simplify the polygons.  This often happens
# with the species that were buffered from the NHD flowlines due to the number of vertices and self intersections


# Varibles to be set by user ###############################################################

# Input File Locations
InGDB = r"C:\Users\Admin\Documents\Jen\SpeceisToRun\Missing_20151228\GDB\Topo_STD_ReNmFWS20151228_2015-12-28.gdb"
abb = "FWS"

# workspace and folder dirctory where output will be saved
ws = "C:\Users\Admin\Documents\Jen\SpeceisToRun"
name_dir = "Missing_20151228"

# in yyyymmdd received date
receivedDate = '20151228'

# number of time the script has run to appended to the end of the file names so nothing is overwritten
runcount = 1


# Field Names
Dissolve = "Dissolve"
JoinFieldFC = "FileName"
abb = "FWS"


# Functions
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


# outputs table from list generated in create FileList
def create_out_table(list_name, csv_name):
    with open(csv_name, "wb") as output:
        writer = csv.writer(output, lineterminator='\n')
        for val in list_name:
            writer.writerow([val])


# Create a new GDB
def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


# ##Static Variables no user input needed###########

# import time
datelist = []
todaydate = datetime.date.today()
datelist.append(todaydate)

path_dir = ws + os.sep + str(name_dir)
outLocationCSV = path_dir + os.sep + "CSV"
OutFolderGDB = path_dir + os.sep + "GDB"

out_nameGDB = "STD_ReNm" + str(abb) + str(receivedDate)

# CREATES FILE NAMES
FailedGDB = "Failed_" + str(out_nameGDB)
TopoGDB = "Topo_" + str(out_nameGDB) + "_" + str(runcount)
OutGDB, outFilegdbpath = create_flnm_timestamp(out_nameGDB, OutFolderGDB, datelist, 'gdb')
FailGDB, outFilefailgdbpath = create_flnm_timestamp(FailedGDB, OutFolderGDB, datelist, 'gdb')
TopoGDB, outfileTopogdbpath = create_flnm_timestamp(TopoGDB, OutFolderGDB, datelist, 'gdb')

arcpy.env.workspace = outFilefailgdbpath
# NOTE Change this to False if you don't want GDB to be overwritten
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = ""


# ####################################################################################################

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
        arcpy.Delete_management("fc_lyr")
        arcpy.Delete_management("outfc_lyr")

        name = str(fc) + "_STD"
        arcpy.MakeFeatureLayer_management(fc, "fc_lyr")
        outfc = outFilegdbpath + os.sep + str(name)
        topofc = outfileTopogdbpath + os.sep + str(fc)
        outFailedFC = outFilefailgdbpath + os.sep + str(fc)
        count = int(arcpy.GetCount_management(fc).getOutput(0))
        if arcpy.Exists(outfc):
            print str(outfc) + ": Already exists"
            continue
        elif arcpy.Exists(topofc):
            print str(outfc) + ":Topo Already exists"
            continue
        else:
            try:
                print "Start dissolve: " + str(fc)
                arcpy.Dissolve_management("fc_lyr", outfc, DissolveList, "", "MULTI_PART", "DISSOLVE_LINES")
                print "Dissolved: " + str(fc)
            except:
                topofc = outfileTopogdbpath + os.sep + str(fc)
                if arcpy.Exists(topofc):
                    continue
                else:
                    print count
                    arcpy.CopyFeatures_management(fc, topofc)
                    arcpy.Delete_management(outfc)
                    print "Failed Topo post Dissolve" + str(topofc)
    except:
        continue

arcpy.env.workspace = outfileTopogdbpath
fc_list = arcpy.ListFeatureClasses()

if len(fc_list) > 0:
    print "Dissolve did  not complete for all files"
# #End clock time script
end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
