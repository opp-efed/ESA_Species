import os
import csv
import datetime

import arcpy
# This script will copy feature classes to the designated gdb, the orginal names will be maintained
# All subfolders/workspaces within the location assigned as the workspace with the env.workspace variable
# Tile: Copy shapefiles to a geodatabase and generate list of files.


# Variables to be set by user

NmCSVCopied = 'CHline_20160809'
out_nameGDB = "CH_line_20160809"

InFileLocations = r"J:\Workspace\ESA_Species\CriticalHabitat\RawDownload\Downloaded_20160819_updated20160728" \
                  r"\Standardize_CHfiles_20160809\CHPoly_20160809\GDB\CH_Polygon_201608098_2016-08-10.gdb"
# Workspace
ws = r"J:\Workspace\ESA_Species\CriticalHabitat\RawDownload\Downloaded_20160819_updated20160728" \
     r"\Standardize_CHfiles_20160809"
# Folder in workspace where outputs will be saved
name_dir = "CHline_20160809"

# in yyyymmdd received date
receivedDate = '20160728'


# FUNCTIONS

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


def copyFCtoGEO(InFileLocations, outFilegdbpath, file_list, failed_list):
    for fc in fcs_in_workspace(InFileLocations):
        try:
            basename, extension = os.path.splitext(fc)
            outFC = arcpy.ValidateTableName(basename)
            copiedFC = outFilegdbpath + os.sep + str(outFC)
            outFeatureClass = os.path.join(outFilegdbpath, copiedFC)
            if not arcpy.Exists(outFeatureClass):
                addSRList = str(fc) + "," + str(outFC)
                print addSRList
                file_list.append(addSRList)
                arcpy.CopyFeatures_management(fc, outFeatureClass)
            else:
                print "FC already copied"
        except:
            print "Failed  " + str(fc)
            addFailed = str(fc)
            failed_list.append(addFailed)
    return file_list, failed_list


# static variable no user input needed unless making change to script structure
datelist = []
today = datetime.date.today()
datelist.append(today)

path_dir = ws + os.sep + str(name_dir)
outLocationCSV = path_dir + os.sep + "CSV"
OutFolderGDB = path_dir + os.sep + "GDB"
NmCSVFailed = 'Failed_' + str(NmCSVCopied)

# Creates file names for out tables with timestamp
OutGDB, outFilegdbpath = create_flnm_timestamp(out_nameGDB, OutFolderGDB, datelist, 'gdb')
csvfile, csvpath = create_flnm_timestamp(NmCSVCopied,
                                         outLocationCSV, datelist,
                                         'csv')  # CSV out table succeed file-complete spatial package
failedcsv, failedcsvpath = create_flnm_timestamp(NmCSVFailed,
                                                 outLocationCSV, datelist,
                                                 'csv')  # CSV out table succeed file-incomplete spatial package


# Empty list used to store information for tables
OrgSRList = []
FailedList = []
# Heading for tables
addSRList = "Original filename" + "," + "Filename in GDB: " + str(OutGDB)
OrgSRList.append(addSRList)
addFailed = "Filename-Original"
FailedList.append(addFailed)

arcpy.env.scratchWorkspace = ""
# NOTE Change this to False if you don't want GDB to be overwritten
arcpy.env.overwriteOutput = True
arcpy.env.workspace = OutFolderGDB


# Start  clock time script
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_directory(path_dir, outLocationCSV, OutFolderGDB)

# Creates outFile GDB and sets location to InFiles allows to overwritten
create_gdb(OutFolderGDB, OutGDB, outFilegdbpath)

# Copy all spatial files found to a file geodatabase created above
addSRList, FailedList = copyFCtoGEO(InFileLocations, outFilegdbpath)

# write data store in lists to out tables in csv format
create_out_table(OrgSRList, csvpath)
create_out_table(FailedList, failedcsvpath)

# End clock time script
end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
