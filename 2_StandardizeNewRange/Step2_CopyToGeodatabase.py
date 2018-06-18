import os
import csv
import datetime

import arcpy
# This script will copy feature classes to the designated gdb, the orginal names will be maintained
# All subfolders/workspaces within the location assigned as the workspace with the env.workspace variable
# Tile: Copy shapefiles to a geodatabase and generate list of files.


# Variables to be set by user

NmCSVCopied = 'NMFS_CH_UpdatedWinter2017'
out_nameGDB = "NMFS_CH_UpdatedWinter2017"

InFileLocations = "L:\Workspace\ESA_Species\NMFS_UpdatedCH\CH\NewNMFS_Winter2017"

# Workspace
ws = "L:\Workspace\ESA_Species\NMFS_UpdatedCH\CH\UpdatedProcess_20171101\NMFS_NewCH_winter2017"
# Folder in workspace where outputs will be saved
name_dir = "UpdatedProcess_December2017"

# in yyyymmdd received date
receivedDate = '20171204'

# FUNCTIONS

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


def copy_fc_to_geo(in_location, out_location_path, file_list, failed_list):
    for fc in fcs_in_workspace(in_location):
        try:
            basename, extension = os.path.splitext(fc)
            out_fc = arcpy.ValidateTableName(basename)
            copied_fc = out_location_path + os.sep + str(out_fc)
            out_feature_class = os.path.join(out_location_path, copied_fc)
            if not arcpy.Exists(out_feature_class):
                add_new = str(fc) + "," + str(out_fc)
                print add_new
                file_list.append(add_new)
                arcpy.CopyFeatures_management(fc, out_feature_class)
            else:
                print "FC already copied"
        except Exception as error:
            print(error.args[0])
            print "Failed  " + str(fc)
            add_failed = str(fc)
            failed_list.append(add_failed)
            if arcpy.Exists(out_feature_class):  # Deletes files that was interrupted during copy it would be incomplete
                arcpy.Delete_management(out_feature_class)
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
addSRList, FailedList = copy_fc_to_geo(InFileLocations, outFilegdbpath, OrgSRList, FailedList)

# write data store in lists to out tables in csv format
create_out_table(OrgSRList, csvpath)
create_out_table(FailedList, failedcsvpath)

# End clock time script
end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
