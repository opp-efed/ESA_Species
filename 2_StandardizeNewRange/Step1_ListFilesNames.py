import os
import csv
import datetime

import arcpy

# Tile: This script will generate a list of file received and their native projections.  This script with go through
# all subfolders/workspaces within the location assigned as the workspace with the env.workspace variable- JCONNOLLY

# ###Varibles to be set by user

# # location of files/Workspace
InFileLocations = "L:\Workspace\StreamLine\Species Spatial Library\UpdateFiles\UpdatedProcess_Jan2019\GDB\STD_ReNmFWS20190130_2019-02-02.gdb"

NameCSV = 'FWS_ECOS_Update2019_STD'

# Workspace
ws = "L:\Workspace\StreamLine\Species Spatial Library\UpdateFiles"
# Folder in workspace where outputs will be saved
name_dir = "UpdatedProcess_Jan2019"

# in yyyymmdd received date
receivedDate = '20190130'


##################################################################################################################

# General functions
# creates date stamped csv
def create_csvflnm_timestamp(namecsvfile, outcsvlocation, datelist):
    filename = str(namecsvfile) + "_" + str(datelist[0]) + '.csv'
    filepath = os.path.join(outcsvlocation, filename)
    return filename, filepath


# recursively checks workspaces found within the inFileLocation and makes list of all feature class
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


# outputs table from list generated in create FileList
def create_out_table(list_name, csv_name):
    with open(csv_name, "wb") as output:
        writer = csv.writer(output, lineterminator='\n')
        for val in list_name:
            writer.writerow([val])


# creates directories to save files
def CreateDirectory(path_dir, outLocationCSV, OutFolderGDB):
    if not os.path.exists(path_dir):
        os.mkdir(path_dir)
        print "created directory {0}".format(path_dir)
    if not os.path.exists(outLocationCSV):
        os.mkdir(outLocationCSV)
        print "created directory {0}".format(outLocationCSV)
    if not os.path.exists(OutFolderGDB):
        os.mkdir(OutFolderGDB)
        print "created directory {0}".format(OutFolderGDB)


# Script specific function
# makes a list of files that are being added or updated
def createFileList(InFileLocations, file_list, failed_list):
    for fc in fcs_in_workspace(InFileLocations):
        try:
            dsc = arcpy.Describe(fc)
            sr = dsc.spatialReference
            add_name = str(fc) + "," + sr.name + "," + sr.type
            print add_name
            file_list.append(add_name)
        except(AttributeError, TypeError):
            print "Failed  " + str(fc)
            add_failed = str(fc)
            failed_list.append(add_failed)

    return file_list, failed_list


# Empty list used to store date that will be appended to end of tables
datelist = []
today = datetime.date.today()
datelist.append(today)

OrgSRList = []  # Empty list to store information about each of the feature classes will be exported to table
FailedList = []

addSRList = "Filename" + "," + "Projection" + "," + "Projection Type"  # Heading for tables
OrgSRList.append(addSRList)

addFailed = "Filename"  # Heading for tables
FailedList.append(addFailed)  # Will contain any files without correct spatial projection

path_dir = ws + os.sep + str(name_dir)
outCSVLocation = path_dir + os.sep + "CSV"
OutFolderGDB = path_dir + os.sep + "GDB"
FailedCSV = "Failed_" + str(NameCSV)

#######################################################################################################################
# Use fcs_in_workspace function to recursively search folders for spatial files
# Creates table lists of original feature class name, and native projection and then standardize the name

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

CreateDirectory(path_dir, outCSVLocation, OutFolderGDB)
# Creates file names for out tables with timestamp

csvfile, csvpath = create_csvflnm_timestamp(NameCSV, outCSVLocation, datelist)
failedcsv, failedcsvpath = create_csvflnm_timestamp(FailedCSV, outCSVLocation, datelist)

OrgSRList, FailedList = createFileList(InFileLocations, OrgSRList, FailedList)

# write data store in lists to out tables in csv format
create_out_table(OrgSRList, csvpath)
create_out_table(FailedList, failedcsvpath)

end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
