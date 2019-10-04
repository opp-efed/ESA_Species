import os
import csv
import datetime

import arcpy

# Author J.Connolly
# Internal deliberative, do not cite or distribute

arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = ""
# path to gdb from step 2a, copied files
InGDB = r"path\gdb"
# csv to be saved for records
QAcsv = "file name"

# Workspace for copied and standardize files; stays static for the whole tool
ws = "path"
# Folder in workspace where outputs will be saved
name_dir = "folder name"

# in yyyymmdd received date
receivedDate = 'date'


# General functions
# creates date stamped csv


def create_csvflnm_timestamp(namecsvfile, outcsvlocation, date_list):
    filename = str(namecsvfile) + "_" + str(date_list[0]) + '.csv'
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


# Start elapse clock
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

datelist = []
today = datetime.date.today()
datelist.append(today)

QA = []
header = "FileName" + "," + "True/False Rows"
QA.append(header)
path_dir = ws + os.sep + str(name_dir)
outLocationCSV = path_dir + os.sep + "CSV"
OutFolderGDB = path_dir + os.sep + "GDB"

csvfile, csvpath = create_csvflnm_timestamp(QAcsv,
                                            outLocationCSV, datelist)

for fc in fcs_in_workspace(InGDB):
    name = str(fc)
    # if the nhumber of rows in the fc is 0 then the file did not copy correctly
    count = int(arcpy.GetCount_management(fc).getOutput(0))
    if count == 0:
        result = str(name) + "," + "FALSE"
        QA.append(result)
    else:
        result = str(name) + "," + "TRUE"
        QA.append(result)

create_out_table(QA, csvpath)

# End clock time script
end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
