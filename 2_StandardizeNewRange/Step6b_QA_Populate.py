import os
import csv

import datetime

import arcpy
# #Tile: Checks to make sure that the entityid in the file name matches the att table and that all fcs have at least one
# row.  Species that did not copy correctly have not data and therefore no rows


InGDB = r"L:\Workspace\ESA_Species\NMFS_UpdatedCH\Range\UpdatedProcess_20171101\GDB\STD_ReNmNMFS20170420_2017-11-02.gdb"

QAcsv = "QA_attributes_20171102"

SearchField = "EntityID"
fileType = "R"
fileSuffix = "poly_20170420_STD"

# Workspace
ws = "L:\Workspace\ESA_Species\NMFS_UpdatedCH\Range"
# Folder in workspace where outputs will be saved
name_dir = "UpdatedProcess_20171101"

# in yyyymmdd received date
receivedDate = '20170420'


# ######################## FUNCTIONS
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield (fc)


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


# #################################################################################################################
# start clock on timing script

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

datelist = []
today = datetime.date.today()
datelist.append(today)

QA = []
header = "FileName" + "," + "True/False Attribute" + "," + "True/False Rows"
QA.append(header)
path_dir = ws + os.sep + str(name_dir)
outLocationCSV = path_dir + os.sep + "CSV"
OutFolderGDB = path_dir + os.sep + "GDB"

csvfile, csvpath = create_flnm_timestamp(QAcsv, outLocationCSV, datelist, 'csv')
print outLocationCSV

for fc in fcs_in_workspace(InGDB):
    name = str(fc)
    ent_name = name.replace((fileType + "_"), "")
    ent_name = ent_name.replace(fileSuffix, "")
    print ent_name
    sc = arcpy.da.SearchCursor(fc, SearchField)
    for row in sc:
        ent_table = str(row[0])
        print ent_table
    if ent_name == ent_table:
        count = int(arcpy.GetCount_management(fc).getOutput(0))
        if count == 0:
            result = str(name) + "," + "TRUE" + "," + "FALSE"
            QA.append(result)
        else:
            result = str(name) + "," + "TRUE" + "," + "TRUE"
            QA.append(result)
    else:
        count = int(arcpy.GetCount_management(fc).getOutput(0))
        if count == 0:
            result = str(name) + "," + "FALSE" + "," + "FALSE"
            QA.append(result)
        else:
            result = str(name) + "," + "FALSE" + "," + "TRUE"
            QA.append(result)


# #End clock time script
end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
