import os
import csv
import datetime

import arcpy


# #Tile: Standardize attribute table schema, and populates
# ##fields are added an update via the script if different field names need to be added or update script will need to be
# adjusted, currently fields include "EntityID, SPCODE, VIPCODE, name and sci name- hard codes will need to be adjusted
# if different columns need to be added


# TODO Dynamically make the join table from master list using panadas and remove hard code/join table?
# join table could be come a problem due to the way arc changes col headers of join
# see script Step6_UpdateAllAtt_NAD83Files_pandas as option for loading data directly from maste


################################################################Varibles to be set by user

# Input File Locations
InGDB = r"L:\Workspace\ESA_Species\NMFS_UpdatedCH\CH\UpdatedProcess_20171101\NMFS_NewCH_winter2017" \
        r"\UpdatedProcess_December2017\GDB\STD_ReNmNMFS20171204_2018-01-10.gdb"
# Table will all information to be added to the att table
JoinTable = r"L:\Workspace\ESA_Species\FWS_GIStobeUpdated\UpdatedProcess_20170410\CSV\std_att.csv"

# Workspace
ws = "L:\Workspace\ESA_Species\NMFS_UpdatedCH\CH\UpdatedProcess_20171101\NMFS_NewCH_winter2017"
# Folder in workspace where outputs will be saved
name_dir = "UpdatedProcess_December2017"

# in yyyymmdd received date
receivedDate = '20171204'

# Column headers for join Names
JoinFieldFC = "FileName"
JoinFieldTable = "NewFileName"
DissolveField = "Dissolve"

# Columns to add can add variables must equal columns from table where data is be extracted
# variable 'a' must have a value for all entries
# Add columns using format fccol_
fccol_a = "EntityID"
fccol_b = "SPCode"
fccol_c = "VIPCode"
fccol_d = "NAME"
fccol_e = "Name_sci"
# add columns as needed
# Columns from table where data is  being extracted; add columns using format tablecol_
tablecol_a = "EntityID"
tablecol_b = "SPCODE"
tablecol_c = "VIPCODE"
tablecol_d = "NAME"
tablecol_e = "NAMESci"


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


# outputs table from list generated in create FileList
def create_out_table(list_name, csv_name):
    with open(csv_name, "wb") as output:
        writer = csv.writer(output, lineterminator='\n')
        for val in list_name:
            writer.writerow([val])


def deletefield(fc, fieldname):
    arcpy.DeleteField_management(fc, fieldname)


# ###################################################################
# ##Static Variables no user input needed###########
# DissolveField ="Dissolve"
fccol = []
fccol += [val for name, val in globals().items() if name.startswith('fccol_')]
fccol.sort()

tablecol = []
tablecol += [val for name, val in globals().items() if name.startswith('tablecol_')]
tablecol.sort()

FailedJoin = []
addFailedJoin = "Filename-(GDB)"
FailedJoin.append(addFailedJoin)
FailedFileNm = []
addFailedNm = "Filename-(GDB)"
FailedFileNm.append(addFailedNm)

# import time
datelist = []
todaydate = datetime.date.today()
datelist.append(todaydate)

FailJoinCSV = "Failed_Joined"
FailFileName = "Failed_FileNmPop"

path_dir = ws + os.sep + str(name_dir)
outLocationCSV = path_dir + os.sep + "CSV"
OutFolderGDB = path_dir + os.sep + "GDB"

# CREATES FILE NAMES

csvfile, csvpath = create_flnm_timestamp(FailJoinCSV, outLocationCSV, datelist, 'csv')
csvfile2, csv2path = create_flnm_timestamp(FailFileName, outLocationCSV, datelist, 'csv')

# ####################################################################################################
# start clock on timing script

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_directory(path_dir, outLocationCSV, OutFolderGDB)

fieldList = []
valcol = []
num = 0
for b in tablecol:
    t = "valcol" + "_" + str(num)
    valcol.append(t)
    num += 1

print 'start delete'
for fc in fcs_in_workspace(InGDB):
    try:
        deletefield(fc, DissolveField)
        for field in fccol:
            deletefield(fc, field)
    except:
        addnmList = str(fc)
        FailedFileNm.append(addnmList)

for fc in fcs_in_workspace(InGDB):
    print "Start: " + str(fc)
    # add col based on import
    for field in fccol:
        name = str(field)
        print name
        if name is fccol_a:
            arcpy.AddField_management(fc, name, "TEXT", "", "", "10", "", "NULLABLE", "NON_REQUIRED", "")
        elif name is fccol_b:
            arcpy.AddField_management(fc, name, "TEXT", "", "", "5", "", "NULLABLE", "NON_REQUIRED", "")
        elif name is fccol_c:
            arcpy.AddField_management(fc, name, "TEXT", "", "", "5", "", "NULLABLE", "NON_REQUIRED", "")
        else:
            arcpy.AddField_management(fc, name, "TEXT", "", "", "75", "", "NULLABLE", "NON_REQUIRED", "")
    try:
        arcpy.Delete_management("fc_lyr")
        arcpy.MakeFeatureLayer_management(fc, "fc_lyr")
        # joins the fc to the JoinTable based on file name
        arcpy.AddJoin_management("fc_lyr", JoinFieldFC, JoinTable, JoinFieldTable, "KEEP_ALL")
        path, filename = os.path.split(JoinTable)
        filename = os.path.splitext(filename)[0]
        filename = str(filename) + '.csv'
        fieldList = []
        # makes a list of the all the vol header found in the join table
        for i in tablecol:
            # used the raw col header and adds the additional joined charaters arc adds to the header
            # note this could be simplified by not doing the join
            calccol = str(filename) + "." + str(i)
            fieldList.append(calccol)
        sc = arcpy.da.SearchCursor("fc_lyr", fieldList)
        # loops through each row fc and calulates the att field value (valcol) based on the corresponding col from the
        # table (table_col with the appended join information from above)
        # uses the index row to extract col info from join table then update the att table with it
        for row in sc:
            index = 0
            values = {}
            # calculates value from indexed location
            for j in valcol:
                # print index
                t = row[index]
                print t
                values[j] = t
                index += 1
            if row[0] is None:
                print "Cannont add attributes for:" + str(fc)
                addfileList = str(fc)
                FailedJoin.append(addfileList)
            rows = arcpy.UpdateCursor(fc)
            for row in rows:
                valcur = 0
                for z in fccol:
                    x = "valcol_" + str(valcur)
                    v = values.get(x)
                    row.setValue(z, v)
                    valcur += 1
                    rows.updateRow(row)
            del rows
    except:
        print "Cannont add attributes for:" + str(fc)
        addfileList = str(fc)
        FailedJoin.append(addfileList)
    if fccol_a is not None:
        print "Updated attributes for:" + str(fc)

arcpy.Delete_management("fc_lyr")
create_out_table(FailedJoin, csvpath)
create_out_table(FailedFileNm, csv2path)
# #End clock time script
end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
