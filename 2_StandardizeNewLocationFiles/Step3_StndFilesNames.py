import os
import csv
import datetime

import arcpy

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# #Tile: Copy shapefiles to a geodatabase and rename to a standard naming convention.

# NOTE prior to running the NmChange check the the concatenated specode and vipcode on the FWS file name and the
# sci names match the master list.  Files were sometime received with typos

# TODO update so that check against the sci name and Concat codes are scripted
# TODO create the NmChange Dict dynamically from masters using Pandas

# Variables to be set by user

# Input File Locations
# Set the workspace for the ListFeatureClass function
# Gdb with copied files from step 2
InGDB = r"L:\Workspace\StreamLine\Species Spatial Library\_CurrentFiles\No Call Species\GDB\R_NoCall_2020-05-12.gdb"
abb = "FWS"  # FWS or NMFS

# Workspace for copied and standardize files; stays static for the whole tool
ws = "L:\Workspace\StreamLine\Species Spatial Library\_CurrentFiles"
# Folder in workspace where outputs will be saved
name_dir = "No Call Species"

# in yyyymmdd received date
receivedDate = '20190130'

# NOTE prior to running the NmChange check the the concatenated specode and vipcode on the FWS file name and the
# sci names match the master list.  Files were sometime received with typos

# #DICT for name change original file to EPA Std
# TODO dynamically set up dictionary to the master list to make file name
# Note date must be in in yyyymmdd in filename

NmChangeDICT = {'PosExtinct_R_191_poly_190130_STD_NAD83_20190204':'R_191_poly_190130',
                'Qual_R_193_poly_190130_STD_NAD83_20190204':'R_193_poly_190130',
                'Extinct_R_68_poly_190130_STD_NAD83_20190204':'R_68_poly_190130',
                'PosExtinct_R_78_poly_190130_STD_NAD83_20190204':'R_78_poly_190130',
                'PosExtinct_R_91_poly_190130_STD_NAD83_20190204':'R_91_poly_190130',
                'PosExtinct_R_93_poly_190130_STD_NAD83_20190204':'R_93_poly_190130',
                'PosExtinct_R_100_poly_190130_STD_NAD83_20190204':'R_100_poly_190130',
                'Qual_R_104_poly_190130_STD_NAD83_20190204':'R_104_poly_190130',
                'Qual_R_106_poly_190130_STD_NAD83_20190204':'R_106_poly_190130',
                'Qual_R_119_poly_190130_STD_NAD83_20190204':'R_119_poly_190130',
                'Qual_R_146_poly_190130_STD_NAD83_20190204':'R_146_poly_190130',
                'Qual_R_147_poly_190130_STD_NAD83_20190204':'R_147_poly_190130',
                'Qual_R_347_poly_190130_STD_NAD83_20190204':'R_347_poly_190130',
                'Qual_R_484_poly_190130_STD_NAD83_20190204':'R_484_poly_190130',
                'Qual_R_485_poly_190130_STD_NAD83_20190204':'R_485_poly_190130',
                'Qual_R_212_poly_190130_STD_NAD83_20190204':'R_212_poly_190130',
                'Qual_R_314_poly_190130_STD_NAD83_20190204':'R_314_poly_190130',
                'Qual_R_4881_poly_190130_STD_NAD83_20190204':'R_4881_poly_190130',
                'R_10077_poly_20141216_STD_NAD83_20190130':'R_10077_poly_20141216',
                'R_10300_poly_20141216_STD_NAD83_20190130':'R_10300_poly_20141216',
                'R_1509_poly_20170420_STD_NAD83_20190130':'R_1509_poly_20170420',
                'R_286_poly_20170328_STD_NAD8_HUC12_20190130':'R_286_poly_20170328',
                'R_2448_poly_20170420_STD_NAD83_20190130':'R_2448_poly_20170420',
                'R_2528_poly_20170420_STD_NAD83_20190130':'R_2528_poly_20170420',
                'R_3398_poly_20170420_STD_NAD83_20190130':'R_3398_poly_20170420',
                'R_3654_poly_20170420_STD_NAD83_20190130':'R_3654_poly_20170420',
                'R_4093_poly_20140930_STD_NAD83_20190130':'R_4093_poly_20140930',
                'R_4112_poly_20140613_STD_NAD83_20170420':'R_4112_poly_20140613',
                'R_4300_poly_20170420_STD_NAD83_20190130':'R_4300_poly_20170420',
                'R_4799_poly_20170420_STD_NAD83_20190130':'R_4799_poly_20170420',
                'R_4992_poly_20170420_STD_NAD83_20190130':'R_4992_poly_20170420',
                'R_5180_poly_20170420_STD_NAD83_20190130':'R_5180_poly_20170420',
                'R_5265_poly_20170420_STD_NAD83_20190130':'R_5265_poly_20170420',
                'R_5658_poly_20140613_STD_NAD83_20190130':'R_5658_poly_20140613',
                'R_5815_poly_20170420_STD_NAD83_20190130':'R_5815_poly_20170420',
                'R_6220_poly_20170420_STD_NAD83_20190130':'R_6220_poly_20170420',
                'R_6578_poly_20170420_STD_NAD83_20190130':'R_6578_poly_20170420',
                'R_6843_poly_20140613_STD_NAD83_20170420':'R_6843_poly_20140613',
                'R_6966_poly_20170420_STD_NAD83_20190130':'R_6966_poly_20170420',
                'R_7590_poly_20170420_STD_NAD83_20190130':'R_7590_poly_20170420',
                'R_7834_poly_20170420_STD_NAD83_20190130':'R_7834_poly_20170420',
                'R_7989_poly_20170420_STD_NAD83_20190130':'R_7989_poly_20170420',
                'R_8241_poly_20170420_STD_NAD83_20190130':'R_8241_poly_20170420',
                'R_8278_poly_20170420_STD_NAD83_20190130':'R_8278_poly_20170420',
                'R_9432_poly_20160816_STD_NAD83_20190130':'R_9432_poly_20160816',
                'Qual_R_538_poly_190130_STD_NAD83_20190204':'R_538_poly_190130',
                'Qual_R_918_poly_190130_STD_NAD83_20190204':'R_918_poly_190130',
                'Qual_R_1060_poly_190130_STD_NAD83_20190204':'R_1060_poly_190130',
                'Qual_R_1187_poly_190130_STD_NAD83_20190204':'R_1187_poly_190130',
                'Qual_R_4961_poly_190130_STD_NAD83_20190204':'R_4961_poly_190130',
                'Qual_R_5104_poly_190130_STD_NAD83_20190204':'R_5104_poly_190130',
                'Qual_R_5991_poly_190130_STD_NAD83_20190204':'R_5991_poly_190130',
                'Qual_R_10230_poly_190130_STD_NAD83_20190204':'R_10230_poly_190130',
                'Qual_R_10592_poly_190130_STD_NAD83_20190204':'R_10592_poly_190130',
                'R_1100':'R_1100_poly_190130',
                'R_10226':'R_10226_poly_190130',
                'R_1102':'R_1102_poly_190130',
                'R_8181_poly_20140613_STD_NAD83_20190130':'R_8181_poly_20140613',
                'Qual_R_422_poly_190130_STD_NAD83_20190204':'R_422_poly_190130',
                'PosExtinct_R_1953_poly_190130_STD_NAD83_20190204':'R_1953_poly_190130',
                'PosExtinct_R_16_poly_190130_STD_NAD83_20190204':'R_16_poly_190130',
                'Qual_R_22_poly_190130_STD_NAD83_20190204':'R_22_poly_190130',
                'Qual_R_1240_poly_190130_STD_NAD83_20190204':'R_1240_poly_190130',
                'PosExtinct_R_1302_poly_190130_STD_NAD83_20190204':'R_1302_poly_190130',
                'R_OahuTreesSnails_9433':'R_9433_poly_190130',
                'R_OahuTreesSnails_9437':'R_9437_poly_190130',
                'R_OahuTreesSnails_9455':'R_9455_poly_190130',
                'R_OahuTreesSnails_9435':'R_9435_poly_190130',
                'R_OahuTreesSnails_9463':'R_9463_poly_190130',
                'R_OahuTreesSnails_9445':'R_9445_poly_190130',
                'R_OahuTreesSnails_9451':'R_9451_poly_190130',
                'R_OahuTreesSnails_9447':'R_9447_poly_190130',
                'R_OahuTreesSnails_9481':'R_9481_poly_190130',

                }


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


# loops through inGDB and makes a copy of each file applying the std filename from the dict in outGDB
def StandName(InGDB, outFilegdbpath, outFilefailgdbpath):
    for fc in fcs_in_workspace(InGDB):
        try:
            dsc = arcpy.Describe(fc)
            sr = dsc.spatialReference
            prj = sr.name.lower()
            NewName = NmChangeDICT[fc]
            addSRList = str(fc) + "," + "Name: " + sr.name + "," + "Type: " + sr.type + "," + str(NewName)
            print addSRList
            OrgSRList.append(addSRList)
            outFeatureClass = os.path.join(outFilegdbpath, NewName)
            # print outFeatureClass
            if not arcpy.Exists(outFeatureClass):
                # print "FC does not exist"
                arcpy.CopyFeatures_management(fc, outFeatureClass)
            else:
                print" FC already exists"

        except:
            print "Failed  " + str(fc)
            addFailed = str(fc)
            FailedList.append(addFailed)
            outFailedFC = os.path.join(outFilefailgdbpath, addFailed)
            arcpy.CopyFeatures_management(fc, outFailedFC)


# static variable no user input needed unless changing code structure
datelist = []
today = datetime.date.today()
datelist.append(today)

OrgSRList = []
FailedList = []
addSRList = "Filename Original (GDB)" + "," + "Original Projection" + "," + "Original Projection Type" + "," + "Standardize Filename"
OrgSRList.append(addSRList)
addFailed = "Filename-Original (GDB)"
FailedList.append(addFailed)

path_dir = ws + os.sep + str(name_dir)
outLocationCSV = path_dir + os.sep + "CSV"
OutFolderGDB = path_dir + os.sep + "GDB"

# Output File Names
ReNmCSVCopied = 'ReNm' + str(abb) + "_" + str(receivedDate)
out_nameGDB = "ReNm_" + str(abb) + "_" + str(receivedDate)
FailedGDB = "Failed_" + str(out_nameGDB)
ReNmCSVFailed = "Failed_" + str(ReNmCSVCopied)

# CREATES FILE NAMES
# CSV out table succeed and faile
csvfile, csvpath = create_flnm_timestamp(ReNmCSVCopied, outLocationCSV, datelist, 'csv')
failedcsv, failedcsvpath = create_flnm_timestamp(ReNmCSVFailed, outLocationCSV, datelist, 'csv')
# GDB succeed and faile
OutGDB, outFilegdbpath = create_flnm_timestamp(out_nameGDB, OutFolderGDB, datelist, 'gdb')
FailGDB, outFilefailgdbpath = create_flnm_timestamp(FailedGDB, OutFolderGDB, datelist, 'gdb')

arcpy.env.scratchWorkspace = ""
# NOTE Change this to False if you don't want GDB to be overwritten
arcpy.env.overwriteOutput = True

# Start script

# Copy shapefiles to a file geodatabase and rename

# start clock on timing script

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_directory(path_dir, outLocationCSV, OutFolderGDB)

create_gdb(OutFolderGDB, OutGDB, outFilegdbpath)
create_gdb(OutFolderGDB, FailGDB, outFilefailgdbpath)
StandName(InGDB, outFilegdbpath, outFilefailgdbpath)

# ##write data store in lists to out tables in csv format
create_out_table(OrgSRList, csvpath)
create_out_table(FailedList, failedcsvpath)
# #End clock time script
end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
