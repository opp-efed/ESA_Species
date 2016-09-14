import os
import csv

import datetime

import arcpy

# #Tile: Project all files into STD geo projection of NAD83.  Output table will track ordinal project and the step
# take to get to the final projection.  Any fc that fail are moved to a failed GDB and fc with an unknown projection
# are not re-projected but added the the unknown list that is exported at the end

# TODO set up user input to dynamically generate the middle gdb and delete it at the end
# TODO see script Step3_ProjectRegionalFiles

# User define variables
InGDB =r"C:\Workspace\Processed_20160906\FWS_Ranges_20160906\GDB\STD_ReNmFWS20160906_2016-09-06.gdb"

# Location of prj files
proj_Folder = 'J:\Workspace\projections'

# Temp GDB for geo transformations
middlegdb = r'C:\WorkSpace\temp.gdb'

# Workspace
ws = "C:\WorkSpace\Processed_20160906"
# Folder in workspace where outputs will be saved
name_dir = "FWS_Ranges_20160906"

# in yyyymmdd received date
receivedDate = '20160906'

ReProjectGDB = "R_polygon_20160906_Reprojected"
CSVName = "R_polygon_20160906_Reprojected"

NADabb = 'NAD83'
Unknowncsv = 'Unknown'

# Geo projections that have been see previously NOTE if a new project turns up then it will  need to be added to the
# projection folder and added to this script
coordFile = proj_Folder + os.sep + 'NAD 1983.prj'
WGScoordFile = proj_Folder + os.sep + 'WGS 1984.prj'
Nad83HarncoordFile = proj_Folder + os.sep + 'NAD 1983 HARN.prj'


# ################################################################################################################


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


# Static variable no user input needed #####################################################################
# Creates file names

datelist = []
todaydate = datetime.date.today()
datelist.append(todaydate)

path_dir = ws + os.sep + str(name_dir)
outLocationCSV = path_dir + os.sep + "CSV"
OutFolderGDB = path_dir + os.sep + "GDB"

NADCSV = str(NADabb) + "_" + str(CSVName)
Unknwn = str(Unknowncsv) + "_" + str(CSVName)

GeoReProject, geocsvfile = create_flnm_timestamp(NADCSV, outLocationCSV, datelist, 'csv')
# CSV out table succeed file-complete spatial package

UnknownProject, unknowncsv = create_flnm_timestamp(Unknwn, outLocationCSV, datelist, 'csv')
FNADabb = "Failed_" + str(NADabb)

NADFailed, NADfailedcsv = create_flnm_timestamp(FNADabb, outLocationCSV, datelist, 'csv')

otherGeoprjGDB = str(NADabb) + "_" + str(ReProjectGDB)

otherGeoGDB, OtherGeoGDBpath = create_flnm_timestamp(otherGeoprjGDB, OutFolderGDB, datelist, 'gdb')

outWorkspaceOtherGeo = OtherGeoGDBpath

# NOTE Change this to False if you don't want GDB to be overwritten
arcpy.env.overwriteOutput = True
arcpy.env.workspace = OutFolderGDB


# Empty list to store information about each of the feature classes

OrgGeoList = []
AddGeoList = "Filename" + "," + "Starting Projection Name" + "," + "Starting Projection Type" + "," + \
             "Filename Reprojected" + "," + "Final Geographic System Name" + "," + "Middle Geographic System Name"
OrgGeoList.append(AddGeoList)

FailedGeoList = []
addFailedGeo = "Filename-(GDB)"
FailedGeoList.append(addFailedGeo)

FailPrjList = []
addFailedPrj = "Filename-(GDB)"
FailPrjList.append(addFailedPrj)

OrgPrjList = []
AddPrjList = "Filename" + "," + "Reproject Projected System Name"
OrgPrjList.append(AddPrjList)

UnknownList = []
addUnknown = "Filename-(GDB)", ",", "Projection"
UnknownList.append(addUnknown)

# #############################################################################################################
# start clock on timing script

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_directory(path_dir, outLocationCSV, OutFolderGDB)

create_gdb(OutFolderGDB, otherGeoGDB, OtherGeoGDBpath)
outgdb = OtherGeoGDBpath


# Extracts spatial information from prj files
dsc = arcpy.Describe(coordFile)
coord_sys = dsc.spatialReference

dscwgs = arcpy.Describe(WGScoordFile)
wgscoord_sys = dscwgs.spatialReference

dscnadharn = arcpy.Describe(Nad83HarncoordFile)
nad83scoord_sys = dscnadharn.spatialReference

grouplist = []
NADabb = "NAD83"

unknownprj = []
group_gdb = InGDB

arcpy.env.workspace = group_gdb
fc_list = arcpy.ListFeatureClasses()

# for each file in in GDB checks the spatial reference, and re-projects into the standard,
# if a transformation this is done to the temp gdb
for fc in fc_list:
    infc = group_gdb + os.sep + fc
    ORGdsc = arcpy.Describe(fc)
    ORGsr = ORGdsc.spatialReference
    ORGprj = ORGsr.name.lower()
    if ORGdsc.spatialReference.Type == "Projected":
        print "Projected Conversion"
        # print ORGsr.GCS.datumName
        if ORGsr.GCS.datumName == "D_North_American_1983":
            fcNAD = str(fc) + "_" + str(NADabb)
            outfc = outgdb + os.sep + fcNAD
            if arcpy.Exists(outfc):
                print str(fc) + " already exists"
                continue
            else:
                arcpy.Project_management(infc, outfc, coord_sys)
                print "completed {0}".format(fc)

        elif ORGsr.GCS.datumName == "D_WGS_1984":
            fcotherGEO = str(fc) + "_WGS84"
            outotherfc = middlegdb + os.sep + fcotherGEO

            fcNAD = str(fcotherGEO) + "_" + str(NADabb)
            outfc = outgdb + os.sep + fcNAD

            if not arcpy.Exists(outotherfc):
                arcpy.Project_management(infc, outotherfc, wgscoord_sys)
            if not arcpy.Exists(outfc):
                arcpy.Project_management(outotherfc, outfc, coord_sys)
                print "completed {0}".format(fc)
            else:
                print str(fc) + " already exists"
        elif ORGsr.GCS.datumName == 'D_North_American_1983_HARN':
            fcotherGEO = str(fc) + "_NAD83HARN"
            outotherfc = middlegdb + os.sep + fcotherGEO

            fcNAD = str(fcotherGEO) + "_" + str(NADabb)
            outfc = outgdb + os.sep + fcNAD

            if not arcpy.Exists(outotherfc):
                arcpy.Project_management(infc, outotherfc, nad83scoord_sys)
            if not arcpy.Exists(outfc):
                arcpy.Project_management(outotherfc, outfc, coord_sys)
                print "completed {0}".format(fc)
            else:
                print str(fc) + " already exists"
        else:
            print ORGsr.name.lower()
            print ORGsr.GCS.datumName
            unknownprj.append(fc)
            print "Unknown projection"

    elif ORGdsc.spatialReference.Type == "Geographic":
        print "Geographic Conversion"
        if ORGsr.GCS.datumName == "D_WGS_1984":
            fcotherGEO = str(fc) + "_WGS84"
            outfc = outgdb + os.sep + fcotherGEO + "_" + str(NADabb)

            if not arcpy.Exists(outfc):
                arcpy.Project_management(infc, outfc, coord_sys)
                print "completed {0}".format(fc)
            else:
                print str(fc) + " already exists"

        elif ORGsr.GCS.datumName == "D_North_American_1983":
            fcNAD = str(fc) + "_" + str(NADabb)
            outfc = outgdb + os.sep + fcNAD
            if not arcpy.Exists(outfc):
                arcpy.Project_management(fc, outfc, coord_sys)
                print "completed {0}".format(fc)
            else:
                print str(fc) + " already exists"

        elif ORGsr.GCS.datumName == 'D_North_American_1983_HARN':
            fcotherGEO = str(fc) + "_NAD83HARN"
            outotherfc = middlegdb + os.sep + fcotherGEO

            fcNAD = str(fcotherGEO) + "_" + str(NADabb)
            outfc = outgdb + os.sep + fcNAD

            if not arcpy.Exists(outotherfc):
                arcpy.Project_management(infc, outotherfc, nad83scoord_sys)
            if not arcpy.Exists(outfc):
                arcpy.Project_management(outotherfc, outfc, coord_sys)
                print "completed {0}".format(fc)
            else:
                print str(fc) + " already exists"
        elif ORGsr.GCS.datumName == 'North_American_Datum_1983':
            fcotherGEO = str(fc) + "_NAD83"
            outfc = outgdb + os.sep + fcotherGEO

            if not arcpy.Exists(outfc):
                arcpy.Project_management(infc, outfc, coord_sys)
                print "completed {0}".format(fc)

            else:
                print str(fc) + " already exists"

        else:
            print  ORGsr.name.lower()
            print ORGsr.GCS.datumName
            unknownprj.append(fc)
            print "Failed to add filename " + str(fc)

    else:
        unknownprj.append(fc)
        print "Failed to add filename " + str(fc)

print "Failed fcs {0}".format(unknownprj)

create_out_table(unknownprj, (outLocationCSV) +os.sep +'Unknown_projection.csv')

# #End clock time script
end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
