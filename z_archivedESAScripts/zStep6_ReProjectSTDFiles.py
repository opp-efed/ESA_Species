# Name: DefineProjection
# Description: This script will set the projection for all feature classes with the workspace first to the assigned
# geographic coordinate system and the into the assigned projected coordinate system

# import system modules
import os
import csv
import time
import datetime

import arcpy

arcpy.env.overwriteOutput = True  # ## Change this to False if you don't want GDB to be overwritten
arcpy.env.scratchWorkspace = ""

# #######################################################################################User define variables
InGDB = r"C:\Users\Admin\Documents\Jen\SpeceisToRun\Missing_20151228\GDB\STD_ReNmFWS20151228_2015-12-28.gdb"

ws = "C:\Users\Admin\Documents\Jen\SpeceisToRun"
name_dir = "Missing_20151228"

# in yyyymmdd received date
receivedDate = '20151228'



ReProjectGDB = "FWS_Missing_20151228_Reprojected"
CSVName = "FWS_Missing_20151228_Reprojected"

NADabb = 'NAD83'
Prjabb = 'Albers'
Unknowncsv = 'Unknown'

# location of the .prj file for the desired geographic coordinate system
# coordFile = "C:\Users\JConno02\AppData\Roaming\ESRI\Desktop10.2\ArcMap\Coordinate Systems\NAD 1983.prj"
# WGScoordFile = "C:\Users\JConno02\AppData\Roaming\ESRI\Desktop10.2\ArcMap\Coordinate Systems\WGS 1984.prj"
# Location of the .prj file for the desire projected coordinate system
# prjFile = r"C:\Users\JConno02\AppData\Roaming\ESRI\Desktop10.2\ArcMap\Coordinate Systems\USA Contiguous Albers Equal Area Conic.prj"
proj_Folder = 'C:\Users\Admin\Documents\Jen\Workspace\projections'
coordFile = proj_Folder + os.sep + 'NAD 1983.prj'
WGScoordFile = proj_Folder + os.sep + 'WGS 1984.prj'
Nad83HarncoordFile = proj_Folder + os.sep + 'NAD 1983 HARN StatePlane Hawaii 3 FIPS 5103 (US Feet).prj'
# Location of the .prj file for the desire projected coordinate system
prjFile = proj_Folder + os.sep + 'USA_Contiguous_Albers_Equal_Area_Conic.prj'


# ################################################################################################################


def create_csvflnm_timestamp(namecsvfile, outcsvlocation):
    filename = str(namecsvfile) + "_" + str(datelist[0]) + '.csv'
    filepath = os.path.join(outcsvlocation, filename)
    return filename, filepath


def create_gdbflnm_timestamp(namegdbfile, outgdblocation):
    filename = str(namegdbfile) + "_" + str(datelist[0])
    filepath = os.path.join(outgdblocation, (filename + '.gdb'))
    return filename, filepath


def create_outtable(listname, csvlocation):
    with open(csvlocation, "wb") as output:
        writer = csv.writer(output, lineterminator='\n')
        for val in listname:
            writer.writerow([val])


def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")


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


# Static variable no user input needed
# Creates file names

datelist = []
todaydate = datetime.date.today()
datelist.append(todaydate)

path_dir = ws + os.sep + str(name_dir)
outLocationCSV = path_dir + os.sep + "CSV"
OutFolderGDB = path_dir + os.sep + "GDB"

NADCSV = str(NADabb) + "_" + str(CSVName)
PrjCSV = str(Prjabb) + "_" + str(CSVName)
Unknwn = str(Unknowncsv) + "_" + str(CSVName)

GeoReProject, geocsvfile = create_csvflnm_timestamp(NADCSV, outLocationCSV)
# CSV out table succeed file-complete spatial package

PrjReproject, prjcsv = create_csvflnm_timestamp(PrjCSV, outLocationCSV)
UnknownProject, unknowncsv = create_csvflnm_timestamp(Unknwn, outLocationCSV)
FNADabb = "Failed_" + str(NADabb)
FPrjabb = "Failed" + str(Prjabb)

NADFailed, NADfailedcsv = create_csvflnm_timestamp(FNADabb, outLocationCSV)
PrjFailed, prjfailedcsv = create_csvflnm_timestamp(FPrjabb, outLocationCSV)

otherGeoprjGDB = "OtherGeo" + "_" + str(ReProjectGDB)
NADProjectGDB = str(NADabb) + "_" + str(ReProjectGDB)
PRJProjectGDB = str(Prjabb) + "_" + str(ReProjectGDB)

NADGDB, NADGDBpath = create_gdbflnm_timestamp(NADProjectGDB, OutFolderGDB)
PRJGDB, PRJGDBpath = create_gdbflnm_timestamp(PRJProjectGDB, OutFolderGDB)
otherGeoGDB, OtherGeoGDBpath = create_gdbflnm_timestamp(otherGeoprjGDB, OutFolderGDB)

outWorkspaceNAD83 = NADGDBpath
outWorkspacePrj = PRJGDBpath
outWorkspaceOtherGeo = OtherGeoGDBpath



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
# #####################################################################################################################
##############################################################################################################
start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)
# Prints Elapse clock

CreateDirectory(path_dir, outLocationCSV, OutFolderGDB)
CreateGDB(OutFolderGDB, NADGDB, NADGDBpath)
CreateGDB(OutFolderGDB, PRJGDB, PRJGDBpath)
CreateGDB(OutFolderGDB, otherGeoGDB, OtherGeoGDBpath)

dsc = arcpy.Describe(coordFile)
coord_sys = dsc.spatialReference

dscprj = arcpy.Describe(prjFile)
prj_sys = dscprj.spatialReference

dscwgs = arcpy.Describe(WGScoordFile)
wgscoord_sys = dscwgs.spatialReference

dscnadharn = arcpy.Describe(Nad83HarncoordFile)
nad83scoord_sys = dscnadharn.spatialReference

arcpy.env.workspace = InGDB
fcList = arcpy.ListFeatureClasses()

for fc in fcList:
    print fc
    # set local variables
    ORGdsc = arcpy.Describe(fc)
    ORGsr = ORGdsc.spatialReference
    ORGprj = ORGsr.name.lower()

    if ORGdsc.spatialReference.Name == "Unknown":
        addUnknownList = str(fc) + "Unknown"
        UnknownList.append(addUnknownList)
    elif ORGdsc.spatialReference.Name == "Undefined":
        addUnknownList = str(fc) + "Undefined"
        UnknownList.append(addUnknownList)

    # projected datasets

    elif ORGdsc.spatialReference.Type == "Projected":
        print "Projected Conversion"
        # print ORGsr.GCS.datumName
        if ORGsr.GCS.datumName == "D_North_American_1983":
            fcNAD = str(fc) + "_" + str(NADabb)
            outfc = os.path.join(outWorkspaceNAD83, fcNAD)
            if arcpy.Exists(outfc):
                print str(fc) + " already exists"
                continue
            if not arcpy.Exists(outfc):
                arcpy.Project_management(fc, outfc, coord_sys)
                print(arcpy.GetMessages(0))
                addGeoList = str(fc) + "," + ORGsr.name + "," + ORGsr.type + "," + str(fcNAD) + "," + \
                             coord_sys.name + ",None"
                OrgGeoList.append(addGeoList)
        if ORGsr.GCS.datumName == "D_North_American_1983_HARN":
            fcotherGEO = str(fc) + "_NAD83HARN"
            outotherfc = os.path.join(outWorkspaceOtherGeo, fcotherGEO)
            fcNAD = str(fcotherGEO) + "_" + str(NADabb)
            outfc = os.path.join(outWorkspaceNAD83, fcNAD)
            if not arcpy.Exists(outotherfc):
                arcpy.Project_management(fc, outotherfc, nad83scoord_sys)
                print(arcpy.GetMessages(0))
                fcNAD = str(fcotherGEO) + "_" + str(NADabb)
                outfc = os.path.join(outWorkspaceNAD83, fcNAD)
                arcpy.Project_management(outotherfc, outfc, coord_sys)
                print(arcpy.GetMessages(0))
                addGeoList = str(fc) + "," + ORGsr.name + "," + ORGsr.type + "," + str(
                    fcNAD) + "," + coord_sys.name + "," + nad83scoord_sys.name
                OrgGeoList.append(addGeoList)
            if arcpy.Exists(outfc):
                    print str(fc) + " already exists"
        else:
            if ORGsr.GCS.datumName == "D_WGS_1984":
                fcotherGEO = str(fc) + "_WGS84"
                outotherfc = os.path.join(outWorkspaceOtherGeo, fcotherGEO)
                fcNAD = str(fcotherGEO) + "_" + str(NADabb)
                outfc = os.path.join(outWorkspaceNAD83, fcNAD)
                if not arcpy.Exists(outotherfc):
                    arcpy.Project_management(fc, outotherfc, wgscoord_sys)
                    print(arcpy.GetMessages(0))
                    fcNAD = str(fcotherGEO) + "_" + str(NADabb)
                    outfc = os.path.join(outWorkspaceNAD83, fcNAD)
                    arcpy.Project_management(outotherfc, outfc, coord_sys)
                    print(arcpy.GetMessages(0))
                    addGeoList = str(fc) + "," + ORGsr.name + "," + ORGsr.type + "," + \
                                 str(fcNAD) + "," + coord_sys.name + "," + wgscoord_sys.name
                    OrgGeoList.append(addGeoList)
                if arcpy.Exists(outfc):
                    print str(fc) + " already exists"
            else:
                print "Failed to add filename " + str(fc)
                addFailList = str(fc)
                FailedGeoList.append(addFailList)

    elif ORGdsc.spatialReference.Type == "Geographic":
        print "Geographic Conversion"
        if ORGsr.GCS.datumName == "D_WGS_1984":
            fcotherGEO = str(fc) + "_WGS84"
            outotherfc = os.path.join(outWorkspaceOtherGeo, fcotherGEO)
            if not arcpy.Exists(outotherfc):
                arcpy.Project_management(fc, outotherfc, wgscoord_sys)
                print(arcpy.GetMessages(0))
                fcNAD = str(fcotherGEO) + "_" + str(NADabb)
                outfc = os.path.join(outWorkspaceNAD83, fcNAD)
                arcpy.Project_management(outotherfc, outfc, coord_sys)
                print(arcpy.GetMessages(0))
                addGeoList = str(fc) + "," + ORGsr.name + "," + ORGsr.type + "," + \
                             str(fcNAD) + "," + coord_sys.name + "," + wgscoord_sys.name
                OrgGeoList.append(addGeoList)
            if arcpy.Exists(outfc):
                print str(fc) + " already exists"
            continue
        if ORGsr.GCS.datumName == "D_North_American_1983":
            fcNAD = str(fc) + "_" + str(NADabb)
            outfc = os.path.join(outWorkspaceNAD83, fcNAD)
            if not arcpy.Exists(outfc):
                arcpy.Project_management(fc, outfc, coord_sys)
                print(arcpy.GetMessages(0))
                addGeoList = str(fc) + "," + ORGsr.name + "," + ORGsr.type + "," + \
                             str(fcNAD) + "," + coord_sys.name + "," + "None"
                OrgGeoList.append(addGeoList)
            if arcpy.Exists(outfc):
                print str(fc) + " already exists"

    else:
        print "Failed to add filename " + str(fc)
        addFailList = str(fc)
        FailedGeoList.append(addFailList)

arcpy.env.workspace = outWorkspaceNAD83
fcList2 = arcpy.ListFeatureClasses()

for fc in fcList2:
    Geodsc = arcpy.Describe(fc)
    fcprj = str(fc) + "_" + str(Prjabb)
    outfc = os.path.join(outWorkspacePrj, fcprj)

    Geosr = Geodsc.spatialReference
    Geoprj = Geosr.name.lower()

    try:
        if not arcpy.Exists(outfc):
            arcpy.Project_management(fc, outfc, prj_sys)
            print(arcpy.GetMessages(0))
            addPrjList = str(fc) + "," + prj_sys.name
            OrgPrjList.append(addPrjList)
        if arcpy.Exists(outfc):
            print str(outfc) + " already exists"

    except:
        print "Failed to add filename " + str(fc)
        addFailList = str(fc)
        FailPrjList.append(addFailList)
if len(FailedGeoList) > 0:
    "Print Check projections of {0} Datumn needs to be added to script".format(FailedGeoList)

if len(FailPrjList) > 0:
    "Print could not project into Albers  {0} ".format(FailPrjList)

###write data store in lists to out tables in csv format
create_outtable(OrgGeoList, geocsvfile)
create_outtable(OrgPrjList, prjcsv)
create_outtable(UnknownList, unknowncsv)
create_outtable(FailedGeoList, NADfailedcsv)
create_outtable(FailPrjList, prjfailedcsv)
##End clock time script

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
