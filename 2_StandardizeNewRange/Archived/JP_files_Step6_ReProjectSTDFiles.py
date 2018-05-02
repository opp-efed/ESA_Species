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
InGDB = r"E:\Species\GIS\FWS\FWS_received09_04_14\GDB\STD_ReNmNMFS_20140904_2015-01-14.gdb"

ws = "E:\Species\GIS\FWS"
name_dir = "FWS_received09_04_14"

ReProjectGDB = "FWS_JP_sept2014_Reprojected"
CSVName = "FWS_JP_sept2014_Reprojected"

NADabb = 'NAD83'
Prjabb = 'Albers'
Unknowncsv = 'Unknown'

# location of the .prj file for the desired geographic coordinate system
native = "C:\Users\Admin\AppData\Roaming\ESRI\Desktop10.2\ArcMap\Coordinate Systems\NAD 1983 UTM Zone  4N.prj"
coordFile = "C:\Users\Admin\AppData\Roaming\ESRI\Desktop10.2\ArcMap\Coordinate Systems\NAD 1983.prj"
WGScoordFile = "C:\Users\Admin\AppData\Roaming\ESRI\Desktop10.2\ArcMap\Coordinate Systems\WGS 1984.prj"
# Location of the .prj file for the desire projected coordinate system
prjFile = r"C:\Users\Admin\AppData\Roaming\ESRI\Desktop10.2\ArcMap\Coordinate Systems\USA_Contiguous_Albers_Equal_Area_Conic.prj"

# ################################################################################################################


def create_csvflnm_timestamp(namecsvfile, outcsvlocation):
    filename = str(namecsvfile) + "_" + str(datelist[0]) + '.csv'
    filepath = os.path.join(outcsvlocation, filename)
    return filename, filepath


def create_gdbflnm_timestamp(namegdbfile, outgdblocation):
    filename = str(namegdbfile) + "_" + str(datelist[0])
    filepath = os.path.join(outgdblocation, (filename + '.gdb'))
    return filename, filepath


def start_times(startclock):
    start_time = datetime.datetime.fromtimestamp(startclock)
    print "Start Time: " + str(start_time)
    print start_time.ctime()


def end_times(endclock, startclock):
    start_time = datetime.datetime.fromtimestamp(startclock)
    end = datetime.datetime.fromtimestamp(endclock)
    print "End Time: " + str(end)
    print end.ctime()
    elapsed = end - start_time
    print "Elapsed  Time: " + str(elapsed)

def create_outtable(listname, csvlocation):
    with open(csvlocation, "wb") as output:
        writer = csv.writer(output, lineterminator='\n')
        for val in listname:
            writer.writerow([val])

def CreateGDB (OutFolder,OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")
def CreateDirectory(path_dir, outLocationCSV, OutFolderGDB ):
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
today = datetime.date.today()
datelist.append(today)

path_dir = ws + os.sep + str(name_dir)
outLocationCSV = path_dir + os.sep + "CSV"
OutFolderGDB= path_dir + os.sep + "GDB"

NADCSV = str(NADabb) + "_" + str(CSVName)
PrjCSV = str(Prjabb) + "_" + str(CSVName)
Unknwn = str(Unknowncsv) + "_" + str(CSVName)

GeoReProject, geocsvfile = create_csvflnm_timestamp(NADCSV,outLocationCSV)
# CSV out table succeed file-complete spatial package

PrjReproject, prjcsv = create_csvflnm_timestamp(PrjCSV,outLocationCSV)
UnknownProject, unknowncsv = create_csvflnm_timestamp(Unknwn, outLocationCSV)
FNADabb = "Failed_" + str(NADabb)
FPrjabb = "Failed" + str(Prjabb)

NADFailed, NADfailedcsv = create_csvflnm_timestamp(FNADabb,outLocationCSV)
PrjFailed, prjfailedcsv = create_csvflnm_timestamp(FPrjabb,outLocationCSV)

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
start = time.time()
start_times(start)
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

arcpy.env.workspace = InGDB
fcList = arcpy.ListFeatureClasses()

for fc in fcList:
    # set local variables
    ORGdsc = arcpy.Describe(fc)
    ORGsr = ORGdsc.spatialReference
    ORGprj = ORGsr.name.lower()

    if ORGdsc.spatialReference.Name == "Unknown":
        addUnknownList = str(fc) + "Unknown"
        UnknownList.append(addUnknownList)
        fcNAD = str(fc) + "_" + str(NADabb)
        outfc = os.path.join(outWorkspaceNAD83, fcNAD)
        if not arcpy.Exists(outfc):
            arcpy.DefineProjection_management(fc ,native)
            #arcpy.CopyFeatures_management(fc, outfc)
            arcpy.Project_management(fc, outfc, coord_sys)
            print(arcpy.GetMessages(0))
            #addGeoList = str(fc) + "," + ORGsr.name + "," + ORGsr.type + "," + str(fcNAD) + "," +coord_sys.name + ",None"
            #OrgGeoList.append(addGeoList)
            continue
        if arcpy.Exists(outfc):
            print str(outfc) + " already exists"
            continue

    elif ORGdsc.spatialReference.Name == "Undefined":
        addUnknownList = str(fc) + "Undefined"
        UnknownList.append(addUnknownList)
        continue
    # add and elif here if go from a project coord to a different geo coor could cause a problem
    elif ORGsr.name == "GCS_North_American_1983":
        fcNAD = str(fc) + "_" + str(NADabb)
        outfc = os.path.join(outWorkspaceNAD83, fcNAD)
        if not arcpy.Exists(outfc):
            arcpy.CopyFeatures_management(fc, outfc)
            print(arcpy.GetMessages(0))
            addGeoList = str(fc) + "," + ORGsr.name + "," + ORGsr.type + "," + \
                         str(fcNAD) + "," + coord_sys.name + "," + "None"
            OrgGeoList.append(addGeoList)
            continue
        if arcpy.Exists(outfc):
            print str(outfc) + " already exisits"
            continue

    elif ORGdsc.spatialReference.Type == "Geographic":
        fcNAD = str(fc) + "_" + str(NADabb)
        outfc = os.path.join(outWorkspaceNAD83, fcNAD)
        if not arcpy.Exists(outfc):
            arcpy.Project_management(fc, outfc, coord_sys)
            print(arcpy.GetMessages(0))
            addGeoList = str(fc) + "," + ORGsr.name + "," + ORGsr.type + "," + \
                         str(fcNAD) + "," + coord_sys.name + "," + "None"
            OrgGeoList.append(addGeoList)
            continue
        if arcpy.Exists(outfc):
            print str(outfc) + " already exists"
            continue

    elif ORGdsc.spatialReference.Type == "Projected":
        print ORGsr.GCS.datumName
        if ORGsr.GCS.datumName == "D_North_American_1983":
            fcNAD = str(fc) + "_" + str(NADabb)
            outfc = os.path.join(outWorkspaceNAD83, fcNAD)
            if not arcpy.Exists(outfc):
                arcpy.Project_management(fc, outfc, coord_sys)
                print(arcpy.GetMessages(0))
                addGeoList = str(fc) + "," + ORGsr.name + "," + ORGsr.type + "," + str(fcNAD) + "," + \
                             coord_sys.name + ",None"
                OrgGeoList.append(addGeoList)
                continue
            if arcpy.Exists(outfc):
                print str(outfc) + " already exists"
                continue
        else:
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
                    continue
                if arcpy.Exists(outfc):
                    print str(outfc) + " already exists"
                    continue
    else:
        print "Failed to add filename " + str(fc)
        addFailList = str(fc)
        FailedGeoList.append(addFailList)
        continue

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
            continue
        if arcpy.Exists(outfc):
            print str(outfc) + " already exists"
            continue




    except:
        print "Failed to add filename " + str(fc)
        addFailList = str(fc)
        FailPrjList.append(addFailList)

###write data store in lists to out tables in csv format
create_outtable(OrgGeoList, geocsvfile)
create_outtable(OrgPrjList, prjcsv)
create_outtable(UnknownList, unknowncsv)
create_outtable(FailedGeoList, NADfailedcsv)
create_outtable(FailPrjList, prjfailedcsv)
##End clock time script
done = time.time()
end_times(done, start)
