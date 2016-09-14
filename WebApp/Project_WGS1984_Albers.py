# Name: DefineProjection
# Description: This script will set the projection for all feature classes with the workspace first to the assigned
# geographic coordinate system and the into the assigned projected coordinate system

# import system modules
import os
import csv
import time
import datetime
import PrepFiles_CoOccur.functions

import arcpy

# ['Amphibians', 'Arachnids','Birds','Clams', 'Conifers and Cycads','Crustaceans','Fishes','Flowering Plants','Insects', 'Lichens','Reptiles', 'Snails']

#infolder = 'J:\Workspace\ESA_Species\Range\NAD83'
#outfolder ='J:\Workspace\ESA_Species\Range\WGS_Albers'

infolder = 'J:\Workspace\ESA_Species\Range\NAD83\\topo'
outfolder ='J:\Workspace\ESA_Species\CriticalHabitat\WGS_Albers'

masterlist = 'J:\Workspace\MasterLists\CSV\MasterListESA_April2015_20151015_20151118.csv'

midGBD = infolder + os.sep + 'middle2.gdb'

prjFolder = 'J:\Workspace\projections'
prjFile = "J:\Workspace\projections\WGS_1984_Albers.prj"

skiplist = []

def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")


def create_outtable(listname, csvlocation):
    with open(csvlocation, "wb") as output:
        writer = csv.writer(output, lineterminator='\n')
        for val in listname:
            writer.writerow([val])


def createdicts(csvfile):
    with open(csvfile, 'rb') as dictfile:
        group = csv.reader(dictfile)
        dictname = {rows[0]: rows[1] for rows in group}
        return dictname


def CreateDirectory(path_dir):
    if not os.path.exists(path_dir):
        os.mkdir(path_dir)
        print "created directory {0}".format(path_dir)


arcpy.env.overwriteOutput = True  # ## Change this to False if you don't want GDB to be overwritten
arcpy.env.scratchWorkspace = ""
start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)

grouplist = []
with open(masterlist, 'rU') as inputFile:
    header = next(inputFile)
    for line in inputFile:
        line = line.split(',')
        entid = line[0]
        group = line[1]
        grouplist.append(group)

inputFile.close()

unq_grps = set(grouplist)
alpha_group = sorted(unq_grps)

for group in alpha_group:
    if group in skiplist:
        continue

    print "\nWorking on {0}".format(group)



    outGDBname = group + '.gdb'
    outGDB = outfolder + os.sep + outGDBname
    outpathgdb = outGDB

    if not arcpy.Exists(outpathgdb):
        CreateGDB(outfolder, outGDBname, outpathgdb)
        inGDB = infolder + os.sep + outGDBname
        CreateGDB(outfolder, outGDBname, outGDB)
        WGScoordFile = prjFolder + os.sep + 'WGS 1984.prj'
            #print prjFile

        arcpy.env.workspace = inGDB
        fcList = arcpy.ListFeatureClasses()

        dscwgs = arcpy.Describe(WGScoordFile)
        wgscoord_sys = dscwgs.spatialReference
        dscprj = arcpy.Describe(prjFile)
        prjsr = dscprj.spatialReference
        prj_datum = prjsr.GCS.datumName


        total = len(fcList)
        for fc in fcList:
                infc = inGDB + os.sep + str(fc)
                ORGdsc = arcpy.Describe(infc)
                ORGsr = ORGdsc.spatialReference
                ORGprj = ORGsr.name.lower()

                if prj_datum == "D_North_American_1983":
                    prj_fcname = fc + "_NADAlbers"
                    prj_fc = outGDB + os.sep + prj_fcname

                    if not arcpy.Exists(prj_fc):
                        arcpy.Project_management(infc, prj_fc, prjsr)
                        print "completed {0} {1} remaining in {2}".format(fc, total, group)
                        total -= 1
                        continue
                    else:
                        total -= 1
                        print str(fc) + " already exists {0} {1} remaining in {2}".format(fc, total, group)
                        continue

                elif prj_datum == "D_WGS_1984":
                    infc = inGDB + os.sep + str(fc)
                    fcotherGEO = str(fc) + "_WGS84"
                    prj_fcname = fc + "_WGSAlbers"

                    outotherfc = midGBD + os.sep + fcotherGEO
                    prj_fc = outGDB + os.sep + prj_fcname

                    if not arcpy.Exists(outotherfc):
                        arcpy.Project_management(infc, outotherfc, wgscoord_sys)

                    if not arcpy.Exists(prj_fc):
                        arcpy.Project_management(infc, prj_fc, prjsr)
                        print "completed {0} {1} remaining in {2}".format(fc, total, group)
                        total -= 1
                        continue
                    else:
                        total -= 1
                        print str(fc) + " already exists {0} {1} remaining in {2}".format(fc, total, group)
                        continue

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
