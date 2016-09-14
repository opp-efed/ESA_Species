# Name: DefineProjection
# Description: This script will set the projection for all feature classes with the workspace first to the assigned
# geographic coordinate system and the into the assigned projected coordinate system

# import system modules
import os
import csv
import time
import datetime

import arcpy

#['Amphibians', 'Arachnids','Birds','Clams', 'Conifers and Cycads','Crustaceans','Fishes','Flowering Plants','Insects', 'Lichens','Reptiles', 'Snails']

#,'Ferns and Allies','Mammals','Corals'
inGDB = 'J:\Workspace\ESA_Species\FinalBE_ForCoOccur\Union_Range.gdb'
outfolder ='J:\Workspace\ESA_Species\FinalBE_ForCoOccur'

#outGDBname = 'Lower48Only_AlbersEqualArea.gdb'
outGDBname ='Projected_UnionRange_20160705.gdb'
outGDB=outfolder+os.sep+outGDBname
midGBD='C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\Catchment\middle.gdb'

prjFolder = "C:\Users\Admin\Documents\Jen\Workspace\projections"
prjFile = "C:\Users\Admin\Documents\Jen\Workspace\projections\Albers_Conical_Equal_Area.prj"

regionname ='Lower48Only'

def fcs_in_workspace(workspace):
  arcpy.env.workspace = workspace
  for fc in arcpy.ListFeatureClasses():
    yield(fc)
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


arcpy.env.overwriteOutput = True  # ## Change this to False if you don't want GDB to be overwritten
arcpy.env.scratchWorkspace = ""
start_script = datetime.datetime.now()

print "Script started at {0}".format(start_script)
CreateGDB(outfolder,outGDBname ,outGDB)

WGScoordFile = prjFolder + os.sep + 'WGS 1984.prj'
print prjFile


arcpy.env.workspace = inGDB
fcList = arcpy.ListFeatureClasses()
#fcList = arcpy.ListRasters()
dscwgs = arcpy.Describe(WGScoordFile)
wgscoord_sys = dscwgs.spatialReference
dscprj = arcpy.Describe(prjFile)
prjsr = dscprj.spatialReference
prj_datum= prjsr.GCS.datumName

print prj_datum
print fcList

for fc in fcList:
    infc = inGDB +os.sep+str(fc)
    print infc

    ORGdsc = arcpy.Describe(infc)
    ORGsr = ORGdsc.spatialReference
    ORGprj = ORGsr.name.lower()
    if prj_datum == "D_WGS_1984":
            infc = inGDB +os.sep+str(fc)
            fcotherGEO = str(fc) + "_WGS84"
            prj_fcname =fcotherGEO+"_" + regionname+"prj"

            outotherfc = midGBD+os.sep+fcotherGEO
            prj_fc = outGDB+os.sep+prj_fcname

            if not arcpy.Exists(outotherfc):
                arcpy.Project_management(infc, outotherfc, wgscoord_sys)
                print(arcpy.GetMessages(0))

            if not arcpy.Exists(prj_fc):
                arcpy.Project_management(infc,prj_fc,prjsr)
                print(arcpy.GetMessages(0))
            else:
                print str(fc) + " already exists"
    else:
        prj_fcname =str(fc) +"_" + "AlbersEqualArea"
        prj_fc = outGDB+os.sep+prj_fcname

        if not arcpy.Exists(prj_fc):
            arcpy.ProjectRaster_management(infc,prj_fc,prjsr)
            #arcpy.Project_management(infc,prj_fc,prjsr)
            print(arcpy.GetMessages(0))
        else:
            print str(fc) + " already exists"

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)

