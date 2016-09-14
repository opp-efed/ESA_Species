# Name: DefineProjection
# Description: This script will set the projection for all feature classes with the workspace first to the assigned
# geographic coordinate system and the into the assigned projected coordinate system

# import system modules
import os
import csv
import datetime
import shutil

import arcpy

masterlist = 'J:\Workspace\MasterLists\April2015Lists\CSV\MasterListESA_April2015_20151015_20151124.csv'
templocation = r'C:\Workspace\temp\temp1.gdb'
inprj_dict = 'J:\Workspace\ESA_Species\ForCoOccur\Dict\Reproject_dict_simplfied.csv'
# TODO incorpoate dict into script so that it does not need to load file separately

skipgroup = ['Amphibians', 'Arachnids', 'Birds', 'Clams', 'Conifers and Cycads', 'Corals', 'Ferns and Allies', 'Fishes',
             'Flowering Plants', 'Insects', 'Lichens', 'Mammals', 'Reptiles', 'Snails']
skipregions = []

# TODO had in try except loop that will export completed regions and groups if the script bombs to be used as inputs when restarted
while True:
    user_input = raw_input('Are you running range files Yes or No? ')
    if user_input not in ['Yes', 'No']:
        print 'This is not a valid answer'
    else:
        if user_input == 'Yes':
            inFolder = 'J:\Workspace\ESA_Species\ForCoOccur\Range'
            proj_Folder = 'J:\Workspace\projections'
            print 'Running range files output will be located at {0}'.format(inFolder)
            speciestype = 'Range'
            break
        else:
            inFolder = 'J:\Workspace\ESA_Species\ForCoOccur\CriticalHabitat'
            proj_Folder = 'J:\Workspace\projections'
            speciestype = 'Critical Habitat'
            print 'Running critical habitat files output will be located at {0}'.format(inFolder)
            break

FirstRun = True


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

patht, gdb = os.path.split(templocation)
temppath = patht + os.sep + gdb
midGBD = temppath
CreateDirectory(patht)
CreateGDB(patht, gdb, temppath)

if os.path.exists(temppath):
    FirstRun = False

grouplist = []
prjdict = createdicts(inprj_dict)

with open(masterlist, 'rU') as inputFile:
    header = next(inputFile)
    for line in inputFile:
        line = line.split(',')
        group = line[1]
        grouplist.append(group)
inputFile.close()

unq_grps = set(grouplist)
alpha_group = sorted(unq_grps)
print alpha_group

breakbool = False
for group in alpha_group:
    if breakbool:
        break
    elif group in skipgroup:
        continue
    groupfolder = inFolder + os.sep + group + os.sep + "Regions"
    infolder = groupfolder + os.sep + "NAD83"
    with open(inprj_dict, 'rU') as inputFile2:
        for line in inputFile2:
            if breakbool:
                break
            else:
                line = line.split(',')
                gdb = str(line[0])
                gdb = gdb.strip("\n")
                regionname = str(line[3])
                regionname = regionname.strip("\n")
                if regionname in skipregions:
                    continue
                prj = line[1]
                prj = proj_Folder + os.sep + prj
                abb = line[2]
                InGDB = infolder + os.sep + gdb
                InGDB = InGDB.strip("\n")
                path, tail = os.path.split(InGDB)

                if not arcpy.Exists(InGDB):
                    continue

                print '\nWorking on {0} in {1} in {2}'.format(group, regionname, prj)
                outgdb_name = regionname + "_" + abb

                arcpy.env.workspace = InGDB
                fcList = arcpy.ListFeatureClasses()

                total = len(fcList)
                if len(fcList) == 0:
                    print "There are no {0} species {2} in {1}".format(group, regionname, speciestype)
                    continue
                else:
                    outgdb_name = outgdb_name.strip('\n')
                    outfolder = groupfolder + os.sep + "ProjectedSinglePart"
                    CreateDirectory(outfolder)
                    outGDB = outfolder + os.sep + outgdb_name

                    if not arcpy.Exists(outGDB):
                        CreateGDB(outfolder, outgdb_name, outGDB)
                    else:

                        arcpy.env.workspace = outGDB
                        fcList2 = arcpy.ListFeatureClasses()

                        if len(fcList) == len(fcList2):
                            print "All {0} species files {2} projected in {1}".format(group, regionname, speciestype)
                            continue
                    # TODO from her below put this got in a function that will do the projection
                    WGScoordFile = proj_Folder + os.sep + 'WGS 1984.prj'
                    prjFile = proj_Folder + os.sep + prj
                    dscwgs = arcpy.Describe(WGScoordFile)
                    wgscoord_sys = dscwgs.spatialReference
                    dscprj = arcpy.Describe(prj)
                    prjsr = dscprj.spatialReference
                    prj_datum = prjsr.GCS.datumName

                    for fc in fcList:

                        infc = InGDB + os.sep + fc
                        ORGdsc = arcpy.Describe(infc)
                        ORGsr = ORGdsc.spatialReference
                        ORGprj = ORGsr.name.lower()

                        if prj_datum == "D_North_American_1983":

                            prj_fcname = fc + "_" + regionname + "prj"
                            prj_fc = outGDB + os.sep + prj_fcname

                            if not arcpy.Exists(prj_fc):
                                arcpy.Project_management(infc, prj_fc, prjsr)
                                print "completed {0} {1} remaining in {2}".format(fc, total, group)
                                total -= 1
                                continue
                            else:
                                total -= 1
                                continue

                        if prj_datum == "D_WGS_1984":
                            infc = InGDB + os.sep + str(fc)

                            fcotherGEO = str(fc) + "_WGS84"
                            prj_fcname = fcotherGEO + "_" + regionname + "prj"

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
                                continue
                    # This will be outside of the function
                    arcpy.env.workspace = outGDB
                    fcList2 = arcpy.ListFeatureClasses()

                    if len(fcList) == len(fcList2):
                        print "All {0} species files {2} projected in {1}".format(group, regionname, speciestype)
                    else:
                        print "Check for missing {0} files in {1}, {2}".format(group, regionname, speciestype)
                        breakbool = True
                        break

    inputFile2.close()

if breakbool:
    print('ERROR Projecting a file')
else:
    while True:
        user_input = raw_input('Do you want to delete temp files at {0} Yes or No: '.format(patht))
        if user_input in ['Yes', 'No']:
            break
        else:
            print('\nThat is not a valid option! Type Yes or No')

    if user_input == 'Yes':
        tempfiles = os.listdir(patht)
        for v in tempfiles:
            delfile = os.path.join(patht, v)
            if os.path.exists(delfile):
                shutil.rmtree(delfile)
                print "deleting {0}".format(delfile)
        print "deleting {0}".format(patht)
        if os.path.exists(patht):
            shutil.rmtree(patht)
    elif user_input == 'No':
        print "Temp files found at {0}".format(patht)
    else:
        print 'Del failed'

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
