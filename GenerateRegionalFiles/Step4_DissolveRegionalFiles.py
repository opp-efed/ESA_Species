import os
import csv
import datetime

import arcpy

dissolveFields = ['NAME', 'Name_sci', 'SPCode', 'VIPCode', 'FileName', 'EntityID']

masterlist = 'J:\Workspace\MasterLists\April2015Lists\CSV\MasterListESA_April2015_20151015_20151124.csv'
gdbRegions_dict = 'J:\Workspace\ESA_Species\ForCoOccur\Dict\gdbRegions_dict.csv'

skipgroup = ['Amphibians', 'Arachnids', 'Birds', 'Clams', 'Conifers and Cycads', 'Corals', 'Ferns and Allies', 'Fishes',
             'Flowering Plants', 'Insects', 'Lichens', 'Mammals', 'Reptiles', 'Snails']
skipregions = []

## TODO check to see if do this  by regions and then group would simplify code
while True:
    user_input = raw_input('Are you running range files Yes or No? ')
    if user_input not in ['Yes', 'No']:
        print 'This is not a valid answer'
    else:
        if user_input == 'Yes':
            infolder = 'J:\Workspace\ESA_Species\ForCoOccur\Range'

            proj_Folder = 'J:\Workspace\projections'
            print 'Running range files output will be located at {0}'.format(infolder)
            speciestype = 'Range'
            break
        else:
            infolder = 'J:\Workspace\ESA_Species\ForCoOccur\CriticalHabitat'

            proj_Folder = 'J:\Workspace\projections'
            print 'Running critical habitat files output will be located at {0}'.format(infolder)
            speciestype = 'Critical Habitat'
            break


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

grouplist = []
with open(masterlist, 'rU') as inputFile:
    header = next(inputFile)
    for line in inputFile:
        line = line.split(',')
        group = line[1]
        grouplist.append(group)
inputFile.close()

unq_grps = set(grouplist)
alpha_group = sorted(unq_grps)
checkbool = False
for group in alpha_group:
    if checkbool:
        break
    elif group in skipgroup:
        continue
    groupfolder = infolder + os.sep + group + os.sep + "Regions"
    with open(gdbRegions_dict, 'rU') as inputFile2:
        for line in inputFile2:

            line = line.split(',')
            gdb = str(line[0])
            gdb = gdb.strip("\n")
            regionsgdb = groupfolder + os.sep + "ProjectedSinglePart" + os.sep + gdb

            if not arcpy.Exists(regionsgdb):
                continue
            else:
                regionname = gdb.split("_")
                regionname = regionname[0]
                if regionname in skipregions:
                    continue
                arcpy.env.workspace = regionsgdb
                # print InGDB
                fcList = arcpy.ListFeatureClasses()
                total = len(fcList)
                if total == 0:
                    print "There are no {1} species {2} in {0}".format(regionname, group, speciestype)
                    continue
                else:
                    outgdb_name = gdb.strip('\n')
                    outGDB = groupfolder + os.sep + outgdb_name
                    if not arcpy.Exists(outGDB):
                        # print outGDB
                        CreateGDB(groupfolder, outgdb_name, outGDB)
                    print "\nWorking on {0} in {1} outfc located at {2} ".format(group, regionname, outGDB)

                    arcpy.env.workspace = outGDB
                    fcList2 = arcpy.ListFeatureClasses()
                    total2 = len(fcList2)
                    if total == total2:
                        print "All {0} species files {2} Dissolved in {1}".format(group, regionname, speciestype)
                        continue
                    else:
                        arcpy.env.workspace = regionsgdb
                        for fc in fcList:
                            # print fc
                            outgdb_name = gdb.strip('\n')

                            infile = regionsgdb + os.sep + fc
                            print infile
                            outfile = outGDB + os.sep + fc
                            # print outGDB

                            if arcpy.Exists(outfile):
                                # print "Already dissolved {0}".format(fc)
                                total -= 1
                                continue
                            else:

                                arcpy.Delete_management("temp_lyr")
                                arcpy.MakeFeatureLayer_management(infile, "temp_lyr")

                                arcpy.Dissolve_management("temp_lyr", outfile, dissolveFields, "", "MULTI_PART",
                                                          "DISSOLVE_LINES")
                                # print "Dissolving {0} in {1}".format(fc, regionname)
                                print "completed {0} {1} remaining in {2}".format(fc, total, group)
                                total -= 1

                arcpy.env.workspace = outGDB
                fcList2 = arcpy.ListFeatureClasses()
                if len(fcList) == len(fcList2):
                    print "All {0} species {2} files Dissolved in {1}".format(group, regionname, speciestype)
                else:
                    print "\nCheck for missing {0} dissolved files in {1}, {2}".format(group, regionname, speciestype)
                    checkbool = True
                    break

    inputFile2.close()

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
