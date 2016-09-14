import os
import datetime

import arcpy


## TODO make updated for append so that the len list is equal to the len count of rows of comp
##T0DO change script so it asks user which groups need to be updated then it archives the current file for that species group
masterlist = 'J:\Workspace\MasterLists\April2015Lists\CSV\MasterListESA_April2015_20151015_20151124.csv'
refFC = 'J:\Workspace\ESA_Species\ForCoOccur\CriticalHabitat\Mammals\Regions\Laysan_WebApp.gdb\Laysan_CH_2891_poly_20150428_NAD83_WGS84_Laysanprj'

gdbRegions_dict = 'J:\Workspace\ESA_Species\ForCoOccur\Dict\gdbRegions_dict.csv'
outFolderCompGDB = r'J:\Workspace\ESA_Species\ForCoOccur\Composites\GDB\June2016'
skipgroup = ['Amphibians', 'Arachnids', 'Birds', 'Clams', 'Conifers and Cycads', 'Corals', 'Ferns and Allies', 'Fishes',
             'Flowering Plants', 'Insects', 'Lichens', 'Mammals', 'Reptiles', 'Snails']
date = '20160615'

compfield = ['EntityID', 'FileName', 'NAME', 'Name_sci', 'SPCODE', 'VPCode']
while True:
    user_input = raw_input('Are you running range files Yes or No? ')
    if user_input not in ['Yes', 'No']:
        print 'This is not a valid answer'
    else:
        if user_input == 'Yes':
            RangeFolder = r"J:\Workspace\ESA_Species\ForCoOccur\Range"
            RangeFile = True
            FileType = "R_"
            filetype = '_R_'
            infolder = r"J:\Workspace\ESA_Species\ForCoOccur\Range"
            projectionpath = 'J:\Workspace\projections'
            break
        else:
            CritHabFolder = r"J:\Workspace\ESA_Species\ForCoOccur\CriticalHabitat"
            RangeFile = False
            FileType = "CH_"
            filetype = '_CH_'
            infolder = r"J:\Workspace\ESA_Species\ForCoOccur\CriticalHabitat"
            projectionpath = 'J:\Workspace\projections'
            break
while True:
    user_input2 = raw_input('Are you running the Lower48? Yes or No ')
    if user_input2 not in ['Yes', 'No']:
        print 'This is not a valid answer'
    else:
        if user_input2 == 'Yes':
            L48 = True
            outGDB = outFolderCompGDB + os.sep + 'L48_' + FileType + 'SpGroup_Composite.gdb'
            regiontype = "_L48_"
            grouptail = "Regions"
            break
        else:
            L48 = False
            user_input3 = raw_input('Are you running the Minor Islands? Yes or No ')
            if user_input2 not in ['Yes', 'No']:
                print 'This is not a valid answer'
            else:
                if user_input3 == 'Yes':
                    L48 = False
                    outGDB = outFolderCompGDB + os.sep + 'MinorIsland_' + FileType + 'SpGroup_Composite.gdb'
                    regiontype = "_MI_"
                    islands = True
                    grouptail = "Regions"
                    mislands = ['Howland', 'Johnston', 'Laysan', 'Mona', 'Necker', 'Nihoa', 'NorthwesternHI', 'Palmyra',
                                'Wake']
                    break
                else:
                    outGDB = outFolderCompGDB + os.sep + 'NL48_' + FileType + 'SpGroup_Composite.gdb'
                    regiontype = "_NL48_"
                    islands = False
                    grouptail = "Regions"
                    break

Region_cross = {'AK': 'AK',
                'AS': 'AS',
                'CNMI': 'CNMI',
                'GU': 'GU',
                'HI': 'HI',
                'Howland': 'Howland_Baker_Jarvis',
                'Johnston': 'Johnston',
                'L48': 'L48',
                'Lower48': 'L48',
                'PLower48': 'L48',
                'Laysan': 'Laysan',
                'Mona': 'Mona',
                'Necker': 'Necker',
                'Nihoa': 'Nihoa',
                'NorthwesternHI': 'NorthwesternHI',
                'PR': 'PR',
                'Palmyra': 'Palmyra_Kingman',
                'VI': 'VI',
                'Wake': 'Wake'}
arcpy.env.overwriteOutput = True  # ## Change this to False if you don't want GDB to be overwritten
arcpy.env.scratchWorkspace = ""
boolbreak = False


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


def createcomp(group, regionname, regiontype, regionsgdb, date, filetype, projection, outGDB, Region_cross):

    Comp_FileName = regionname + filetype + group + regiontype + projection + "_" + str(date)
    filepath = outGDB + os.sep + Comp_FileName
    if arcpy.Exists(filepath):
        count = int(arcpy.GetCount_management(filepath).getOutput(0))
    else:
        count = 0

    arcpy.env.workspace = regionsgdb
    FeatureType = "Polygon"
    if regionname == 'Lower48':
        wildcard = str("L48_" + FileType) + "*"
    else:
        region = Region_cross[regionname]
        wildcard = str(region + "_" + FileType) + "*"
        print wildcard
    fcList = arcpy.ListFeatureClasses(wildcard, FeatureType)

    if len(fcList) == 0:

        pass
    elif len(fcList) == count:
        print "  \n  Working on group {0}...".format(group)
        print "    Already completed {0}".format(Comp_FileName)
    else:
        print "  \n  Working on group {0}...".format(group)
        pathlist = []
        for v in fcList:
            fc = regionsgdb + os.sep + v
            pathlist.append(fc)
        print '    Total files are {0}'.format(len(pathlist))
        arcpy.env.workspace = outGDB
        arcpy.env.overwriteOutput = True

        ORGdsc = arcpy.Describe(fc)
        ORGsr = ORGdsc.spatialReference
        print '    Spatial projections is: {0}'.format(ORGsr.name)

        arcpy.CreateFeatureclass_management(outGDB, Comp_FileName, "POLYGON", refFC, 'DISABLED', 'DISABLED', ORGsr)
        count = int(arcpy.GetCount_management(filepath).getOutput(0))
        print '    Created blank fc {0} with row count {1}'.format(Comp_FileName, count)
        print '    Appending files for {0} ....'.format(group)
        arcpy.Append_management(pathlist, filepath)
        count = int(arcpy.GetCount_management(filepath).getOutput(0))
        if count != len(fcList):
            print "    Check {0} for missing files {1}, projection , {0}".format(group, regionname, ORGsr, count)
            boolbreak = True
            return boolbreak
        else:
            boolbreak = False
            return boolbreak

        loop = datetime.datetime.now()
        print "Elapse time for loop {0}".format(loop - start_script)


start_script = datetime.datetime.now()
if not os.path.exists(outGDB):
    path, gdb = os.path.split(outGDB)
    CreateGDB(outFolderCompGDB, gdb, outGDB)

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

with open(gdbRegions_dict, 'rU') as inputFile2:
    for line in inputFile2:
        if boolbreak:
            break
        line = line.split(',')
        gdb = str(line[0])
        gdb = gdb.strip("\n")
        prj = str(line[2])
        prj = projectionpath + os.sep + prj
        projection = str(line[1])
        projection = projection.strip('.gdb')
        regionsplit = gdb.split("_")
        regionname = regionsplit[0]

        if L48:
            if regionname != "Lower48":
                continue
            print '\nWorking on Region {0}, {1}...'.format(regionname, projection)
        else:
            if islands:
                if regionname not in mislands:
                    continue
            if not islands:
                if regionname in mislands:
                    continue

            if regionname == "Lower48":
                continue
            print '\nWorking on Region {0},  {1}...'.format(regionname, projection)

        for group in alpha_group:
            if boolbreak:
                break
            if group in skipgroup:
                continue

            groupfolder = infolder + os.sep + group + os.sep + grouptail
            if group == "Ferns and Allies":
                group = "Ferns"
            elif group == 'Conifers and Cycads':
                group = 'Conifers'
            elif group == 'Flowering Plants':
                group = 'Plants'
            regionsgdb = groupfolder + os.sep + gdb

            if not arcpy.Exists(regionsgdb):
                continue

            boolbreak = createcomp(group, regionname, regiontype, regionsgdb, date, filetype, projection, outGDB,
                                   Region_cross)

if boolbreak:
    print 'Check for missing file for previous output'
end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
