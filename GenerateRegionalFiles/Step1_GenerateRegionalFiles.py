import os
import datetime

import arcpy

masterlist = 'J:\Workspace\MasterLists\April2015Lists\CSV\MasterListESA_April2015_20151015_20151124.csv'

PossAnswers = ['Yes', 'No']

skip = ['Amphibians', 'Arachnids', 'Birds', 'Clams', 'Conifers and Cycads', 'Corals', 'Ferns and Allies',
        'Fishes']  ##species groups that were already run

QAanswer = True

while QAanswer:
    user_input = raw_input('Are you running range files? Yes or No ')
    if user_input not in PossAnswers:
        print 'This is not a valid answer'

    else:
        QAanswer = False
        if user_input == 'Yes':
            infolder = 'J:\Workspace\ESA_Species\Range\NAD83'
            outfolder = 'J:\Workspace\ESA_Species\ForCoOccur\Range'
            print 'Running range files output will be located at {0}'.format(outfolder)
        else:
            infolder = 'J:\Workspace\ESA_Species\CriticalHabitat\NAD_Final'
            outfolder = 'J:\Workspace\ESA_Species\ForCoOccur\CriticalHabitat'
            print 'Running critical habitat files output will be located at {0}'.format(outfolder)




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


start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)

grouplist = []
lower48only_list = []
nonLoweronly_list = []
both_list = []
noRegion = []
with open(masterlist, 'rU') as inputFile:
    header = next(inputFile)
    for line in inputFile:
        line = line.split(',')
        entid = str(line[0])
        group = line[1]
        Lower48only = line[5]
        nonloweronly = line[6]
        both = line[4]
        grouplist.append(group)
inputFile.close()

unq_grps = set(grouplist)
alpha_group = sorted(unq_grps)
print alpha_group

for group in alpha_group:
    if group in skip:
        continue
    print "Current group is {0}".format(group)

    with open(masterlist, 'rU') as inputFile:
        header2 = next(inputFile)
        for line in inputFile:
            line = line.split(',')
            entid = str(line[0])
            grouptable = line[1]
            Lower48only = str(line[5])
            nonloweronly = str(line[6])
            both = str(line[4])
            if grouptable == group:
                if Lower48only == 'Yes':
                    lower48only_list.append(entid)
                elif nonloweronly == 'Yes':
                    nonLoweronly_list.append(entid)
                elif both == 'Yes':
                    both_list.append(entid)
                else:
                    noRegion.append(entid)
    inputFile.close()
    # print lower48only_list
    # print nonLoweronly_list
    # print both_list

    group_dir = outfolder + os.sep + str(group)
    if not os.path.exists(group_dir):
        os.mkdir(group_dir)
        print "created directory {0}".format(group_dir)

    l48_gdb = group_dir + os.sep + "Lower48Only.gdb"
    # print l48_gdb
    nl48_gdb = group_dir + os.sep + "SingleNonLower48only.gdb"
    both_gdb = group_dir + os.sep + "SingleBoth.gdb"

    if not arcpy.Exists(l48_gdb):
        CreateGDB(group_dir, "Lower48Only.gdb", l48_gdb)

    if not arcpy.Exists(nl48_gdb):
        CreateGDB(group_dir, "SingleNonLower48only.gdb", nl48_gdb)

    if not arcpy.Exists(both_gdb):
        CreateGDB(group_dir, "SingleBoth.gdb", both_gdb)

    group_gdb = str(infolder) + os.sep + str(group) + '.gdb'

    arcpy.env.workspace = group_gdb
    fclist = arcpy.ListFeatureClasses()
    total = len(fclist)
    entlist_fc = []
    for fc in fclist:
        infile = group_gdb + os.sep + fc
        ent = fc.split("_")
        entid = str(ent[1])
        if entid in lower48only_list:
            outfile = l48_gdb + os.sep + fc
            if not arcpy.Exists(outfile):
                total -= 1
                print "Copied lower48 species {0}  remaining {1} in {2}".format(fc, total, group)
                arcpy.CopyFeatures_management(infile, outfile)
            else:
                total -= 1

        elif entid in nonLoweronly_list:
            outfile = nl48_gdb + os.sep + fc
            if not arcpy.Exists(outfile):
                total -= 1
                print "Copied {0} to NL48 GDB remaining {1} in {2}".format(fc, total, group)
                arcpy.MultipartToSinglepart_management(infile, outfile)
            else:
                total -= 1
        elif entid in both_list:
            outfile = both_gdb + os.sep + fc
            if not arcpy.Exists(outfile):
                total -= 1
                print "Copied {0} to Both GDB {0}remaining {1} in {2}".format(fc, total, group)
                arcpy.MultipartToSinglepart_management(infile, outfile)
            else:
                total -= 1
    print "Species without a regions {0}".format(noRegion)
    print "Current group is {0}".format(group)

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
