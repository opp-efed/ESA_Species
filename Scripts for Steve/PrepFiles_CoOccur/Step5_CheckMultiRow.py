import arcpy
import os
import datetime


# Tile: Check for species that still needs to be dissolve, ie a file with multiple rows

# TODO remove hard code to masterlist

# User input variable
masterlist = 'J:\Workspace\ESA_Species\ForCoOccur\Documents_FinalBE\MasterListESA_June2016_20160725.csv'

infolder = 'J:\Workspace\ESA_Species\CriticalHabitat\NAD_Final'

# species groups that can be skipped
skiplist = []

# #########Functions


# recursively checks workspaces found within the inFileLocation and makes list of all feature class
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc

# generates a list of sp groups from masterlist
def get_group_list(master_list):
    grouplist = []
    with open(master_list, 'rU') as inputFile:
        header = next(inputFile)
        for line in inputFile:
            line = line.split(',')
            group = line[7]
            grouplist.append(group)
    inputFile.close()

    unq_grps = set(grouplist)
    sorted_group = sorted(unq_grps)
    print sorted_group
    del header, grouplist
    return sorted_group

start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)

alpha_group = get_group_list(masterlist)

for group in alpha_group:
    if group in skiplist:
        continue
    print "\nWorking on {0}".format(group)

    group_gdb = infolder + os.sep + str(group) + '.gdb'

    arcpy.env.workspace = group_gdb
    fclist = arcpy.ListFeatureClasses()
    entlist_fc = []
    for fc in fclist:
        filepath = group_gdb + os.sep + fc
        count = int(arcpy.GetCount_management(filepath).getOutput(0))
        if count > 1:
            print "{0} file {1} needs to be dissolved".format(group, fc)

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
