import arcpy
import os
import datetime

# Tile: Checks for missing critical habitat spatial files

# NOTE  Make sure all commas are removed from master table before running this script to a find all and replace
# TODO Update cross check to pandas df so that the commas are no longer a problem
# THIS IS HARD CODED TO ONLY WORK WITH CH FILES


# User input variable
masterlist = 'J:\Workspace\ESA_Species\ForCoOccur\Documents_FinalBE\MasterListESA_June2016_20160725.csv'
# Spatial library for critical habitat files
infolder = 'J:\Workspace\ESA_Species\CriticalHabitat\NAD_Final'
# also need to set the hard code to the index number for the cols reference in loop species

# #########Functions


# recursively checks workspaces found within the inFileLocation and makes list of all feature class
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


# for each specie group will compare the species in that group to the files in found in the species GDB in the spatial
# library noted by the infolder if a file is not present that it is appended to list and printed at the end
def loop_species(group):
    print "\nWorking on {0}".format(group)
    group_entlist = []
    with open(masterlist, 'rU') as inputFile:
        header = next(inputFile)
        for line in inputFile:
            line = line.split(',')
            group_current = str(line[7])  # group column
            entid = str(line[0])  # entityID col
            ch_GIS = str(line[26])
            # if group is the current group entity added to the current entlist
            if group == group_current:
                # species is flaged in masters has having crithab and having a gis file that is available
                if ch_GIS == 'TRUE':
                    group_entlist.append(entid)
            else:
                continue
    print group_entlist
    inputFile.close()
    del header
    return group_entlist


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


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

alpha_group = get_group_list(masterlist)

# for each specie group will compare the species in that group to the files in found in the species GDB in the spatial
# library noted by the infolder if a file is not present that it is appended to list and printed at the end
for group in alpha_group:
    # list all species in current group with critical habitat
    ent_list = loop_species(group)

    # species group gdb in spatial library
    group_gdb = infolder + os.sep + str(group) + '.gdb'

    # Generates list of all species files in the current sp group gdb at the spatial library
    arcpy.env.workspace = group_gdb
    fclist = arcpy.ListFeatureClasses()
    entlist_fc = []
    counter = 0
    for fc in fclist:
        # extract entityID information from each file in the group gdb
        entid = fc.split('_')
        entid = str(entid[1])

        # if there is a file for an ent not in the current ent_list then it is is the wrong file; this is printed on the
        # output so user can address it
        if entid not in ent_list:
            # if these is a file
            print "FILE IN WRONG FOLDER {0}".format(entid)
            counter += 1
            continue
        else:
            entlist_fc.append(entid)

    dup_files = []
    tracker = []
    missing_files = []

    # checks for species will multiple spatial files
    for value in entlist_fc:
        if value in tracker:
            dup_files.append(value)
        else:
            tracker.append(value)

    dup_val_set = set(dup_files)

    # check for species without a file
    for value in ent_list:
        if value not in entlist_fc:
            missing_files.append(value)
        else:
            continue

    total_ingroup = len(ent_list)
    totalfc = len(fclist)
    dup = len(dup_val_set)
    # current total account for speices equals the total number of fc + duplicate files- files in the wrong sp group gdb
    totalfc = totalfc - dup - counter


    # print feedback about species missing files or having duplicate files
    if totalfc == total_ingroup:
        print "Species group {0} is complete".format(group)

    elif len(dup_val_set) > 0:
        print "Missing {0}".format(total_ingroup - totalfc)
        print "Species group {0} has duplicate values {1}".format(group, sorted(dup_val_set))
        if len(missing_files) > 0:
            print "And missing species {0}".format(sorted(missing_files))

    else:
        print "Missing {0}".format(total_ingroup - totalfc)
        print "Missing species {0} for {1}".format(sorted(missing_files), group)

end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
