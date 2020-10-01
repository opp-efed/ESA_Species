import arcpy
import os
import datetime
import pandas as pd

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# Tile: Checks for missing critical habitat spatial files

# NOTE  Make sure all commas are removed from master table before running this script to a find all and replace
# TODO Update loopspecies to pandas df so that the commas are no longer a problem
# THIS IS HARD CODED TO ONLY WORK WITH CH FILES
# TODO update to filter qualitative species that are excluded from overlap out of the counts

#CONFIRMED with ESA team that CH for qualtitave species should also be excluded from overlap # Fall 2017

# User input variable
masterlist = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\MasterListESA_Dec2018_20200427.csv"
# Spatial library for critical habitat files generalized or non
infolder =  r'L:\Workspace\StreamLine\Species Spatial Library\_CurrentFiles\Generalized files\CriticalHabitat'
# also need to set the hard code to the index number for the cols reference in loop species


group_colindex = 'Group'  # col header for group from species list
entid_colindex = "EntityID"  # col header fro entityid from species list
not_considered_colindex = 'not_considered_BE_GIS'  # Species on master but not be considered in BE
dev_colindex = 'Range under development'  # species range is under development
ch_gis_colindex = "CH_GIS"
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
    masterlist_df = pd.read_csv(masterlist)

    df_group = masterlist_df.loc[(masterlist_df[group_colindex] == group) & (masterlist_df[ch_gis_colindex] == "Yes")].copy()
    df_dropped = df_group.loc[(df_group[not_considered_colindex] == "Yes")].copy()
    df_dev = df_group.loc[(df_group[dev_colindex] == "Yes")].copy()
    df_cnty = df_group.loc[(df_group[dev_colindex] == "Used county")].copy()

    group_entlist = df_group[entid_colindex].values.tolist()
    removed = df_dropped[entid_colindex].values.tolist()
    devlist = df_dev[entid_colindex].values.tolist()
    cntylist = df_cnty[entid_colindex].values.tolist()

    return group_entlist, removed, devlist, cntylist




start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# generate a list of specie groups from master
masterlist_df = pd.read_csv(masterlist)
sp_groups_df = masterlist_df.ix[:,group_colindex]
sp_groups_df = sp_groups_df.drop_duplicates()
alpha_group = sorted(sp_groups_df.values.tolist())

print alpha_group

# for each specie group will compare the species in that group to the files in found in the species GDB in the spatial
# library noted by the infolder if a file is not present that it is appended to list and printed at the end
for group in alpha_group:
    # list all species in current group with critical habitat
    entlist, removed, devlist, cntylist  = loop_species(group)

    # print removed, devlist
    group_gdb = infolder + os.sep + str(group) + '.gdb'
    print group_gdb

    arcpy.env.workspace = group_gdb
    fclist = arcpy.ListFeatureClasses()
    entlist_fc = []
    counter = 0
    for fc in fclist:
        entid = fc.split('_')
        entid = str(entid[1])

        if entid not in entlist:
            print "FILE IN WRONG FOLDER {0}".format(entid)
            counter += 1
            continue
        else:
            entlist_fc.append(entid)

    dup_files = []
    tracker = []
    missing_files = []
    archivedfiles = []

    # checks for species will multiple spatial files
    for value in entlist_fc:
        if value in tracker:
            dup_files.append(value)
        else:
            tracker.append(value)
    dup_val_set = set(dup_files)

    # check for species without a file
    for value in entlist:
        if value not in entlist_fc:
            missing_files.append(value)
        else:
            continue
    # removes species from missing list that have been flagged as should not be considered
    for value in sorted(missing_files):
        if value in removed:
            missing_files.remove(value)
        else:
            continue
    # removes species from missing list that have been flagged as under development
    for value in sorted(missing_files):
        if value in devlist:
            missing_files.remove(str(value))
        else:
            continue

    remove_cnt = len(removed)

    totalfc = len(fclist)
    devspe = len(devlist)
    dup = len(dup_val_set)
    total_ingroup = len(entlist) - remove_cnt - devspe


    # current total account for species equals the total number of fc + the files under deve - duplicate files
    # - files in the wrong sp group gdb
    totalfc = totalfc - dup


    # print feedback about species missing files or having duplicate files
    if totalfc == total_ingroup:
        print "Species group {0} is complete".format(group)
        if len(archivedfiles) > 0:
            print "But Received files for species that have to be archived {0}".format(sorted(archivedfiles))
        if len(removed) > 0:
            print "species {0} where removed before overlap; Critical habitat not included if available".format(removed)

        if len(cntylist) > 0:
            print "Used county for species {0} while refined range under development".format(sorted(cntylist))
        if len(devlist) > 0:
            print "Refined species range{0} under development no county".format(sorted(devlist))

    elif len(dup_val_set) > 0:
        print "Missing {0}".format(total_ingroup - totalfc)
        print "Species group {0} has duplicate values {1}".format(group, sorted(dup_val_set))
        if len(missing_files) > 0:
            print "And missing species {0}".format(sorted(missing_files))

        if len(archivedfiles) > 0:
            print "And received files for species that need to be archived {0}".format(sorted(archivedfiles))
        if len(cntylist) > 0:
            print "Used county for species {0} while refined range under development".format(sorted(cntylist))
        if len(devlist) > 0:
            print "Refined species range{0} under development no county".format(sorted(devlist))

    elif len(devlist) > 0:
        print "Missing {0}".format(total_ingroup - totalfc)
        print "Species {0} under devlopment check for county".format(sorted(devlist))
        if len(cntylist) > 0:
            print "Used county for species {0} while refined range under development".format(sorted(cntylist))
        if len(missing_files) > 0:
            print "And missing species {0} for {1}".format(sorted(missing_files), group)
        if len(archivedfiles) > 0:
            print "And received some files for species that need to be archived {0}".format(sorted(archivedfiles))

    else:
        print "Missing {0}".format(total_ingroup - totalfc)
        print "Missing species {0} for {1}".format(sorted(missing_files), group)


end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
