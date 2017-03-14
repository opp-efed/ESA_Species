import arcpy
import os
import datetime
import pandas as pd

# Tile: Checks for missing critical habitat spatial files

# NOTE  Make sure all commas are removed from master table before running this script to a find all and replace
# TODO Update cross check to pandas df so that the commas are no longer a problem
# THIS IS HARD CODED TO ONLY WORK WITH CH FILES


# User input variable
masterlist = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\CSVs\MasterListESA_June2016_20170117.csv'
# Spatial library for critical habitat files
infolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_WebApp_Composite.gdb'
group_fc_index = 1
# also need to set the hard code to the index number for the cols reference in loop species
group_colindex = 7
entid_colindex = 0
CHGIS_colindex = 20
# not_considered_colindex = 10 #TODO ADD THIS VARIABLE TO CH  # Species on master but not be considered in BE
# dev_colindex = 11  #TODO ADD THIS VARIABLE TO CH # species range is under development
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
    removed = []
    devlist = []

    with open(masterlist, 'rU') as inputFile:
        header = next(inputFile)
        for line in inputFile:
            line = line.split(',')
            group_current = str(line[group_colindex])  # group column
            entid = str(line[entid_colindex])  # entityID col
            ch_GIS = str(line[CHGIS_colindex])
            #TODO uncomment when added
            # removedvalue = str(line[not_considered_colindex])  # Species on master but not be considered in BE
            # dev = str(line[dev_colindex])  # species is under development
            # if group is the current group entity added to the current entlist
            if group == group_current:
                # species is flaged in masters has having crithab and having a gis file that is available
                if ch_GIS == 'TRUE':
                    group_entlist.append(entid)
                # if removedvalue == "Yes":
                #     removed.append(entid)
                # elif dev == 'Yes':
                #     devlist.append(entid)


                else:
                    pass
    print group_entlist
    inputFile.close()
    del header
    return group_entlist, removed, devlist



start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# generate a list of specie groups from master
masterlist_df = pd.read_csv(masterlist)
sp_groups_df = masterlist_df['Group']
sp_groups_df = sp_groups_df.drop_duplicates()
alpha_group = sorted(sp_groups_df.values.tolist())
print alpha_group

# for each specie group will compare the species in that group to the files in found in the species GDB in the spatial
# library noted by the infolder if a file is not present that it is appended to list and printed at the end
for group in alpha_group:


    print "\nWorking on {0}".format(group)
    # list all species in current group, species flagged as not longer considered, species flagged as range under dev,
    # and species that we are using the county range for
    entlist, removed, devlist = loop_species(group)

    arcpy.env.workspace = infolder
    fclist = arcpy.ListFeatureClasses()
    entlist_fc = []
    counter = 0
    for fc in fclist:
        fcparse = fc.split("_")
        group_fc = str(fcparse[group_fc_index])

        if group_fc == 'Conifers':
            group_fc = 'Conifers and Cycads'
        if group_fc == 'Flowering':
            group_fc = 'Flowering Plants'
        if group_fc == 'Ferns':
            group_fc = 'Ferns and Allies'
        if group_fc == group:
            with arcpy.da.SearchCursor(fc, ['EntityID']) as cursor:
                for row in cursor:
                    entid = str(row[0])
                    if entid not in entlist:
                        print "FILE IN WRONG Composite {0}".format(entid)
                        counter += 1
                        continue
                    else:
                        if entid not in entlist_fc:
                            entlist_fc.append(entid)
        else:
            continue

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
    total_ingroup = len(entlist) - remove_cnt
    devspe = len(devlist)
    dup = len(dup_val_set)

    # current total account for speices equals the total number of fc + the files under deve - duplicate files
    # - files in the wrong sp group gdb



    # print feedback about species missing files or having duplicate files
    if entlist_fc == entlist and len(missing_files) == 0:
        print "Species group {0} is complete".format(group)
        if len(archivedfiles) > 0:
            print "But Received files for species that have to be archived {0}".format(sorted(archivedfiles))
        if len(removed) > 0:
            print "species {0} where removed before receiving a shape".format(removed)


        if len(devlist) > 0:
            print "Refined species range{0} under development no county".format(sorted(devlist))

    elif len(dup_val_set) > 0:
        print "Missing {0}".format(len(missing_files))
        print "Species group {0} has {1} species in multiple regions".format(group, sorted(dup_val_set))
        if len(missing_files) > 0:
            print "And missing species {0}".format(sorted(missing_files))

        if len(archivedfiles) > 0:
            print "And received files for species that need to be archived {0}".format(sorted(archivedfiles))

        if len(devlist) > 0:
            print "Refined species range{0} under development no county".format(sorted(devlist))


    elif len(devlist) > 0:
        print "Missing {0}".format(len(missing_files))
        print "Species {0} under devlopment check for county".format(sorted(devlist))

        if len(missing_files) > 0:
            print "And missing species {0} for {1}".format(sorted(missing_files), group)
        if len(archivedfiles) > 0:
            print "And received some files for species that need to be archived {0}".format(sorted(archivedfiles))


    else:
        print "Missing {0}".format(len(missing_files))
        print "Missing species {0} for {1}".format(sorted(missing_files), group)

end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
