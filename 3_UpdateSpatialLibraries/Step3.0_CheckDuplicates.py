import arcpy
import os
import datetime
import pandas as pd

# Tile: Checks for species with duplicate files in spatial library; this indicated that files has been updated, or a
# species has two files that need to be merged

# ## Make sure all commas are removed to get accurate results!
# User input variable
# input table

masterlist =  r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\MasterListESA_Dec2018_20200427.csv"
# Spatial Library for location files range or critical habitat (generalized or non) \CriticalHabitat or \Range
in_folder = r'L:\Workspace\StreamLine\Species Spatial Library\_CurrentFiles\Generalized files\CriticalHabitat'
group_colindex = 16  # Index position of the group column in the master species list
# #########Functions

# generates a list of sp groups from master list


def get_group_list(master_list):
    grouplist = []
    with open(master_list, 'rU') as inputFile:
        header = next(inputFile)
        for line in inputFile:
            line = line.split(',')
            group = line[group_colindex]
            grouplist.append(group)
    inputFile.close()

    unq_grps = set(grouplist)
    sorted_group = sorted(unq_grps)
    print sorted_group
    del header, grouplist
    return sorted_group


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

masterlist_df = pd.read_csv(masterlist)
sp_groups_df = masterlist_df.ix[:,group_colindex]
sp_groups_df = sp_groups_df.drop_duplicates()
alpha_group = sorted(sp_groups_df.values.tolist())
print alpha_group


duplicate_files = []
for group in alpha_group:

    print "\nWorking on {0}".format(group)
    group_gdb = in_folder + os.sep + str(group) + '.gdb'

    arcpy.env.workspace = group_gdb
    fclist = arcpy.ListFeatureClasses()
    entlist_fc = []

    for fc in fclist:
        entid = fc.split('_')
        entid = str(entid[1])
        counter = 0
        if entid not in entlist_fc:
            entlist_fc.append(entid)
            counter += 1
            continue
        else:
            print 'Species {0} has a duplicate file'.format(entid)
            duplicate_files.append(entid)

print 'Species {0} has a duplicate file'.format(duplicate_files)

end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
