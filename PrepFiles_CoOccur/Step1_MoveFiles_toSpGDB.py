import arcpy
import os
import datetime
import pandas as pd
# TODO remove hard code to col index in the loop master function
# ToDO merge with other step 1 scripts and add the user prompts
# TODO update to pandas excel reader so csv of master does not need to be made

# Tile: Moves Range files found in the current update gdb and moves them to the corresponding group gdb in the
# the spatial library found at the outfolder location
#
# NOTE HARD CODE TO WORK WITH CH files

# master species list
masterlist = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\FinalLists\FinalBE_December2016\csv\MasterListESA_June2016_20160907.csv'
# the location of the spatial library
outfolder = 'J:\Workspace\ESA_Species\Range\NAD83'
# Location of new and updated files to be moved this is an excel file with the path the different gdb
file_dict = 'J:\Workspace\ESA_Species\ForCoOccur\Dicts\Move_Files_SpatialLibrary.csv'
# col index from master
group_colindex = 7
entid_colindex = 0


# ## FUNCTIONS
# recursively checks workspaces found within the inFileLocation and makes list of all feature class
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


# Create a new GDB
def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


# for each line in the import table, loops through fc from the gdb on the line, and exported a copy
# to the spatial library for that specie group

def loop_species(file_dict_fun, group_list, out_path):
    with open(file_dict_fun, 'rU') as inputFile:
        header = next(inputFile)
        for line in inputFile:
            filepath = line.split(",")
            filepath = filepath[0].strip('\n')
            print "Working on {0}".format(filepath)
            # loop over fcs
            for fc in fcs_in_workspace(filepath):
                # checks to make sure it is a Range files THIS IS THE HARD CODE
                CHcheck = fc[:2]
                if CHcheck != 'CH':
                    entid = fc.split("_")
                    entid = entid[1]
                    # extract entid from filename then compare to the list of species in the current group to see if the
                    # file should be moved to the current outpath
                    if entid not in group_list:
                        continue
                    infc = filepath + os.sep + str(fc)
                    outfc = out_path + os.sep + str(fc)

                    # moves files in found in the current group to the outGDB for that group in the spatial library
                    # found at the outfolder location
                    if not arcpy.Exists(outfc):
                        print "Moving species {0}".format(entid)
                        arcpy.CopyFeatures_management(infc, outfc)
                        print "Exported file {0}".format(outfc)
                    else:
                        print "Previously exported {0}".format(entid)
                else:
                    continue
        del header


# loop through master list and makes a list of all species found in current group
def loop_master(current_group, masterlist):
    grouplist = []
    with open(masterlist, 'rU') as inputFile:
        header = next(inputFile)
        for line in inputFile:
            line = line.split(',')
            group = line[group_colindex]
            entid = line[entid_colindex]
            if group == current_group:
                grouplist.append(entid)
            else:
                pass
        del header
    inputFile.close()
    return grouplist


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# generate a list of specie groups from master
masterlist_df = pd.read_csv(masterlist)
sp_groups_df = masterlist_df['Group']
sp_groups_df = sp_groups_df.drop_duplicates()

sp_groups = sorted(sp_groups_df.values.tolist())
print sp_groups

for group in sp_groups:
    current_group = group
    print "Current group is {0}".format(current_group)
    # a list of species from masterlist found in the current species group
    grouplist = loop_master(current_group, masterlist)
    print grouplist

    # out gdb for the current species found in the spatial library indicated as the outfolder
    outpath = str(outfolder) + os.sep + str(current_group) + '.gdb'

    if not arcpy.Exists(outpath):
        create_gdb(outfolder, str(current_group), outpath)
    # moves files in found in the current group to the outGDB for that group in the spatial library
    # found at the outfolder location
    loop_species(file_dict, grouplist, outpath)

    # check the species group GDB in the spatial library to see what files are missing after this move
    arcpy.env.workspace = outpath
    fcList = arcpy.ListFeatureClasses()

    fc_entlist = []
    for fc in fcList:
        ent = fc.split("_")
        ent = str(ent[1])
        if ent not in fc_entlist:
            fc_entlist.append(ent)
        else:
            continue

    if len(grouplist) != len(fc_entlist):
        print "all files did not export"
        for ent in fc_entlist:
            if ent not in grouplist:
                print "missing species {0}".format(ent)
            else:
                continue

print "Current group is {0}".format(current_group)

end = datetime.datetime.now()
print "End Time: " + end.ctime()

elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
