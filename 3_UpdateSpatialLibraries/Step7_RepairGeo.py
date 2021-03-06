import arcpy
import os

#
# Tile: Will run the repair geometry tool on files with topology  errors in the spatial libraries
# Set to True if running on individual species files and false if running on composite file if true in folder must be a
# single gdb

ind_sp_file = True  # set to false if using the tool on th composite files
masterlist = masterlist = 'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
                          '\_ExternalDrive\_CurrentSupportingTables\MasterLists\MasterListESA_Feb2017_20180109.csv'
group_colindex = 15  # index position of the speices 'group' from master list
# folder or gdb
# infolder = r'C:\Users\JConno02\One_Drive_fail\Documents_C_drive\Projects\ESA\_ExternalDrive\_CurrentSpeciesSpatialFiles' \
#           '\SpatialLibrary\Generalized files\Range'
infolder = 'C:\Users\JConno02\One_Drive_fail\Documents_C_drive\Projects\ESA\_ExternalDrive' \
           '\_CurrentSpeciesSpatialFiles\SpatialLibrary\Generalized files\CriticalHabitat'

# 'Amphibians', 'Arachnids', 'Birds', 'Clams', 'Conifers and Cycads','Corals', 'Crustaceans','Ferns and Allies',
# 'Flowering Plants','Fishes', 'Insects', 'Lichens', 'Mammals'
skiplist = ['Amphibians', 'Arachnids', 'Birds', 'Clams', 'Conifers and Cycads', 'Corals', 'Crustaceans',
            'Ferns and Allies',
            'Insects', 'Lichens', 'Mammals', 'Reptiles', 'Snails', 'Flowering Plants']


# ########### Functions
# recursively checks workspaces found within the inFileLocation and makes list of all feature class


def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield (fc)


def extract_in_master(master_list):
    grouplist = []
    with open(master_list, 'rU') as inputFile:
        header = next(inputFile)
        for line in inputFile:
            line = line.split(',')
            group_master = line[group_colindex]
            grouplist.append(group_master)

    inputFile.close()

    unq_grps = set(grouplist)
    sorted_group = sorted(unq_grps)
    del header
    return sorted_group


if ind_sp_file:
    if infolder[-3:] != 'gdb':
        alpha_group = extract_in_master(masterlist)
        for group in alpha_group:
            if group in skiplist:
                continue
            print "\nWorking on {0}".format(group)

            group_gdb = infolder + os.sep + str(group) + '.gdb'

            for fc in fcs_in_workspace(group_gdb):
                print "\nProcessing " + fc
                lyr = 'temporary_layer'

                arcpy.MakeFeatureLayer_management(fc, lyr)
                arcpy.RepairGeometry_management(lyr)
                print(arcpy.GetMessages(0))
                arcpy.Delete_management(lyr)
    else:
        group_gdb = infolder

        for fc in fcs_in_workspace(group_gdb):
            print "\nProcessing " + fc
            lyr = 'temporary_layer'

            arcpy.MakeFeatureLayer_management(fc, lyr)
            arcpy.RepairGeometry_management(lyr)
            print(arcpy.GetMessages(0))
            arcpy.Delete_management(lyr)

else:
    if infolder[-3:] == 'gdb':
        ingdb = infolder
        for fc in fcs_in_workspace(ingdb):
            print "\nProcessing " + fc
            lyr = 'temporary_layer'

            arcpy.MakeFeatureLayer_management(fc, lyr)
            arcpy.RepairGeometry_management(lyr)
            print(arcpy.GetMessages(0))
            arcpy.Delete_management(lyr)

    else:
        list_ws = os.listdir(infolder)
        print list_ws
        for v in list_ws:
            if v[-3:] == 'gdb':
                ingdb = infolder + os.sep + v
                for fc in fcs_in_workspace(ingdb):
                    print "\nProcessing " + fc
                    lyr = 'temporary_layer'

                    arcpy.MakeFeatureLayer_management(fc, lyr)
                    arcpy.RepairGeometry_management(lyr)
                    print(arcpy.GetMessages(0))
                    arcpy.Delete_management(lyr)
