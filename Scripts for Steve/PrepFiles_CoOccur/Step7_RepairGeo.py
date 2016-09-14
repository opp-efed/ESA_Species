import arcpy
import os
# #Tile: Will run the repair geometry tool on files with topology  errors
# Set to True if running on individual species files and false if running on composite file if true in folder must be a
# single gdb

ind_sp_file = False
masterlist = 'J:\Workspace\ESA_Species\ForCoOccur\Documents_FinalBE\MasterListESA_June2016_20160725.csv'

infolder = 'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\CH_SpGroupComposite.gdb'
skiplist = []

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
            group_master = line[7]
            grouplist.append(group_master)

    inputFile.close()

    unq_grps = set(grouplist)
    sorted_group = sorted(unq_grps)
    del header
    return sorted_group


if ind_sp_file:
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
