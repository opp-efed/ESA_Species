import arcpy
import os
import datetime

masterlist = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\CSV\MasterListESA_April2015_20151015_20151111.csv'

infolder = 'C:\WorkSpace\ESA_Species\BySpeciesGroup\NAD83'
topo_simple = 'C:\WorkSpace\ESA_Species\BySpeciesGroup\NAD83\\topo\Simplified_topoCorrected.gdb'
topo = "C:\WorkSpace\ESA_Species\BySpeciesGroup\NAD83\\topo\Topo_FailedSimple.gdb"

def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)

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

arcpy.env.workspace = topo_simple
fclist = arcpy.ListFeatureClasses()
toposimple_list = []
for fc in fclist:
    entid = fc.split('_')
    entid = str(entid[1])
    counter = 0
    toposimple_list.append(entid)

arcpy.env.workspace = topo
fclist = arcpy.ListFeatureClasses()
topo_list = []

for fc in fclist:
    entid = fc.split('_')
    entid = str(entid[1])
    counter = 0
    topo_list.append(entid)

for group in alpha_group:
    print "\nWorking on {0}".format(group)
    entlist = []
    removed = []
    devlist = []
    remove_simple=[]
    remove_topo =[]
    with open(masterlist, 'rU') as inputFile:
        header = next(inputFile)
        for line in inputFile:
            line = line.split(',')
            group_current = str(line[1])
            entid = str(line[0])
            removedvalue = str(line[2])
            dev=str(line[3])
            if group == group_current:
                entlist.append(entid)

    inputFile.close()

    group_gdb = infolder + os.sep + str(group) + '.gdb'

    arcpy.env.workspace = group_gdb
    fclist = arcpy.ListFeatureClasses()
    entlist_fc = []
    for fc in fclist:
        entid = fc.split('_')
        entid = str(entid[1])
        counter = 0
        if entid in entlist:
            if entid in topo_list:
                remove_topo.append(entid)
            elif entid in toposimple_list:
                remove_simple.append(entid)
            else:
                continue

    print "Removed {0} from simple".format(remove_simple)
    print "Removed {0} from topo".format(remove_topo)
