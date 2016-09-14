import arcpy
import os
import datetime
# TODO remove hard code to col index in the loop master function
# ToDO merge with other step 1 scripts and add the user prompts
masterlist = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\CSV\MasterListESA_April2015_20151015_20151105.csv'

infolder = 'C:\WorkSpace\ESA_Species\BySpeciesGroup'
topo_simple = 'E:\TopoErrors\GDB\AllTopoErrors_asof20150608.gdb'
outtopo="C:\\WorkSpace\\ESA_Species\\BySpeciesGroup\\topo\TopoError.gdb"

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
simpledict ={}
for fc in fclist:
    entid = fc.split('_')
    entid = str(entid[1])
    counter = 0
    toposimple_list.append(entid)
    simpledict[entid]=str(fc)

for group in alpha_group:
    print "\nWorking on {0}".format(group)
    entlist = []
    removed = []
    with open(masterlist, 'rU') as inputFile:
        header = next(inputFile)
        for line in inputFile:
            line = line.split(',')
            group_current = str(line[1])
            entid = str(line[0])
            removedvalue = str(line[2])
            if group == group_current:
                entlist.append(entid)
                if removedvalue == "Yes":
                    removed.append(entid)
                else:
                    continue
    inputFile.close()

    group_gdb = infolder + os.sep + str(group) + '.gdb'

    arcpy.env.workspace = group_gdb
    fclist = arcpy.ListFeatureClasses()
    entlist_fc = []
    for fc in fclist:
        entid = fc.split('_')
        entid = str(entid[1])
        counter = 0
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

    for value in entlist_fc:
        if value in tracker:
            dup_files.append(value)
        else:
            tracker.append(value)

    dup_val_set = set(dup_files)

    for value in entlist:
        if value not in entlist_fc:
            missing_files.append(value)
        else:
            continue

    for value in sorted(missing_files):
        if value in removed:
            missing_files.remove(value)
        else:
            continue

    for value in removed:
        if value in entlist_fc:
            archivedfiles.append(value)
        else:
            continue

    total_ingroup = len(entlist)
    totalfc = len(fclist)
    totalfc = totalfc - counter
    toposimple_group = []
    topo_group =[]

    for value in toposimple_list:
        if value in missing_files:
            missing_files.remove(value)
            toposimple_group.append(value)
        else:
            continue

    for value in topo_list:
        if value in missing_files:
            missing_files.remove(value)
            topo_group.append(value)
        else:
            continue

    if totalfc == total_ingroup:
        print "Species group {0} is complete".format(group)
        if len(archivedfiles) > 0:
            print "But Received files for species that have to be archived {0}".format(sorted(archivedfiles))
        if len(toposimple_group ) > 0:
            print "But simplifed species {0} to correct topo".format(sorted(toposimple_group))
        if len(topo_group) > 0:
            print "And failed simplifed species {0} to correct topo".format(sorted(topo_group))
    elif len(dup_val_set) > 0:
        print "Species group {0} has duplicate values {1}".format(group, sorted(dup_val_set))
        if len(missing_files) > 0:
            print "And missing species {0}".format(sorted(missing_files))
        if len(toposimple_group ) > 0:
            print "And simplifed species {0} to correct topo".format(sorted(toposimple_group ))
        if len(topo_group) > 0:
            print "And failed simplifed species {0} to correct topo".format(sorted(topo_group))
        if len(archivedfiles) > 0:
            print "And received files for species that need to be archived {0}".format(sorted(archivedfiles))
    elif len(toposimple_group ) > 0:
        print "But simplifed species {0} to correct topo".format(sorted(toposimple_group ))
        if len(missing_files) > 0:
            print "And missing species {0} for {1}".format(missing_files, group)
    elif len(topo_group) > 0:
            print "And failed simplifed species {0} to correct topo".format(sorted(topo_group))
    else:
        print "Missing species {0} for {1}".format(missing_files, group)
        if len(archivedfiles) > 0:
            print "And received some files for species that need to be archived {0}".format(sorted(archivedfiles))

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
