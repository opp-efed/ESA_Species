import arcpy
import os
import datetime

# NOTE  Make sure all commas are removed from master table before running this script to a find all and
# replace
# TODO Update cross check to pandas df so that the commas are no longer a problem

masterlist = 'J:\Workspace\MasterLists\CSV\MasterListESA_April2015_20151015_20151124.csv'
# Spatial Library for rangefiles
infolder = 'J:\Workspace\ESA_Species\Range\NAD83'

includedTopo= False
topo_simple = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\NAD83\\topo\Simplified_topoCorrected.gdb'
topo = "C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\NAD83\\topo\Topo_FailedSimple.gdb"

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

if includedTopo:
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
else:
    topo_list = []
    toposimple_list = []


for group in alpha_group:
    print "\nWorking on {0}".format(group)
    entlist = []
    removed = []
    devlist = []
    cntylist=[]
    with open(masterlist, 'rU') as inputFile:
        header = next(inputFile)
        for line in inputFile:
            line = line.split(',')
            group_current = str(line[1])
            entid = str(line[0])
            removedvalue = str(line[2]) # Species on master but not be considered in BBE
            dev=str(line[3]) #species is under development
            GIS = str(line[26])#Range file name from the last list
            if group == group_current:


                entlist.append(entid)
                if removedvalue == "Yes":
                    removed.append(entid)
                elif dev =='Yes':
                    devlist.append(entid)
                elif dev =='Used County':
                    cntylist.append((entid))
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
    toposimple_group = []
    topo_group = []


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

    for value in sorted(missing_files):
        if value in devlist:
            missing_files.remove(str(value))
        else:
            continue

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

    remove_cnt =len(removed)
    total_ingroup = len(entlist)-remove_cnt
    totalfc = len(fclist)
    toposimple = len(toposimple_group)
    topogroup = len(topo_group)
    devspe = len(devlist)
    dup = len(dup_val_set)


    totalfc = totalfc  + toposimple + topogroup + devspe-dup-counter
    #print totalfc
    #print total_ingroup
    if totalfc == total_ingroup:
        print "Species group {0} is complete".format(group)
        if len(archivedfiles) > 0:
            print "But Received files for species that have to be archived {0}".format(sorted(archivedfiles))
        if len(removed) > 0:
            print "species {0} where removed before receiving a shape".format(removed)
        if len(toposimple_group) > 0:
            print "Complete but simplified species {0} to correct topo".format(sorted(toposimple_group))
        if len(topo_group) > 0:
            print "And failed simplified species {0} to correct topo".format(sorted(topo_group))
        if len(cntylist) > 0:
            print "Used county for species {0} while refined range under development".format(sorted(cntylist))
        if len(devlist) > 0:
            print "Refined species range{0} under development no county".format(sorted(devlist))

    elif len(dup_val_set) > 0:
        print "Missing {0}".format(total_ingroup-totalfc)
        print "Species group {0} has duplicate values {1}".format(group, sorted(dup_val_set))
        if len(missing_files) > 0:
            print "And missing species {0}".format(sorted(missing_files))
        if len(toposimple_group) > 0:
            print "And simplified species {0} to correct topo".format(sorted(toposimple_group))
        if len(topo_group) > 0:
            print "And failed simplified species {0} to correct topo".format(sorted(topo_group))
        if len(archivedfiles) > 0:
            print "And received files for species that need to be archived {0}".format(sorted(archivedfiles))
        if len(cntylist) > 0:
            print "Used county for species {0} while refined range under development".format(sorted(cntylist))
        if len(devlist) > 0:
            print "Refined species range{0} under development no county".format(sorted(devlist))

    elif len(toposimple_group) > 0:
        print "Missing {0}".format(total_ingroup-totalfc)
        print "Simplified species {0} to correct topo".format(sorted(toposimple_group))
        if len(dup_val_set) > 0:
            print "Species group {0} has duplicate values {1}".format(group, sorted(dup_val_set))
        if len(topo_group) > 0:
            print "And failed simplified species {0} to correct topo".format(sorted(topo_group))
        if len(cntylist) > 0:
            print "Used county for species {0} while refined range under development".format(sorted(cntylist))
        if len(devlist) > 0:
            print "Refined species range{0} under development no county".format(sorted(devlist))
        if len(missing_files) > 0:
            print "And missing species {0} for {1}".format(sorted(missing_files), group)
        if len(archivedfiles) > 0:
            print "And received some files for species that need to be archived {0}".format(sorted(archivedfiles))


    elif topogroup  > 0:
        print "Missing {0}".format(total_ingroup-totalfc)
        print "And failed simplifed species {0} to correct topo".format(sorted(topo_group))
        if len(cntylist) > 0:
            print "Used county for species {0} while refined range under development".format(sorted(cntylist))
        if len(devlist) > 0:
            print "Refined species range{0} under development no county".format(sorted(devlist))
        if len(missing_files) > 0:
            print "And missing species {0} for {1}".format(sorted(missing_files), group)
        if len(archivedfiles) > 0:
            print "And received some files for species that need to be archived {0}".format(sorted(archivedfiles))

    elif len(devlist) > 0:
        print "Missing {0}".format(total_ingroup-totalfc)
        print "Species {0} under devlopment check for county".format(sorted(devlist))
        if len(cntylist) > 0:
            print "Used county for species {0} while refined range under development".format(sorted(cntylist))
        if len(missing_files) > 0:
            print "And missing species {0} for {1}".format(sorted(missing_files), group)
        if len(archivedfiles) > 0:
            print "And received some files for species that need to be archived {0}".format(sorted(archivedfiles))


    else:
        print "Missing {0}".format(total_ingroup-totalfc)
        print "Missing species {0} for {1}".format(sorted(missing_files), group)



end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
