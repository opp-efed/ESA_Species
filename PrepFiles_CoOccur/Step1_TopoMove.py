import arcpy
import os
import datetime
# TODO remove hard code to col index in the loop master function
# ToDO merge with other step 1 scripts and add the user prompts

topo_simple = 'C:\Workspace\ESA_Species\BySpeciesGroup\\topo\Simplified_topoCorrected.gdb'
topo = 'C:\Workspace\ESA_Species\BySpeciesGroup\\topo\TopoError.gdb'
outtopo = 'C:\Workspace\ESA_Species\BySpeciesGroup\\topo\Topo_FailedSimple.gdb'


def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)

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

notin_simple_topo =[]
for fc in fclist:
    entid = fc.split('_')
    entid = str(entid[1])
    if entid in toposimple_list:
        continue
    else:
        notin_simple_topo.append((entid))
        #topofc = outtopo + os.sep + fc
        #if not arcpy.Exists(topofc):
            #infc = topo + os.sep + fc
            #arcpy.CopyFeatures_management(infc, topofc)
            #print "exported: {0}".format(topofc)
addtoTopoError =[]

for value in toposimple_list:
    if value not in topo_list:
        addtoTopoError.append(value)

print "Species that were not simplified"
print notin_simple_topo

print "Species to be added to Topo Error"
print addtoTopoError

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
