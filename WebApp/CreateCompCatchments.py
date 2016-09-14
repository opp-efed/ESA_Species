import arcpy
import os
import datetime

masterlist = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\CSV\MasterListESA_April2015_20151015_20151124.csv'

inGDB = 'J:\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Range\Fishes\Range_Catchments\GDB\FishCatchmentDissolved.gdb'


outname ='ClamsCatchments.shp'
CompGroup= 'Clams'


def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


start_script = datetime.datetime.now()
print "Script started at {0} for group {1} ".format(start_script,CompGroup)

grouplist = []
comname_dict = {}
sciename_dict = {}
spcode_dict = {}
vipcode_dict = {}

targetGroup =[]

with open(masterlist, 'rU') as inputFile:
    header = next(inputFile)
    for line in inputFile:
        line = line.split(',')
        comname = line[7]
        sciname = line[8]
        spcode = line[26]
        vipcode = line[27]
        entid = line[0]
        group = line[1]
        grouplist.append(group)
        if group == CompGroup:
            targetGroup.append(entid)
        comname_dict[entid] = comname
        sciename_dict[entid] = sciname
        spcode_dict[entid] = spcode
        vipcode_dict[entid] = vipcode
inputFile.close()

print targetGroup
unq_grps = set(grouplist)
alpha_group = sorted(unq_grps)

group_gdb = inGDB

arcpy.env.workspace = group_gdb
fclist = arcpy.ListFeatureClasses()

complist =[]
for fc in fclist:
    with arcpy.da.SearchCursor (fc,('EntityID')) as cursor:
        for row in cursor:
            if row[0] in targetGroup:
                infc= inGDB + os.sep + str(fc)
                if infc not in complist:
                    complist.append(infc)
            else:
                continue

compSet = set(complist)
print complist
outFC ='J:\Workspace\ESA_Species\WebApp\Features\CatchmentsComps'+os.sep +outname
arcpy.Merge_management(complist, outFC)

