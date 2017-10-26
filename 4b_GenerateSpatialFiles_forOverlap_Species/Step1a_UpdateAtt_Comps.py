import os
import datetime

import arcpy


# ###############user input variables

masterlist = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\CSVs\MasterListESA_June2016_201601101.csv'
infolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb'  # folder of GDB

# species groups to skip
skiplist = []

ColIndexDict = dict(comname=4, sciname=5, spcode=14, vipcode=15, entid=0, group=7, popabb=8,status =6)
final_fields = ['NAME', 'Name_sci', 'SPCode', 'VIPCode', 'FileName', 'EntityID', 'Pop_Abb', 'Status']
final_fieldsindex = dict(NAME=0, Name_sci=4, SPCode=5, VIPCode=7, EntityID=1, Pop_Abb=3,Status=6)#group is in pos 2 and not being added
singleGDB = True

########Static variables
reqindex = {'entid': 'Q1', 'group': 'Q2'}
entid_indexfilenm = 1
inputlist = ['Yes', 'No']
speciestoQA = '1'
updatefiles = False

start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)


# Functions
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


def askuser(col, q, listQtype):
    if q == 'Q1':
        user_input = raw_input('What is the column index for the EntityID (base 0): ')
        return user_input
    else:
        print listQtype
        user_input = raw_input('What is the column index for the {0}(base 0): '.format(col))
        return user_input


def output_update(fc, value):
    print "     Updated {0} for files {1}".format(value, fc)


def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")


## may be able to call dissolve field in update and so that is does  not need to re-load the fc list
# TODO update files so that character limits in the attribute table doesn't block data update
def updateFilesloop(inGDB, final_fields, speinfodict):
    group_gdb = inGDB
    arcpy.env.workspace = group_gdb
    fclist = arcpy.ListFeatureClasses()
    for fc in fclist:
        addedfield = ['EntityID']
        fclist_field = [f.name for f in arcpy.ListFields(fc) if not f.required]
        for field in final_fields:
            if field not in fclist_field:
                arcpy.AddField_management(fc, field, "TEXT")
                print "added field {1} for {0}".format(fc, field)
                addedfield.append(field)

        with arcpy.da.UpdateCursor (fc,  addedfield ) as lookup:
            for row in lookup:
                entid = row[0]
                try:
                    listspecies = speinfodict[entid]
                    index = 1
                    for field in addedfield:
                        if field == 'EntityID':
                            continue
                        else:
                            indexfield = final_fieldsindex[field]
                            value = listspecies[indexfield]
                            row[index] = value
                            lookup.updateRow(row)

                            index += 1
                except:
                    print entid
            output_update(fc, field)
        fclist_field = [f.name for f in arcpy.ListFields(fc) if not f.required]
        for field in fclist_field:
                if field not in final_fields:
                    arcpy.DeleteField_management(fc, field)
                    print 'Deleted {0}'.format(field)
                else:
                    pass




def LoadSpeciesinfo_frommaster(ColIndexDict, reqindex, masterlist):
    grouplist = []
    speciesinfo_dict = {}
    listKeys = ColIndexDict.keys()
    listKeys.sort()
    reqlistkeys = sorted(reqindex.keys())
    q1 = False
    q2 = False
    for val in reqlistkeys:
        if val in listKeys:
            continue
        else:
            question = reqindex[val]
            vars()[question] = True
            vars()[val] = askuser(val, question, listKeys)
            if q1:
                ColIndexDict['entid'] = vars()[val]
            if q2:
                ColIndexDict['group'] = vars()[val]

    listKeys = ColIndexDict.keys()
    listKeys.sort()
    with open(masterlist, 'rU') as inputFile:
        header = next(inputFile)
        for line in inputFile:
            speciesinfo = []
            line = line.split(',')
            entid = line[int(ColIndexDict['entid'])]
            group = line[int(ColIndexDict['group'])]
            if group not in grouplist:
                grouplist.append(group)
            for v in listKeys:
                vars()[v] = line[int(ColIndexDict[v])]
                speciesinfo.append(vars()[v])
            speciesinfo_dict[entid] = speciesinfo
    inputFile.close()
    alpha = sorted(grouplist)
    # print alpha_group
    return listKeys, speciesinfo_dict, alpha


listKeys, speciesinfo_dict, alpha_group = LoadSpeciesinfo_frommaster(ColIndexDict, reqindex, masterlist)

print listKeys
print speciesinfo_dict[speciestoQA]

while not updatefiles:
    user_inputupdated = raw_input('Please confirm that the species information is in the correct order: Yes or No: ')
    if user_inputupdated not in inputlist:
        print 'This is not a valid answer'
    elif user_inputupdated == 'Yes':
        updatefiles = True
        print 'Files will be updated now'
        if singleGDB:
            inGDB = infolder
            print inGDB
            updateFilesloop(inGDB, final_fields, speciesinfo_dict)


        else:
            for group in alpha_group:
                start_loop = datetime.datetime.now()
                if group in skiplist:
                    continue
                print "\nWorking on {0}".format(group)
                inGDB = infolder + os.sep + str(group) + '.gdb'
                updateFilesloop(inGDB, final_fields, speciesinfo_dict)

                endloop = datetime.datetime.now()
                print "Elapse time {0}".format(endloop - start_script)

    else:
        print 'Files will not be updated check the input indexes'
        updatefiles = True
        break

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
