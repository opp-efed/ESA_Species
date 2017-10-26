import arcpy
import os

CH_intable = 'C:\WorkSpace\MasterOverlap\CriticalHabitat.gdb\MasterUse_20150903_CH_20151208'
R_intable = 'C:\WorkSpace\MasterOverlap\Range.gdb\MasterUse_20150903_R_20151208'
folderDict = 'J:\Workspace\ESA_Species\ForCoOccur\Dict\AcresDict.csv'
acres ='C:\WorkSpace\MasterOverlap\Acres_20151204.dbf'

def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc

ch_acres = {}
r_acres={}
with arcpy.da.SearchCursor(acres, ["EntityID", "Full_R",'Full_CH']) as cursor:
    for row in cursor:
        ent = str(row[0])
        range = str(row[1])
        ch=str(row[2])


        ch_acres[ent] = ch
        r_acres[ent]= range

with open(folderDict, 'rU') as inputFile:
    for line in inputFile:
        line = line.split(',')
        finalGDB = str(line[0])
        finalGDB= finalGDB.strip('\n')

        for fc in fcs_in_workspace(finalGDB):
            print fc
            CHcheck = fc.split ("_")
            CHcheck = CHcheck[1]
            print CHcheck
            if CHcheck == 'CH':
                field = 'CH_Acres'
                intable = CH_intable
            else:
                field = 'RAcres'
                intable = R_intable


            filename_dict = {}
            entlist = []
            infc = finalGDB + os.sep + fc
            with arcpy.da.SearchCursor(infc, ["EntityID", "Acres", "Filename"]) as cursor:
                for row in cursor:
                    ent = row[0]
                    filename_dict[ent] = row[2]
                    entlist.append(ent)
                del row, cursor
            with arcpy.da.UpdateCursor(intable, ["EntityID", field, "FileName"]) as cursor:
                print intable
                print fc
                for row in cursor:
                    ent = str(row[0])
                    if ent in entlist:
                        if field == 'RAcres':
                            acres = r_acres[ent]
                        else:
                            acres = ch_acres[ent]

                        print acres
                        filename = filename_dict[ent]
                        print filename
                        row[1] = acres
                        row[2] = filename
                        cursor.updateRow(row)
