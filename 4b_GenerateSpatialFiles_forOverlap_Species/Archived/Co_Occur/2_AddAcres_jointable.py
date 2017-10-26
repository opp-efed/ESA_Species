import arcpy
import os

intable = 'C:\WorkSpace\MasterOverlap\Acres.gdb\Acres_20151207'
folderDict = 'J:\Workspace\ESA_Species\ForCoOccur\Dict\AcresDict.csv'


def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for feature in arcpy.ListFeatureClasses():
        yield (feature)
    for ws in arcpy.ListWorkspaces():
        for feature in fcs_in_workspace(ws):
            yield feature


with open(folderDict, 'rU') as inputFile:
    for line in inputFile:
        line = line.split(',')
        finalGDB = str(line[0])
        finalGDB = finalGDB.strip('\n')
        print finalGDB

        for fc in fcs_in_workspace(finalGDB):
            # print fc
            CHcheck = fc.split("_")
            CHcheck = CHcheck[1]
            # print CHcheck
            if CHcheck == 'CH':
                region = fc.split("_")
                region = str(region[0])
                region = region.strip('\n')
                # print region
                if region == "PartialLower48":
                    col = 'PartL48'
                elif region == "Lower48Only":
                    col = 'L48only'
                else:
                    col = region

                field = col + "_" + CHcheck
                # print field

                acres_dict = {}
                entlist = []

                infc = finalGDB + os.sep + fc
                with arcpy.da.SearchCursor(infc, ["EntityID", "Acres"]) as cursor:
                    for row in cursor:
                        ent = str(row[0])
                        # print row[1]
                        acres_dict[ent] = row[1]
                        entlist.append(str(ent))
                    del row, cursor

                with arcpy.da.UpdateCursor(intable, ["EntityID", field]) as cursor:
                    # print entlist

                    for row in cursor:
                        ent = str(row[0])
                        if ent in entlist:
                            # print field
                            print ent
                            acres = acres_dict[ent]
                            row[1] = acres
                            print acres
                            cursor.updateRow(row)
                        else:
                            # print ent
                            # print 'not in list'
                            continue
                    del row, cursor

            else:
                region = fc.split("_")
                region = str(region[0])
                region = region.strip('\n')
                # print region
                if region == "PartialLower48":
                    col = 'PartL48'
                elif region == "Lower48Only":
                    col = 'L48only'
                else:
                    col = region

                field = col + "_R"
                # print field

                acres_dict = {}
                entlist = []

                infc = finalGDB + os.sep + fc
                with arcpy.da.SearchCursor(infc, ["EntityID", "Acres"]) as cursor:
                    for row in cursor:
                        ent = str(row[0])
                        acres_dict[ent] = row[1]
                        entlist.append(str(ent))
                    del row, cursor

                with arcpy.da.UpdateCursor(intable, ["EntityID", field]) as cursor:
                    # print entlist

                    for row in cursor:
                        ent = str(row[0])
                        # print ent
                        if ent in entlist:
                            # print field
                            print ent
                            acres = acres_dict[ent]
                            row[1] = acres
                            print acres
                            cursor.updateRow(row)
                        else:
                            # print 'not in list'
                            # print ent
                            continue
                    del row, cursor
