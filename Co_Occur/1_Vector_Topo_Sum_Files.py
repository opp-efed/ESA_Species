import functions
import datetime
import os
import arcpy
# loop through folders using dict colunms final GDB, column, ID, acresID, total acres,fctype
# store acres in dicts by folder
# update percent overlap then update table

print 'start'
topolist = 'C:\Users\Admin\Documents\Jen\Workspace\MasterOverlap\SpeByRegions.gdb\Topo_results'  # arc_table
folderDict = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Dict\TopoTable_Vector.csv'
overwrite= False
# R_acres= table of acres by region
# CH_acres = table of ch by region


start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)


species_neg =[]
with open(folderDict, 'rU') as inputFile:
    header = next(inputFile)
    for line in inputFile:
        line = line.split(',')
        finalGDB = str(line[0])
        finalGDB = finalGDB.strip('\n')
        use = str(line[1])
        use= use.strip('\n')
        outtable = finalGDB + os.sep + "Topo_results_" +use

        if arcpy.Exists(outtable):
            intable =outtable
        else:
            intable =topolist


        print finalGDB

        for fc in functions.fcs_in_workspace(finalGDB):
            #print fc
            current_update = {}
            CHchecklist = fc.split("_")
            CHcheck = CHchecklist[0]
            if CHcheck != 'Topo':
                continue
            elif str(CHchecklist[1]) =='T':
                continue
            elif str(CHchecklist[1]) =='R':
                entlist = fc.split("_")
                ent = str(entlist[2])
                filename = str(entlist[1])+ "_" +str(entlist[2])
                #print filename

            else:
                entlist = fc.split("_")
                ent = str(entlist[1])
                filename = "R_" + str(entlist[1])
                #print filename


            infc = finalGDB + os.sep + fc
            fclist_field = [f.name for f in arcpy.ListFields(infc) if not f.required]
            if "EntityID" not in fclist_field:
                print "Updating EntityID"
                arcpy.AddField_management(infc,"EntityID","TEXT")
                with arcpy.da.UpdateCursor(infc,["EntityID"]) as cursor:
                    for row in cursor:
                        row[0] = str(ent)
                        cursor.updateRow(row)
                    del cursor, row
            if "FileName" not in fclist_field:
                print "Updating FileName"
                arcpy.AddField_management(infc,"FileName","TEXT")
                with arcpy.da.UpdateCursor(infc,["FileName"]) as cursor:
                    for row in cursor:
                        row[0] = filename
                        cursor.updateRow(row)
                    del cursor, row
            with arcpy.da.SearchCursor(intable, ["EntityID", "SUM", "FileName"]) as cursor:
                #print intable
                for row in cursor:
                    entid = str(row[0])

                    if entid!= ent:
                        continue
                    elif overwrite ==False:
                        if row[1] is not None:
                            print "Species {0} has total sum of {1}".format(entid,str(row[1]))

                        else:
                            with arcpy.da.SearchCursor(infc, ["EntityID", "CoOccur_Acres", "FileName"]) as cursor:

                                for row in cursor:
                                    row_sum= row[1]
                                    if row_sum < 0:
                                        species_neg.append(ent)
                                        row_sum =0

                                print "Sum for species {0} is {1}".format(entid,row_sum)
                                del cursor, row

                                use_sum = row_sum
                                current_update[ent] = use_sum


                            with arcpy.da.UpdateCursor(intable, ["EntityID", "SUM","FileName"]) as cursor:
                                print intable
                                for row in cursor:
                                    entid = str(row[0])
                                    if entid != ent:
                                        continue
                                    else:
                                        print entid
                                        if overwrite ==False:
                                            if row[1] is not None:
                                                continue

                                            else:
                                                use_sum = current_update[entid]
                                                row[1] = use_sum
                                                row[2] = filename
                                                print use_sum
                                                print filename
                                                cursor.updateRow(row)
                                                print "updated table for species {0} with total {1}".format(entid,use_sum)
                                        del row, cursor
                                #del row, cursor
                    #del row, cursor

        if not arcpy.Exists(outtable):
            arcpy.Copy_management(topolist, outtable)
            print "Exported: " + str(outtable)

        with arcpy.da.UpdateCursor(topolist, ("SUM")) as cursor:
            for row in cursor:
                if row[0] > -2:
                    row[0] = None
                    cursor.updateRow(row)

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
