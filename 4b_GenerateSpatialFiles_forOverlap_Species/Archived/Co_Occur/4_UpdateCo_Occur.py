import datetime
import os

import arcpy

import functions

# loop through folders using dict colunms final GDB, column, ID, acresID, total acres,fctype
# store acres in dicts by folder
# update percent overlap then update table

print 'start'

ch_overlaplist  = 'C:\Users\Admin\Documents\Jen\Workspace\MasterOverlap\Collapsed_CriticalHabitat.gdb\MasterUse_20150903_CH_201512010'
overlaplist= 'C:\Users\Admin\Documents\Jen\Workspace\MasterOverlap\Collapsed_Range.gdb\MasterUse_20150903_R_201512010'

#folderDict = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Dict\CoUpdate_Vector,csv'
#folderDict = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Dict\CoUpdate.csv'
#folderDict = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Dict\CoUpdate_Diaz.csv'
folderDict = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Dict\CoUpdate_NonCONUS_1.csv'
#folderDict = 'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\BySpeciesGroup\ForCoOccur\Dict\CoUpdate_floweringplants.csv'

# R_acres= table of acres by region
# CH_acres = table of ch by region


start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)


def calculation(typefc, usesum, cell):
    msq_conversion = cell * cell
    if typefc == "Raster":
        # print use_sum
        msq = usesum * msq_conversion
        # print msq
        acres = msq * 0.000247
        print acres
        return acres
    else:
        print "ERROR ERROR"


ent_tracker = []
doubleRange = []
ent_CHtracker = []
double_CH = []
totalAcreas = {}
totalAcres_CH = {}

# with open(R_acres, 'rU') as inputFile:
# for line in inputFile:
# line = line.split(',')
# entid = str(line[0])
# total_acresID = str(line[1])
# totalAcreas[entid] = total_acresID

# inputFile.close()

# with open(CH_acres, 'rU') as inputFile:
# for line in inputFile:
# line = line.split(',')
# entid = str(line[0])
# total_acresID = str(line[1])
# totalAcres_CH[entid] = total_acresID

# inputFile.close()

with open(folderDict, 'rU') as inputFile:
    header = next(inputFile)
    for line in inputFile:
        line = line.split(',')
        finalGDB = str(line[0])
        finalGDB = finalGDB.strip('\n')
        columnID = str(line[2])
        columnID = columnID.strip('\n')
        print columnID
        fcType = str(line[3])
        fcType = fcType.strip('\n')
        cellsize = float(line[4])

        print finalGDB

        for fc in functions.fcs_in_workspace(finalGDB):
            print fc
            filenameDict = {}
            current_update = {}
            CHcheckslist = fc.split("_")

            CHcheck = str(CHcheckslist[0])
            nonsimple = str(CHcheckslist[1])

            if CHcheck == 'Topo':
                if nonsimple !='T':
                    continue
            infc = finalGDB + os.sep + fc
            fclist_field = [f.name for f in arcpy.ListFields(fc) if not f.required]
            with arcpy.da.SearchCursor(infc, ["EntityID", "SUM", "FileName"]) as cursor:
                for row in cursor:
                    if CHcheck == 'CH':
                        entid = str(row[0])
                        print entid
                        if row in ent_CHtracker:
                            double_CH.append(entid)
                        else:
                            ent_CHtracker.append(entid)
                    else:
                        entid = str(row[0])
                        if row in ent_tracker:
                            doubleRange.append(entid)
                        else:
                            ent_tracker.append(entid)
                    use_sum = row[1]
                    filename = row[2]
                    current_update[entid] = use_sum
                    filenameDict[entid] = filename

            del cursor, row

            fc_entlist = current_update.keys()  # get a list of keys in dict
            print fc_entlist
            if CHcheck == 'CH':
                print CHcheck
                tablelist_field = [f.name for f in arcpy.ListFields(ch_overlaplist) if not f.required]
                # print tablelist_field
                with arcpy.da.UpdateCursor(ch_overlaplist, ["EntityID", columnID, "CH_Acres", "Critical_Habitat_",
                                                            "FileName"]) as cursor:
                    for row in cursor:

                        entid = str(row[0])
                        #print entid
                        CHAcres = float(row[2])


                        if entid not in fc_entlist:

                            continue

                        elif row[4] ==-44:
                            row[1] = -44
                            print "species {0} has no report acres".format(entid)
                        else:
                            try:
                                print entid
                                if use_sum is None or CHAcres is None:
                                    row[1] = -44
                                else:
                                    use_sum = current_update[entid]
                                    use_acres = use_sum
                                        # print use_acres

                                    p_overlap = (use_acres / CHAcres) * 100
                                    print p_overlap
                                    filename = filenameDict[entid]
                                        # final_overlap = int(round(p_overlap))
                                        # print final_overlap
                                    row[1] = p_overlap

                                filename = filenameDict[entid]
                                row[4] = filename
                                cursor.updateRow(row)
                                continue
                            except:
                                row[1] ==-22
                                cursor.updateRow(row)
                                continue
            else:
                tablelist_field = [f.name for f in arcpy.ListFields(overlaplist) if not f.required]
                # print tablelist_field

                with arcpy.da.UpdateCursor(overlaplist,
                                           ["EntityID", columnID, "RAcres", "RRange", "FileName"]) as cursor:
                   for row in cursor:
                        entid = str(row[0])
                        RAcres = float(row[2])

                        if entid not in fc_entlist:
                            continue
                        elif row[3] == 'Under Development':
                            row[1] = -33
                        elif row[3] == 'Removed':
                            row[1] == -99
                        elif row[4] ==-44:
                            row[1] = -44
                            print "species {0} has no report acres".format(entid)

                        else:
                            try:
                                print entid
                                if use_sum is None or RAcres is None:
                                    row[1] = -44
                                else:
                                    use_sum = current_update[entid]
                                    use_acres = use_sum
                                        # print use_acres

                                    p_overlap = (use_acres / RAcres) * 100
                                    print p_overlap
                                    filename = filenameDict[entid]
                                        # final_overlap = int(round(p_overlap))
                                        # print final_overlap
                                    row[1] = p_overlap

                                filename = filenameDict[entid]
                                row[4] = filename
                                cursor.updateRow(row)
                                continue
                            except:
                                row[1] ==-22
                                cursor.updateRow(row)
                                continue

print "Species with multiple CH files {0}".format(double_CH)
print "Species with multiple Range files {0}".format(doubleRange)
end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
