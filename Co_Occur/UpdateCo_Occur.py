import functions
import datetime

import arcpy
# loop through folders using dict colunms final GDB, column, ID, acresID, total acres,fctype
# store acres in dicts by folder
# update percent overlap then update table

overlaplist = ''  # arc_table
ch_overlaplist = ''
folderDict = ''
R_acres = ''
CH_acres =''


start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)


def calulation(fcType, use_sum):
    if fcType == "Raster":
        msq = use_sum * 900
        acres = msq * 0.0002
        return acres
    else:
        acres = use_sum
        return acres


ent_tracker = []
doubleRange = []
ent_CHtracker = []
double_CH = []
totalAcreas = {}
totalAcres_CH ={}

with open(R_acres, 'rU') as inputFile:
    for line in inputFile:
        line = line.split(',')
        entid = str(line[0])
        total_acresID = str(line[1])
        totalAcreas[entid] = total_acresID

inputFile.close()

with open(CH_acres, 'rU') as inputFile:
    for line in inputFile:
        line = line.split(',')
        entid = str(line[0])
        total_acresID = str(line[1])
        totalAcres_CH[entid] = total_acresID

inputFile.close()

with open(folderDict, 'rU') as inputFile:
    for line in inputFile:
        line = line.split(',')
        finalGDB = str(line[0])
        columnID = str(line[1])
        acres_colid = str(line[2])
        total_acresID = str(line[3])
        fcType = str(line[4])



        print finalGDB

        for fc in functions.fcs_in_workspace(finalGDB):
            filenameDict ={}
            current_update = {}
            CHcheck = fc[:-3]
            with arcpy.da.SearchCursor(fc, ["EntityID", "SUM","FileName"]) as cursor:
                for row in cursor:
                    if CHcheck == 'CH_':
                        entid = row[0]
                        if row in ent_CHtracker:
                            double_CH.append(entid)
                        else:
                            ent_CHtracker.append(entid)
                    else:
                        entid = row[0]
                        if row in ent_tracker:
                            doubleRange.append(entid)
                        else:
                            ent_tracker.append(entid)
                    use_sum = row[1]
                    filename = row[2]
                    current_update[entid] = use_sum
                    filenameDict[entid]= filename

            del cursor, row

            fc_entlist = current_update.keys()  # get a list of keys in dict
            if CHcheck == 'CH:':
                with arcpy.da.UpdateCursor(ch_overlaplist, ["EntityID", columnID, "CHAcres","FileName"]) as cursor:
                    for row in cursor:
                        entid = row[0]
                        CHAcres = totalAcres_CH[entid]
                        filename_ent = filenameDict[entid]
                        if entid not in fc_entlist:
                            continue
                        else:
                            regionAcres = row[2]
                            use_sum = current_update[entid]
                            acres = calulation(fcType, use_sum)

                            p_overlap = (acres / CHAcres) * 100
                            final_overlap = int(round(p_overlap))
                            row[1] = final_overlap
                            row[2] = CHAcres
                            row[3] = filename_ent
                            cursor.updateRow(row)
                            continue
            else:
                with arcpy.da.UpdateCursor(overlaplist, ["EntityID", columnID, "RAcres","FileName"]) as cursor:
                    for row in cursor:
                        entid = row[0]
                        RAcres = totalAcreas[entid]
                        filename_ent = filenameDict[entid]
                        if entid not in fc_entlist:
                            continue
                        else:
                            regionAcres = row[2]
                            use_sum = current_update[entid]
                            acres = calulation(fcType, use_sum)

                            p_overlap = (acres / RAcres) * 100
                            final_overlap = int(round(p_overlap))
                            row[1] = final_overlap
                            row[2] = RAcres
                            row[3] = filename_ent
                            cursor.updateRow(row)
                            continue

print "Species with multiple CH files {0}".format(double_CH)
print "Species with multiple Range files {0}".format(doubleRange)
end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
