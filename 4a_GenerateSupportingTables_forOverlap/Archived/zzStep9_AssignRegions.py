import os
import datetime

import pandas as pd
import arcpy

#  Title- places species into the different regions and exports a table of species with each region it occurs in

# Input files
masterlist = "L:\master.csv"
# update the column index location in the col included dict

# Spatial library to be considered can put both and get on file, or one at a time for a list of just range or crithab
# locations
inlocation = 'L:\Workspace\StreamLine\Species Spatial Library\_CurrentFiles\Generalized files\Range'

# region fc
regionsfc = r'L:\Workspace\StreamLine\Boundaries.gdb\Regions_dissolve'

# output files
outfile = r'"L:\SpeciesRegions.csv"'

# TODO split this so that it generates one file for the Range and one for the Critical habitat in one run
# TODO clean up code to be more streamlined see acres table

# dictionary, col name and index number- base 0 from master list above
# Note entid if hard code for tracking!!!!
colincluded = {'EntityID': 0,
               'Group': 15,
               'comname': 5,
               'sciname': 6,
               'status_text': 8}

skipgroup = []


# verifies location of entityid and load species info into dictionary
def verify_entityid_location(regionsfc, colincluded):
    with arcpy.da.SearchCursor(regionsfc, ['Region']) as cursor:
        regions = sorted({row[0] for row in cursor})

    listKeys = colincluded.keys()
    regions.sort()
    listKeys.sort()
    # empty list and dict
    regionsindex = {}
    listheader = []

    if colincluded['EntityID'] != 0:
        user_input = raw_input('What is the column index for the EntityID (base 0): ')
        user_input2 = raw_input('Is the Column Heading for the EntityID [EntityID] Yes or No:')
        if user_input2 == 'Yes':
            entheading = 'EntityID'
        else:
            entheading = user_input2
        regionsindex['EntityID'] = user_input
        index = 0
    else:
        entheading = 'EntityID'
        regionsindex['EntityID'] = 0
        index = 1

    listheader.append('EntityID')
    for value in listKeys:
        if value == entheading:
            continue
        else:
            listheader.append(value)

    for value in regions:
        listheader.append(value)

    for value in listheader:

        try:
            index = regionsindex[value]

            index += 1
        except KeyError:
            regionsindex[value] = index
            index += 1

    allspecies = []
    rowindex = 0
    rowdict = {}

    with open(masterlist, 'rU') as inputFile:
        speciesinfo = {}
        header = next(inputFile)
        for line in inputFile:
            currentline = []
            line = line.split(',')
            if colincluded['EntityID'] == 0:
                entid = str(line[0])

            else:
                entid = str(line[int(user_input)])
            rowdict[entid] = rowindex

            currentline.append(entid)
            for value in listheader:
                if value == 'EntityID':
                    continue
                elif value in listKeys:
                    vars()[value] = line[colincluded[value]]
                    currentline.append(vars()[value])
                else:
                    nan = 'nan'
                    currentline.append(nan)
            speciesinfo[entid] = currentline
            allspecies.append(currentline)
            rowindex += 1

    inputFile.close()
    return allspecies, listheader, rowdict, rowindex, regionsindex


def assign_region(in_gdb, count_gdb, total_gdb, regions_fc, out_df, row_dict):
    arcpy.Delete_management("regions")
    arcpy.MakeFeatureLayer_management(regions_fc, "regions")
    print "\nWorking on GDB {0} of {1}...".format(count_gdb, total_gdb)
    arcpy.env.workspace = in_gdb
    fclist = arcpy.ListFeatureClasses()
    totalfc = len(fclist)
    count_fc = 1
    for fc in fclist:
        infc = ingdb + os.sep + fc
        print "Working on fc {2}, {0} of {1}...and GDB {3} of {4} ".format(count_fc, totalfc, fc, count_gdb, total_gdb)
        with arcpy.da.SearchCursor(infc, ["EntityID"]) as cursor:
            for row in cursor:
                entid = str(row[0])

                whereclause = "EntityID = '%s'" % entid

                arcpy.Delete_management("lyr")
                arcpy.Delete_management("slt_lyr")
                arcpy.MakeFeatureLayer_management(fc, "lyr", whereclause)

                arcpy.SelectLayerByLocation_management("regions", "INTERSECT", "lyr")
                arcpy.MakeFeatureLayer_management("regions", "slt_lyr")
                result = arcpy.GetCount_management("slt_lyr")
                count = int(result.getOutput(0))
                if count == 0:
                    regions = []
                else:
                    with arcpy.da.SearchCursor("slt_lyr", ["Region"]) as cursor:
                        regions = sorted({row[0] for row in cursor})
                    del row, cursor

                irow = row_dict[entid]
                # if species occurs in region load yes in the region col for that species
                for s in regions:
                    out_df.loc[irow, s] = 'Yes'

        count_fc += 1
    count_gdb += 1
    return count_gdb, out_df


start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

allspecies, listheader, rowdict, rowindex, regionsindex = verify_entityid_location(regionsfc, colincluded)

if inlocation[-3:] == 'gdb':
    sp_group = inlocation[:-4]
    sp_group = sp_group.replace(" ", "_")
    ingdb = inlocation
    outDF = pd.DataFrame(allspecies, columns=listheader)
    arcpy.MakeFeatureLayer_management(regionsfc, "regions")
    totalGDB = 1
    countGBD = 1
    countGBD, outDF = assign_region(ingdb, countGBD, totalGDB, regionsfc, outDF, rowdict)
else:
    list_ws = os.listdir(inlocation)
    print list_ws
    outDF = pd.DataFrame(allspecies, columns=listheader)
    totalGDB = len([v for v in list_ws if v.endswith('gdb')])
    countGBD = 1
    for v in list_ws:
        if v[-3:] == 'gdb':
            sp_group = v[:-4]
            sp_group = sp_group.replace(" ", "_")
            if sp_group not in skipgroup:
                ingdb = inlocation + os.sep + v
                countGBD, outDF = assign_region(ingdb, countGBD, totalGDB, regionsfc, outDF, rowdict)

print outDF
outDF.to_csv(outfile)
print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
