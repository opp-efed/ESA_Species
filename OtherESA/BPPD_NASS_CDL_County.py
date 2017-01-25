# import system modules
import arcpy
import os
import pandas as pd

import datetime

# location of files/Workspace

infcs = [
    'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb\CH_Insects_Composite_MAG_20161102',
    ]
outLocation = "C:\Users\JConno02\Documents\Projects\SmallProject\BEADOverlap"
outfilename = 'CH_ESA_Insects_20170123'
counties = "L:\Workspace\ESA_Species\Range\Cnty_State_Range20150410\ESA_State_CntyRanges_Report2015Jan.gdb\Boundaries\Counties_NASS_Table160418_2"
raw_counties = 'L:\Workspace\ESA_Species\Range\Cnty_State_Range20150410\ESA_State_CntyRanges_Report2015Jan.gdb\Boundaries\Counties_all'
raw_states = 'L:\Workspace\ESA_Species\Range\Cnty_State_Range20150410\ESA_State_CntyRanges_Report2015Jan.gdb\Boundaries\States_tiger2014'
# TODO create county file with crop information on the fly at the beginning and point to raw county file in the path above


masterlist = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\CSVs\MasterListESA_June2016_20170117.csv'
entid_index = 0
status_index = 6
# Table Names
speinfo = ['EntityID', 'NAME', 'Name_sci', 'SPCode', 'VIPCode', 'FileName']
entIndex = 0

useLookup = {'10': 'Corn',
             '20': 'Cotton',
             '30': 'Rice',
             '40': 'Soybean',
             '50': 'Wheat',
             '60': 'Veg Ground Fruit',
             '70': 'Other trees',
             '80': 'Other grains',
             '90': ' Other row crops',
             '100': 'Other Crops',
             '110': 'Pasture/Hay/Forage'
             }

# def clean_text(row):
# return the list of decoded cell in the Series instead
# return [r.decode('unicode_escape').encode('ascii', 'ignore') for r in row]


start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)
csvfile = outLocation + os.sep + outfilename + '.csv'
statusDict = {}

with open(masterlist, 'rU') as inputFile:
    header = next(inputFile)
    for line in inputFile:
        line = line.split(',')
        entid = str(line[entid_index])
        status = str(line[status_index])
        statusDict[str(entid)] = str(status)

ST_name_Dict = {}
with arcpy.da.SearchCursor(raw_states, ["STATEFP", "Name"]) as cursor:
    for row in cursor:
        ST_name_Dict[str(row[0])] = str(row[1])

# print ST_name_Dict
# Empty list to store information about each of the feature classes
SpeTables = []  # Empty list to store information about each of the feature classes
header = []
for i in speinfo:
    header.append(str(i))
header.extend(('Status', 'Group'))

arcpy.MakeFeatureLayer_management(counties, "cntyclp_lyr")
result = arcpy.GetCount_management("cntyclp_lyr")
count_cnty = int(result.getOutput(0))

fieldList = [f.name for f in arcpy.ListFields(counties) if not f.required]

for i in fieldList:
    check = i.split("_")
    if check[0] == 'CDL':
        code = str(check[1])
        use = useLookup[code]
        i = 'CDL_' + use
    elif check[0] == 'NASS':
        code = str(check[1])
        use = useLookup[code]
        i = 'NASS_' + use

    else:
        i = i
    header.append(str(i))

header.append('State')
state_code = fieldList.index("STATEFP")
print header
for fc in infcs:
    print fc
    arcpy.Delete_management("fc_lyr")
    arcpy.MakeFeatureLayer_management(fc, "fc_lyr")
    path, tail = os.path.split(fc)
    group = tail.split("_")
    group = str(group[1])
    with arcpy.da.SearchCursor("fc_lyr", speinfo) as cursor1:
        for sprow in cursor1:
            currentspecies = []
            colindexspec = 0
            entid = str(sprow[entIndex])

            currentspecies.append(str(entid))
            status = str(statusDict[entid])
            for v in speinfo:
                if v == 'EntityID':
                    colindexspec += 1
                    continue
                else:
                    value = str(sprow[colindexspec])
                    currentspecies.append(value)
                    colindexspec += 1

            currentspecies.append(status)
            currentspecies.append(group)

            whereclause = "EntityID = '%s'" % entid
            arcpy.SelectLayerByAttribute_management("fc_lyr", "NEW_SELECTION", whereclause)
            arcpy.Delete_management("row_lyr")
            arcpy.MakeFeatureLayer_management("fc_lyr", "row_lyr")
            arcpy.SelectLayerByLocation_management("cntyclp_lyr", "intersect", "row_lyr")

            arcpy.Delete_management("cntyrow_lyr")
            arcpy.MakeFeatureLayer_management("cntyclp_lyr", "cntyrow_lyr")
            result = arcpy.GetCount_management("cntyrow_lyr")
            count_selection = int(result.getOutput(0))
            print 'Species {0} is found in {2} of {1} counties'.format(entid, count_cnty, count_selection)
            # for NL48 species no county will be selected therefore feature layer will equal all counties, and not selection, these species should be skipped
            if count_selection == 0:
                continue
            else:
                fieldList2 = [f.name for f in arcpy.ListFields(counties) if not f.required]
                with arcpy.da.SearchCursor("cntyrow_lyr", fieldList) as cursor:
                    for row in cursor:
                        currentcnty = []
                        for val in currentspecies:
                            currentcnty.append(str(val))
                        colindex = 0
                        for field in fieldList:
                            value = (row[colindex])
                            try:
                                # print value
                                value = value.encode('ascii', 'ignore')
                            except:
                                value = str(value)
                            currentcnty.append(value)
                            colindex += 1
                        state = ST_name_Dict[str(row[0])]
                        currentcnty.append(state)
                        SpeTables.append(currentcnty)

outDF = pd.DataFrame(SpeTables, columns=header)

print 'Cleaning DF'

print outDF
outDF.to_csv(csvfile)

print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
