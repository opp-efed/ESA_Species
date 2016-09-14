import datetime
import os

import pandas as pd
import arcpy

masterlist = 'J:\Workspace\MasterLists\April2015Lists\CSV\MasterListESA_April2015_20151015_20151124.csv'
# TODO take table and load as a list in script so that there doesn't need to be a standalone document
# TODO Clean up Region_cross the region field in att table should not have the _


outcsv = r'J:\Workspace\ESA_Species\ForCoOccur\Composites\CurrentComps\WebApp\CheckMissingGIS_20160518.csv'

index_dict = {'EntityID': 0,
              'ComName': 1,
              'Group': 2,
              'SciName': 3,
              'Status_text': 4,
              'AK': 5,
              'AS': 6,
              'CNMI': 7,
              'GU': 8,
              'HI': 9,
              'Howland Baker Jarvis': 10,
              'Johnston': 11,
              'L48': 12,
              'Laysan': 13,
              'Mona': 14,
              'Necker': 15,
              'Nihoa': 16,
              'NorthwesternHI': 17,
              'PR': 18,
              'Palmyra Kingman': 19,
              'VI': 20,
              'Wake': 21}

Region_cross = {'AK': 'AK',
                'AS': 'AS',
                'CNMI': 'CNMI',
                'GU': 'GU',
                'HI': 'HI',
                'Howland_Baker_Jarvis': 'Howland Baker Jarvis',
                'Howland Baker Jarvis': 'Howland Baker Jarvis',
                'Johnston': 'Johnston',
                'L48': 'L48',
                'Lower48': 'L48',
                'PLower48': 'L48',
                'Laysan': 'Laysan',
                'Mona': 'Mona',
                'Necker': 'Necker',
                'Nihoa': 'Nihoa',
                'NorthwesternHI': 'NorthwesternHI',
                'PR': 'PR',
                'Palmyra Kingman': 'Palmyra Kingman',
                'Palmyra_Kingman': 'Palmyra Kingman',
                'VI': 'VI',
                'Wake': 'Wake'}

# listKeys = sorted(index_dict.keys())
# print listKeys

PossAnswers = ['Yes', 'No']

QAanswer = True

while QAanswer:
    user_input = raw_input('Are you running range files? Yes or No? ')
    if user_input not in PossAnswers:
        print 'This is not a valid answer'

    else:
        QAanswer = False
        if user_input == 'Yes':
            infolder = 'J:\Workspace\ESA_Species\ForCoOccur\Composites\CurrentComps\WebApp\R_WebApp_Composite.gdb'
            regionallocations = 'J:\Workspace\MasterOverlap\Panda\NAD83_Range_SpeciesRegions_all_20160421.xlsx'
            fields = ['EntityID', 'Region', 'FileName']

        else:
            infolder = 'J:\Workspace\ESA_Species\ForCoOccur\Composites\CurrentComps\WebApp\CH_WebApp_Composite.gdb'
            regionallocations = 'J:\Workspace\MasterOverlap\Panda\NAD83_CH_SpeciesRegions_all_20160421.xlsx'
            fields = ['EntityID', 'Region', 'FileName']


def CreateDirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)


def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


start_script = datetime.datetime.now()
print "Started: {0}".format(start_script)

regionaldf = pd.read_excel(regionallocations, sheetname='Sheet1', header=0, skiprows=0,
                           skip_footer=0, index_col=None,
                           parse_cols=None, parse_dates=False, date_parser=None, na_values=None, thousands=None,
                           convert_float=True, has_index_names=None, converters=None, engine=None)
rowcount = regionaldf.count(axis=0, level=None, numeric_only=False)
rowindex = rowcount.values[0]
print rowindex

listheader = regionaldf.columns.values.tolist()
print listheader

colcount = len(listheader)
print colcount
outDF = pd.DataFrame(columns=listheader)

col = 0
row = 0

outdict = {}
entid_list = []
while row <= (rowindex - 1):
    currentlist = []
    entidnext = index_dict['EntityID']
    entid = str(regionaldf.iloc[row, entidnext])
    if entid not in entid_list:
        entid_list.append(entid)
    for vol in listheader:
        colvalue = index_dict[str(vol)]
        value = regionaldf.iloc[row, colvalue]
        currentlist.append(value)
    outdict[entid] = currentlist
    row += 1

GISdict = {}
filenamedict = {}

for fc in fcs_in_workspace(infolder):
    loop_start = datetime.datetime.now()
    print "Loop Started: {0} for {1}".format(loop_start, fc)
    with arcpy.da.SearchCursor(fc, fields) as cursor:
        for row in cursor:
            entid = str(row[0])
            region = str(row[1])
            regionalloc = outdict[entid]
            regionlookup = Region_cross[region]
            region_index = index_dict[regionlookup]
            currentvalues = regionalloc[region_index]
            del regionalloc[region_index]
            regionalloc.insert(region_index, (str(currentvalues) + ', ' + 'GIS'))
            GISdict[entid] = regionalloc
            filenamedict[entid] = row[2]



col = 0
row = 0

outdict = {}
while row <= (rowindex - 1):
    currentlist = []
    entidnext = index_dict['EntityID']
    entid = str(regionaldf.iloc[row, entidnext])
    for vol in listheader:
        colvalue = index_dict[str(vol)]
        value = regionaldf.iloc[row, colvalue]
        currentlist.append(value)
    outdict[entid] = currentlist
    row += 1
addatt = ["GISFile", "FileName"]
listheader.extend(addatt)
GISent = GISdict.keys()
outlist = []
for value in entid_list:
    append = 'NA'
    filename = 'NA'
    orgvalue = outdict[value]
    counter = 0

    if value in GISent:
        checkvalues = GISdict[value]
        while counter < colcount:
            GIS = str(checkvalues[counter])
            old = str(orgvalue[counter])
            if GIS == old:
                pass
            elif GIS == 'Yes, GIS' and old == 'Yes':
                append = 'Yes'
            elif GIS == 'nan, GIS' and old == 'nan':
                append = 'Yes'
            elif GIS == 'Yes' and old == 'nan':
                append = 'Yes'
                del checkvalues[counter]
                checkvalues.insert(counter, ('nan, GIS'))
            elif GIS == 'Yes' and old is None:
                append = 'Yes'
                del checkvalues[counter]
                checkvalues.insert(counter, ('nan, GIS'))
            elif old == 'Yes':
                del checkvalues[counter]
                checkvalues.insert(counter, ('Yes, None'))
                append = 'Error'
            else:
                del checkvalues[counter]
                checkvalues.insert(counter, ('Error'))
            counter += 1
        filename = str(filenamedict[value])
        checkvalues.append(append)
        checkvalues.append(filename)
        outlist.append(checkvalues)
    else:
        counter = 5  ## Hard code to start it at the first colem that is a range
        while counter < colcount:
            old = str(orgvalue[counter])
            if old == 'Yes':
                del orgvalue[counter]
                orgvalue.insert(counter, ('Yes, None'))
                append = 'No'
            elif old == 'nan':
                pass
            else:
                print 'Error'
                orgvalue.insert(counter, ('Error'))
            counter += 1
        orgvalue.append(append)
        orgvalue.append(filename)
        outlist.append(orgvalue)
print outlist
outDF = pd.DataFrame(outlist, columns=listheader)

# print outDF
outDF.to_csv(outcsv)

print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
