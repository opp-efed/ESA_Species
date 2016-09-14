import datetime
import os

import pandas as pd
import arcpy

masterlist = 'J:\Workspace\MasterLists\April2015Lists\CSV\MasterListESA_April2015_20151015_20151124.csv'
# TODO take table and load as a list in script so that there doesn't need to be a standalone document

skiplist = ['Amphibians', 'Arachnids', 'Birds', 'Clams', 'Conifers and Cycads', 'Corals', 'Ferns and Allies',
            'Fishes', 'Flowering Plants', 'Insects', 'Lichens', 'Mammals', 'Reptiles',
            'Snails']  # species groups that were already run

index_dict = {'EntityID': 0,
              'AK': 5,
              'AS': 6,
              'CNMI': 7,
              'GU': 8,
              'HI': 9,
              'Howland_Baker_Jarvis': 10,
              'Johnston': 11,
              'L48': 12,
              'Laysan': 13,
              'Mona': 14,
              'Necker': 15,
              'Nihoa': 16,
              'NorthwesternHI': 17,
              'PR': 18,
              'Palmyra_Kingman': 19,
              'VI': 20,
              'Wake': 21}

GDB_Dict = {'AK': 'AK_Singlepart.gdb',
            'AS': 'AS_Singlepart.gdb',
            'CNMI': 'CNMI_Singlepart.gdb',
            'GU': 'GU_Singlepart.gdb',
            'HI': 'HI_Singlepart.gdb',
            'Howland_Baker_Jarvis': 'Howland_Singlepart.gdb',
            'Johnston': 'Johnston_Singlepart.gdb',
            'L48': 'Lower48_Singlepart.gdb',
            'Laysan': 'Laysan_Singlepart.gdb',
            'Mona': 'Mona_Singlepart.gdb',
            'Necker': 'Necker_Singlepart.gdb',
            'Nihoa': 'Nihoa_Singlepart.gdb',
            'NorthwesternHI': 'NorthwesternHI_Singlepart.gdb',
            'PR': 'PR_Singlepart.gdb',
            'Palmyra_Kingman': 'Palmyra_Singlepart.gdb',
            'VI': 'VI_Singlepart.gdb',
            'Wake': 'Wake_Singlepart.gdb'}

listKeys = sorted(index_dict.keys())
print listKeys

PossAnswers = ['Yes', 'No']

QAanswer = True

while QAanswer:
    user_input = raw_input('Are you running range files? Yes or No? ')
    if user_input not in PossAnswers:
        print 'This is not a valid answer'

    else:
        QAanswer = False
        if user_input == 'Yes':
            infolder = 'J:\Workspace\ESA_Species\ForCoOccur\Range'
            regionallocations = 'J:\Workspace\MasterOverlap\Panda\NAD83_Range_SpeciesRegions_all_20160421.xlsx'
            print 'Running range files output will be located at {0}'.format(infolder)
        else:
            infolder = 'J:\Workspace\ESA_Species\ForCoOccur\CriticalHabitat'
            print 'Running critical habitat files output will be located at {0}'.format(infolder)
            regionallocations = 'J:\Workspace\MasterOverlap\Panda\NAD83_CH_SpeciesRegions_all_20160421.xlsx'

gdblist = ["SingleNonLower48only.gdb", "SingleBoth.gdb", "Lower48Only.gdb"]


def CreateDirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)


def CreateGDB(OutFolder, OutName, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(OutFolder, OutName, "CURRENT")


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

listheader = regionaldf.columns.values.tolist()
colcount = len(listheader)
outDF = pd.DataFrame(columns=listheader)

col = 0
row = 0

outdict = {}
while row <= (rowindex - 1):
    currentlist = []
    entidnext = index_dict['EntityID']
    entid = str(regionaldf.iloc[row, entidnext])
    for vol in listKeys:
        colvalue = index_dict[vol]
        # print 'Current index is {0} , {1}'.format(row,colvalue)
        value = regionaldf.iloc[row, colvalue]
        currentlist.append(value)
    # print currentlist
    outdict[entid] = currentlist
    row += 1

# print outdict
grouplist = []
with open(masterlist, 'rU') as inputFile:
    header = next(inputFile)
    for line in inputFile:
        line = line.split(',')
        entid = line[0]
        group = line[1]
        grouplist.append(group)

unq_grps = set(grouplist)
alpha_group = sorted(unq_grps)
print alpha_group

for group in alpha_group:
    if group in skiplist:
        continue
    print "\nWorking on {0}".format(group)

    for regions in listKeys:
        if regions == 'EntityID':  # TODO remove this hard code
            continue
        orgregion = regions
        print "Working with {0}".format(regions)
        resultfolder = infolder + os.sep + group
        regionsDIR = resultfolder + os.sep + "Regions"
        CreateDirectory(resultfolder)
        CreateDirectory(regionsDIR)

        NAD83DIR = regionsDIR + os.sep + 'NAD83'
        CreateDirectory(NAD83DIR)
        orggdb = GDB_Dict[regions]

        for value in gdblist:
            if value == "SingleBoth.gdb":
                if regions == "L48":
                    gdb = 'PLower48_Singlepart.gdb'
                    outpathgdb = NAD83DIR + os.sep + gdb
                    region = 'PLower48'
            else:
                gdb = orggdb
                outpathgdb = NAD83DIR + os.sep + gdb
                region = orgregion

            inGDB = resultfolder + os.sep + value

            for fc in fcs_in_workspace(inGDB):
                infc = inGDB + os.sep + fc
                entid = fc.split("_")
                entid = str(entid[1])
                regionindex = listKeys.index(regions)
                speinfo = outdict[entid]
                # print speinfo
                regionval = str(speinfo[regionindex])
                # print regionval
                if regionval != 'nan':
                    outfc = outpathgdb + os.sep + region + "_" + str(fc)
                    if not arcpy.Exists(outfc):
                        if not arcpy.Exists(outpathgdb):
                            CreateGDB(NAD83DIR, gdb, outpathgdb)
                        arcpy.CopyFeatures_management(infc, outfc)
                        print "Copied file {0} in region {1}".format(outfc, region)
                    else:
                        continue
                else:
                    continue

print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
