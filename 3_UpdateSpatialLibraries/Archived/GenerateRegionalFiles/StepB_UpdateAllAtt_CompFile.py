import os
import datetime

import arcpy
import pandas as pd

masterlist = 'J:\Workspace\MasterLists\April2015Lists\CSV\MasterListESA_April2015_20151015_20151124.csv'
infolder = r'J:\Workspace\ESA_Species\ForCoOccur\Composites\CurrentComps\WebApp\CH_WebApp_Composite.gdb'  # folder of GDB

# species groups to skip
skiplist = []

final_fields = ['FileName', 'EntityID', 'NAME', 'Name_sci', 'SPCode', 'VIPCode', 'Status',
                'PopName']  # in order to be added
colorderDict = dict(EntityID='entityid', NAME='comname', Name_sci='sciname', SPCode='spcode', VIPCode='vipcode',
                    Status='status',
                    PopName='popname')  # link the var used to store species info from table to a col in fc
final_fieldsindex = dict(NAME=2, Name_sci=3, SPCode=4, VIPCode=5, EntityID=1, Status=6,
                         FileName=0, PopName=7)  # used to check if FC has field in the correct order

CheckForChanges_col = ['entityid', 'spcode', 'vipcode', 'sciname', 'comname', 'group',
                       'status', 'popname']  # col that will be looked up in master

oldColindex = {'entityid': 0,
               'spcode': 26,
               'vipcode': 27,
               'sciname': 8,
               'comname': 7,
               'group': 1,
               'status': 11,
               'popname': 14}
########Static variables

entid_indexfilenm = 1
inputlist = ['Yes', 'No']
speciestoQA = '1'
updatefiles = False
singleGDB = False

extention = infolder.split(".")
cnt = len(extention)
if cnt > 1:
    if extention[1] == 'gdb':
        singleGDB = True
    else:
        singleGDB = False
else:
    singleGDB = False

start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)


# Functions
def CreatDicts(list_vars):
    for v in list_vars:
        dictname = "{0}".format(v)
        globals()[dictname] = {}


def list_dicts():
    # global spcode, vipcode, sciname, comname, invname, pop_abbrev, pop_desc, family, status, status_text, lead_agency, lead_region, country, listing_date, dps, refuge_occurrence
    listitems = []
    listdicts = []
    for name in globals():
        if not name.startswith('__'):
            listitems.append(name)
    for value in listitems:
        if type((globals()[value])) is dict:
            listdicts.append(value)
    return listdicts


def LoadDatainDicts(incsv, colIndex, dict_list):
    df = pd.read_csv(incsv)
    group_df = df['Group']
    unq_groups = group_df.drop_duplicates()

    header = list(df.columns.values)

    entidindex = colIndex["entityid"]

    rowcount = df.count(axis=0, level=None, numeric_only=False)
    rowindex = rowcount.values[0]
    colindex = len(header) - 1  # to make base 0

    row = 0
    while row < (rowindex):
        entid = str(df.iloc[row, entidindex])
        # print "Working on species {0}, row {1}".format(entid, row)
        for dictionary in dict_list:
            col = colIndex[str(dictionary)]  ## assuming an index row from pandas export
            value = df.iloc[row, col]
            ((globals()[dictionary][entid])) = str(value)

        row += 1
    return group_df


def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc


def output_update(fc, value):
    print "     Updated {0} for files {1}".format(value, fc)


def CheckFieldOrder(inGDB, final_fieldsindex, final_fields):
    group_gdb = inGDB
    arcpy.env.workspace = group_gdb
    fclist = arcpy.ListFeatureClasses()
    checkorder = {}
    for fc in fclist:
        print fc
        fclist_field = [f.name for f in arcpy.ListFields(fc) if not f.required]
        for z in final_fields:
            if z not in fclist_field:
                arcpy.AddField_management(fc, z, "TEXT")
                print "Added field {0} to {1}".format(z, fc)
        with arcpy.da.SearchCursor(fc, final_fields)as s_cursor:
            for row in s_cursor:
                entid = row[1]  ##TO DO update hard code
                for field in final_fields:
                    if field == 'FileName':
                        continue
                    else:
                        colindex = final_fieldsindex[field]
                        current = row[colindex]
                        val_dict = colorderDict[field]
                        m_value = ((globals()[val_dict][entid]))
                        if current != m_value:
                            with arcpy.da.UpdateCursor(fc, [field, 'EntityID']) as cursor:
                                for line in cursor:
                                    if line[1] == entid:
                                        line[0] = m_value
                                        cursor.updateRow(line)
                                        output_update(fc, field)
                                    else:
                                        continue
                                del cursor
                        else:
                            continue


CreatDicts(CheckForChanges_col)
allDicts = list_dicts()
group_df = LoadDatainDicts(masterlist, oldColindex, CheckForChanges_col)

if singleGDB:
    inGDB = infolder
    CheckFieldOrder(inGDB, final_fieldsindex, final_fields)


else:
    alpha_group = group_df['Group'].values.tolist
    for group in alpha_group:
        start_loop = datetime.datetime.now()
        if group in skiplist:
            continue
        print "\nWorking on {0}".format(group)
        inGDB = infolder + os.sep + str(group) + '.gdb'
        CheckFieldOrder(inGDB, final_fieldsindex, final_fields)
        endloop = datetime.datetime.now()
        print "Elapse time {0}".format(endloop - start_script)

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
