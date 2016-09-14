__author__ = 'JConno02'

import datetime
import os
import csv

import pandas as pd

# TODO update cross check to arrays and not dictionaries
date = 20160608
oldMaster = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\TESSQueries\20160606\FilteredinPandas\ChangesJustFWS\FWS_MasterListESA_April2015_20151015_20151124.csv'
newMaster = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\TESSQueries\20160606\FilteredinPandas\ChangesJustFWS\FilteredTessPandas_FWS_20160607.csv'
table_id_list = ['old', 'new']
outpath = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\TESSQueries\20160606\FilteredinPandas\ChangesJustFWS'

CheckForChanges_col = ['entity_id', 'spcode', 'vipcode', 'sciname', 'comname', 'family', 'status_text', 'country']

newColindex = {'entity_id': 1,
               'spcode': 10,
               'vipcode': 11,
               'sciname': 3,
               'comname': 2,
               'family': 9,
               'status_text': 6,
               'country': 14}
oldColindex = {'entity_id': 0,
               'spcode': 26,
               'vipcode': 27,
               'sciname': 8,
               'comname': 7,
               'family': 9,
               'status_text': 11,
               'country': 13}


def CreatDicts(list_vars, table_id):
    for v in list_vars:
        dictname = "{0}_{1}".format(v, table_id)
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


def LoadDatainDicts(incsv, colIndex, dict_list, table_id):
    ent_df = pd.DataFrame(columns=['EntityID'])
    df = pd.read_csv(incsv)
    header = list(df.columns.values)
    # print header
    entidindex = colIndex["entity_id"]
    # print entidindex
    rowcount = df.count(axis=0, level=None, numeric_only=False)
    rowindex = rowcount.values[0]
    colindex = len(header) - 1  # to make base 0

    row = 0
    while row < (rowindex):
        entid = str(df.iloc[row, entidindex])
        ent_df = ent_df.append({'EntityID': entid}, ignore_index=True)
        # print "Working on species {0}, row {1}".format(entid, row)
        for dictionary in dict_list:
            if dictionary.endswith(table_id):
                str_remove = "_" + str(table_id)
                colkey = dictionary.replace(str_remove, '')
                col = colIndex[colkey]  ## assuming an index row from pandas export
                value = df.iloc[row, col]
                ((globals()[dictionary][entid])) = str(value)

        row += 1
    addSpecies_df = df[df['EntityID'].isin(ent_df['EntityID'])]
    return ent_df


def CheckForChanges(dictlist, cols, allent_df):
    for v in cols:
        start = datetime.datetime.now()
        if v == 'entity_id':
            continue
        else:
            print "Checking for changes in {0}".format(v)
            current = []
            for dictionay in dictlist:
                if dictionay.startswith(v):
                    current.append(dictionay)
            if len(current) != 2:
                print "Only one dictionary can't compare {0}".format(current)
            else:

                rowcount = allent_df.count(axis=0, level=None, numeric_only=False)
                rowindex = rowcount.values[0]
                row = 0
                ent_df = pd.DataFrame(columns=['EntityID', 'Old Value', 'New Value', 'Change'])
                while row < (rowindex):
                    entid = str(allent_df.iloc[row, 0])
                    for value in current:
                        if value.endswith('new'):
                            try:
                                new = ((globals()[value][entid]))

                            except KeyError:
                                new = 'Removed'
                        else:
                            try:
                                old = ((globals()[value][entid]))

                            except KeyError:
                                old = 'Newly Added'
                    row += 1
                    if old == new:
                        change = 'No'
                    else:
                        change = 'Yes'
                    # result =pd.DataFrame([entid,old,new,change])
                    # ent_df =ent_df.append(result)
                    ent_df = ent_df.append({'EntityID': entid, 'Old Value': old, 'New Value': new, 'Change': change},
                                           ignore_index=True)

                ent_df = ent_df[ent_df['Change'].isin(['Yes'])]
                outcsv = outpath + os.sep + 'Changes_' + str(v) + '.csv'
                ent_df.to_csv(outcsv)
                print "Exported {0} completed in {1}".format(outcsv, (datetime.datetime.now() - start))


def CreateSpecisTable(species_entList, species_info_var, colOrder):
    list_cols = colOrder.values()
    list_index = colOrder.keys()

    counter = ((max(list_index)) + 1)
    for v in species_info_var:
        if v not in list_cols:
            # print v
            # print counter
            colOrder[counter] = v
            counter += 1
        else:
            continue

    list_final = colOrder.keys()
    header = colOrder.values()

    outlist = []
    for i in species_entList:
        col = 1
        current_species = []
        entid = str(i)
        current_species.append(entid)
        while col < ((max(list_final) + 1)):
            current_col = colOrder[col]
            value = ((globals()[current_col][entid]))
            try:
                vclean = value.encode('utf8', 'replace')
                current_species.append(vclean)
                col += 1
            except (UnicodeEncodeError, UnicodeDecodeError):
                current_species.append("removed")
                col += 1

        outlist.append(current_species)
    return outlist, header


def create_outtable(outInfo, csvname, header):
    ## CHANGE added a function to export to the dictionaries and lists to csv to QA the intermediate steps and to have copies of the final tables
    if type(outInfo) is dict:
        with open(csvname, "wb") as output:
            writer = csv.writer(output, lineterminator='\n')
            writer.writerow(header)
            for k, v in outInfo.items():
                val = []
                val.append(k)
                val.append(outInfo[k])
                writer.writerow(val)
    elif type(outInfo) is list:
        with open(csvname, "wb") as output:
            writer = csv.writer(output, delimiter=",", quoting=csv.QUOTE_MINIMAL)
            writer.writerow(header)
            for val in outInfo:
                writer.writerow([val])


CreatDicts(CheckForChanges_col, "old")
CreatDicts(CheckForChanges_col, "new")
allDicts = list_dicts()

ent_df_old = LoadDatainDicts(oldMaster, oldColindex, allDicts, "old")
ent_df_new = LoadDatainDicts(newMaster, newColindex, allDicts, "new")

addSpecies_df = ent_df_new[ent_df_new['EntityID'].isin(ent_df_old['EntityID']) == False]
removedspecies_df = ent_df_old[ent_df_old['EntityID'].isin(ent_df_new['EntityID']) == False]

allents_singlelist = ent_df_old.append([ent_df_new])
allents_singlelist = allents_singlelist.drop_duplicates()

add_outfilter = outpath + os.sep + 'added_TESS' + str(date) + '.csv'
remove_outfilter = outpath + os.sep + 'RemovedTESS_' + str(date) + '.csv'

addSpecies_df.to_csv(add_outfilter, encoding='utf-8')
removedspecies_df.to_csv(remove_outfilter, encoding='utf-8')

CheckForChanges(allDicts, CheckForChanges_col, allents_singlelist)

# print "Added Species {0}".format(addSpecies_df)
# print "Removed Species {0}".format(removedspecies_df)



# finalheader = ['EntityID']
# for v in header:
#     finalheader.append(v)
#
# for value in sp_info_need:
#     create_outtable(globals()[value], (outpath + os.sep + str(value) + "_" + str(date) + '.csv'), finalheader)
#
# outfilter = outpath + os.sep + 'FilteredTess_' + str(date) + '.csv'
#
# outDF_filtered = pd.DataFrame(filterResults, columns=finalheader)
# outDF_filtered.to_csv(outfilter, encoding='utf-8')



# create_outtable(filterResults, outfilter, finalheader)
