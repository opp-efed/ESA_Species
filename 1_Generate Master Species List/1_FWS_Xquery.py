from bs4 import BeautifulSoup
import requests
import sys
import os
import datetime

import pandas as pd

import csv

__author__ = 'JConno02'


today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

outpath = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
          r'\_ExternalDrive\Update_SpeciesList_Dec2018'
url = "https://ecos.fws.gov/services/TessQuery?request=query&xquery=/SPECIES_DETAIL"


deafultTags = ['spcode', 'vipcode', 'sciname', 'comname', 'invname', 'pop_abbrev', 'pop_desc', 'family', 'status',
               'status_text', 'lead_agency', 'lead_region', 'country', 'listing_date', 'dps', 'refuge_occurrence',
               'delisting_date']
delcolumns = ['species_detail', 'results','tsn']

colOrder = {
    9: 'spcode',
    10: 'vipcode',
    2: 'sciname',
    1: 'comname',
    3: 'invname',
    6: 'pop_abbrev',
    7: 'pop_desc',
    8: 'family',
    4: 'status',
    5: 'status_text',
    11: 'lead_agency',
    12: 'lead_region',
    13: 'country',
    14: 'listing_date',
    15: 'dps',
    16: 'refuge_occurrence'}


def CheckXML_changes(xml):
    taglist = []
    possAnswer = ['Yes', 'No']
    askQ = True

    while askQ:
        user_input = raw_input('Would you like to check for changes with the tags? : Yes or No: ')
        if user_input not in possAnswer:
            print 'This is not a valid answer'
        else:
            break
    if user_input == 'Yes':
        print 'Looking for tags'
        for tag in xml.findChildren():
            if tag.name not in taglist:
                taglist.append(str(tag.name))
            else:
                continue
        print taglist

        while askQ:
            user_input2 = raw_input('Which tag is the identifier ie entity id? ')
            if user_input2 not in taglist:
                print 'This is not a valid answer'
            else:
                break
        identifier = str(user_input2)
        taglist.remove(user_input2)

        while askQ:
            user_input3 = raw_input('Which tag is the break between species- if no change -species details ')
            if user_input3 not in taglist:
                print 'This is not a valid answer'
            else:
                break
        speciesbreak = str(user_input3)
        taglist.remove(user_input3)

        while askQ:
            user_input4 = raw_input(
                'What columns should be removed {0}, if none answer None? '.format(taglist))  # ie results, tsn
            if user_input4 == 'None' or user_input4 == 'none':
                break
            else:
                listinput = user_input4.split(",")
                for v in listinput:
                    if v not in taglist:
                        print v
                        askQ = True
                        print 'This is not a valid answer: values must be separated by a comma without a space'
                    else:
                        askQ = False
                        taglist.remove(v)
                if askQ == False:
                    break

    else:
        print 'Using default tags of {0}'.format(deafultTags)
        taglist = deafultTags
        speciesbreak = 'species_detail'
        identifier = 'entity_id'

    globals()[identifier] = []
    globals()[speciesbreak] = []

    for v in taglist:
        globals()[v] = {}
    print '\nSpecies information that will be extracted {0} using the identifier {1}'.format(taglist, identifier)
    return taglist, speciesbreak, identifier


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
    listdicts.remove('colOrder')
    return listdicts


def find_textxml(row, id, sp_info_need):
    for value in sp_info_need:
        try:
            c_value = (row.find(value, recursive=False).text)
            clean = c_value.encode('utf8', 'replace')
        except:
            clean = 'None'
        (globals()[value])[id] = str((clean.replace(",", " ")))


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


def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

createdirectory(outpath)

r = requests.get(url)
reload(sys)
sys.setdefaultencoding('utf8')
## These are the variables use within the xml, if FWS changes their variable this will need to be updated

createdirectory(outpath+os.sep+'FWS')
outpath = outpath+os.sep+'FWS'

# Use Beautiful Soup to Parse the HTML
soup = BeautifulSoup(r.content, 'html.parser')
tagslist, speciesbreak, identifier = CheckXML_changes(soup)

sp_info_need = list_dicts()

print 'Downloading information from {0}...'.format(url)
for row in soup.find_all(speciesbreak):
    # print row
    entid = row.find(identifier, recursive=False).text
    # print entid
    globals()[identifier].append(entid)
    find_textxml(row, entid, sp_info_need)


Full_list = globals()[identifier]
FullResults, header = CreateSpecisTable(Full_list, tagslist, colOrder)

finalheader = ['EntityID']
for v in header:
    finalheader.append(v)

fulltable = outpath + os.sep + 'FullTess_' + str(date) + '.csv'

outDF_Full = pd.DataFrame(FullResults, columns=finalheader)
outDF_Full.to_csv(fulltable, encoding='utf-8')
# create_outtable(FullResults, fulltable, finalheader)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)