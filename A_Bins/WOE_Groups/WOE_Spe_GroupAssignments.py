import csv
import os
import datetime

### this script is set up to over values is dict tracker- this was done because each species group can only be one of 2
### WOE group.  If we need to break a species group into more than 2 WOE we will need to add multiple dict trackers and
### check each one before adding a new assignment

CSVbins = 'C:\\Users\\JConno02\\Documents\\Projects\\ESA\\A_Bins\\ExportDataBase\\20151105\\Aquatic_wReassign_20151105_long.csv'

spegrps = 'C:\\Users\\JConno02\\Documents\\Projects\ESA\\WOE\Dictionary\\spegrp_dict.csv'
amp_csv = 'C:\\Users\\JConno02\\Documents\\Projects\ESA\\WOE\Dictionary\\amp.csv'
fish_csv = 'C:\\Users\\JConno02\\Documents\\Projects\ESA\\WOE\Dictionary\\fish.csv'
birds_csv = 'C:\\Users\\JConno02\\Documents\\Projects\ESA\\WOE\Dictionary\\birds.csv'
clams_csv = 'C:\\Users\\JConno02\\Documents\\Projects\ESA\\WOE\Dictionary\\clams.csv'
inverts_csv = 'C:\\Users\\JConno02\\Documents\\Projects\ESA\\WOE\Dictionary\\inverts.csv'
mammals_csv = 'C:\\Users\\JConno02\\Documents\\Projects\ESA\\WOE\Dictionary\\mammals.csv'
plants_csv = 'C:\\Users\\JConno02\\Documents\\Projects\ESA\\WOE\Dictionary\\plants.csv'
reptiles_csv = 'C:\\Users\\JConno02\\Documents\\Projects\ESA\\WOE\Dictionary\\reptiles.csv'

csvlocation = 'C:\\Users\\JConno02\\Documents\\Projects\\ESA\\WOE\\OutTables\\20151105'


def createdicts(csvfile, key):
    with open(csvfile, 'rb') as dictfile:
        group = csv.reader(dictfile)
        dictname = {rows[0]: rows[1] for rows in group}
        return dictname


start = datetime.datetime.now()
print "Script start at {0}".format(start)

if not os.path.exists(csvlocation):
    os.mkdir(csvlocation)
    print "created directory {0}".format(csvlocation)

ListCSV = []
ListCSV += [val for name, val in globals().items() if name.endswith('_csv')]
ListCSV.sort()

spe_grps = ['Amphibians', 'Fishes', 'Birds', 'Mammals', 'Flowering Plants', 'Reptiles', 'Arachnids', 'Clams',
            'Conifers and Cycads', 'Crustaceans', 'Ferns and Allies', 'Insects', 'Lichens', 'Snails', 'Corals']
listDicts = {}
listvar = []
dicts = {}
spe_group_dict = {}

for z in ListCSV:
    basename = os.path.basename(z)
    group = basename[:-4]
    dict = str(group) + "_d"
    listDicts[dict] = z
    listvar.append(dict)

spe_group_dict = createdicts(spegrps, spe_group_dict)

for key in listDicts:
    file = listDicts[key]
    group_dict = createdicts(file, key)
    dicts[key] = group_dict

for key in dicts:
    group_dict = dicts[key]
    for k in group_dict:
        value = group_dict[k]
WOEList = []
Tracker = []
noBintracker = []
dict_tracker = {}
dict_tracker2 = {}
dict_tracker3 = {}
header = "EntityID" + ',' + "Group" + "," "WOE_Group" + ',' + "Common_Name" + ',' + "Sci_Name" + ',' + "Status" + ',' + "Species_Occurs_in_Multiple_Groups"
header2 = "EntityID"
col_name = 'Group'
removeList = []
for key in spe_group_dict:
    value = spe_group_dict[key]
    if value in dicts:
        gp_dict = dicts[value]
        with open(CSVbins)as binfile:
            reader = csv.DictReader(binfile)
            title = next(reader)
            col_index = col_name
            for row in reader:
                if row[col_index] == key:
                    entid = row['ENTITYID']
                    comname = row['COMNAME']
                    sciname = row['SCINAME']
                    status = row['STATUS_TEXT']
                    group = row['Group']
                    bin = row['A_Bins']
                    binvalue = row['Value']
                    if binvalue == '1':
                        continue
                    elif binvalue == '4':
                        continue
                    elif binvalue == '5':
                        continue
                    elif binvalue == '7':
                        continue
                    else:

                        WOEgrp = gp_dict[bin]
                        entid = str(entid)
                        assignment = str(entid) + ',' + str(group) + "," + str(WOEgrp) + ',' + str(comname) + ',' + str(
                            sciname) + ',' + str(status)
                        WOEList.append(assignment)
                        Tracker.append(entid)
                        if group =="Clams" and binvalue=='8' or binvalue=='10' or binvalue=='11' or binvalue=='28' or binvalue=='210' or binvalue=='211':
                            assignment = str(entid) + ',' + str(group) + "," + "Freshwater Fish" + ',' + str(comname) + ',' + str(
                            sciname) + ',' + str(status)
                        WOEList.append(assignment)
                        Tracker.append(entid)
                        if group =='Corals':
                            assignment = str(entid) + ',' + str(group) + "," + "Aquatic Plants Non-Vascular" + ',' + str(comname) + ',' + str(
                            sciname) + ',' + str(status)
                        WOEList.append(assignment)
                        Tracker.append(entid)

WOEset = set(WOEList)
Trackerset = set(Tracker)
track = []
dup_assignments = []
finalAssginment = []

for WoE in WOEset:
    list_WoE = WoE.split(',')
    entid = list_WoE[0]
    print entid
    if entid in track:
        dup_assignments.append(entid)
    else:
        track.append(entid)

for WoE in WOEset:
    list_WoE = WoE.split(',')
    entid = list_WoE[0]
    group = list_WoE[1]
    if entid in dup_assignments:
        assign = WoE + "," + "***"
        finalAssginment.append(assign)
        continue
    else:
        finalAssginment.append(WoE)

dups = set(dup_assignments)
final = set(finalAssginment)

f = open(csvlocation + os.sep + 'WOE_spegroup_all_20151105.csv', "wb")
writer = csv.writer(f, delimiter=' ', quotechar=',', quoting=csv.QUOTE_MINIMAL)
writer.writerow([header])
for val in final:
    writer.writerow([val])

f = open(csvlocation + os.sep + 'Tracker_enId.csv', "wb")
writer = csv.writer(f, delimiter=' ', quotechar=',', quoting=csv.QUOTE_MINIMAL)
writer.writerow([header2])
for val in Trackerset:
    writer.writerow([val])

f = open(csvlocation + os.sep + 'Dup_enId.csv', "wb")
writer = csv.writer(f, delimiter=' ', quotechar=',', quoting=csv.QUOTE_MINIMAL)
writer.writerow([header2])
for val in dups:
    writer.writerow([val])

end = datetime.datetime.now()
print"Elapse time was {0}".format(end - start)
