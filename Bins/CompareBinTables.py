import datetime

import pandas as pd

start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

masterlist = 'J:\Workspace\MasterLists\April2015Lists\CSV\MasterListESA_April2015_20151015_20151124.csv'
FileOld = 'C:\Users\JConno02\Desktop\currentBins.xlsx'
FileNew = 'C:\Users\JConno02\Desktop\ChucksTable.xlsx'
outlocation = 'C:\Users\JConno02\Desktop\checkbins_non.csv'
speinfo = ['NAME', 'Name_sci']
df_old = pd.read_excel(FileOld, sheetname='Sheet1', header=0, skiprows=0, skip_footer=0, index_col=None,
                       parse_cols=None, parse_dates=False, date_parser=None, na_values=None, thousands=None,
                       convert_float=True, has_index_names=None, converters=None, engine=None)

entid_index = 0
huc2_index = 1
colstart_index = 2
colindex = 7
rowindex = 2959

df_new = pd.read_excel(FileNew, sheetname='Sheet1', header=0, skiprows=0, skip_footer=0, index_col=None,
                       parse_cols=None, parse_dates=False, date_parser=None, na_values=None, thousands=None,
                       convert_float=True, has_index_names=None, converters=None, engine=None)
entid_index_2 = 0
huc2_index_2 = 1
colstart_index_2 = 2
colindex_2 = 7
rowindex_2 = 1030




# Bin1_new = {}
Bin2_new = {}
Bin3_new = {}
Bin4_new = {}
Bin5_new = {}
Bin6_new = {}
Bin7_new = {}
# Bin8_new = {}
# Bin9_new = {}
# Bin10_new = {}

# Bin1_old = {}
Bin2_old = {}
Bin3_old = {}
Bin4_old = {}
Bin5_old = {}
Bin6_old = {}
Bin7_old = {}
# Bin8_old = {}
# Bin9_old = {}
# Bin10_old = {}

commonname = {}
sciname = {}
specode = {}
vipcode = {}
statusDict = {}
groupdict = {}
entlist = []

with open(masterlist, 'rU') as inputFile:
    header = next(inputFile)
    for line in inputFile:
        line = line.split(',')
        entid = str(line[0])
        if entid not in entlist:
            entlist.append(entid)
        statusDict[str(entid)] = str(line[11])
        commonname[str(entid)] = str(line[7])
        sciname[str(entid)] = str(line[8])
        specode[str(entid)] = str(line[26])
        vipcode[str(entid)] = str(line[27])
        groupdict[str(entid)] = str(line[1])
inputFile.close()

row = 0
list = df_new.columns.values.tolist()

checkkeys = []
outlist = []
print "reading in {0}".format(FileOld)
while row < (rowindex):
    entid = df_old.iloc[row, entid_index]
    # print entid
    huc2 = df_old.iloc[row, huc2_index]
    # print huc2
    spkey = str(entid) + "_" + str(huc2)
    if spkey not in checkkeys:
        checkkeys.append(spkey)

    col = colstart_index
    counter = 2
    while col < (colindex + 1):
        if counter == 8:
            break
        colheader = list[col]
        bindict = 'Bin' + str(counter) + "_old"
        # print "Working on row {0}, col {1}".format(row, col)

        value = df_old.iloc[row, col]
        vars()[bindict][spkey] = value

        col += 1
        counter += 1

    row = row + 1

row = 0
list2 = df_old.columns.values.tolist()

counter = 2
print "reading in {0}".format(FileNew)
while row < (rowindex_2):
    entid = df_new.iloc[row, entid_index_2]
    huc2 = df_new.iloc[row, huc2_index_2]
    spkey = str(entid) + "_" + str(huc2)

    col = colstart_index_2
    counter = 2
    while col < (colindex + 1):
        if counter == 8:
            break
        colheader = list2[col]
        bindict = 'Bin' + str(counter) + "_new"
        # print "Working on row {0}, col {1}".format(row, col)

        value = df_new.iloc[row, col]
        vars()[bindict][spkey] = value

        col += 1
        counter += 1

    row = row + 1

counter = 2
entBins = {}

while counter < 8:
    print "starting loop {0}".format(counter)
    print entBins
    listents = entBins.keys()
    # print listents
    for value in checkkeys:
        currentlist = []
        # print value
        extract = value.split("_")
        entid = str(extract[0])
        currentlist.append(str(entid))
        huc = str(extract[1])
        currentlist.append(str(huc))
        bindictnew = 'Bin' + str(counter) + "_new"
        bindictold = 'Bin' + str(counter) + "_old"

        binv_new = (vars()[bindictnew].get(value))
        binv_old = (vars()[bindictold].get(value))

        if binv_new >= 2 and binv_old >= 2 or binv_new == binv_old:
            v = 'True, ' + str(binv_old)
        elif binv_old == 1 and binv_new is None:
            v = 'True, ' + str(binv_old)
        elif binv_old == 1 and binv_new >= 2:
            v = 'False, No bin in bin table but PWC is {1} for bin  number {2}'.format(binv_old, binv_new, counter)
        else:
            # print "Species {0} in Huc {1} do not match".format(entid,huc)
            print "current bin {0} PWC {1} for bin  number {2}".format(binv_old, binv_new, counter)
            v = 'False, current bin {0} PWC {1} for bin  number {2}'.format(binv_old, binv_new, counter)

        if value in listents:
            currentlist = entBins[value]
            currentlist.append(v)
        else:
            currentlist.append(v)
            entBins[value] = currentlist

    print counter
    counter += 1

listents = entBins.keys()
for v in listents:
    list = entBins[v]
    entid = v.split("_")
    entid = entid[0]
    comname = commonname[entid]
    sname = sciname[entid]
    group = groupdict[entid]
    status = statusDict[entid]
    list.extend((comname, sname, group, status))
    outlist.append(list)
print entBins

outDF = pd.DataFrame(outlist)

# print outDF

outDF.to_csv(outlocation)
print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
