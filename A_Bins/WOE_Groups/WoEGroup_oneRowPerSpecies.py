import os
import pandas as pd
import datetime

WoeTableCSV = 'C:\Users\JConno02\Documents\Projects\ESA\WOE\WOE_spegroup_all_20151118.csv'
outlocation = 'C:\Users\JConno02\Documents\Projects\ESA\WOE'
start_script = datetime.datetime.now()
print "Script started at: {0}".format(start_script)

WoeTable = pd.read_csv(WoeTableCSV)
rowcount = WoeTable.count(axis=0, level=None, numeric_only=False)
rowindex = rowcount.values[0]

row = 0
countacres = 0
WoEGroupdict = {}
while row < rowindex:
    currentvalue = []
    EntID = WoeTable.iloc[row, 0]
    #print EntID
    WoEgroup = WoeTable.iloc[row, 2]
    try:
        currentvalue = WoEGroupdict[EntID]
        currentvalue.append(WoEgroup)

    except:
        currentvalue = [WoEgroup]
        WoEGroupdict[EntID] = currentvalue
    #print currentvalue
    row += 1

results = []
keylist = WoEGroupdict.keys()
for v in keylist:
    value = WoEGroupdict[v]
    value.sort()
    refineddict = {}
    for l in value:

        splitgroup = l.split(" ")
        count = len(splitgroup)

        if splitgroup[count - 1] == 'Plants':
            try:
                refinedgroup = refineddict[v]
                if refinedgroup == 'Aquatic Plants':
                    continue
                else:
                    if splitgroup[0] == 'Aquatic':
                        refinedgroup = 'Aquatic Plants'
                        refineddict[v] = refinedgroup
                    else:
                        refinedgroup = 'Terrestrial Plants'
                        refineddict[v] = refinedgroup
            except:

                if splitgroup[0] == 'Aquatic':
                    refinedgroup = 'Aquatic Plants'
                    refineddict[v] = refinedgroup
                else:
                    refinedgroup = 'Terrestrial Plants'
                    refineddict[v] = refinedgroup
        elif splitgroup[count - 1] == 'Mammals':
            print v
            print l
            print splitgroup
            print count
            print splitgroup[count - 1]
            try:
                print 'Check for prevous assign {0}'.format(refinedgroup)
                refinedgroup = refineddict[v]
                print 'Check for prevous assign {0}'.format(refinedgroup)
                if refinedgroup == 'Marine Mammals':
                    continue
                else:
                    print "groups is {0}".format(splitgroup[0])
                    if splitgroup[0] == 'Marine':

                        #if v != '2' or v != '4110' or v != '1302' or v != '7753':
                        refinedgroup = 'Marine Mammals'
                        refineddict[v] = refinedgroup
                        continue
                        #else:
                            #refinedgroup = 'Terrestrial Mammals'
                            #refineddict[v] = refinedgroup
                    else:
                        refinedgroup = 'Terrestrial Mammals'


            except:
                if splitgroup[0] == 'Marine':
                    #if v != '2' or v != '4110' or v != '1302' or v != '7753':
                    refinedgroup = 'Marine Mammals'
                    print refinedgroup
                    refineddict[v] = refinedgroup
                    continue
                else:
                    if splitgroup[0] == 'Terrestrial' or splitgroup[0] == 'Wetland':
                        refinedgroup = 'Terrestrial Mammals'
                        refineddict[v] = refinedgroup
                    else:
                        refinedgroup = 'Terrestrial Mammals'
                        refineddict[v] = refinedgroup
            print refinedgroup

        elif splitgroup[count - 1] == 'Invertebratess' or splitgroup[count - 1] == 'Invertebrates' or splitgroup[
                    count - 1] == 'Invertebrate':
            try:
                refinedgroup = refineddict[v]
                if refinedgroup == 'Aquatic Invertebrates':
                    continue
                else:
                    if splitgroup[0] == 'Marine' or splitgroup[0] == 'Aquatic' or splitgroup[0] == 'Freshwater':
                        refinedgroup = 'Aquatic Invertebrates'
                        refineddict[v] = refinedgroup
                    else:
                        refinedgroup = 'Terrestrial Invertebrates'
                        refineddict[v] = refinedgroup

            except:

                if splitgroup[0] == 'Marine' or splitgroup[0] == 'Aquatic' or splitgroup[0] == 'Freshwater':
                    refinedgroup = 'Aquatic Invertebrates'
                    refineddict[v] = refinedgroup
                else:
                    refinedgroup = 'Terrestrial Invertebrates'
                    refineddict[v] = refinedgroup
        elif splitgroup[count - 1] == 'Non-Vascular':
            try:
                refinedgroup = refineddict[v]
                if refinedgroup == 'Aquatic Invertebrates':
                    continue
                else:
                    refinedgroup = 'Aquatic Invertebrates'
                    refineddict[v] = refinedgroup

            except:
                refinedgroup = 'Aquatic Invertebrates'
                refineddict[v] = refinedgroup
        else:
            splitgroup = l.split(" ")
            count = len(splitgroup)
            refinedgroup = splitgroup[count - 1]
            refineddict[v] = refinedgroup

    valuestr = ', '.join(value)
    try:
        refinedgroup = refineddict[v]
    except:
        print "\n ERROR ERROR {0}".format(v)
        refinedgroup = value[0]

    listresult = [v, refinedgroup, valuestr]
    results.append(listresult)

outDF = pd.DataFrame(results)
# print outDF
outfile = 'WoEGroupByEntID_20151118b' + '.csv'
outpath = outlocation + os.sep + outfile
outDF.to_csv(outpath)

print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
