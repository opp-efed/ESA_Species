import datetime
import os
import arcpy

import pandas as pd

#TODO update to merge two dfs rather than dictionary;
masterlist = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\April2016\addColumnsMaster_June2016' \
             r'\MasterJune2016_20160628.csv'


colIndex_entId = 0

otherlist=r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\April2016\addColumnsMaster_June2016' \
          r'\BinsDraftBE_20161118.csv'
colIndex_entId_other = 2

addcol = 'BinsAssigned' # Column from other list to be added to master

outfile = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\April2016\addColumnsMaster_June2016' \
          r'\MasterJune2016_20160628_bins.csv'

addinfo_dict = {}
def loadData_othertable(incsv,entid_index, value_dict):
    df = pd.read_csv(incsv)
    entidindex = entid_index
    rowcount = df.count(axis=0, level=None, numeric_only=False)
    rowindex = rowcount.values[0]
    row = 0
    while row < (rowindex):
        entid = str(df.iloc[row, entidindex])
        value_dict[entid]= 'TRUE'
        # print "Working on species {0}, row {1}".format(entid, row)
        row += 1

start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)


loadData_othertable(otherlist,colIndex_entId_other,addinfo_dict)
print addinfo_dict
list_entid = addinfo_dict.keys()


Master_df = pd.read_csv(masterlist)
listheader = Master_df.columns.values.tolist()

listheader.append(addcol)

entidindex = colIndex_entId
rowcount = Master_df.count(axis=0, level=None, numeric_only=False)
rowindex = rowcount.values[0]
colindex = len(listheader) - 2  # to make base 0

row = 0
while row < rowindex:
    entid = str(Master_df.iloc[row, entidindex])
    if entid in list_entid:
        add_value= str(addinfo_dict[entid])
        Master_df.loc[row,addcol] = add_value
    else:
        Master_df.loc[row,addcol] = 'FALSE'

    row += 1

Master_df.to_csv(outfile, header=listheader)

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
