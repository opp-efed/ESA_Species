import arcpy
from simpledbf import Dbf5
import datetime
import os
import pandas as pd

inFolder = 'J:\Workspace\ESA_Species\Step3\ZonalHis_GAP\Insect_csv'
outfolder = 'J:\Workspace\ESA_Species\Step3\ZonalHis_GAP\Insects_GAP'
Gapcsv = 'J:\Workspace\ESA_Species\Step3\ZonalHis_GAP\ESLU_Description_joinedNational.csv'
outpath =''
def CheckForChanges(dictlist,cols,allent_df):
    for v in cols:
        start =datetime.datetime.now()
        if v== 'entity_id':
            continue
        else:
            print "Checking for changes in {0}".format(v)
            current=[]
            for dictionay in dictlist:
                if dictionay .startswith(v):
                    current.append(dictionay)
            if len(current)!= 2:
                print "Only one dictionary can't compare {0}".format(current)
            else:

                rowcount = allent_df.count(axis=0, level=None, numeric_only=False)
                rowindex = rowcount.values[0]
                row =0
                ent_df = pd.DataFrame(columns=['EntityID','Old Value','New Value','Change'])
                while row < (rowindex):
                    entid = str(allent_df.iloc[row,0])
                    for value in current:
                        if value.endswith('new'):
                            try:
                                new= ((globals()[value][entid]))

                            except KeyError:
                                new= 'Removed'
                        else:
                            try:
                                old= ((globals()[value][entid]))

                            except KeyError:
                                old= 'Newly Added'
                    row +=1
                    if old == new:
                        change = 'No'
                    else:
                        change= 'Yes'
                    #result =pd.DataFrame([entid,old,new,change])
                    #ent_df =ent_df.append(result)
                    ent_df =ent_df.append({'EntityID':entid,'Old Value':old,'New Value':new,'Change':change},ignore_index=True)


                ent_df = ent_df[ent_df['Change'].isin(['Yes'])]
                outcsv = outpath + os.sep + 'Changes_' + str(v) + '.csv'
                ent_df.to_csv(outcsv)
                print "Exported {0} completed in {1}".format(outcsv, (datetime.datetime.now()-start))

counter = 0

files = os.listdir(inFolder)
print files
for f in files:
    incsv = inFolder+os.sep+f
    GAP_df =pd.read_csv(Gapcsv)
    GAP_df["SuitHab Yes/No"] = ""
    df= pd.read_csv(incsv)
    #result=pd.merge(df, GAP_df, on='Value')
    result =pd.concat([df, GAP_df], axis=1)
    #result = pd.merge(df, GAP_df, left_on='Value', right_on='Value')
    print result
    outcsv = outfolder +os.sep + f
    result.to_csv(outcsv)





