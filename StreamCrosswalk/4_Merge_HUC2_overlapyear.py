import pandas as pd
import os
import datetime

inFolder_AG = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_percentOverlap'
inFolder_NonAG ='L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_percentOverlap_NonAG'

outFolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\FinalTables'
years = ['2010', '2011', '2012', '2013', '2014', '2015']


def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)




start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

list_folder_ag = os.listdir(inFolder_AG)
list_folder_nonag = os.listdir(inFolder_NonAG)

final_df = pd.DataFrame()
final_cols = ['HUC12','Acres_prj']
for folder in list_folder_nonag:
    print folder
    huc2_df = pd.DataFrame()
    HUC2 = folder.replace('NHDPlus', '_')
    csvFolder = inFolder_NonAG+os.sep+folder
    list_csv = os.listdir(csvFolder)
    list_csv = [csv for csv in list_csv if csv.endswith('csv')]

    list_csv = [csv for csv in list_csv if csv.startswith('Albers')]
    for csv in list_csv:
        in_csv = csvFolder + os.sep + csv

        in_df = pd.read_csv(in_csv)
        in_df.drop('Unnamed: 0', axis=1,inplace=True)
        cols = huc2_df.columns.values.tolist()
        if 'Acres_prj' not in cols:
            new_cols = in_df.columns.values.tolist()
            [final_cols.append(x) for x in new_cols if x not in final_cols]
            huc2_df = pd.concat([huc2_df, in_df],axis=1)
            huc2_df= huc2_df.fillna(0)
        else:
            in_df.drop('Acres_prj',axis=1,inplace=True)
            new_cols = in_df.columns.values.tolist()
            [final_cols.append(x) for x in new_cols if x not in final_cols]
            huc2_df = pd.merge(huc2_df, in_df, on='HUC12', how='left')
            huc2_df= huc2_df.fillna(0)


    final_df = pd.concat([final_df, huc2_df],axis=0)

print final_cols
final_df = final_df.reindex(columns=final_cols)
final_df = final_df.fillna(0)
final_csv = outFolder+os.sep+'NonAg.csv'
final_df.to_csv(final_csv)

for year in years:
    print year
    final_ag_year = pd.DataFrame()
    for folder in list_folder_ag:
        print folder
        huc2_df = pd.DataFrame()

        csvFolder = inFolder_AG+os.sep+folder
        list_csv = os.listdir(csvFolder)
        list_csv = [csv for csv in list_csv if csv.endswith('csv')]
        list_csv = [csv for csv in list_csv if csv.split("_")[1] ==year]
        for csv in list_csv:
            in_csv = csvFolder + os.sep + csv
            in_df = pd.read_csv(in_csv)
            try:
                in_df.drop('Unnamed: 0', axis=1,inplace=True)
                in_df.drop('Unnamed: 0.1',axis=1,inplace=True)
            except:
                pass
            huc2_df = pd.concat([huc2_df, in_df],axis=1)
            huc2_df= huc2_df.fillna(0)
        try:
            final_ag_year = pd.concat([final_ag_year,huc2_df], axis=0)
        except:

            print final_ag_year.columns.values.tolist()
            print huc2_df.columns.values.tolist()
            break
    outcsv = outFolder +os.sep+ 'CDL_{0}.csv'.format(year)
    final_ag_year.to_csv(outcsv)
    final_year = pd.merge(final_df, final_ag_year, on='HUC12', how='left')
    out_merge =outFolder +os.sep+ 'CDL_{0}_withNonAG.csv'.format(year)
    final_year.to_csv(out_merge)



