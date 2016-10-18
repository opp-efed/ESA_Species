import pandas as pd
import os
import datetime
import arcpy

inFolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_results'
csvFolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_csv'
outFolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_transposed_NonAG'
in_dbf_folder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_results'


def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

list_folder = os.listdir(inFolder)

for folder in list_folder:
    out_folder = outFolder + os.sep + folder
    createdirectory(out_folder)
    HUC2 = folder.replace('NHDPlus', '_')
    list_csv = os.listdir(csvFolder + os.sep + folder)
    list_csv = [csv for csv in list_csv if csv.endswith('csv')]
    list_csv = [csv for csv in list_csv if csv.startswith('Albers')]
    for csv in list_csv:
        out_csv = out_folder + os.sep + csv

        if not os.path.exists(out_csv):
            split_name = csv.split("_")
            use = split_name[5]

            dbf = csv.replace('.csv', '')
            gdb = dbf.replace(HUC2, '')
            gdb = gdb + '.gdb'
            in_csv = csvFolder + os.sep + folder + os.sep + csv
            in_dbf = in_dbf_folder + os.sep + folder + os.sep + gdb + os.sep + dbf
            in_df = pd.read_csv(in_csv, dtype=object)
            in_df.drop('Unnamed: 0', axis=1, inplace=True)
            if use == 'Cultivated' or use == 'NonCultivated':
                in_df = in_df.ix[1:]

            else:
                in_df = in_df.ix[0:0, ]

            in_df.drop('OBJECTID', axis=1, inplace=True)
            transposed_df = in_df.transpose()
            transposed_df.columns = [use]
            transposed_df.to_csv(out_csv)
        else:
            pass
            #print 'Already export {0}'.format(out_csv)
