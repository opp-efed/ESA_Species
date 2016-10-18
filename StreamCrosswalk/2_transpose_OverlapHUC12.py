import pandas as pd
import os
import datetime
import arcpy

inFolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_results'
csvFolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_csv'
outFolder= 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_transposed'
in_dbf_folder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\HUC12_results'


out_cols = ['Corn',
            'Corn/soybeans',
            'Corn/wheat',
            'Corn/grains',
            'Cotton',
            'Cotton/wheat',
            'Cotton/vegetables',
            'Rice',
            'Soybeans',
            'Soybeans/cotton',
            'Soybeans/wheat',
            'Soybeans/grains',
            'Wheat',
            'Wheat/vegetables',
            'Wheat/grains',
            'Vegetables and ground fruit',
            '(ground fruit)',
            'Vegetables/grains',
            'Orchards and grapes',
            'Other trees',
            'Other grains',
            'Other row crops',
            'Other crops',
            'Pasture/hay/forage',
            'Developed - open',
            'Developed - low',
            'Developed - med',
            'Developed - high',
            'Forest',
            'Shrubland',
            'Water',
            'Wetlands - woods',
            'Wetlands - herbaceous',
            'Miscellaneous land'
            ]


def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

list_folder = os.listdir(inFolder)

for folder in list_folder:

    out_folder= outFolder +os.sep + folder

    createdirectory(out_folder)
    HUC2 = folder.replace('NHDPlus', '_')
    list_csv = os.listdir(csvFolder + os.sep + folder)
    list_csv = [csv for csv in list_csv if csv.endswith('csv')]
    list_csv = [csv for csv in list_csv if not csv.startswith('Albers')]
    for csv in list_csv:
        out_csv = out_folder +os.sep+ csv
        if not os.path.exists(out_csv):
            print csv
            dbf = csv.replace('.csv', '')
            gdb = dbf.replace(HUC2, '')
            gdb = gdb + '.gdb'
            in_csv = csvFolder + os.sep + folder + os.sep + csv
            in_dbf = in_dbf_folder + os.sep + folder + os.sep + gdb + os.sep + dbf
            in_df = pd.read_csv(in_csv, dtype=object)
            in_df.drop('Unnamed: 0', axis=1, inplace=True)

            listfields = [f.name for f in arcpy.ListFields(in_dbf)]

            cols = in_df.columns.values.tolist()
            if len(listfields) == len(cols):
                in_df.columns = listfields

            else:
                print 'Columns do  not match for csv {0} df has {1} and cols list have {2}'.format(csv, len(cols), len(listfields))
                print listfields
                print cols

            in_df.drop('OBJECTID', axis=1, inplace=True)

            transposed_df = in_df.transpose()

            transposed_cols = transposed_df.columns.values.tolist()

            if len(transposed_cols) == len (out_cols):
                transposed_df.columns = out_cols
                transposed_df.to_csv(out_csv)

            else:
                transposed_df= transposed_df.ix[:,0:(len(transposed_cols)-2)]
                transposed_cols = transposed_df.columns.values.tolist()
                if len(transposed_cols) == len (out_cols):
                    transposed_df.columns = out_cols
                    transposed_df.to_csv(out_csv)
                else:
                    print 'Columns do  not match csv {0} df has {1} and cols list have {2}'.format(csv, len(transposed_cols), len(out_cols))
                    print transposed_cols
                    print out_cols
                    print transposed_df


        else:
            print 'Already export {0}'.format(out_csv)

