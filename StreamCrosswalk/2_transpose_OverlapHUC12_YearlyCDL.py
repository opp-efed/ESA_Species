import pandas as pd
import os
import datetime
import arcpy

inFolder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\L48\HUC12\YearlyCDL'
outFolder= r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\HUC12\YearlyCDL\HUC12_transposed'
in_dbf_folder = inFolder
years = ['2010', '2011', '2012', '2013', '2014', '2015']
out_cols = ['HUC12',
            'Corn',
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

createdirectory(outFolder)
list_folder = os.listdir(inFolder)
final_df = pd.DataFrame()
# by folder in in location, generates a list of csv(s) then transposes do it is HUC12 by use
for year in years:
    final_df = pd.DataFrame()
    print year
    for folder in list_folder:
        final_huc2 = pd.DataFrame(columns=['HUC12'])
        csvFolder = inFolder + os.sep + folder
        list_csv = os.listdir(csvFolder)
        list_csv = [csv for csv in list_csv if csv.endswith('csv')]
        list_csv = [csv for csv in list_csv if csv.split("_")[1] == year]

        out_folder = outFolder +os.sep + folder
        createdirectory(out_folder)
        HUC2 = folder.replace('NHDPlus', '_')

        # list_csv = [csv for csv in list_csv if not csv.startswith('Albers')]
        for csv in list_csv:
            out_csv = out_folder + os.sep + csv
            print csv
            # check cols from original dbf to make sure the col count of csv matches the len of the original dbf
            dbf = csv.replace('.csv', '')
            gdb = dbf.replace(HUC2, '')
            gdb = gdb + '.gdb'
            in_csv = inFolder + os.sep + folder + os.sep + csv
            in_dbf = in_dbf_folder + os.sep + folder + os.sep + gdb + os.sep + dbf

            in_df = pd.read_csv(in_csv, dtype=object)

            in_df.drop('Unnamed: 0', axis=1, inplace=True)
            in_df.drop('OBJECTID', axis=1, inplace=True)

            in_df = in_df .drop_duplicates()
            list_fields = [f.name for f in arcpy.ListFields(in_dbf)]
            list_fields.remove('OBJECTID')
            cols = in_df.columns.values.tolist()
            if len(list_fields) == len(cols):
                in_df.columns = list_fields
            else:
                print 'Columns do  not match for csv {0} df has {1} and cols list have {2}'.format(csv, len(cols),
                                                                                                   len(list_fields))

            transposed_df = in_df.transpose()
            transposed_df.reset_index(inplace=True)
            transposed_df = transposed_df.ix[1:, ]
            transposed_cols = transposed_df.columns.values.tolist()
            # checks that the col len of the transposed df matches the len of the out_cols list that will be the col header

            transposed_df.columns = out_cols
            transposed_df = transposed_df.drop_duplicates()
            transposed_df.to_csv(out_csv)
            transposed_df.to_csv(out_csv)
            final_huc2 = pd.merge(final_huc2, transposed_df, on='HUC12', how='outer')

        final_df = pd.concat([final_df , final_huc2], axis=0)

    final_df_csv = outFolder + os.sep +'Transposed_AllHUC2_' + year + '.csv'
    final_df.to_csv(final_df_csv)



end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)

print 'Check the duplicate hucs 30701060504 delete duplicate, and  11000060405 sum together, and removed blanks '