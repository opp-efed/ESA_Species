import pandas as pd
import os
import datetime
import arcpy


csvFolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\Indiv_Year_raw\Range\CONUS'
outFolder= 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\Indiv_Year_raw\Range\Transposed_Ag'
out_folders = ['2010','2011','2012','2013','2014','2015']

cdl_recodes = ['10', '14', '15', '18', '20', '25', '26', '30', '40', '42', '45', '48', '50', '56', '58', '60', '61',
            '68', '70', '75', '80', '90', '100', '110', '121', '122', '123', '124', '140', '160', '180',
            '190', '195', '200']

out_cols = ['ZoneID',
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

print len(cdl_recodes)
print len(out_cols)
for folder in out_folders:

    out_folder= outFolder +os.sep + folder

    createdirectory(out_folder)

    list_csv = os.listdir(csvFolder)
    list_csv = [csv for csv in list_csv if csv.endswith( str(folder) +'_rec.csv')]

    for csv in list_csv:
        out_csv = out_folder +os.sep+ csv
        #if not os.path.exists(out_csv):
        print csv

        in_csv = csvFolder + os.sep + csv
        in_df = pd.read_csv(in_csv, dtype=object)

        in_df.drop('OID', axis=1, inplace=True)
        in_df.drop('TableID', axis=1, inplace=True)

        cols = in_df.columns.values.tolist()

        transposed_df = in_df.transpose()
        transposed_df_col= transposed_df.ix[0,:].values.tolist()

        transposed_df= transposed_df.ix[1:]
        transposed_df.columns = transposed_df_col
        print transposed_df

        transposed_df= transposed_df.reindex(columns=cdl_recodes)
        transposed_df = transposed_df.reset_index()
        transposed_df.columns= out_cols

        transposed_df['ZoneID'] = transposed_df['ZoneID'].map(lambda x: x.replace('VALUE_', '')).astype(str)
        print transposed_df

        transposed_cols = transposed_df.columns.values.tolist()
        # transposed_df.columns = out_cols
        transposed_df.to_csv(out_csv)
