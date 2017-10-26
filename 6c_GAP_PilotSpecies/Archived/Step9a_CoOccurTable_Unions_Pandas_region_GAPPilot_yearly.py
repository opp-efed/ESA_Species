import pandas as pd
import os
import datetime
import arcpy


csvFolder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\L48\PilotGAP\YearlyCDL\CONUS'
out_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\PilotGAP species\YearlyCDL\SumSpecies'
years= ['2010','2011','2012','2013','2014','2015']
cdl_recodes = ['LABEL','10', '14', '15', '18', '20', '25', '26', '30', '40', '42', '45', '48', '50', '56', '58', '60', '61',
            '68', '70', '75', '80', '90', '100', '110', '121', '122', '123', '124', '140', '160', '180',
            '190', '195', '200']

out_cols = ['EntityID',
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

list_csv = os.listdir(csvFolder)

final_df = pd.DataFrame(columns=out_cols)
print len(cdl_recodes)
print len(out_cols)
for year in years:
    list_csv = [csv for csv in list_csv if csv.endswith( year+'_rec.csv')]
    for csv in list_csv:
        print csv
        entid = csv.split("_")[1]

        #if not os.path.exists(out_csv):
        print csv

        in_csv = csvFolder + os.sep + csv
        in_df = pd.read_csv(in_csv, dtype=object)
        print in_df

        in_df.drop('OID', axis=1, inplace=True)
        in_df.drop('TableID', axis=1, inplace=True)

        cols = in_df.columns.values.tolist()

        transposed_df = in_df.transpose()

        transposed_df.reset_index(inplace=True)

        transposed_df_col= transposed_df.ix[0,:].values.tolist()

        transposed_df= transposed_df.ix[1:]
        transposed_df.columns = transposed_df_col

        # this checks for the duplicated col of date for code 200 mis land
        if len(transposed_df_col)> len (cdl_recodes):
            transposed_df = transposed_df.ix[:,0:len(cdl_recodes)]


        # sets the order of the columns so when overwriten by word it is in the correct order
        transposed_df= transposed_df.reindex(columns=cdl_recodes)
        transposed_df.columns= out_cols

        transposed_df.iloc[0,0]= entid
        print transposed_df

        final_df = pd.concat([final_df,transposed_df])
    final_df = final_df.fillna(0)
    final_df.to_csv(out_location+os.sep+ year+'_YearlyOverlap_Sum.csv')
