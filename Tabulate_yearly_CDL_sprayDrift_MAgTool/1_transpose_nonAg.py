import pandas as pd
import os
import datetime
import arcpy


csvFolder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\L48\Agg_layers\Ag\CriticalHabitat'
outFolder= 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\Agg_layers\Ag\CriticalHabitat\Mag_Spray\Transposed_Spray'
out_folders =  os.listdir(csvFolder)


def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()


for folder in out_folders:
    current_folder = csvFolder+os.sep+folder


    out_folder= outFolder +os.sep + folder

    createdirectory(out_folder)

    list_csv = os.listdir(current_folder)
    list_csv = [csv for csv in list_csv if csv.endswith('csv')]
    print list_csv


    for csv in list_csv:
        csv_use= csv.replace('__','_')
        use= csv_use.split("_")[3]
        print use
        out_csv = out_folder +os.sep+ csv
        #if not os.path.exists(out_csv):


        in_csv =current_folder +os.sep + csv
        in_df = pd.read_csv(in_csv, dtype=object)

        in_df.drop('OID', axis=1, inplace=True)
        in_df.drop('TableID', axis=1, inplace=True)

        cols = in_df.columns.values.tolist()

        transposed_df = in_df.transpose()
        transposed_df_col= transposed_df.ix[0,:].values.tolist()

        transposed_df= transposed_df.ix[1:]
        use_transposed_df= transposed_df.iloc[:,0]

        use_transposed_df = use_transposed_df.reset_index()
        use_transposed_df.columns = ['ZoneID', str(use)]


        use_transposed_df['ZoneID'] = use_transposed_df['ZoneID'].map(lambda x: x.replace('VALUE_', '')).astype(str)
        # print use_transposed_df

        use_transposed_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
