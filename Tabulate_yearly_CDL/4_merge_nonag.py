import pandas as pd
import os
in_folder=r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\Agg_layers\NonAg\Range\Mag_RawUse\PercentOverlap'
out_location = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\Agg_layers\NonAg\Range\Merge_NonAg'

def createdirectory(DBF_dir):
    if not os.path.exists(DBF_dir):
        os.mkdir(DBF_dir)
        print "created directory {0}".format(DBF_dir)

createdirectory(out_location)
list_folder = os.listdir(in_folder)
final_df = pd.DataFrame(columns=['EntityID'])
for folder in list_folder:
    print folder
    list_csv = os.listdir(in_folder+os.sep+folder)
    out_df =pd.DataFrame()
    for csv in list_csv:
        print csv
        csv_use =csv.replace('__','_')
        use = csv_use.split("_")[3]

        out_csv = out_location + os.sep + 'Merge_'+ str(folder)+'.csv'
        current_csv = in_folder+os.sep+folder+os.sep+csv
        in_df = pd.read_csv(current_csv)
        use = csv.split('_')[3]
        in_df = in_df.ix[:,['EntityID',use]]
        print in_df

        out_df= pd.concat([out_df,in_df],axis=0)

    out_col = ['EntityID',use]


    out_df= out_df.reindex(columns=out_col)
    out_df.to_csv(out_csv)
    final_df = pd.merge(final_df, out_df, on='EntityID', how='outer')
final_df.to_csv(out_location+os.sep+'NonAg_Merged.csv')

