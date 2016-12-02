import pandas as pd
import os
in_folder=r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Using_BE_Compfiles\Range\YearlyCDL_results\NonAg_PercentOverlap'

list_folder = os.listdir(in_folder)
final_df = pd.DataFrame()
for folder in list_folder:
    print folder
    list_csv = os.listdir(in_folder+os.sep+folder)
    out_df =pd.DataFrame()
    for csv in list_csv:
        print csv
        csv_use =csv.replace('__','_')
        use = csv_use.split("_")[3]
        out_location = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Using_BE_Compfiles\Range\YearlyCDL_results\Merge_NonAg'
        out_csv = out_location + os.sep + 'Merge_'+ str(folder)+'.csv'
        current_csv = in_folder+os.sep+folder+os.sep+csv
        in_df = pd.read_csv(current_csv)
        out_df= pd.concat([out_df,in_df],axis=0)

    out_col = ['EntityID',	'Group',	'comname'	,'sciname'	,'status_text'	,'Des_CH'	,'acres',use]


    out_df= out_df.reindex(columns=out_col)
    out_df.to_csv(out_csv)


