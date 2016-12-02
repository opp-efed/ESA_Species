import pandas as pd
import os
in_folder=r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\Indiv_Year_raw\Range\PercentOverlap'
col = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'Corn', 'Corn/soybeans', 'Corn/wheat',
           'Corn/grains', 'Cotton', 'Cotton/wheat', 'Cotton/vegetables', 'Rice', 'Soybeans', 'Soybeans/cotton',
           'Soybeans/wheat', 'Soybeans/grains', 'Wheat', 'Wheat/vegetables', 'Wheat/grains',
           'Vegetables and ground fruit', '(ground fruit)', 'Vegetables/grains', 'Orchards and grapes', 'Other trees',
           'Other grains', 'Other row crops', 'Other crops', 'Pasture/hay/forage', 'Developed - open',
           'Developed - low', 'Developed - med', 'Developed - high', 'Forest', 'Shrubland', 'Water', 'Wetlands - woods',
           'Wetlands - herbaceous', 'Miscellaneous land', 'acres']

list_folder = os.listdir(in_folder)

for folder in list_folder:
    print folder
    list_csv = os.listdir(in_folder+os.sep+folder)
    out_df =pd.DataFrame()
    for csv in list_csv:
        print csv
        out_location = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\Indiv_Year_raw\Range\Merged'
        out_csv = out_location + os.sep + 'Merge_'+ str(folder)+'.csv'
        current_csv = in_folder+os.sep+folder+os.sep+csv
        in_df = pd.read_csv(current_csv, dtype=object)
        out_df= pd.concat([out_df,in_df],axis=0)
    out_df= out_df.reindex(columns=col)
    out_df.to_csv(out_csv)