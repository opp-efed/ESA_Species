import pandas as pd
import os

in_location = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\SAM_2\PwcScenarios'
xwalk  = pd.read_table(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\SAM_2\PwcScenarios\met_crosswalk.txt')
xwalk ['weather_grid (Trip)'] = xwalk ['weather_grid (Trip)'].map(lambda x: str(x).split('.')[0]).astype(str)

csvs = [v for v in os.listdir(in_location) if v.endswith('.csv')]

for v in csvs:
    if v.endswith('meta_.csv'):
        continue
    else:
        df_c = pd.read_csv(in_location + os.sep + v)
        df_c['weather_grid'] = df_c['weather_grid'].map(lambda x: str(x).split('.')[0]).astype(str)
        merge_df = pd.merge(df_c,xwalk, how='left', left_on='weather_grid', right_on= 'weather_grid (Trip)')
        merge_df.to_csv(in_location + os.sep + v.replace('.csv','_wea_statationID.csv'))