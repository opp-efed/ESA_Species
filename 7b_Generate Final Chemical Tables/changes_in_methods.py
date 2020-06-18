import pandas as pd
import os

input_step1 = "C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\Documents\Methomyl_Carbaryl_Summary\GIS_Step1_R_Methomyl.csv"

out_location = r'E:\Workspace\StreamLine\ESA\Tabulated_UsageHUCABHabitat\result_summary'
chemical = 'Methomyl'

aa_column_conus = 'CONUS_Methomyl AA_792'
aa_column_nl48 = 'NL48_Methomyl AA_792'

df = pd.read_csv(input_step1)


df_opps = df.loc[(df[aa_column_conus] == 0) & (df['L48/NL48'] =='CONUS')|(df[aa_column_nl48] == 0) & (df['L48/NL48'] == 'NL48')]
df_publics = df.loc[(df[aa_column_conus] == 0) & (df['L48/NL48'] =='CONUS')
                    |(df[aa_column_nl48] == 0) & (df['L48/NL48'] == 'NL48')
                    |(df[aa_column_nl48] < 0.44) & (df['L48/NL48'] == 'NL48')|
                    (df[aa_column_conus] <0.44) & (df['L48/NL48'] =='CONUS')]



df_opps.to_csv(out_location + os.sep + chemical +"_opps.csv")
