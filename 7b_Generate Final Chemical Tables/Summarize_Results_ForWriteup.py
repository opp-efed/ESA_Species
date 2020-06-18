import pandas as pd
import os

in_table = r"C:\Users\JConno02\Environmental Protection Agency (EPA)" \
           r"\Endangered Species Pilot Assessments - OverlapTables\ChemicalTables\Methomyl\Summarized Tables" \
           r"\2_PCT\max\Upper\R_PCT_GIS_Step2_Methomyl.csv"
out_name = 'R_metho_Mean_PCTRedOnOff.csv'
out_path = r'E:\Workspace\StreamLine\ESA\Tabulated_UsageHUCABHabitat\result_summary\Methomyl'

def mean_std(df):
    mean = df.mean(axis = 0, skipna = True)
    std = df.std(axis = 0, skipna = True)
    out_df = pd.DataFrame(data=[mean,std]).T.reset_index()
    out_df.columns = ['Uses_Distance','Mean', 'STD']
    uses = out_df['Uses_Distance'].values.tolist()
    use = [v for v in uses if v.startswith('CONUS') or v.startswith("NL48")]
    out_df = out_df.loc[out_df['Uses_Distance'].isin(use)]
    out_df['Use'] = out_df['Uses_Distance'].map(lambda r: r.split('_')[1]).astype(str)
    out_df['Distance'] = out_df['Uses_Distance'].map(lambda r: r.split('_')[2]).astype(str)
    out_df = out_df.sort_values('Distance')
    return out_df

in_df = pd.read_csv(in_table)
conus_cols = [v for v in in_df.columns.values.tolist() if v.startswith("CONUS")]
nl48_cols = [v for v in in_df.columns.values.tolist() if v.startswith("NL48")]
conus_df = in_df.loc[in_df['L48/NL48'].isin(['CONUS','Both'])]
nl48_df = in_df.loc[in_df['L48/NL48'].isin(['NL48','Both'])]
conus_df = conus_df.loc[:,conus_cols]
nl48_df= nl48_df.loc[:,nl48_cols]

conus_out = mean_std(conus_df)
nl48_out =mean_std(nl48_df)
conus_out.to_csv(out_path +os.sep+'CONUS_'+out_name)
nl48_out.to_csv(out_path +os.sep+'NL48_'+out_name)

