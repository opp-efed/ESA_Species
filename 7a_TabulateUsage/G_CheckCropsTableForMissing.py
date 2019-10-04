import pandas as pd

# out table from step e
df = pd.read_csv(r"path\tableS.csv")
df_drop = df.dropna(subset=['GenClass'])

dict_gen = pd.Series(df_drop['GenClass'].values, index=df_drop['CONCAT USE SITE']).to_dict()
dict_ag = pd.Series(df_drop['AgClass'].values, index=df_drop['CONCAT USE SITE']).to_dict()
dict_crop = pd.Series(df_drop['Crop'].values, index=df_drop['CONCAT USE SITE']).to_dict()
dict_states = pd.Series(df_drop['States with Reported Usage'].values, index=df_drop['CONCAT USE SITE']).to_dict()
dict_state_final = pd.Series(df_drop['States with Reported Usage_Final'].values,
                             index=df_drop['CONCAT USE SITE']).to_dict()
dict_notes = pd.Series(df_drop['Notes'].values, index=df_drop['CONCAT USE SITE']).to_dict()
dict_state_noncrop = pd.Series(df_drop['Non-Crop Regional Treated/Total Acres Region'].values,
                               index=df_drop['CONCAT USE SITE']).to_dict()

null_values = df[df['GenClass'].isnull()]

for index, row in null_values.iterrows():
    look_state = null_values.ix[index, ['State']].iloc[0]
    look_up = null_values.ix[index, ['CONCAT USE SITE']].iloc[0]
    genclass = dict_gen[look_up]
    ag = dict_ag[look_up]
    crop = dict_crop[look_up]
    state = dict_states[look_up]
    st_final = dict_state_final[look_up]
    notes = dict_notes[look_up]
    noncrop = dict_state_noncrop[look_up]
    # col headers
    df.loc[(df["CONCAT USE SITE"] == look_up) & (df['State'] == look_state), ['GenClass', 'AgClass',  'Crop','States with Reported Usage','States with Reported Usage_Final', 'Notes','Non-Crop Regional Treated/Total Acres Region']] = genclass, ag,crop,state,st_final,notes,noncrop
# output location
df.to_csv(r"out path\file name.csv")