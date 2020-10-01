import pandas as pd
import os
import datetime
# Author J.Connolly
# Internal deliberative, do not cite or distribute

# out table from step e
in_table = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\Glyphosate\GLY_findsurrogate_v1.csv"
census_parent = "C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ED_results\_CurrentSupportingTables\Usage\ParentTable_national_20181217.csv"


today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
df = pd.read_csv(in_table)
df_drop = df.dropna(subset=['AA'])


check_census = pd.read_csv(in_table)
census = pd.read_csv(census_parent)
census_crops = (check_census["CONCAT USE SITE"].values.tolist())
census_filtered = census[census["CONCAT USE SITE"].isin(census_crops)]
state_crops = census_filtered["State_Census"].values.tolist()

check_census["State_Census"] = check_census["State"] + "_"+check_census["CONCAT USE SITE"]
captured_combos = check_census["State_Census"].values.tolist()
df_captured = check_census[check_census["State_Census"].isin(state_crops)]
df_missing = census[(~census["State_Census"].isin(captured_combos)) &(census["CONCAT USE SITE"].isin(census_crops ))]
# output location

df_captured.to_csv(os.path.dirname(in_table) +os.sep +'Census_Captured_'+os.path.basename(in_table))
df_missing.to_csv(os.path.dirname(in_table) +os.sep +'Census_State_Missing_' +os.path.basename(in_table))


# Archived- used to check for missing Non-Ag
# out_table ="propazine_checkmissing_crops_v1"
# dict_gen = pd.Series(df_drop['UDL'].values, index=df_drop['CONCAT USE SITE']).to_dict()
# dict_ag = pd.Series(df_drop['AgClass'].values, index=df_drop['CONCAT USE SITE']).to_dict()
# dict_crop = pd.Series(df_drop['Crop'].values, index=df_drop['CONCAT USE SITE']).to_dict()
# dict_states = pd.Series(df_drop['States with Reported Usage'].values, index=df_drop['CONCAT USE SITE']).to_dict()
# dict_state_final = pd.Series(df_drop['States with Reported Usage'].values,
#                              index=df_drop['CONCAT USE SITE']).to_dict()
# dict_notes = pd.Series(df_drop['Notes'].values, index=df_drop['CONCAT USE SITE']).to_dict()
# dict_state_noncrop = pd.Series(df_drop['Non-Crop Regional Treated/Total Acres Region'].values,
#                                index=df_drop['CONCAT USE SITE']).to_dict()
#
# null_values = df[df['UDL'].isnull()]
#
# for index, row in null_values.iterrows():
#     look_state = null_values.ix[index, ['State']].iloc[0]
#     look_up = null_values.ix[index, ['CONCAT USE SITE']].iloc[0]
#     UDL = dict_gen[look_up]
#     ag = dict_ag[look_up]
#     crop = dict_crop[look_up]
#     state = dict_states[look_up]
#     st_final = dict_state_final[look_up]
#     notes = dict_notes[look_up]
#     noncrop = dict_state_noncrop[look_up]
#     # col headers
#     df.loc[(df["CONCAT USE SITE"] == look_up) & (df['State'] == look_state), ['UDL', 'AgClass',  'Crop','States with Reported Usage', 'Notes','Non-Crop Regional Treated/Total Acres Region']] = UDL, ag,crop,state,st_final,notes,noncrop
# df.to_csv(os.path.dirname(in_table) +os.sep +out_table+"_" +date+'.csv')