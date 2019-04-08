import pandas as pd


range_df = pd.read_excel(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive'
                         r'\Projects\Risk Assessments\GMOs\dicamba\Overlap_30_42_60meters.xlsx')

# move sheet to the first position
ch_df = pd.read_excel(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive'
                         r'\Projects\Risk Assessments\GMOs\dicamba\Overlap_30_42_60meters.xlsx')

on_off_df = pd.read_excel(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive'
                          r'\Projects\ESA\_ExternalDrive\_CurrentSupportingTables\Species Collection_cmr_jc.xlsx')
cnty = pd.read_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects'
                   r'\Risk Assessments\GMOs\dicamba\cnty_lookup.csv')
ch_pce = pd.read_excel(r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects"
                       r"\Risk Assessments\GMOs\dicamba\Copy of CH_PCE Table_CHD Updates_JC.xlsx")

range_df = pd.merge(range_df,on_off_df,how='left', on ='EntityID')
range_df = pd.merge(range_df,cnty,how='left', on ='GEOID')

range_df_0 = range_df.loc[(range_df['CONUS_Soybeans_0']>=1)|(range_df['CONUS_Cotton_0']>=1)]
range_df_30 = range_df.loc[(range_df['CONUS_Soybeans_30']>=1)|(range_df['CONUS_Cotton_30']>=1)]
range_df_60 = range_df.loc[(range_df['CONUS_Soybeans_60']>=1)|(range_df['CONUS_Cotton_60']>=1)]
range_df_90 = range_df.loc[(range_df['CONUS_Soybeans_90']>=1)|(range_df['CONUS_Cotton_90']>=1)]
range_df_42 = range_df.loc[(range_df['CONUS_Soybeans_42']>=1)|(range_df['CONUS_Cotton_42']>=1)]

range_df_0 = range_df.loc[(range_df['CONUS_Soybeans_0']>=1)|(range_df['CONUS_Cotton_0']>=1)]
range_df_30 = range_df.loc[(range_df['CONUS_Soybeans_30']>=1)|(range_df['CONUS_Cotton_30']>=1)]
range_df_60 = range_df.loc[(range_df['CONUS_Soybeans_60']>=1)|(range_df['CONUS_Cotton_60']>=1)]
range_df_90 = range_df.loc[(range_df['CONUS_Soybeans_90']>=1)|(range_df['CONUS_Cotton_90']>=1)]
range_df_42 = range_df.loc[(range_df['CONUS_Soybeans_42']>=1)|(range_df['CONUS_Cotton_42']>=1)]

range_df_0_cnty= range_df_0[[u'EntityID', u'Common Name', u'Scientific Name', u'On/Off field',   u'NAME',  u'STUSPS_y']]
range_df_30_cnty= range_df_30[[u'EntityID', u'Common Name', u'Scientific Name', u'On/Off field',   u'NAME',  u'STUSPS_y']]
range_df_42_cnty= range_df_42[[u'EntityID', u'Common Name', u'Scientific Name', u'On/Off field',   u'NAME',  u'STUSPS_y']]
range_df_60_cnty= range_df_60[[u'EntityID', u'Common Name', u'Scientific Name', u'On/Off field',   u'NAME',  u'STUSPS_y']]
range_df_90_cnty= range_df_90[[u'EntityID', u'Common Name', u'Scientific Name', u'On/Off field',   u'NAME',  u'STUSPS_y']]

range_df_0_cnty["CountyState"] = range_df_0_cnty["NAME"].map(str) +', '+range_df_0_cnty["STUSPS_y"].map(str)
range_df_0_cnty["Species"] = range_df_0_cnty["Common Name"].map(str) + " ("+range_df_0_cnty["Scientific Name"]+")"
range_df_30_cnty["CountyState"] = range_df_30_cnty["NAME"].map(str) +', '+range_df_30_cnty["STUSPS_y"].map(str)
range_df_30_cnty["Species"] = range_df_30_cnty["Common Name"].map(str) + " ("+range_df_30_cnty["Scientific Name"]+")"
range_df_42_cnty["CountyState"] = range_df_42_cnty["NAME"].map(str) +', '+range_df_42_cnty["STUSPS_y"].map(str)
range_df_42_cnty["Species"] = range_df_42_cnty["Common Name"].map(str) + " ("+range_df_42_cnty["Scientific Name"]+")"
range_df_60_cnty["CountyState"] = range_df_60_cnty["NAME"].map(str) +', '+range_df_60_cnty["STUSPS_y"].map(str)
range_df_60_cnty["Species"] = range_df_60_cnty["Common Name"].map(str) + " ("+range_df_60_cnty["Scientific Name"]+")"
range_df_90_cnty["CountyState"] = range_df_90_cnty["NAME"].map(str) +', '+range_df_90_cnty["STUSPS_y"].map(str)
range_df_90_cnty["Species"] = range_df_90_cnty["Common Name"].map(str) + " ("+range_df_90_cnty["Scientific Name"]+")"

table_30 = range_df_30_cnty.groupby([u'EntityID', 'Species', u'On/Off field' ])['CountyState'].apply(list)
table_0 = range_df_0_cnty.groupby([u'EntityID', 'Species', u'On/Off field' ])['CountyState'].apply(list)
table_60 = range_df_60_cnty.groupby([u'EntityID', 'Species', u'On/Off field' ])['CountyState'].apply(list)
table_90 = range_df_90_cnty.groupby([u'EntityID', 'Species', u'On/Off field' ])['CountyState'].apply(list)
table_42 = range_df_42_cnty.groupby([u'EntityID', 'Species', u'On/Off field' ])['CountyState'].apply(list)

table_0.to_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\pivots\0.csv', encoding ='utf-8')
table_30.to_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\pivots\30.csv', encoding ='utf-8')
table_60.to_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\pivots\60.csv', encoding ='utf-8')
table_90.to_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\pivots\90.csv', encoding ='utf-8')
table_42.to_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\pivots\42.csv', encoding ='utf-8')


ch_df = pd.merge(ch_df,on_off_df,how='left', on ='EntityID')
ch_df = pd.merge(ch_df,cnty,how='left', on ='GEOID')

ch_df_0 = ch_df.loc[(ch_df['CONUS_Soybeans_0']>=1)|(ch_df['CONUS_Cotton_0']>=1)]
ch_df_30 = ch_df.loc[(ch_df['CONUS_Soybeans_30']>=1)|(ch_df['CONUS_Cotton_30']>=1)]
ch_df_60 = ch_df.loc[(ch_df['CONUS_Soybeans_60']>=1)|(ch_df['CONUS_Cotton_60']>=1)]
ch_df_90 = ch_df.loc[(ch_df['CONUS_Soybeans_90']>=1)|(ch_df['CONUS_Cotton_90']>=1)]
ch_df_42 = ch_df.loc[(ch_df['CONUS_Soybeans_42']>=1)|(ch_df['CONUS_Cotton_42']>=1)]


ch_df_0_cnty= ch_df_0[[u'EntityID', u'Common Name', u'Scientific Name', u'On/Off field',   u'NAME',  u'STUSPS_y']]
ch_df_30_cnty= ch_df_30[[u'EntityID', u'Common Name', u'Scientific Name', u'On/Off field',   u'NAME',  u'STUSPS_y']]
ch_df_42_cnty= ch_df_42[[u'EntityID', u'Common Name', u'Scientific Name', u'On/Off field',   u'NAME',  u'STUSPS_y']]
ch_df_60_cnty= ch_df_60[[u'EntityID', u'Common Name', u'Scientific Name', u'On/Off field',   u'NAME',  u'STUSPS_y']]
ch_df_90_cnty= ch_df_90[[u'EntityID', u'Common Name', u'Scientific Name', u'On/Off field',   u'NAME',  u'STUSPS_y']]

ch_df_0_cnty["CountyState"] = ch_df_0_cnty["NAME"].map(str) +', '+ch_df_0_cnty["STUSPS_y"].map(str)
ch_df_0_cnty["Species"] = ch_df_0_cnty["Common Name"].map(str) + " ("+ch_df_0_cnty["Scientific Name"]+")"
ch_df_30_cnty["CountyState"] = ch_df_30_cnty["NAME"].map(str) +', '+ch_df_30_cnty["STUSPS_y"].map(str)
ch_df_30_cnty["Species"] = ch_df_30_cnty["Common Name"].map(str) + " ("+ch_df_30_cnty["Scientific Name"]+")"
ch_df_42_cnty["CountyState"] = ch_df_42_cnty["NAME"].map(str) +', '+ch_df_42_cnty["STUSPS_y"].map(str)
ch_df_42_cnty["Species"] = ch_df_42_cnty["Common Name"].map(str) + " ("+ch_df_42_cnty["Scientific Name"]+")"
ch_df_60_cnty["CountyState"] = ch_df_60_cnty["NAME"].map(str) +', '+ch_df_60_cnty["STUSPS_y"].map(str)
ch_df_60_cnty["Species"] = ch_df_60_cnty["Common Name"].map(str) + " ("+ch_df_60_cnty["Scientific Name"]+")"
ch_df_90_cnty["CountyState"] = ch_df_90_cnty["NAME"].map(str) +', '+ch_df_90_cnty["STUSPS_y"].map(str)
ch_df_90_cnty["Species"] = ch_df_90_cnty["Common Name"].map(str) + " ("+ch_df_90_cnty["Scientific Name"]+")"

ch_df_0_cnty= pd.merge(ch_df_0_cnty,ch_pce,how='left', left_on ='EntityID', right_on = 'Entity ID')
ch_df_30_cnty= pd.merge(ch_df_30_cnty,ch_pce,how='left', left_on ='EntityID', right_on = 'Entity ID')
ch_df_60_cnty= pd.merge(ch_df_60_cnty,ch_pce,how='left', left_on ='EntityID', right_on = 'Entity ID')
ch_df_90_cnty= pd.merge(ch_df_90_cnty,ch_pce,how='left', left_on ='EntityID', right_on = 'Entity ID')
ch_df_42_cnty= pd.merge(ch_df_42_cnty,ch_pce,how='left',left_on ='EntityID', right_on = 'Entity ID')

table_30 = ch_df_30_cnty.groupby([u'EntityID', 'Species', u'Notes on Relevant Primary Constituent Elements (PCE)', u'Non-monocot plant?', u'PCEs include Ag fields?', u'PCE\u2019s include Non-monocot plants', u'Mitigation required for \u201cno modification\u201d'])['CountyState'].apply(list)
table_0 = ch_df_0_cnty.groupby([u'EntityID', 'Species',  u'Notes on Relevant Primary Constituent Elements (PCE)', u'Non-monocot plant?', u'PCEs include Ag fields?', u'PCE\u2019s include Non-monocot plants', u'Mitigation required for \u201cno modification\u201d'])['CountyState'].apply(list)
table_60 = ch_df_60_cnty.groupby([u'EntityID', 'Species',  u'Notes on Relevant Primary Constituent Elements (PCE)', u'Non-monocot plant?', u'PCEs include Ag fields?', u'PCE\u2019s include Non-monocot plants', u'Mitigation required for \u201cno modification\u201d'])['CountyState'].apply(list)
table_90 = ch_df_90_cnty.groupby([u'EntityID', 'Species',  u'Notes on Relevant Primary Constituent Elements (PCE)', u'Non-monocot plant?', u'PCEs include Ag fields?', u'PCE\u2019s include Non-monocot plants', u'Mitigation required for \u201cno modification\u201d'])['CountyState'].apply(list)
table_42 = ch_df_42_cnty.groupby([u'EntityID', 'Species',  u'Notes on Relevant Primary Constituent Elements (PCE)', u'Non-monocot plant?', u'PCEs include Ag fields?', u'PCE\u2019s include Non-monocot plants', u'Mitigation required for \u201cno modification\u201d'])['CountyState'].apply(list)

table_0.to_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\pivots\ch_0.csv', encoding ='utf-8')
table_30.to_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\pivots\ch_30.csv', encoding ='utf-8')
table_60.to_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\pivots\ch_60.csv', encoding ='utf-8')
table_90.to_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\pivots\ch_90.csv', encoding ='utf-8')
table_42.to_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\pivots\ch_42.csv', encoding ='utf-8')
