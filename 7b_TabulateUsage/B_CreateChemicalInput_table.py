import pandas as pd
import re
import os
import datetime

# TODO confirm assumption with BEAD that if a commonity is found in the suum using a specific term, any associated
# sub commonitys only found in NASS can be ignored

chemical_name = 'Malathion'
# suum_excel = 'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
#              '\_ExternalDrive\_CurrentSupportingTables\Usage\SUUMs\Malathion SUUM spreadsheet.final.041118.final.xlsx'
# sheet_name = 'Malathion Table 2 data'
#
#
# survey_excel = 'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
#              '\_ExternalDrive\_CurrentSupportingTables\Usage\SUUMs\Malathion SUUM spreadsheet.final.041118.final.xlsx'
# sheet_name = 'Malathion Table 1 data'

suum_table2 = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              '\_ExternalDrive\_CurrentSupportingTables\Usage\Mal_suum_table2.csv'
suum_table1 = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              '\_ExternalDrive\_CurrentSupportingTables\Usage\Mal_suum_table1.csv'

# crop_excel ='C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
#             '\_ExternalDrive\_CurrentSupportingTables\Usage\Malathion_florida acres and survey.xlsx'
# sheet_name_1 ='Malathion Florida'

crop_excel = 'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
             '\_ExternalDrive\_CurrentSupportingTables\Usage\ParentTable_FL.csv'

# crosswalk_excel ='C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
#             '\_ExternalDrive\_CurrentSupportingTables\Usage\ULUT Data Source Summary-crosswalk (draft for review).xlsx'
# sheet_name_2 ='AG Crops (survey status)(+)'

# NOTE any crop terms used in SUUM no found on crosswalk are added to the 'SUUM Crop not in xwalk' columns with
# support information.  Confirm supporting information with bead before going final
crosswalk_excel = 'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
                  '\_ExternalDrive\_CurrentSupportingTables\Usage\crosswalk_melt_2.csv'

col_in_xwalk_chk =['Site', 'Name in CDL', 'Name in NASS', 'CENSUS Crop', 'SUUM Crop not in xwalk']
out_location = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
               r'\_ExternalDrive\_CurrentSupportingTables\Usage\ChemicalInput_tables'


def add_parent_crop(row, col_suum, df_x, col_xwalk, look_up_list):
    v = row[col_suum]
    upper_v = row['Crop Uppercase']
    xwalk_crop = 'no_match'
    for col in look_up_list:

        if v in df_x[col].values.tolist():
            xwalk_crop = df_x.loc[df_x[col] == v, col_xwalk].iloc[0]
            break
        elif upper_v in df_x[col].values.tolist():
            xwalk_crop = df_x.loc[df_x[col] == upper_v, col_xwalk].iloc[0]
            break
    print v, xwalk_crop
    return xwalk_crop


def add_acres(row, col, NASS_col):
    v = row[col]
    if v >= 0:
        return (v)
    else:
        return (row[NASS_col])


def add_source_acres(row, col):
    v = row[col]
    if v >= 0:
        return 'SUUM'
    else:
        return ('NASS')


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

# df = pd.read_excel(suum_excel, sheet_name, header=1 ,encoding='sys.getfilesystemencoding()')
df = pd.read_csv(suum_table2)
# df_survey = pd.read_excel(survey_excel,header=1)
df_survey = pd.read_csv(suum_table1)

df_crop = pd.read_csv(crop_excel)

df_xwalk = pd.read_csv(crosswalk_excel)

# strip charaters from columns = need to be consistent across table
for v in df.columns.values.tolist():
    # removes remove all special character in a string except dot and comma
    df[v] = df[v].map(lambda x: re.sub('[^.,a-zA-Z0-9 \n\.]', '', str(x).encode('utf8', 'replace')))
    df[v] = df[v].map(lambda x: str(x).strip())
    if v == 'State':
        df[v] = df[v].map(lambda x: x.upper())
df['Crop Uppercase'] = df['Crop'].map(lambda x: str(x).upper())  # col header in SUUM must be crop
for v in df_crop.columns.values.tolist():
    # removes remove all special character in a string except dot and comma
    df_crop[v] = df_crop[v].map(lambda x: re.sub('[^.,a-zA-Z0-9 \n\.]', '', str(x)))
    df_crop[v] = df_crop[v].map(lambda x: str(x).strip())
for v in df_xwalk.columns.values.tolist():
    # removes remove all special character in a string except dot and comma
    if v in ['SUUM Crop not in xwalk']:
        df_xwalk[v] = df_xwalk[v].map(lambda x: re.sub('[^.,a-zA-Z0-9 \n\.]', '', str(x)))
        df_xwalk[v] = df_xwalk[v].map(lambda x: str(x).strip())

for v in df_survey.columns.values.tolist():
    # removes remove all special character in a string except dot and comma
    if v in ['Crop']:
        df_survey[v] = df_survey[v].map(lambda x: re.sub('[^.,a-zA-Z0-9 \n\.]', '', str(x)))
        df_survey[v] = df_survey[v].map(lambda x: str(x).strip())

df_crop['Crop Uppercase'] = df_crop['Commodity sub type'].map(lambda x: str(x).upper())
df_crop['Name in CDL'] = df_crop.apply(lambda row: add_parent_crop(row, 'Commodity sub type', df_xwalk, 'Name in CDL',col_in_xwalk_chk),axis=1)
df_crop['GenClass'] = df_crop.apply(lambda row: add_parent_crop(row, 'Commodity sub type', df_xwalk, 'GenClass', col_in_xwalk_chk),axis=1)
df_crop['CompositeClass'] = df_crop.apply(lambda row: add_parent_crop(row, 'Commodity sub type', df_xwalk, 'CompositeClass', col_in_xwalk_chk), axis=1)
df_crop['Name in CDL'] = df_crop.apply(lambda row: add_parent_crop(row, 'Commodity', df_xwalk, 'Name in CDL',col_in_xwalk_chk),axis=1)
df_crop['GenClass'] = df_crop.apply(lambda row: add_parent_crop(row, 'Commodity', df_xwalk, 'GenClass', col_in_xwalk_chk),axis=1)
df_crop['CompositeClass'] = df_crop.apply(lambda row: add_parent_crop(row, 'Commodity', df_xwalk, 'CompositeClass', col_in_xwalk_chk), axis=1)


df_survey['Crop Uppercase'] = df_survey['Crop'].map(lambda x: str(x).upper())  # col header in SUUM must be crop
df_survey['Commodity sub type'] = df_survey.apply (lambda row: add_parent_crop(row, 'Crop', df_xwalk, 'CENSUS Crop', col_in_xwalk_chk ), axis=1)
df_survey['States with Reported Usage'] = df_survey['States with Reported Usage'].map (lambda x: 'Not Surveyed' if x == '**' else x)
df_survey['States with Reported Usage'] = df_survey['States with Reported Usage'].map (lambda x: 'No Usage National' if x == '*' else x)
df_survey.to_csv(out_location + os.sep + chemical_name + "_survey_info_joined" + date + '.csv')

# TODO look up one and return all 4 cols that will be split into new columns
df['Commodity sub type'] = df.apply (lambda row: add_parent_crop(row, 'Crop', df_xwalk, 'CENSUS Crop', col_in_xwalk_chk),axis=1)


df.to_csv(out_location + os.sep + chemical_name + "_InputTables_w_SUUM_INFO_" + date + '.csv')

out_df = pd.merge(df_crop, df, left_on=['State', 'Commodity sub type'], right_on=['State', 'Commodity sub type'],
                  how='left')
out_df['Acres'] = out_df.apply(lambda row: add_acres(row, 'Avg. Annual Crop Acres Grown ', 'Value'), axis=1)
out_df['Acres Source'] = out_df.apply(lambda row: add_source_acres(row, 'Avg. Annual Crop Acres Grown ', ), axis=1)
out_df.drop('Avg. Annual Crop Acres Grown ', axis=1, inplace=True)
out_df.drop('Value', axis=1, inplace=True)

out_df = pd.merge(out_df, df_survey, on='Commodity sub type', how='left').copy()
out_df['States with Reported Usage'].fillna('Not Registered', inplace=True)
out_df['Crop Uppercase'] = out_df['Crop Uppercase_x']

temp_df = out_df.reindex(columns=['State', 'Commodity', 'Commodity sub type', 'Data Item', 'Notes', 'Crop Group_x',
                                  'Crop_x', 'Data Source_x', 'Avg. Annual Total Lbs. AI Applied a', 'Min. Annual PCT',
                                  'Max. Annual PCT', 'Avg. Annual PCT', 'Crop Uppercase_x', 'Acres',
                                  'Acres Source', 'Crop Group_y', 'Crop_y', 'Data Source_y',
                                  'States with Reported Usage',
                                  'Avg. Annual Pounds AI Applied a', 'Avg. Annual Total Acres Treated b',
                                  '% Applied by Air', 'Avg. Single AI Rate', 'Max Single Labeled Rate lb/a d',
                                  'Name in CDL', 'GenClass', 'CompositeClass', ])

temp_df.to_csv(out_location + os.sep + chemical_name + "_InputTables_w_SUUM_INFO_" + date + '.csv')

out_df_drop = out_df.drop_duplicates(subset=['Commodity', 'Acres', 'Acres Source']).copy()
print out_df_drop.columns.values.tolist()
suum_row = out_df_drop.loc[out_df_drop['Acres Source'] == 'SUUM'].copy()
suum_commodity =suum_row['Commodity'].values.tolist()
out_df_drop  = out_df_drop.loc[(out_df_drop['Acres Source'] == 'SUUM') |((out_df_drop['Acres Source'] == 'NASS') & (~out_df_drop['Commodity'].isin(suum_commodity)))]


out_df_drop ['GenClass'] = out_df_drop.apply(lambda row: add_parent_crop(row, 'Commodity sub type', df_xwalk, 'GenClass', col_in_xwalk_chk),axis=1)
out_df_drop ['CompositeClass'] = out_df_drop.apply(lambda row: add_parent_crop(row, 'Commodity sub type', df_xwalk, 'CompositeClass', col_in_xwalk_chk), axis=1)
#TODO drop the sub type commodity for crops accoutned for in the SUUM


out_df = out_df.reindex(columns=['State', 'Commodity', 'Commodity sub type', 'Acres',
                                 u'Avg. Annual Total Lbs. AI Applied a', u'Min. Annual PCT', u'Max. Annual PCT',
                                 u'Avg. Annual PCT', 'Name in CDL', 'GenClass', 'CompositeClass', 'Acres Source',
                                 'States with Reported Usage'])

out_df_drop = out_df_drop.reindex(columns=['State', 'Commodity', 'Commodity sub type', 'Acres',
                                           u'Avg. Annual Total Lbs. AI Applied a', u'Min. Annual PCT',
                                           u'Max. Annual PCT',
                                           u'Avg. Annual PCT', 'Name in CDL', 'GenClass', 'CompositeClass',
                                           'Acres Source', 'States with Reported Usage'])
out_df.to_csv(out_location + os.sep + chemical_name + "_InputTables_" + date + '.csv')
out_df_drop.to_csv(out_location + os.sep + chemical_name + "_InputTablesDrop_" + date + '.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)

# parent_df.columns = ['State', 'Parent Crop', 'Parent sub type', 'Value']
#
# parent_crops = list(set(parent_df['Parent sub type'].values.tolist()))
# plus_crops = list(set(df_xwalk['Name in PLUS'].values.tolist()))
# ir4_crops = list(set(df_xwalk['Site'].values.tolist()))
# pur_crops = list(set(df_xwalk['Name in PUR'].values.tolist()))
# cdl_crops = list(set(df_xwalk['Name in CDL'].values.tolist()))
# suum_crops = list(set(df_xwalk['SUUM Crop not in xwalk'].values.tolist()))
#
# # census_crops = df_xwalk['Name in Census'].values.tolist()
# census_crops = df_xwalk['CENSUS Crop'].values.tolist()
#
# parent_in_sum = df.loc[df['Crop'].isin(parent_crops)].copy()
# plus_in_sum = df.loc[df['Crop'].isin(plus_crops)].copy()
# pur_in_sum = df.loc[df['Crop'].isin(pur_crops)].copy()
# ir4_in_sum = df.loc[df['Crop'].isin(ir4_crops)].copy()
# cdl_in_sum = df.loc[df['Crop'].isin(cdl_crops)].copy()
# census_in_sum = df.loc[df['Crop Uppercase'].isin(census_crops)].copy()
# suum_in_sum = df.loc[df['Crop'].isin(suum_crops)].copy()
#
# parent_in_table1 = df_survey.loc[df_survey['Crop'].isin(parent_crops)].copy()
# plus_in_table1 = df_survey.loc[df_survey['Crop'].isin(plus_crops)].copy()
# pur_in_table1 = df_survey.loc[df_survey['Crop'].isin(pur_crops)].copy()
# ir4_in_table1 = df_survey.loc[df_survey['Crop'].isin(ir4_crops)].copy()
# cdl_in_table1 = df_survey.loc[df_survey['Crop'].isin(cdl_crops)].copy()
# census_in_table1 = df_survey.loc[df_survey['Crop Uppercase'].isin(census_crops)].copy()
# suum_in_table1 = df_survey.loc[df_survey['Crop'].isin(suum_crops)].copy()

# merge_ir4 = df_xwalk[['Site', 'CENSUS Crop', 'Name in CDL', 'GenClass', 'CompositeClass']].copy()
# merge_cdl = df_xwalk[['Name in CDL', 'CENSUS Crop', 'GenClass', 'CompositeClass']].copy()
# merge_census = df_xwalk[['CENSUS Crop', 'Name in CDL', 'GenClass', 'CompositeClass']].copy()
# merge_suum = df_xwalk[['SUUM Crop not in xwalk', 'CENSUS Crop', 'Name in CDL', 'GenClass', 'CompositeClass']].copy()
#
# ir4_in_sum = pd.merge(ir4_in_sum, merge_ir4, left_on=['Crop'], right_on=['Site'], how='left',
#                       suffixes=('_ir4', '_ir4_xwalk'))
# cdl_in_sum = pd.merge(cdl_in_sum, merge_cdl, left_on=['Crop'], right_on=['Name in CDL'], how='left',
#                       suffixes=('_cdl', '_cdl_xwalk'))
# census_in_sum = pd.merge(census_in_sum, merge_census, left_on=['Crop Uppercase'], right_on=['CENSUS Crop'],
#                          how='left', suffixes=('_census', '_census_xwalk'))
# suum_in_sum = pd.merge(suum_in_sum, merge_suum, left_on=['Crop'], right_on=['SUUM Crop not in xwalk'],
#                        how='left', suffixes=('_suum', '_suum_xwalk'))
#
# w_parent_df = pd.merge(parent_df, ir4_in_sum, left_on=['State', 'Parent sub type'], right_on=['State', 'CENSUS Crop'],
#                        how='left', suffixes=('', '_ir4'))
#
# w_parent_df = pd.merge(w_parent_df, cdl_in_sum, left_on=['State', 'Parent sub type'], right_on=['State', 'CENSUS Crop'],
#                        how='left', suffixes=('', '_cdl'))
#
# w_parent_df = pd.merge(w_parent_df, census_in_sum, left_on=['State', 'Parent sub type'],
#                        right_on=['State', 'CENSUS Crop'],
#                        how='left', suffixes=('', '_census'))
#
# w_parent_df = pd.merge(w_parent_df, suum_in_sum, left_on=['State', 'Parent sub type'],
#                        right_on=['State', 'CENSUS Crop'],
#                        how='left', suffixes=('', '_suum_no_match'))
#
# w_parent_df.to_csv(out_location + os.sep + chemical_name + "_" + date + '.csv')
#
# # suffixes in working tables with repeated col header prefix, occurs when merging to each crop data set
# suffixes = ['', '_cdl','_suum_no_match']
# # col headers from NASS used as parents
# parent_cols = ['State', 'Parent Crop', 'Parent sub type', 'Value']
# # col headers used as keys to join to SUUM excludes
# crop_keys = ['Crop Uppercase', 'Site', 'CENSUS Crop', 'SUUM Crop not in xwalk']
# # 'name in CDL because we want that col in the final table
# parent_table = w_parent_df[parent_cols]
#
# out_cols = []
# for t in w_parent_df.columns.values.tolist():
#     y = t.split("_")[0]
#     if y not in out_cols and y not in parent_cols and y not in crop_keys:
#         out_cols.append(y)
#
# final_cols = parent_cols + out_cols
# out_df = pd.DataFrame(columns=final_cols)
#
# for y in suffixes:
#     c_cols = [v + y for v in out_cols]
#     filter_cols = parent_cols + c_cols
#     filter_df = w_parent_df[filter_cols].copy()
#     filter_df.columns = final_cols
#     out_df = pd.concat([out_df, filter_df])
# out_df.drop_duplicates(inplace=True)
