import pandas as pd
import re
import os
import datetime

chemical_name = 'Malathion'
suum_excel ='C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
            '\_ExternalDrive\_CurrentSupportingTables\Usage\SUUMs\Malathion SUUM spreadsheet.final.041118.final.xlsx'
sheet_name ='Malathion Table 2 data'

# crop_excel ='C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
#             '\_ExternalDrive\_CurrentSupportingTables\Usage\Malathion_florida acres and survey.xlsx'
# sheet_name_1 ='Malathion Florida'

crop_excel ='C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
            '\_ExternalDrive\_CurrentSupportingTables\Usage\ParentTable_FL.csv'

# crosswalk_excel ='C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
#             '\_ExternalDrive\_CurrentSupportingTables\Usage\ULUT Data Source Summary-crosswalk (draft for review).xlsx'
# sheet_name_2 ='AG Crops (survey status)(+)'

crosswalk_excel ='C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
                 '\_ExternalDrive\_CurrentSupportingTables\Usage\crosswalk_melt_2.csv'


out_location = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
               r'\_ExternalDrive\_CurrentSupportingTables\Usage\ChemicalInput_tables'

def add_parent_crop(row, col, x_df, col_match, col_return):
    v = row[col]
    state = row['State']
    return x_df.loc[x_df[col_match] == v , col_return].iloc[0]

def add_acres(row, col, NASS_col):
    v = row[col]
    if v >=0:
        return (v)
    else:
        return (row[NASS_col])

def add_source_acres(row, col):
    v = row[col]
    if v >=0:
        return 'SUUM'
    else:
        return ('NASS')
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

df = pd.read_excel( suum_excel, sheet_name, header = 1)
# df_crop = pd.read_excel(crop_excel, sheet_name_1, header = 4)
df_crop = pd.read_csv(crop_excel)
# df_xwalk = pd.read_excel(crosswalk_excel, sheet_name_2, header = 6)
df_xwalk = pd.read_csv(crosswalk_excel)

for v in df.columns.values.tolist():
    # removes remove all special character in a string except dot and comma
    df [v] = df[v].map(lambda x: re.sub('[^.,a-zA-Z0-9 \n\.]', '', str(x)))
    df [v] = df[v].map(lambda x: x.strip())
    if v == 'State':
        df [v] = df[v].map(lambda x: x.upper())
df['Crop Uppercase'] = df['Crop'].map(lambda x: x.upper()) # col header in SUUM must be crop
for v in df_crop.columns.values.tolist():
    # removes remove all special character in a string except dot and comma
    df_crop [v] = df_crop[v].map(lambda x: re.sub('[^.,a-zA-Z0-9 \n\.]', '', str(x)))
    df_crop [v] = df_crop[v].map(lambda x: str(x).strip())

parent_df = df_crop.ix[:,['State', 'Commodity','Commodity sub type','Value']]
parent_df.columns = ['State','Parent Crop', 'Parent sub type', 'Value']


parent_crops = parent_df['Parent sub type'].values.tolist()
plus_crops = df_xwalk['Name in PLUS'].values.tolist()
ir4_crops = df_xwalk['Site'].values.tolist()
pur_crops = df_xwalk['Name in PUR'].values.tolist()
cdl_crops = df_xwalk['Name in CDL'].values.tolist()
# census_crops = df_xwalk['Name in Census'].values.tolist()
census_crops = df_xwalk['CENSUS Crop'].values.tolist()


parent_in_sum =df.loc[df['Crop'].isin(parent_crops)].copy()
plus_in_sum = df.loc[df['Crop'].isin(plus_crops)].copy()
pur_in_sum = df.loc[df['Crop'].isin(pur_crops)].copy()
ir4_in_sum = df.loc[df['Crop'].isin(ir4_crops)].copy()
cdl_in_sum = df.loc[df['Crop'].isin(cdl_crops)].copy()
census_in_sum = df.loc[df['Crop Uppercase'].isin(census_crops)].copy()


print len(parent_in_sum), len(plus_in_sum), len(pur_in_sum), len(ir4_in_sum),len(cdl_in_sum)

merge_ir4 = df_xwalk[['Site', 'CENSUS Crop', 'Name in CDL',	'GenClass', 'CompositeClass']].copy()
merge_cdl = df_xwalk[['Name in CDL', 'CENSUS Crop', 'GenClass', 'CompositeClass']].copy()
merge_census = df_xwalk[['CENSUS Crop','Name in CDL','GenClass', 'CompositeClass']].copy()
# ir4_in_sum.ix[:,'Parent sub type'] = ir4_in_sum.apply(lambda row: add_parent_crop(row, 'Crop', df_xwalk, 'Site', 'CENSUS Crop'),axis=1)
# cdl_in_sum.ix[:,'Parent sub type'] = cdl_in_sum .apply(lambda row: add_parent_crop(row, 'Crop', df_xwalk, 'Name in CDL', 'CENSUS Crop'),axis=1)

ir4_in_sum = pd.merge(ir4_in_sum, merge_ir4 , left_on=['Crop'], right_on =['Site'], how='left',
                      suffixes=('_ir4', '_ir4_xwalk'))
cdl_in_sum  = pd.merge(cdl_in_sum , merge_cdl  , left_on=['Crop'], right_on =['Name in CDL'], how='left',
                       suffixes=('_cdl', '_cdl_xwalk'))
census_in_sum = pd.merge(census_in_sum , merge_census , left_on=['Crop Uppercase'], right_on =['CENSUS Crop'],
                         how='left',suffixes=('_census', '_census_xwalk'))

w_parent_df = pd.merge(parent_df, ir4_in_sum, left_on=['State','Parent sub type'],right_on=['State','CENSUS Crop'],
                       how='left',suffixes=('_parent', '_ir4'))

w_parent_df = pd.merge(w_parent_df, cdl_in_sum, left_on=['State','Parent sub type'],right_on=['State','CENSUS Crop'],
                       how='left',suffixes=('_parent', '_cdl'))

w_parent_df = pd.merge(w_parent_df, census_in_sum, left_on=['State','Parent sub type'],right_on=['State','CENSUS Crop'],
                       how='left',suffixes=('_parent', '_census'))


w_parent_df.to_csv(out_location +os.sep+ chemical_name + "_" +date +'.csv')

# suffixes in working tables with repeated col header prefix, occurs when merging to each crop data set
suffixes = ['','_parent', '_cdl']
parent_cols =['State', 'Parent Crop', 'Parent sub type', 'Value']  # col headers from NASS used as parents
crop_keys = ['Crop Uppercase', 'Site', 'CENSUS Crop', ]  # col headers used as keys to join to SUUM excludes
# 'name in CDL because we want that col in the final table
parent_table = w_parent_df[parent_cols]
out_cols = []
for t in w_parent_df.columns.values.tolist():
    y = t.split("_")[0]
    if y not in out_cols and y not in parent_cols and y not in crop_keys:
        out_cols.append(y)

final_cols = parent_cols + out_cols
out_df = pd.DataFrame(columns = final_cols)
for y in suffixes:
    c_cols = [v+y for v in out_cols]
    filter_cols = parent_cols + c_cols
    filter_df = w_parent_df[filter_cols].copy()
    filter_df.columns = final_cols
    out_df = pd.concat([out_df,filter_df])
out_df.drop_duplicates(inplace=True)

out_df['Acres'] = out_df.apply(lambda row: add_acres(row, 'Avg. Annual Crop Acres Grown ', 'Value'),axis=1)
out_df['Acres Source'] = out_df.apply(lambda row: add_source_acres(row, 'Avg. Annual Crop Acres Grown ',),axis=1)
out_df.drop('Avg. Annual Crop Acres Grown ', axis=1, inplace=True)
out_df.drop('Value', axis=1, inplace=True)

print out_df.columns.values.tolist()
temp_df = out_df.reindex(columns=['Parent Crop', 'Parent sub type','State', 'Acres', u'Crop Group', u'Crop',
                          u'Data Source',u'Avg. Annual Total Lbs. AI Applied a', u'Min. Annual PCT', u'Max. Annual PCT',
                          u'Avg. Annual PCT',  'Name in CDL', 'GenClass', 'CompositeClass','Acres Source'])

temp_df.to_csv(out_location +os.sep+ chemical_name + "_InputTables_w_SUUM_INFO_" +date +'.csv')
out_df = out_df.reindex(columns=['Parent Crop', 'Parent sub type','State', 'Acres',
                                 u'Avg. Annual Total Lbs. AI Applied a',u'Min. Annual PCT', u'Max. Annual PCT',
                                 u'Avg. Annual PCT','Name in CDL', 'GenClass', 'CompositeClass','Acres Source'])
out_df.to_csv(out_location +os.sep+ chemical_name + "_InputTables_" +date +'.csv')


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)