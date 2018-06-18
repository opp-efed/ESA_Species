import pandas as pd
import re
import os
import datetime

chemical_name = 'Malathion'
suum_excel ='C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
            '\_ExternalDrive\_CurrentSupportingTables\Usage\SUUMs\Malathion SUUM spreadsheet.final.041118.final.xlsx'
sheet_name ='Malathion Table 2 data'

crop_excel ='C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
            '\_ExternalDrive\_CurrentSupportingTables\Usage\Malathion_florida acres and survey.xlsx'
sheet_name_1 ='Malathion Florida'
crosswalk_excel ='C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
            '\_ExternalDrive\_CurrentSupportingTables\Usage\ULUT Data Source Summary-crosswalk (draft for review).xlsx'
sheet_name_2 ='AG Crops (survey status)(+)'

out_location = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
               r'\_ExternalDrive\_CurrentSupportingTables\Usage\ChemicalInput_tables'

def add_parent_crop(row, col, x_df, col_match, col_return):
    v = row[col]
    state = row['State']
    return x_df.loc[x_df[col_match] == v , col_return].iloc[0]


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

df = pd.read_excel( suum_excel, sheet_name, header = 1)
df_crop = pd.read_excel(crop_excel, sheet_name_1, header = 4)
df_xwalk = pd.read_excel(crosswalk_excel, sheet_name_2, header = 6)

for v in df.columns.values.tolist():
    df [v] = df[v].map(lambda x: re.sub(r'([^\s\w.]|_)+', '', str(x)))


parent_df = df_crop.ix[:,['State','Commodity sub type', 'Value']]
parent_df.columns = ['State','Parent Crop', 'Value']

parent_crops = parent_df['Parent Crop'].values.tolist()
plus_crops = df_xwalk['Name in PLUS'].values.tolist()
ir4_crops = df_xwalk['Site'].values.tolist()
pur_crops = df_xwalk['Name in PUR'].values.tolist()
cdl_crops = df_xwalk['Name in CDL'].values.tolist()
census_crops = df_xwalk['Name in Census'].values.tolist()

print parent_crops
print census_crops
parent_in_sum =df.loc[df['Crop'].isin(parent_crops)].copy()
plus_in_sum = df.loc[df['Crop'].isin(plus_crops)].copy()
pur_in_sum = df.loc[df['Crop'].isin(pur_crops)].copy()
ir4_in_sum = df.loc[df['Crop'].isin(ir4_crops)].copy()
cdl_in_sum = df.loc[df['Crop'].isin(cdl_crops)].copy()
# census_in_sum  = df.loc[df['Crop'].isin(census_crops)].copy()

print len(parent_in_sum), len(plus_in_sum), len(pur_in_sum), len(ir4_in_sum),len(cdl_in_sum)
#census_in_sum.ix[:,'Parent Crop'] = census_in_sum .apply(lambda row: add_parent_crop(row, 'Crop', df_xwalk, 'Name in Census', 'Name in Census'),axis=1)
ir4_in_sum.ix[:,'Parent Crop'] = ir4_in_sum.apply(lambda row: add_parent_crop(row, 'Crop', df_xwalk, 'Site', 'Name in Census'),axis=1)
ir4_in_sum_filter = ir4_in_sum.loc[ir4_in_sum['State'] == 'Florida']
cdl_in_sum.ix[:,'Parent Crop'] = cdl_in_sum .apply(lambda row: add_parent_crop(row, 'Crop', df_xwalk, 'Name in CDL', 'Name in Census'),axis=1)
cdl_in_sum_filter = cdl_in_sum.loc[cdl_in_sum['State'] == 'Florida']



parent_df = pd.merge(parent_df, ir4_in_sum_filter, on='Parent Crop', how='left')
parent_df = pd.merge(parent_df, cdl_in_sum_filter, on='Parent Crop', how='left')
print parent_df

parent_df.to_csv(out_location +os.sep+ chemical_name + "_" +date +'.csv')
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)