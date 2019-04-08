import pandas as pd
import re
import os
import datetime
import sys
import numpy as np

# BEFORE START BE SURE TO REMOVE - symbols
# if ascii syb are causing a problem run in consol and print values for df col to list and search for the xa of an ascii
# TODO CHECK ALL CROPS COMING BACK AS NOT IN CROSSWALK
# TODO confirm assumption with BEAD that if a commonity is found in the suum using a specific term, any associated
# TODO set up format to match current out with Ag in the same table
# TODO QC ALL MERGES
# TODO - can this be simplified -
# TODO CONFIRM WHAT CROPS USE DIFFERENT TERMS IN SUM
# TABLE 1 and TABLE MUST USE THE SAME NAME - Tomatoe  tomatoes and melons, cantaloupe v cantaloupee
# sub commonitys only found in NASS can be ignored

chemical_name = 'Carbaryl'
not_surveyed_symbol = 'Not Surveyed'
surveyed_no_usage = 'NR*'
# suum_excel = 'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
#              '\_ExternalDrive\_CurrentSupportingTables\Usage\SUUMs\Malathion SUUM spreadsheet.final.041118.final.xlsx'
# sheet_name = 'Malathion Table 2 data'
#
#
# survey_excel = 'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
#              '\_ExternalDrive\_CurrentSupportingTables\Usage\SUUMs\Malathion SUUM spreadsheet.final.041118.final.xlsx'
# sheet_name = 'Malathion Table 1 data'

# Split the percentage for CA into the own column so State name can be used as lookup
suum_table2 = "C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA" \
              "\_ExternalDrive\_CurrentSupportingTables\Usage\SUUMs\Carbaryl\Carbaryl_table2.csv"
suum_table1 = "C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA" \
              "\_ExternalDrive\_CurrentSupportingTables\Usage\SUUMs\Carbaryl\Carbaryl_table1.csv"

# crop_excel ='C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
#             '\_ExternalDrive\_CurrentSupportingTables\Usage\Malathion_florida acres and survey.xlsx'
# sheet_name_1 ='Malathion Florida'

crop_excel = 'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
             '\_ExternalDrive\_CurrentSupportingTables\Usage\ParentTable_national_20181217.csv'

# crosswalk_excel ='C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
#             '\_ExternalDrive\_CurrentSupportingTables\Usage\ULUT Data Source Summary-crosswalk (draft for review).xlsx'
# sheet_name_2 ='AG Crops (survey status)(+)'

# NOTE any crop terms used in SUUM no found on crosswalk are added to the 'SUUM Crop not in xwalk' columns with
# support information.  Confirm supporting information with bead before going final
crosswalk_excel = 'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
                  '\_ExternalDrive\_CurrentSupportingTables\Usage\crosswalk_melt_v3.csv'

col_in_xwalk_chk = ['SUUM Crop not in xwalk','CDL Class Name', 'Name in NASS', 'Site']
col_in_xwalk_chk = ['CDL Class Name', 'CENSUS Name','Site_Clean','SUUM Crop not in xwalk']

out_location = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ExternalDrive\_CurrentSupportingTables\Usage\SUUMs\Carbaryl'


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
        return ('CENSUS')


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()



today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

# read in data from input tables
# df = pd.read_excel(suum_excel, sheet_name, header=1 ,encoding='sys.getfilesystemencoding()')

df = pd.read_csv(suum_table2 )

# df_survey = pd.read_excel(survey_excel,header=1)
df_survey = pd.read_csv(suum_table1 )
df_crop = pd.read_csv(crop_excel)
df_xwalk = pd.read_csv(crosswalk_excel )

# strip extra characters, numbers, spaces from columns to try and keep information consistent across tables
for v in df.columns.values.tolist():
    # removes remove all special character in a string except dot and comma and strips whitespaces on ends
    df[v] = df[v].map(lambda x: re.sub('[^.,a-zA-Z0-9'
                                       ' \n\.]', '', str(x).encode('utf8', 'replace')))
    df[v] = df[v].map(lambda x: str(x).strip())
    if v == 'Location':
        # HARD CODE CENSUS TABLE HAS STATE IN ALL CAPS SO UPDATE SUUM TABLE TO ALL CAPS
        df[v] = df[v].map(lambda x: x.upper())
df['Crop Uppercase'] = df['Crop'].map(lambda x: str(x).upper())  # col header in SUUM must be crop
df['Location'] = df['State'].map(lambda x: str(x).upper())  # HARD CODE

for v in df_xwalk.columns.values.tolist():
    # removes remove all special character in a string except dot and comma and strips whitespaces on ends
    if v in ['SUUM Crop not in xwalk']:
        df_xwalk[v] = df_xwalk[v].map(lambda x: re.sub('[^.,a-zA-Z0-9 \n\.]', '', str(x)))
        df_xwalk[v] = df_xwalk[v].map(lambda x: str(x).strip())

for v in df_survey.columns.values.tolist():
    # removes remove all special character in a string except dot and comma and strips whitespaces on ends
    if v in ['Crop']:
        df_survey[v] = df_survey[v].map(lambda x: re.sub('[^.,a-zA-Z0-9 \n\.]', '', str(x)))
        df_survey[v] = df_survey[v].map(lambda x: str(x).strip())

df_survey.drop('Crop Group', axis=1, inplace=True)  # col not need to do the xwalk
df_survey['Crop Uppercase'] = df_survey['Crop'].map(lambda x: str(x).upper())  # HARD CODE header in SUUM must be crop

# subsets the crosswalk to just the columns we will compare to to try to find a match to the term used in SUUM and the
# other columns we want to retain ['CENSUS Crop', 'UseLayer Lookup','CompositeClass']
df_join = df_xwalk[col_in_xwalk_chk + ['CENSUS Crop', 'UseLayer Lookup','CompositeClass']]
# add the crop column to working crosswalk df; info from each col that will be compare to SUUM will be loaded to this
# column and the associated CENSUS CROP 'UseLayer Lookup', 'CompositeClass' retain, the sources of the match is also added
df_join = df_join.reindex(columns=['Crop'] + col_in_xwalk_chk +['CENSUS Crop', 'UseLayer Lookup','CompositeClass'])

# empty data frame with columns that will be retain in the final crosswalk tables
df_join_final = pd.DataFrame(columns=['Crop Values Any Source', 'CENSUS Crop', 'Source', 'UseLayer Lookup', 'CompositeClass'])

# for each column to compare, filters to working xwalk to just rows with blanks for the Crop, then maps the val
# found in the current col from list to a 'Crop Values Any Source'.  The filtered xwalk is then re-filtered to the
# values just captured in 'Crop Values Any Source' removing nulls and nan, the sources of the populated information is
# added then appended to the final xwalk
for col in col_in_xwalk_chk:
    filtered = df_join[~df_join['Crop'].notnull()]
    filtered['Crop Values Any Source'] = filtered[col].map(lambda x: x)
    filtered = filtered[filtered['Crop Values Any Source'].notnull()]
    # print filtered.head()
    # print filtered['Crop Values Any Source'].values.tolist()
    # TODO figure out why this isn't need
    # filtered = filtered[filtered['Crop Values Any Source'] == 'nan']
    if len(filtered) > 1:
        append = filtered[['Crop Values Any Source', 'CENSUS Crop', 'UseLayer Lookup', 'CompositeClass']]
        append = append.reindex(
            columns=['Crop Values Any Source', 'CENSUS Crop', 'Source', 'UseLayer Lookup', 'CompositeClass'])
        append['Source'].fillna(str(col), inplace=True)
        df_join_final = pd.concat([df_join_final, append])

# removed white space from the left side of the values in the 'Crop Values Any Source' and CENSUS Crop columns
for v in df_join_final .columns.values.tolist():

    # removes remove all special character in a string except dot and comma and strips whitespaces on ends
    df_join_final[v] = df_join_final[v].map(lambda x: re.sub('[^.,a-zA-Z0-9 \n\.]', '', str(x)))
    df_join_final['Crop Values Any Source'] = df_join_final['Crop Values Any Source'].apply(lambda x: str(x).lstrip())
df_join_final['CENSUS Crop'] = df_join_final['CENSUS Crop'].apply(lambda x: str(x).lstrip())
# replaces any nulls or nan in the CENSUS crop columns to no_match
df_join_final['CENSUS Crop'].fillna("no_match", inplace=True)
df_join_final['CENSUS Crop'].replace('nan', "no_match", inplace=True)
df_join_final.drop_duplicates(inplace=True)  # drops duplicate values
df_join_final.to_csv(out_location + os.sep + chemical_name + "Final Crop_Census Lookup" + date + '.csv')  # saved output

# Joins the table with the crop names from all sources with the associated Census Crop to the SUUM table 2
df = pd.merge(df, df_join_final, how='inner', left_on='Crop', right_on='Crop Values Any Source')
df['CONCAT USE SITE'] = df['CENSUS Crop'].map(lambda x: x)
df.to_csv(out_location + os.sep + chemical_name + "_SUUM2_mergeCrops_" + date + '.csv')  # saved working output

# Joins the table with the crop names from all sources with the associated Census Crop to the SUUM table 1
df_survey = pd.merge(df_survey, df_join_final, how='inner', left_on='Crop', right_on='Crop Values Any Source')
df_survey['CONCAT USE SITE'] = df_survey['CENSUS Crop'].map(lambda x: x)
df_survey.drop_duplicates(inplace=True)
# turned the symbols used in the SUUM into not surveyed and no usage national
# HARD CODE NOTE IF SYMBOLS CHANGE THIS WILL NEED TO BE UPDATED
df_survey['States with Reported Usage'] = df_survey['States with Reported Usage'].map(
    lambda x: 'Not Surveyed' if x == not_surveyed_symbol else x)
df_survey['States with Reported Usage'] = df_survey['States with Reported Usage'].map(
    lambda x: 'No Usage Reported' if x == surveyed_no_usage else x)
df_survey.to_csv(out_location + os.sep + chemical_name + "_survey_info_joined" + date + '.csv')  # saved working output



df_crop = pd.merge(df_crop, df_join_final, how='left', left_on='CONCAT USE SITE', right_on='CENSUS Crop')
df_crop.to_csv(out_location + os.sep + chemical_name + "_CropTableParent_" + date + '.csv')
df.to_csv(out_location + os.sep + chemical_name + "_CropTableParent_2_" + date + '.csv')
# merges parent crop table from Census to table 2 of the sum which now has the CENSUS 'CONCAT USE SITE' and the
# state in all caps

out_df = pd.merge(df_crop, df, left_on=['Location', 'CONCAT USE SITE'] +
                                       df_join_final.columns.values.tolist(),
                  right_on=['Location', 'CONCAT USE SITE'] + df_join_final.columns.values.tolist(), how='left')
out_df = out_df.drop_duplicates()  # drops duplicates

# Added the Acres column, if it is found in SUUM this value is Avg. Annual Crop Acres Grown use otherwise it is pulled
# from the Value column in the CENSUS info and retains the sources info (SUUM OR CENSUS) then drops the original cols
# from the SUUM and CENSUS
# HARD SUUM TABLE Acre COL = 'Avg. Annual Crop Acres Grown ' # with the extra space
# HARD CENSUS TABLE Acre COL = 'VALUE'

out_df.to_csv(out_location + os.sep + chemical_name + "_CropTableParent_2" + date + '.csv')

out_df['Acres'] = out_df.apply(lambda row: add_acres(row, 'Avg. Annual Crop Acres Grown', 'Value'), axis=1)
out_df['Acres Source'] = out_df.apply(lambda row: add_source_acres(row, 'Avg. Annual Crop Acres Grown', ), axis=1)
out_df.drop('Avg. Annual Crop Acres Grown', axis=1, inplace=True)
out_df.drop('Value', axis=1, inplace=True)


# drop columns that are duplicated in from table 1 (df_survey) and table 2 (df) the values from table 2 (df were joined
# above but do not contain the national information found in table 1. Table 1 and table 2 values will match and should
# keep values with national information found in table 1.
out_df.drop('Crop', axis=1, inplace=True)
out_df.drop('Crop Uppercase', axis=1, inplace=True)

# Joins working table to survey information found in table 1.Any crop without a value in 'States with Reported Usage' is
# not registered, and information about this crop will be pulled in from the Census
out_df = pd.merge(out_df, df_survey, on=['CONCAT USE SITE'] + df_join_final.columns.values.tolist(),
                  how='outer').copy()


# Identified crops that at in the suum based on acres or based on 'SUUM Crop not in xwalk'
# 'SUUM Crop not in xwalk' are crops identified as no_match when table 1 from the SUUM is merged with the lookup this is
# daved output df_survey.to_csv(out_location + os.sep + chemical_name + "_survey_info_joined" + date + '.csv')
# Crops that did not join where then added to master xwalk table (crosswalk_excel) in the not SUUM Crop not in xwalk
out_df.loc[((out_df['Acres Source'] == 'SUUM') |(out_df['Source'] =='SUUM Crop not in xwalk')
            |(out_df['States with Reported Usage'].notnull())),'Merge Source'] ='SUUM'
out_df.loc[((out_df['Merge Source'] != 'SUUM')),'Merge Source'] = 'CENSUS'

# OUTPUT WILL ALL COLUMNS from all joins AND NO PREFERRED SELECTION TO SUUM DATA; all info from crosswalk is retain is a
# long format rather than wide
temp_df = out_df.copy()
temp_df = temp_df.drop_duplicates()

# NOTE NOTE: This is a good table to look at if final table does look right or isn't capturing the correct registered
# info.  Check to see if the commodity anf sub commodity is present - if not the crop is missing from parent; check if
# there are duplicate locations in crosswalk for crop, check to see if spelling is the same on table 1 and table 2
temp_df.to_csv(out_location + os.sep + chemical_name + "_InputTables_w_SUUM_INFO_" + date + '.csv')

# OUTPUT TABLE WITHOUT THE PREFERRED SELECTION FOR SUUM INFO BUT LIMITED COLUMNS TO THE FINAL OUTPUT
# Table will contain a lot of duplicate rows from the different sources
out_df = out_df.reindex(columns=['Location', 'Commodity', 'CONCAT USE SITE', 'Acres',
                                 u'Avg. Annual Total Lbs. AI Applied a', u'Min. Annual PCT', u'Max. Annual PCT',
                                 u'Avg. Annual PCT',  'UseLayer Lookup', 'CompositeClass', 'Acres Source',
                                 'Crop','States with Reported Usage','Source','Merge Source'])
out_df.to_csv(out_location + os.sep + chemical_name + "_InputTables_" + date + '.csv')

# STARTS THE PREFERRED SELECTION TO SUUM DATA FROM JOINED TABLES
# drops the duplicate rows for a based on a commodity - this will eliminate things like when the SUUM report cotton
# but the CENSUS report cotton, pima and cotton, upland- cotton would be the commodity
out_df_drop = out_df.drop_duplicates(subset=['Commodity', 'Acres', 'Merge Source']).copy()
# KEEPS ALL DATA THAT WAS MATCH TO SUUM THEN FILLS IN ADDITIONAL INFORMATION FOR NOT REGISTERED CROPS AND ACRES FOR NOT
# SURVEYED CROPS FROM CENSUS

suum_row = out_df_drop.loc[(out_df_drop['Merge Source'] == 'SUUM')].copy()
suum_commodity = suum_row['Commodity'].values.tolist()

# Drops all CENSUS values that were captured by the SUUM, keep the associated SUUM data for that crop, and any crops
# not captured in SUUM the Census data is retained
out_df_drop = out_df_drop.loc[((out_df_drop['Merge Source'] == 'SUUM') | ((out_df_drop['Merge Source'] == 'CENSUS') & (~out_df_drop['Commodity'].isin(suum_commodity))))]

out_df_drop.drop_duplicates(inplace=True)
# ## final output table to be used as input WITH PREFERENCE MADE TO SUUM DATA
out_df_drop = out_df_drop.reindex(columns=['Location', 'Commodity', 'CONCAT USE SITE', 'Acres',
                                           u'Avg. Annual Total Lbs. AI Applied a', u'Min. Annual PCT',
                                           u'Max. Annual PCT',
                                           u'Avg. Annual PCT',  'UseLayer Lookup', 'CompositeClass',
                                           'Acres Source', 'Crop','States with Reported Usage','Merge Source'])
print out_df_drop.head()
# EXCEPTION TO CATCH CROP IN SUUM COMING BACK AS NOT REGISTERED

out_df_drop.to_csv(out_location + os.sep + chemical_name + "_Test_" + date + '.csv', encoding ='utf-8')

if  len (out_df_drop.loc[(out_df_drop['Acres Source'] == 'SUUM') & (~(out_df_drop['States with Reported Usage'].notnull()))])>0:
    out_df_drop_suum = (out_df_drop.loc[(out_df_drop['Acres Source'] == 'SUUM') & (~(out_df_drop['States with Reported Usage'].notnull()))].copy())
    out_df_drop_suum['States with Reported Usage'].fillna(out_df_drop_suum['Location'], inplace=True)
    states_df = out_df_drop_suum.groupby(['CONCAT USE SITE'])['States with Reported Usage'].apply((list)).reset_index()
    out_df_drop = pd.merge(out_df_drop, states_df , on= ['CONCAT USE SITE'], how='left')
    out_df_drop ['States with Reported Usage'] = out_df_drop['States with Reported Usage_x'].map(str).replace('nan','')\
                                             + (out_df_drop ['States with Reported Usage_y'].map(str).replace('nan','')).replace(']','').replace('[','')


# check for crops that are in suum but for a differenct state so the states with reported usage is blank but shouldn't
# be
look_up_usage = out_df_drop.loc[:,['CONCAT USE SITE','Crop','States with Reported Usage']].copy()
look_up_usage= look_up_usage.loc[look_up_usage['Crop'].notnull()]
look_up_usage.drop_duplicates(inplace=True)
look_up_usage.to_csv(out_location + os.sep + chemical_name + "_LookupUsageType_" + date + '.csv', encoding ='utf-8')

def add_reg(row, look_df,col):
    look_value = row[col]
    if row['Acres'] == 0:
        return_value = 'Not Grown in State'
    elif row['Acres Source'] == 'SUUM'and row ['Max. Annual PCT'] == '':
        return_value = 'No Usage Reported in State'
    elif look_value in look_df[col].values.tolist():
        return_value = look_df.loc[look_df[col] == look_value,'States with Reported Usage'].iloc[0]
    else:
        return_value = 'Not Registered'

    if return_value == 'No Usage Reported':
        return_value ='Not Survey in State; No usage where surveyed'
    return return_value

out_df_drop['States with Reported Usage_Final'] = out_df_drop.apply(lambda row: add_reg(row, look_up_usage , 'CONCAT USE SITE'), axis=1)


# out_df_drop ['States with Reported Usage'].replace('',"Not Registered", inplace=True)
# out_df_drop ['States with Reported Usage'].fillna("Not Registered", inplace=True)
out_df_count = out_df_drop.groupby(['Location', 'CONCAT USE SITE']).size().reset_index()
out_df_drop = pd.merge(out_df_drop, out_df_count , how='outer', left_on=['Location', 'CONCAT USE SITE'], right_on=['Location', 'CONCAT USE SITE'])
# if acres crop has acres values from both SUUM and Census preferentially picks SUUM
out_df_drop = out_df_drop.loc[((out_df_drop[0l] == 1) | ((out_df_drop['Acres Source'] == 'SUUM') & (out_df_drop[0l]== 2))) | (out_df_drop[0l]>2) ]

out_df_drop = out_df_drop.reindex(columns=['Location', 'Commodity', 'CONCAT USE SITE', 'Acres',
                                           u'Avg. Annual Total Lbs. AI Applied a', u'Min. Annual PCT',
                                           u'Max. Annual PCT',
                                           u'Avg. Annual PCT',  'UseLayer Lookup', 'CompositeClass',
                                           'Acres Source', 'Crop','States with Reported Usage','Merge Source','States with Reported Usage_Final'])
out_df_drop.to_csv(out_location + os.sep + chemical_name + "_Final_" + date + '.csv', encoding ='utf-8')
print out_location + os.sep + chemical_name + "_Final_" + date + '.csv'
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
