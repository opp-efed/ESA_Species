import pandas as pd

crosswalk_excel ='C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
                 '\_ExternalDrive\_CurrentSupportingTables\Usage\ULUT Data Source Summary-crosswalk (interim draft for ESA)_20181107.csv'

# REading directly form excel
# sheet_name_2 ='AG Crops (survey status)(+)'
# df = pd.read_excel(crosswalk_excel, sheet_name_2, header = 6)

#read from csv
df = pd.read_csv(crosswalk_excel)

# columns with crops names from census split in excel using text to columns split by ; original col = Name in Census
val_vars =['CENSUS Crops', 1, 2, 3, 4]
# all original cols/ all cols not in val_vars

print df.columns.values.tolist()
id_vars_melt =[u'IR-4 Group #', u'Site', u'Parent/Included Sub-Groups ', u'Notes', u'Name in PLUS', u'Name in Census',
               u'National CAG', u'Name in iMap', u'Fungicide', u'Herbicide', u'Insecticide', u'Nematicide', u'Growth Regulator',
               u'Group in NASS', u'Name in NASS', u'Fungicide.1', u'Herbicide.1', u'Insecticide.1', u'Other', u'Years Surveyed',
               u'Name in PUR', u"CA CAG                           (from 2012 Census of Ag/ or CA Ag Commissioners' report  ?)",
               u'Percent in CA (based on 2012 Census of Ag Acres)', u'Study', u'Notes.1', u'CDL Class Name', u'Reclass Category',
               u'Double Crop (Y)', u'Reclass Code', u'COMMENTS']

df_melt_row = pd.melt(df, id_vars=id_vars_melt, value_vars=val_vars, var_name='melt_var',
                      value_name='CENUS_CROP')

df_melt_row.to_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA'
                   r'\_ExternalDrive\_CurrentSupportingTables\Usage\crosswalk_melt_v3_20181107.csv')