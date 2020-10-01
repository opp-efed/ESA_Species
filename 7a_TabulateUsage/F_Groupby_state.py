import pandas as pd
import os
import datetime
# Author J.Connolly
# Internal deliberative, do not cite or distribute

def add_cnty(row, col, ):
    value = row[col]
    while len(str(value)) <3:
        value = "0" + str(value)

    return value


# chemical county mask

cnty_mask = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\Glyphosate\GLY_1_0_cnty.csv"
cnty_not_in_census = r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ED_results\_CurrentSupportingTables\Usage\Cnty_NotInCensus.csv"
cnty_lookup = r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ED_results\_CurrentSupportingTables\Usage\Cnty_St_ANSI_Lookup.csv"
# uses not pulled from CoA ['Action Area', 'Action Area Ag', 'Action Area NonAg', 'Federal Lands']
col_to_include = ['Action Area', 'Action Area Ag', 'Action Area NonAg', 'GlyphosateDrift', 'Federal Lands', 'Ag']
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

cnty_1_0 = pd.read_csv(cnty_mask, header=2)
not_in_census = pd.read_csv(cnty_not_in_census)

groupby_col =['Registration', 'UDL', 'Location']
group_cols_cnty = []
for v in cnty_1_0.columns.values.tolist():
    if v not in group_cols_cnty:
        group_cols_cnty.append(v)

udl_w_crops_cnty = cnty_1_0.groupby(groupby_col)[group_cols_cnty].sum().reset_index()
udl_w_crops_cnty_udl = cnty_1_0.groupby(['UDL'])[group_cols_cnty].sum().reset_index()
# this able provides the number of crops per udl in each county
udl_w_crops_cnty_udl.to_csv(os.path.dirname(cnty_mask) +os.sep+'udl_cnty_mask.csv')

lookup = pd.read_csv(cnty_lookup)
lookup['State ANSI'] = lookup['State ANSI'].map(lambda x: str(x) if len(str(x)) == 2 else "0" +str(x)).astype(str)
lookup['County ANSI']  = lookup.apply(lambda row: add_cnty(row, 'County ANSI'), axis=1)

lookup["GEOID"] = lookup['State ANSI'].map(str) + lookup['County ANSI'].map(str)
# lookup.to_csv(os.path.dirname(cnty_mask) + os.sep+ 'cnty_fips.csv')

udl_w_crops_cnty_udl_t = udl_w_crops_cnty_udl.T.reset_index()
cols = udl_w_crops_cnty_udl_t.iloc[0].values.tolist()
udl_w_crops_cnty_udl_t.columns = cols
cols.remove('UDL')
udl_w_crops_cnty_udl_t= udl_w_crops_cnty_udl_t.reindex(udl_w_crops_cnty_udl_t.index.drop(0))
add_fips = pd.merge(udl_w_crops_cnty_udl_t,lookup, how = 'left', left_on='UDL',right_on= 'Location')
add_fips[cols] = add_fips[cols].apply(lambda x: [y if y == 0 else 1 for y in x])
for col in col_to_include:
    add_fips [col] = 1

add_fips = pd.merge(not_in_census,add_fips,on=['Location', 'GEOID', 'State ANSI'], how= 'outer')
add_fips.fillna(1, inplace=True)
add_fips['FIPS'] = add_fips['GEOID']
add_fips.to_csv(os.path.dirname(cnty_mask)+ os.sep+ 'Cnty_Mask_'+date+"_" +os.path.basename(cnty_mask))


# State groups when we were included usage into the action area
# df_1_0 = pd.read_csv(r"path\State_0_1.csv")  # pre/ab table from census of ag
#
# udl = df_1_0.groupby(['UDL'])[
#     ['ALABAMA', 'ALASKA', 'ARIZONA', 'ARKANSAS', 'CALIFORNIA', 'COLORADO', 'CONNECTICUT', 'DELAWARE', 'FLORIDA',
#      'GEORGIA', 'HAWAII', 'IDAHO', 'ILLINOIS', 'INDIANA', 'IOWA', 'KANSAS', 'KENTUCKY', 'LOUISIANA', 'MAINE',
#      'MARYLAND', 'MASSACHUSETTS', 'MICHIGAN', 'MINNESOTA', 'MISSISSIPPI', 'MISSOURI', 'MONTANA', 'NEBRASKA', 'NEVADA',
#      'NEW HAMPSHIRE', 'NEW JERSEY', 'NEW MEXICO', 'NEW YORK', 'NORTH CAROLINA', 'NORTH DAKOTA', 'OHIO', 'OKLAHOMA',
#      'OREGON', 'PENNSYLVANIA', 'RHODE ISLAND', 'SOUTH CAROLINA', 'SOUTH DAKOTA', 'TENNESSEE', 'TEXAS', 'UTAH',
#      'VERMONT', 'VIRGINIA', 'WASHINGTON', 'WEST VIRGINIA', 'WISCONSIN', 'WYOMING']].sum()
# print udl
#
# udl_w_crops = df_1_0.groupby(['UDL', 'Location'])[
#     ['ALABAMA', 'ALASKA', 'ARIZONA', 'ARKANSAS', 'CALIFORNIA', 'COLORADO', 'CONNECTICUT', 'DELAWARE', 'FLORIDA',
#      'GEORGIA', 'HAWAII', 'IDAHO', 'ILLINOIS', 'INDIANA', 'IOWA', 'KANSAS', 'KENTUCKY', 'LOUISIANA', 'MAINE',
#      'MARYLAND', 'MASSACHUSETTS', 'MICHIGAN', 'MINNESOTA', 'MISSISSIPPI', 'MISSOURI', 'MONTANA', 'NEBRASKA', 'NEVADA',
#      'NEW HAMPSHIRE', 'NEW JERSEY', 'NEW MEXICO', 'NEW YORK', 'NORTH CAROLINA', 'NORTH DAKOTA', 'OHIO', 'OKLAHOMA',
#      'OREGON', 'PENNSYLVANIA', 'RHODE ISLAND', 'SOUTH CAROLINA', 'SOUTH DAKOTA', 'TENNESSEE', 'TEXAS', 'UTAH',
#      'VERMONT', 'VIRGINIA', 'WASHINGTON', 'WEST VIRGINIA', 'WISCONSIN', 'WYOMING']].sum().reset_index()
#
# melt = pd.melt(udl, id_vars=['UDL', 'Location', ],
#                value_vars=['ALABAMA', 'ALASKA', 'ARIZONA', 'ARKANSAS', 'CALIFORNIA', 'COLORADO', 'CONNECTICUT',
#                            'DELAWARE', 'FLORIDA', 'GEORGIA', 'HAWAII', 'IDAHO', 'ILLINOIS', 'INDIANA', 'IOWA', 'KANSAS',
#                            'KENTUCKY', 'LOUISIANA', 'MAINE', 'MARYLAND', 'MASSACHUSETTS', 'MICHIGAN', 'MINNESOTA',
#                            'MISSISSIPPI', 'MISSOURI', 'MONTANA', 'NEBRASKA', 'NEVADA', 'NEW HAMPSHIRE', 'NEW JERSEY',
#                            'NEW MEXICO', 'NEW YORK', 'NORTH CAROLINA', 'NORTH DAKOTA', 'OHIO', 'OKLAHOMA', 'OREGON',
#                            'PENNSYLVANIA', 'RHODE ISLAND', 'SOUTH CAROLINA', 'SOUTH DAKOTA', 'TENNESSEE', 'TEXAS',
#                            'UTAH', 'VERMONT', 'VIRGINIA', 'WASHINGTON', 'WEST VIRGINIA', 'WISCONSIN', 'WYOMING'])
