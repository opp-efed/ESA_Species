import pandas as pd

# Author J.Connolly
# Internal deliberative, do not cite or distribute

def add_cnty(row, col, ):
    value = row[col]
    while len(str(value)) <3:
        value = "0" + str(value)

    return value

df_1_0 = pd.read_csv(
    r"path\State_0_1.csv")  # pre/ab table from census of ag

udl = df_1_0.groupby(['UDL'])[
    ['ALABAMA', 'ALASKA', 'ARIZONA', 'ARKANSAS', 'CALIFORNIA', 'COLORADO', 'CONNECTICUT', 'DELAWARE', 'FLORIDA',
     'GEORGIA', 'HAWAII', 'IDAHO', 'ILLINOIS', 'INDIANA', 'IOWA', 'KANSAS', 'KENTUCKY', 'LOUISIANA', 'MAINE',
     'MARYLAND', 'MASSACHUSETTS', 'MICHIGAN', 'MINNESOTA', 'MISSISSIPPI', 'MISSOURI', 'MONTANA', 'NEBRASKA', 'NEVADA',
     'NEW HAMPSHIRE', 'NEW JERSEY', 'NEW MEXICO', 'NEW YORK', 'NORTH CAROLINA', 'NORTH DAKOTA', 'OHIO', 'OKLAHOMA',
     'OREGON', 'PENNSYLVANIA', 'RHODE ISLAND', 'SOUTH CAROLINA', 'SOUTH DAKOTA', 'TENNESSEE', 'TEXAS', 'UTAH',
     'VERMONT', 'VIRGINIA', 'WASHINGTON', 'WEST VIRGINIA', 'WISCONSIN', 'WYOMING']].sum()
print udl

udl_w_crops = df_1_0.groupby(['UDL', 'Location'])[
    ['ALABAMA', 'ALASKA', 'ARIZONA', 'ARKANSAS', 'CALIFORNIA', 'COLORADO', 'CONNECTICUT', 'DELAWARE', 'FLORIDA',
     'GEORGIA', 'HAWAII', 'IDAHO', 'ILLINOIS', 'INDIANA', 'IOWA', 'KANSAS', 'KENTUCKY', 'LOUISIANA', 'MAINE',
     'MARYLAND', 'MASSACHUSETTS', 'MICHIGAN', 'MINNESOTA', 'MISSISSIPPI', 'MISSOURI', 'MONTANA', 'NEBRASKA', 'NEVADA',
     'NEW HAMPSHIRE', 'NEW JERSEY', 'NEW MEXICO', 'NEW YORK', 'NORTH CAROLINA', 'NORTH DAKOTA', 'OHIO', 'OKLAHOMA',
     'OREGON', 'PENNSYLVANIA', 'RHODE ISLAND', 'SOUTH CAROLINA', 'SOUTH DAKOTA', 'TENNESSEE', 'TEXAS', 'UTAH',
     'VERMONT', 'VIRGINIA', 'WASHINGTON', 'WEST VIRGINIA', 'WISCONSIN', 'WYOMING']].sum().reset_index()

melt = pd.melt(udl, id_vars=['UDL', 'Location', ],
               value_vars=['ALABAMA', 'ALASKA', 'ARIZONA', 'ARKANSAS', 'CALIFORNIA', 'COLORADO', 'CONNECTICUT',
                           'DELAWARE', 'FLORIDA', 'GEORGIA', 'HAWAII', 'IDAHO', 'ILLINOIS', 'INDIANA', 'IOWA', 'KANSAS',
                           'KENTUCKY', 'LOUISIANA', 'MAINE', 'MARYLAND', 'MASSACHUSETTS', 'MICHIGAN', 'MINNESOTA',
                           'MISSISSIPPI', 'MISSOURI', 'MONTANA', 'NEBRASKA', 'NEVADA', 'NEW HAMPSHIRE', 'NEW JERSEY',
                           'NEW MEXICO', 'NEW YORK', 'NORTH CAROLINA', 'NORTH DAKOTA', 'OHIO', 'OKLAHOMA', 'OREGON',
                           'PENNSYLVANIA', 'RHODE ISLAND', 'SOUTH CAROLINA', 'SOUTH DAKOTA', 'TENNESSEE', 'TEXAS',
                           'UTAH', 'VERMONT', 'VIRGINIA', 'WASHINGTON', 'WEST VIRGINIA', 'WISCONSIN', 'WYOMING'])

# chemical county mask
cnty_1_0 = pd.read_csv("path\meth_0_1_all.csv",
    header=2)

groupby_col =['State reported usage', 'UDL', 'Location']
group_cols_cnty = []
for v in cnty_1_0.columns.values.tolist():
    if v not in group_cols_cnty:
        group_cols_cnty.append(v)

udl_w_crops_cnty = cnty_1_0.groupby(groupby_col)[group_cols_cnty].sum().reset_index()
udl_w_crops_cnty_udl = cnty_1_0.groupby(['UDL'])[group_cols_cnty].sum().reset_index()

udl_w_crops_cnty_udl.to_csv(r'out paths\udl_cnty_mask.csv')

lookup = pd.read_csv(r"path\Cnty_St_ANSI_Lookup.csv")
lookup['State ANSI'] = lookup['State ANSI'].map(lambda x: str(x) if len(str(x)) == 2 else "0" +str(x)).astype(str)
lookup['County ANSI']  = lookup.apply(lambda row: add_cnty(row, 'County ANSI'), axis=1)

lookup["GEOID"] = lookup['State ANSI'].map(str) + lookup['County ANSI'].map(str)
lookup.to_csv(r'out path\cnty_fips.csv')

udl_w_crops_cnty_udl_t = udl_w_crops_cnty_udl.T.reset_index()
cols = udl_w_crops_cnty_udl_t.iloc[0].values.tolist()
udl_w_crops_cnty_udl_t.columns = cols
cols.remove('UDL')
udl_w_crops_cnty_udl_t= udl_w_crops_cnty_udl_t.reindex(udl_w_crops_cnty_udl_t.index.drop(0))
add_fips = pd.merge(udl_w_crops_cnty_udl_t,lookup, how = 'left', left_on='UDL',right_on= 'Location')
add_fips[cols] = add_fips[cols].apply(lambda x: [y if y == 0 else 100 for y in x])
add_fips.to_csv(r'out path\meth_cnty_mask.csv')

