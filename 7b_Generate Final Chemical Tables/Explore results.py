import pandas as pd
import os

# df =pd.read_excel(r"C:\Users\JConno02\Desktop\Explore_results.xlsx", sheet= "carb_avguniform")
df = pd.read_csv(r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\ChemicalTables\Methomyl\Summarized Tables\2_PCT\max\Upper\R_PCT_GIS_Step2_Methomyl.csv")
csv = 'metho_max_Upper_PCT.csv'
out_location = r'E:\Workspace\StreamLine\ESA\Tabulated_UsageHUCABHabitat\result_summary\Methomyl'
columns = df.columns.values.tolist()
conus_col = [v for v in columns if v.startswith("CONUS") and v.endswith("_0")]
print conus_col
nl48_col = [v for v in columns if v.startswith("NL48") and v.endswith("_0")]

conus_col.remove('CONUS_Methomyl AA_0')
conus_col.remove('CONUS_Methomyl AA Ag_0')
# conus_col.remove('CONUS_Methomyl AA NonAg_0')
conus_col.remove('CONUS_Federal Lands_0')
# conus_col.remove('CONUS_Sum_Composites')
# conus_col.remove('CONUS_Ag_Ag_Factor')
# conus_col.remove('CONUS_NonAg_NonAg_Factor')
# conus_col.remove('CONUS_Composite_Factor')
# conus_col.remove('CONUS_Sum_Ag')
# conus_col.remove('CONUS_Sum_NonAg')

# conus_col_ag = [u'CONUS_Other Crops_0', u'CONUS_Corn_0', u'CONUS_Rice_0', u'CONUS_Soybeans_0',
#                 u'CONUS_Vegetables and Ground Fruit_0', u'CONUS_Other Orchards_0', u'CONUS_Grapes_0',
#                 u'CONUS_Citrus_0', u'CONUS_Other Grains_0', u'CONUS_OtherRow Crops_0', u'CONUS_Pasture_0']
# conus_col_non_ag = [ u'CONUS_Developed_0', u'CONUS_Forest Trees_0', u'CONUS_Nurseries_0',
#                      u'CONUS_Open Space Developed_0', u'CONUS_Rangeland_0', u'CONUS_Right of Way_0', ]
conus_col_ag = ['CONUS_Corn_0', 'CONUS_Cotton_0', 'CONUS_Other Grains_0', 'CONUS_Other Orchards_0',
                'CONUS_OtherRow Crops_0', 'CONUS_Pasture_0', 'CONUS_Soybeans_0', 'CONUS_Vegetables and Ground Fruit_0',
                'CONUS_Methomyl Alley Crop_0',  'CONUS_Bermuda Grass_0',
                'CONUS_Methomyl Citrus_0', 'CONUS_Methomyl Wheat_0']
conus_col_non_ag = []
conus_all_uses = conus_col_ag + conus_col_non_ag
nl48_col.remove('NL48_Methomyl AA_0')
nl48_col.remove('NL48_Methomyl AA Ag_0')
# nl48_col.remove('NL48_Methomyl AA NonAg_0')
nl48_col.remove('NL48_Federal Lands_0')
# nl48_col.remove('NL48_Sum_Composites')
# nl48_col.remove('NL48_Ag_Ag_Factor')
# nl48_col.remove('NL48_NonAg_NonAg_Factor')
# nl48_col.remove('NL48_Composite_Factor')
# nl48_col.remove('NL48_Sum_Ag')
# nl48_col.remove('NL48_Sum_NonAg')

# nl48_col_ag = [u'NL48_Ag_0', u'NL48_Pasture_0']
# #
# nl48_col_non_ag = [ u'NL48_Rangeland_0', u'NL48_Developed_0', u'NL48_Forest Trees_0', u'NL48_Nurseries_0',
#                     u'NL48_Open Space Developed_0', u'NL48_Right of Way_0']
nl48_col_ag = [u'NL48_Ag_0']
nl48_col_non_ag = []
nl48_all_uses = nl48_col_ag + nl48_col_non_ag

df['CONUS_Sum_All'] = df[conus_col].sum(axis=1)
df['CONUS_Sum_Ag'] = df[conus_col_ag].sum(axis=1)
df['CONUS_Ag_Max'] = df[conus_col_ag].max(axis=1)
df['CONUS_Sum_NonAg'] = df[conus_col_non_ag].sum(axis=1)
df['CONUS_NonAg_Max'] = df[conus_col_non_ag].max(axis=1)

df['NL48_Sum_All'] = df[nl48_col].sum(axis=1)
df['NL48_Sum_Ag'] = df[nl48_col_ag].sum(axis=1)
df['NL48_Ag_Max'] = df[nl48_col_ag].max(axis=1)
df['NL48_Sum_NonAg'] = df[nl48_col_non_ag].sum(axis=1)
df['NL48_NonAg_Max'] = df[nl48_col_non_ag].max(axis=1)

df['Sum_All'] = df[conus_col + nl48_col].sum(axis=1)


def nonag_uses(row, cols, max_col):
    uses = []

    if row[max_col] < 1:
        for col in cols:
            if row[col] > 1 and len(uses) < 1:
                uses.append(col)
            elif row[col] > 1:
                uses.append( col)
            else:
                pass
    else:
        pass
    # print uses
    return uses


def nonag_counts(row, cols, max_col):
    uses = []
    if row[max_col] < 1:
        for col in cols:
            if row[col] > 1 and len(uses) < 1:
                uses.append(col)
            elif row[col] > 1:
                uses.append( col)
            else:
                pass
    count_use = len(uses)
    return count_use


def all_uses(row, cols):
    uses = []

    for col in cols:
        if row[col] > 1 and len(uses) < 1:
            uses.append(col)
        elif row[col] > 1:
            uses.append(col)
        else:
            pass
    else:
        pass
    # print uses
    return uses


def all_counts(row, cols):
    uses = []
    for col in cols:
        if row[col] > 1 and len(uses) < 1:
            uses.append(col)
        elif row[col] > 1:
            uses.append(col)
        else:
            pass
    count_use = len(uses)
    return count_use


def counts_both(row, col, cols_group_col):
    uses = row[col]
    # print uses
    grp_uses = [v for v in uses if v in cols_group_col]

    count_use = len(grp_uses)
    return count_use


df['CONUS NonAg Uses'] = df.apply(lambda row: nonag_uses(row, conus_col_non_ag, 'CONUS_Ag_Max'), axis=1)
df['CONUS Count Non Ag Uses'] = df.apply(lambda row: nonag_counts(row, conus_col_non_ag, 'CONUS_Ag_Max'), axis=1)
df['CONUS Ag Uses'] = df.apply(lambda row: nonag_uses(row, conus_col_ag, 'CONUS_NonAg_Max'), axis=1)
df['CONUS Count Ag Uses'] = df.apply(lambda row: nonag_counts(row, conus_col_ag, 'CONUS_NonAg_Max'), axis=1)
df['CONUS All Uses'] = df.apply(lambda row: all_uses(row, conus_all_uses), axis=1)
df['CONUS Count All Uses'] = df.apply(lambda row: all_counts(row, conus_all_uses), axis=1)
df['CONUS Count All Uses-Ag'] = df.apply(lambda row: counts_both(row, 'CONUS All Uses', conus_col_ag), axis=1)
df['CONUS Count All Uses-NonAg'] = df.apply(lambda row: counts_both(row, 'CONUS All Uses', conus_col_non_ag), axis=1)

df['NL48 NonAg Uses']= df.apply(lambda row: nonag_uses (row, nl48_col_non_ag, 'NL48_Ag_Max'), axis=1)
df['NL48 Count Non Ag Uses'] = df.apply(lambda row: nonag_counts (row, nl48_col_non_ag, 'NL48_Ag_Max'), axis=1)
df['NL48 Ag Uses']= df.apply(lambda row: nonag_uses (row, nl48_col_ag, 'NL48_NonAg_Max'), axis=1)
df['NL48 Count Ag Uses'] = df.apply(lambda row: nonag_counts (row, nl48_col_ag, 'NL48_NonAg_Max'), axis=1)
df['NL48 All Uses'] = df.apply(lambda row: all_uses(row, nl48_all_uses), axis=1)
df['NL48 Count All Uses'] = df.apply(lambda row: all_counts(row, nl48_all_uses), axis=1)
df['NL48 Count All Uses-Ag'] = df.apply(lambda row: counts_both(row, 'NL48 All Uses', nl48_col_ag), axis=1)
df['NL48 Count All Uses-NonAg'] = df.apply(lambda row: counts_both(row, 'NL48 All Uses', nl48_col_non_ag), axis=1)


df.to_csv(out_location + os.sep + csv, encoding="utf-8")
