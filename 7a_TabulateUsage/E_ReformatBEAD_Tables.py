import pandas as pd
import datetime
import os
import numpy as np

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# NOTES:
# Assumptions:
# ~ Percentages are base 100 ie 0-100 not 0-1
# ~ Formatting needs are met; described below
#   ~ State/ BEAD combinations are unique
#   ~ assumptions for crops will not change - all crops to be considered will be included in the SUUM
#   ~ assumptions related to assumed PCTs will not change - all PCTs values must be final before running overlap
#           ~**CHANGES TO ASSUMPTIONS WILL MEAN UPDATING ALL OVERLAP TABLES**
#
# FORMATTING:
#       ~ Required columns with data:
#               ~Crop (BEAD crop)
#               ~State (for row of data)
#               ~Acreage (of BEAD crop in State),
#               ~Min PCT (from SUUM or assumed based on agreements)
#               ~Max PCT(from SUUM or assumed based on agreements)
#               ~Avg PCT(from SUUM or assumed based on agreements)
#               ~UDL(applied from crosswalk)
#               ~AgClass (applied from crosswalk)
#
#       ~ in_long_table: must include all crops to be considering for the chemical; changes to this will require
#               generating a new input table **BEFORE** overlap can be run
#       ~ ****ALL STATE/ BEAD CROP COMBINATION MUST BE UNIQUE****
#       ~ All BEAD crops must be crossed to use layer category
#       ~ All BEAD crops/use must be flagged as either Ag or NonAg to apply aggregated PCT to the all Ag composite
#
# Known limitations:
#       ~ PCTs are inflated; undisclosed acres filtered out

# Propazine
chemical = 'Glyphosate'  # chemical names
in_long_table = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\Glyphosate\GLY_withsurrogate_v2_20200811.csv" # long table for state crops
outlocation = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\Glyphosate\PCT'


state_list = ['ALABAMA', 'ALASKA', 'ARIZONA', 'ARKANSAS', 'CALIFORNIA', 'COLORADO', 'CONNECTICUT', 'DELAWARE',
              'DISTRICT OF COLUMBIA','FLORIDA', 'GEORGIA', 'HAWAII', 'IDAHO', 'ILLINOIS', 'INDIANA', 'IOWA', 'KANSAS',
              'KENTUCKY', 'LOUISIANA','MAINE', 'MARYLAND', 'MASSACHUSETTS', 'MICHIGAN', 'MINNESOTA', 'MISSISSIPPI',
              'MISSOURI', 'MONTANA','NEBRASKA', 'NEVADA', 'NEW HAMPSHIRE', 'NEW JERSEY', 'NEW MEXICO', 'NEW YORK',
              'NORTH CAROLINA','NORTH DAKOTA', 'OHIO', 'OKLAHOMA', 'OREGON', 'PENNSYLVANIA', 'RHODE ISLAND',
              'SOUTH CAROLINA','SOUTH DAKOTA', 'TENNESSEE', 'TEXAS', 'UTAH', 'VERMONT', 'VIRGINIA', 'WASHINGTON',
              'WEST VIRGINIA','WISCONSIN', 'WYOMING', 'AMERICAN SAMOA',	'GUAM',
              'COMMONWEALTH OF THE NORTHERN MARIANA ISLANDS','PUERTO RICO', 'UNITED STATES VIRGIN ISLANDS']


def weight_pct(df, working_df, st, group):
    # PCT for UDL = (SUM[PCT BEAD Crop*(Avg. Annual Crop Acres Grown)]/Total of Avg Annual Acres in State
    total_crop_state = df['Acreage'].sum()

    df.ix[:, 'Max PCT'] = df.ix[:, 'Max PCT'] / 100
    df.ix[:, 'Treated_max'] = ( df.ix[:,'Acreage'] * df.ix[:, 'Max PCT'])
    df.ix[:, 'Max PCT Group'] = (df['Treated_max'].sum()) / (total_crop_state)
    total_treated_max= df['Treated_max'].sum()

    df.ix[:, 'Avg PCT'] = df.ix[:, 'Avg PCT'] / 100
    df.ix[:, 'Treated_avg'] = ( df.ix[:,'Acreage']* df.ix[:, 'Avg PCT'])
    df.ix[:, 'Avg PCT Group'] = (df['Treated_avg'].sum()) / (total_crop_state)
    total_treated_avg = df['Treated_avg'].sum()

    df.ix[:, 'Min PCT'] = df.ix[:, 'Min PCT'] / 100
    df.ix[:, 'Treated_min'] = ( df.ix[:,'Acreage']* df.ix[:, 'Min PCT'])
    df.ix[:, 'Min PCT Group'] = df['Treated_min'].sum() / (total_crop_state)
    total_treated_min = df['Treated_min'].sum()

    working_df = pd.concat([working_df, df], axis=0)

    working_df .loc[((working_df ['State'] == st) & (working_df ['UDL'] == group)), ['Treated Max', 'Treated Avg', 'Treated Min']] = total_treated_max, total_treated_avg, total_treated_min

    return working_df


def no_use(df, working_df):
    df.ix[:, 'Max PCT Group'] = 0
    df.ix[:, 'Avg PCT Group'] = 0
    df.ix[:, 'Min PCT Group'] = 0
    working_df = pd.concat([working_df, df], axis=0)
    return working_df


today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

in_df = pd.read_csv(in_long_table,
                    dtype={'State': str, 'Crop': str, 'Acreage': str, 'Min PCT': float, 'Max PCT': float,
                           'Avg PCT': float, 'UDL': str, 'CompositeClass': str}
                    )
[in_df.drop(m, axis=1, inplace=True) for m in in_df.columns.values.tolist() if m.startswith('Unnamed')]
in_df['Acreage'] = in_df['Acreage'].apply(lambda x: -9 if x == 'D' or x == 'Z' else x)
in_df['Acreage'] = in_df['Acreage'].apply(lambda x: str(x).replace(',', '')).astype(float)

in_df['Treated Max'] = ""
in_df['Treated Avg'] = ''
in_df['Treated Min']= ''
out_path = outlocation + os.sep + chemical
if not os.path.exists(out_path):
    os.mkdir(out_path)

out_df = pd.DataFrame(columns=[
    ['Crop', 'State', 'Acreage', 'Min PCT', 'Max PCT', 'Avg PCT', 'UDL',
     'Max PCT Group', 'Avg PCT Group', 'Min PCT Group', 'Treated Max ', 'Treated Avg', 'Treated Min']])


states = list(set(in_df['State'].values.tolist()))
# Replace -9999 with 0
# out_df = no_use((in_df.loc[in_df['Acreage'] == -9999]).copy(), out_df)

for state in states:
    print state
    state_df = in_df.loc[in_df['State'] == state].copy()
    UDLes = list(set(state_df['UDL'].values.tolist()))
    for UDL in UDLes:
        print UDL
        UDL_df = state_df.loc[(in_df['UDL'] == UDL) & (state_df['Acreage'] != -9999)].copy()
        if len(UDL_df) > 0:
            # print out_df.columns.values.tolist()
            out_df = weight_pct(UDL_df, out_df, state, UDL)

out_df.fillna(0, inplace=True)
out_df.to_csv(out_path + os.sep + chemical + "_working" + "_" + date + '.csv')

max = out_df.pivot_table(values='Max PCT Group', index=['UDL'], columns='State')
max = max.reindex(columns=state_list)

min = out_df.pivot_table(values='Min PCT Group', index=['UDL'], columns='State')
min = min.reindex(columns=state_list)

avg = out_df.pivot_table(values='Avg PCT Group', index=['UDL'], columns='State')
avg = avg.reindex(columns=state_list)

max.to_csv(out_path + os.sep + chemical + "_max" + "_" + date + '.csv')
min.to_csv(out_path + os.sep + chemical + "_min" + "_" + date + '.csv')
avg.to_csv(out_path + os.sep + chemical + "_avg" + "_" + date + '.csv')

max_t = out_df.pivot_table(values='Treated Max', index=['UDL'], columns='State')
max_t = max_t.reindex(columns=state_list)
min_t = out_df.pivot_table(values='Treated Min', index=['UDL'], columns='State')
min_t = min_t.reindex(columns=state_list)
avg_t = out_df.pivot_table(values='Treated Avg', index=['UDL'], columns='State')
avg_t = avg_t.reindex(columns=state_list)

max_t.to_csv(out_path + os.sep + chemical + "_max_treated" + "_" + date + '.csv')
min_t.to_csv(out_path + os.sep + chemical + "_min_treated" + "_" + date + '.csv')
avg_t.to_csv(out_path + os.sep + chemical + "_avg_treated" + "_" + date + '.csv')

# used pivot rather than picot_table because values are strings
sur_crop_table = out_df.pivot_table(values='Surrogate PCT', index=['CONCAT USE SITE'], columns='State',aggfunc =np.sum)
sur_crop_table .to_csv(out_path + os.sep + chemical + "_Surrogate PCTs_Crops" + "_" + date + '.csv')
sur_udl_table = out_df.pivot_table(values='Surrogate PCT', index=['UDL'], columns='State',aggfunc =np.sum)
sur_udl_table .to_csv(out_path + os.sep + chemical + "_Surrogate PCTs_UDL" + "_" + date + '.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
