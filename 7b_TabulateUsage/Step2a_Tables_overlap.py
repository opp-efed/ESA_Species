import pandas as pd
import os
import datetime
import numpy as np

# TODO take update redudancy loop out so that it pulls in the factor adjustments from the no_adjusted tables
chemical_name = 'Carbaryl'
# chemical_name = 'Methomyl'
use_lookup = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
             r'\SupportingTables' + os.sep + chemical_name + "_Uses_lookup_20180430.csv"

on_off_excel = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
               r'\_ExternalDrive\_CurrentSupportingTables\Species Collection_cmr_jc.xlsx'
max_drift = '765'
l48_BE_sum = r'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
             r'\Endangered Species Pilot Assessments - OverlapTables\SupportingTables\ParentTables' \
             r'\CH_AllUses_BE_L48_20180522.csv'

nl48_BE_sum = r'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
              r'\Endangered Species Pilot Assessments - OverlapTables\SupportingTables\ParentTables' \
              '\CH_AllUses_BE_NL48_20180522.csv'

master_list = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'

col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country','Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']


out_location = 'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
               '\Endangered Species Pilot Assessments - OverlapTables\ChemicalTables'


find_file_type = os.path.basename(l48_BE_sum)
if find_file_type.startswith('R'):
    file_type = 'R_'
else:
    file_type = 'CH_'


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


# Notes overlap cutoffs Summer/Fall 2017:  As part of the ESA streamlining set on 1% cut-off for and NE call and 5%
# cut-off for an NLAA issue.  For the purposes of precision this was set as anything < x.5 would meet the cut-off
# and >=x.5 would not - the specifics of < vs >= were not discussed with the team and can be adjusted if the group feels
# it should be something else.

# The quality of spatial files received by the Services was also discuss when setting these cut-offs.  JC proposed
# binning the files based on how confident we are in the file but the team felt setting these criteria for confidence
# would be too difficult.  Also, as the data we received need to be considered as 'best available' we should accept the
# files for face value. There are several concerns with this  - see pilot bird/PUR from diazinon as an example of all
# three of these concerns
#   1) When range is an overlap underestimated may be diluted due to the large denominator
#   2) Overlap may be overestimate due to the additional areas with use overlap
#   3) The use driving concern may not be correct due to concerns 1 and 2
#   4) These concerns can be further compounded when applying usage -  added Feb 2018


def step_1_ED(row, col_l48):
    col_nl48 = col_l48.replace('CONUS', 'NL48')
    # Federal lands statement needs to be updated to an and statement that will still catch overlap from either L48 or
    # NL48.  These or statements take advantage of species in both the L48 and NL48 have low overlap with federal lands
    # but these may not be true in all cases

    col_nl48 = col_l48.replace('CONUS','NL48')


    if row[col_l48] < 0.44 and row[col_nl48] < 0.44:
        value = 'No Effect - Overlap'
    elif row[col_l48] <= 4.45 and row[col_nl48] < 4.45:
        value = 'NLAA - Overlap - 5percent'
    elif row['CONUS_Federal Lands_0'] >= 98.5 or row['NL48_Federal Lands_0'] >= 99:
        value = 'No Effect - Federal Land'
    elif row['CONUS_Federal Lands_0'] >= 94.5 or row['NL48_Federal Lands_0'] >= 94.5:
        value = 'NLAA - Federal Land'
    elif row[col_l48] >= 0.45 or row[col_nl48] >= 0.45:
        value =  'May Affect'

    if file_type == 'CH_':
        if row['Source of Call final BE-Critical Habitat'] != 'Terr WoE' and \
                row['Source of Call final BE-Critical Habitat'] !='Aqua WoE' and \
                row['Source of Call final BE-Critical Habitat'] != 'Terr and Aqua WoE':
            value = 'No CritHab'
        if str(row['Source of Call final BE-Critical Habitat']).startswith('Qual'):
            value = 'Qualitative'

    if file_type == 'R_':

        if str(row['Source of Call final BE-Range']).startswith('Qu'):
            value = 'Qualitative'

    return value


def NLAA_overlap(row,cols_l48, cols_nl48):
    max_col = []
    #TODO do this dynamically
    # for col in cols:
    #     if col =='EntityID':
    #         pass
    #     elif col.split("_")[1].endswith('AA') or col.split("_")[2].endswith('_0') :
    #         pass
    #     elif col in max_col:
    #         pass
    #     else:
    #         use = col.split("_")[1]
    #         if col.split("_")[0] +"_" +use+"_765" in cols:
    #             if col.split("_")[0] +"_" +use+"_765" not in max_col:
    #                 max_col.append(col.split("_")[0] +"_" +use+"_765")
    #             elif col.split("_")[0] +"_" +use+"_305" in cols:
    #                 if col.split("_")[0] +"_" +use+"_765" not in cols:
    #                     if col.split("_")[0] +"_" +use+"_305" not in max_col:
    #                         max_col.append(col.split("_")[0] +"_" +use+"_305")
    #             else:
    #                 pass
    for col in cols_l48:


        if len(col.split("_"))==3:
            if col.split("_")[2] =='765':
                max_col.append(col)
            else:
                pass
    for col in cols_nl48:
        if len(col.split("_"))==3:

            if col.split("_")[2] =='765':
                max_col.append(col)
            else:
                pass
    final_col =[]
    for col in max_col:
        if col.split("_") [0]!= 'CONUS':
            val = 'NL48_' +col.split("_") [1] + "_"+col.split("_") [2]
            final_col.append(val)
        else:
            final_col.append(col)
    list_values = row[final_col]

    if len(list_values) ==0:
        value= row['Step 2 ED Comment']
    elif max(list_values) < 0.45:
        value = 'NLAA Overlap - 1percent'
    else:
        value = row['Step 2 ED Comment']
    if file_type == 'CH_':
        if row['Source of Call final BE-Critical Habitat'] != 'Terr WoE' and \
                row['Source of Call final BE-Critical Habitat'] !='Aqua WoE' and \
                row['Source of Call final BE-Critical Habitat'] != 'Terr and Aqua WoE':
            value=  'No CritHab'
        if str(row['Source of Call final BE-Critical Habitat']).startswith('Qual'):
            value = 'Qualitative'

    if file_type == 'R_':
        if str(row['Source of Call final BE-Range']).startswith('Qu'):
            value = 'Qualitative'
    return value


def on_off_field(row, cols, df, on_off_species):
    ent_id = str(row['EntityID'])
    if ent_id in on_off_species:
        col = [v for v in cols if v.endswith("_0")]
        for i in col:
            df.loc[df['EntityID'] == ent_id, i] = 0
        for other_col in cols:
            if other_col.endswith("_0"):
                pass
            else:
                direct_over = row[other_col.split("_")[0] + "_" + other_col.split("_")[1] + "_0"]
                value = row[other_col]
                df.loc[df['EntityID'] == ent_id, other_col] = value - direct_over

    else:
        pass


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

create_directory(out_location + os.sep + chemical_name)
out_path = out_location + os.sep + chemical_name
use_lookup_df = pd.read_csv(use_lookup)
l48_df = pd.read_csv(l48_BE_sum)
nl48_df = pd.read_csv(nl48_BE_sum)
list_final_uses = list(set(use_lookup_df['FinalUseHeader'].values.tolist()))

# Extract columns that will be adjusted for redundancy
other_layer = use_lookup_df.loc[(use_lookup_df['other layer'] == 'x')]
other_layer_cols = other_layer['Chem Table FinalColHeader'].values.tolist()
ag_headers = use_lookup_df.loc[(use_lookup_df['Included AA Ag'] == 'x')]
ag_cols = ag_headers['Chem Table FinalColHeader'].values.tolist()

# Try except loop to set a boolean variable to skip non ag adjustment if there are no non ag uses
try:
    non_ag_headers = use_lookup_df.loc[(use_lookup_df['Included AA NonAg'] == 'x')]
    nonag_cols = non_ag_headers['Chem Table FinalColHeader'].values.tolist()
    if len(non_ag_headers) == 0:
        skip_non_ag_adjustment = True
    else:
        skip_non_ag_adjustment = False
except TypeError:
    non_ag_headers =[]
    nonag_cols =[]
    skip_non_ag_adjustment = True

# ## Extract columns that will be adjusted for the on/off field
on_off_cult = use_lookup_df.loc[(use_lookup_df['On/Off_AG'] == 'x')]
on_off_pasture = use_lookup_df.loc[(use_lookup_df['On/Off_Pasture'] == 'x')]
# on_off_orchard = use_lookup_df.loc[(use_lookup_df['On/Off_Orchard_Plantation'] == 'x')]

on_off_cult_cols = on_off_cult['FinalColHeader'].values.tolist()
on_off_pasture_cols = on_off_pasture['FinalColHeader'].values.tolist()
# on_off_orchard_cols = on_off_orchard['FinalColHeader'].values.tolist()

# Load species that should be adjusted for on/off calls from master list
on_off_df = pd.read_excel(on_off_excel)
on_off_df['EntityID'] = on_off_df['EntityID'].map(lambda x: x).astype(str)
on_off_cult_df = on_off_df.loc[(on_off_df['On/Off_AG'] == 'OFF')]
on_off_cult_species = on_off_cult_df['EntityID'].values.tolist()

on_off_pasture_df = on_off_df.loc[(on_off_df['On/Off_Pasture'] == 'OFF')]
on_off_pasture_species = on_off_pasture_df['EntityID'].values.tolist()

on_off_orchards_df = on_off_df.loc[(on_off_df['On/Off_Orchard_Plantation'] == 'OFF')]
on_off_orchards_species  = on_off_orchards_df['EntityID'].values.tolist()

on_off_res_df = on_off_df.loc[(on_off_df['On/Off_Res'] == 'OFF')]
on_off_res_species = on_off_res_df['EntityID'].values.tolist()
collapsed_dict = {}

# ## Species info from master list
species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]
base_sp_df['EntityID'] = base_sp_df['EntityID'].map(lambda x: x).astype(str)
# ##Filter L48 AA
aa_layers_CONUS = use_lookup_df.loc[((use_lookup_df['Included AA'] == 'x') | (use_lookup_df['other layer'] == 'x')) & (
    use_lookup_df['Region'] == 'CONUS')]

col_prefix_CONUS = aa_layers_CONUS['FinalColHeader'].values.tolist()

col_selection_aa = ['EntityID']
for col in col_prefix_CONUS:
    col_selection_aa.append(col + "_0")
    if len(aa_layers_CONUS.loc[(aa_layers_CONUS['FinalColHeader'] == col) & (aa_layers_CONUS['ground'] == 'x')]) > 0:
        col_selection_aa.append(col + "_305")
    if len(aa_layers_CONUS.loc[(aa_layers_CONUS['FinalColHeader'] == col) & (aa_layers_CONUS['aerial'] == 'x')]) > 0:
        col_selection_aa.append(col + "_765")

cols_w_overlap_l48 = [v for v in l48_df.columns.values.tolist() if v in col_selection_aa]


l48_df = l48_df[cols_w_overlap_l48]
l48_df = l48_df.reindex(columns=col_selection_aa)

chemical_step1 = l48_df[col_selection_aa]
final_use = aa_layers_CONUS.loc[(aa_layers_CONUS['Action Area'] == 'x') & (aa_layers_CONUS['aerial'] == 'x')], [
    'FinalColHeader']
if len(aa_layers_CONUS.loc[(aa_layers_CONUS['Action Area'] == 'x') & (aa_layers_CONUS['aerial'] == 'x')]) > 0:
    final_use = str(final_use) + '_765'
else:
    final_use = str(final_use) + '_305'

chemical_step1 = pd.merge(base_sp_df, chemical_step1, on='EntityID', how='left')


# ##Filter NL48 AA
aa_layers_NL48 = use_lookup_df.loc[((use_lookup_df['Action Area'] == 'x') | (use_lookup_df['other layer'] == 'x')) | (
    use_lookup_df['Included AA'] == 'x') & (use_lookup_df['Region'] != 'CONUS')]
col_prefix_NL48 = aa_layers_NL48['FinalColHeader'].values.tolist()
col_selection_nl48 = ['EntityID']

for col in col_prefix_NL48:
    col_selection_nl48.append(col + "_0")
    if len(aa_layers_NL48.loc[(aa_layers_NL48['FinalColHeader'] == col) & (aa_layers_NL48['ground'] == 'x')]) > 0:
        col_selection_nl48.append(col + "_305")
    if len(aa_layers_NL48.loc[(aa_layers_NL48['FinalColHeader'] == col) & (aa_layers_NL48['aerial'] == 'x')]) > 0:
        col_selection_nl48.append(col + "_765")

cols_w_overlap = [v for v in nl48_df.columns.values.tolist() if v in col_selection_nl48]
nl48_df = nl48_df[cols_w_overlap]
nl48_df = nl48_df.reindex(columns=col_selection_nl48)

binned_use = []
for x in cols_w_overlap:
    if x in col_include_output:
        pass
    else:
        if x.split("_")[1] not in binned_use:
            binned_use.append(x.split("_")[1])

for bin_col in binned_use:
    list_col = [v for v in nl48_df.columns.values.tolist() if bin_col in v.split("_")]
    interval_list = list(set([t.split("_")[2] for t in list_col]))
    for interval in interval_list:
        list_col_interval = [z for z in list_col if z.endswith(interval)]
        # use_results_df = binned_df.apply(pd.to_numeric, errors='coerce')
        nl48_df['NL48_' + list_col_interval[0].split("_")[1] + "_" + list_col_interval[0].split("_")[2]] = nl48_df[
            list_col_interval].sum(axis=1)

out_col = [v for v in nl48_df.columns.values.tolist() if v.startswith('NL48_')]

out_col.insert(0, 'EntityID')
out_nl48_df = nl48_df[out_col]

chemical_step1 = pd.merge(chemical_step1, out_nl48_df, on='EntityID', how='left')

binned_use = []
for x in chemical_step1.columns.values.tolist():
    if x in col_include_output:
        pass
    else:
        if x.split("_")[0] + "_" + x.split("_")[1] not in binned_use and x.split("_")[1] != 'Federal Lands':
            binned_use.append(x.split("_")[0] + "_" + x.split("_")[1])

cult_use_cols = []

for z in chemical_step1.columns.values.tolist():
    for p in on_off_cult_cols:
        if z.startswith(p):
            cult_use_cols.append(z)

pasture_use_cols = []

for z in chemical_step1.columns.values.tolist():
    for p in on_off_pasture_cols:
        if z.startswith(p):
            pasture_use_cols.append(z)

# orchard_use_cols = []
# for z in chemical_step1.columns.values.tolist():
#     for p in on_off_orchard_cols:
#         if z.startswith(p):
#           if z.endswith("_0"):
#               orchard_use_cols.append(z)


# no adjustment
out_path_no_adjustment = out_path+ os.sep + 'No Adjustment'
create_directory(out_path_no_adjustment)
chemical_step1['Step 2 ED Comment'] = chemical_step1.apply(lambda row: step_1_ED(row, 'CONUS_' + chemical_name + " AA""_" + max_drift),axis=1)
chemical_step1['Step 2 ED Comment'] = chemical_step1.apply(lambda row: NLAA_overlap(row, col_selection_aa, cols_w_overlap),axis=1)
chemical_step1.fillna(0, inplace=True)
chemical_step1.to_csv(out_path_no_adjustment + os.sep + 'GIS_Step2_' + file_type + chemical_name + '.csv')

conus_cols = [v for v in chemical_step1.columns.values.tolist() if v.startswith('CONUS') or v in col_include_output]
nl48_cols_f = [v for v in chemical_step1.columns.values.tolist() if v.startswith('NL48') or v in col_include_output]

chemical_step1.fillna(0, inplace=True)
conus_df_step1 = chemical_step1[conus_cols]
nl48_df_step1 = chemical_step1[nl48_cols_f]

conus_df_step1.to_csv(out_path_no_adjustment + os.sep + 'CONUS_Step2_' + file_type + chemical_name + '.csv')
nl48_df_step1.to_csv(out_path_no_adjustment  + os.sep + 'NL48_Step2_' + file_type + chemical_name + '.csv')


# redundancy adjustments - adjusts the direct overlap based on the ag, nonag and composite factors that are calculated
# ## set up outpath
out_path_redundancy = out_path+ os.sep + 'Redundancy_only'
create_directory(out_path_redundancy)

# Set up the different list of columns that apply to conus vs nl48, aa, composites, use, ag and non-ag
# conus/nonl48
overlap_cols_conus = [x for x in conus_cols if x not in col_include_output]
overlap_cols_nl48 = [x for x in nl48_cols_f if x not in col_include_output]
# layer flag as other that should not be adjusted ie Federal Lands
other_layer_header  =[x for x in other_layer_cols]

# AA and composite columnms - HARD CODE composite and actions areas must have AA in the use name
aa_col_conus = [x for x in overlap_cols_conus if 'AA' in x.split('_')[1].split(' ')]
aa_col_nl48 = [x for x in overlap_cols_nl48  if 'AA' in x.split('_')[1].split(' ')]

# Use columns
use_col_conus = [x for x in overlap_cols_conus if not 'AA' in x.split('_')[1].split(' ') and (x.split('_')[0]+"_"+x.split('_')[1])  not in other_layer_cols]
use_col_nl48 = [x for x in overlap_cols_nl48  if not 'AA' in x.split('_')[1].split(' ')and (x.split('_')[0]+"_"+x.split('_')[1]) not in other_layer_cols]

# filters list so only the direct overlap is in list (represent by an _0 at the end of the column header
use_direct_only_conus_ag = [x for x in use_col_conus if x.endswith('_0') and (x.split('_')[0]+"_"+x.split('_')[1])   in ag_cols]
use_direct_only_conus_nonag = [x for x in use_col_conus if x.endswith('_0') and (x.split('_')[0]+"_"+x.split('_')[1])  in nonag_cols]
use_direct_only_nl48_ag  = [x for x in use_col_nl48  if x.endswith('_0')and (x.split('_')[0]+"_"+x.split('_')[1])  in ag_cols]
use_direct_only_nl48_nonag  = [x for x in use_col_nl48  if x.endswith('_0') and (x.split('_')[0]+"_"+x.split('_')[1])  in nonag_cols]

use_direct_only_conus_ag_aa = [x for x in aa_col_conus if x.endswith('_0') and 'Ag' in x.split("_")[1].split(" ")]
use_direct_only_conus_nonag_aa = [x for x in aa_col_conus if x.endswith('_0') and 'NonAg' in x.split("_")[1].split(" ")]
use_direct_only_nl48_ag_aa = [x for x in aa_col_nl48 if x.endswith('_0') and 'Ag' in x.split("_")[1].split(" ")]
use_direct_only_nl48_nonag_aa = [x for x in aa_col_nl48 if x.endswith('_0') and 'NonAg' in x.split("_")[1].split(" ")]
use_direct_only_conus_aa = [x for x in aa_col_conus if x.endswith('_0') and 'Ag' not in x.split("_")[1].split(" ")and 'NonAg' not in x.split("_")[1].split(" ")]
use_direct_only_nl48_aa  = [x for x in aa_col_nl48   if x.endswith('_0')and 'Ag' not in x.split("_")[1].split(" ")and 'NonAg' not in x.split("_")[1].split(" ")]

# filters list so only the ground and aerial overlap is in list (represent by an _305 and _765 at the end of the column header
use_direct_only_conus_ag_ground = [x for x in use_col_conus if x.endswith('_305') and (x.split('_')[0]+"_"+x.split('_')[1])   in ag_cols]
use_direct_only_conus_nonag_ground = [x for x in use_col_conus if x.endswith('_305') and (x.split('_')[0]+"_"+x.split('_')[1])  in nonag_cols]
use_direct_only_nl48_ag_ground  = [x for x in use_col_nl48  if x.endswith('_305')and (x.split('_')[0]+"_"+x.split('_')[1])  in ag_cols]
use_direct_only_nl48_nonag_ground  = [x for x in use_col_nl48  if x.endswith('_305') and (x.split('_')[0]+"_"+x.split('_')[1])  in nonag_cols]

use_direct_only_conus_ag_aerial = [x for x in use_col_conus if x.endswith('_765') and (x.split('_')[0]+"_"+x.split('_')[1])   in ag_cols]
use_direct_only_conus_nonag_aerial = [x for x in use_col_conus if x.endswith('_765') and (x.split('_')[0]+"_"+x.split('_')[1])  in nonag_cols]
use_direct_only_nl48_ag_aerial = [x for x in use_col_nl48  if x.endswith('_765')and (x.split('_')[0]+"_"+x.split('_')[1])  in ag_cols]
use_direct_only_nl48_nonag_aerial = [x for x in use_col_nl48  if x.endswith('_765') and (x.split('_')[0]+"_"+x.split('_')[1])  in nonag_cols]

# store an unadjusted version of the 4 dataframes Ag/NonAg for both the CONUS and L48
unadjusted_conus_ag = chemical_step1[['EntityID'] + use_direct_only_conus_ag].copy()
unadjusted_conus_nonag = chemical_step1[['EntityID'] + use_direct_only_conus_nonag].copy()
unadjusted_nl48_ag = chemical_step1[['EntityID'] + use_direct_only_nl48_ag].copy()
unadjusted_nl48_nonag = chemical_step1[['EntityID'] + use_direct_only_nl48_nonag].copy()

# Confirms all number are set to a numeric data type
chemical_step1.ix[:, use_direct_only_conus_ag] = chemical_step1.ix[:,use_direct_only_conus_ag].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:, use_direct_only_conus_nonag] = chemical_step1.ix[:,use_direct_only_conus_nonag].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:, use_direct_only_nl48_ag] = chemical_step1.ix[:,use_direct_only_nl48_ag].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:, use_direct_only_nl48_nonag] = chemical_step1.ix[:,use_direct_only_nl48_nonag].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:, use_direct_only_conus_ag_aa + use_direct_only_conus_nonag_aa] = chemical_step1.ix[:, use_direct_only_conus_ag_aa + use_direct_only_conus_nonag_aa].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:, use_direct_only_nl48_ag_aa + use_direct_only_nl48_nonag_aa] = chemical_step1.ix[:, use_direct_only_nl48_ag_aa + use_direct_only_nl48_nonag_aa].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:,use_direct_only_conus_aa[0]] = chemical_step1.ix[:,use_direct_only_conus_aa[0]].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:,use_direct_only_nl48_aa[0]] = chemical_step1.ix[:,use_direct_only_nl48_aa[0]].apply(pd.to_numeric, errors='coerce')

# Sum uses in the three groups ag, non ag and composites for both CONUS adn NL48
chemical_step1['CONUS_Sum_Ag'] = chemical_step1[use_direct_only_conus_ag].sum(axis=1)
chemical_step1['CONUS_Sum_NonAg'] = chemical_step1[use_direct_only_conus_nonag].sum(axis=1)
chemical_step1['NL48_Sum_Ag'] = chemical_step1[use_direct_only_nl48_ag].sum(axis=1)
chemical_step1['NL48_Sum_NonAg'] = chemical_step1[use_direct_only_nl48_nonag].sum(axis=1)

chemical_step1['CONUS_Sum_Composites'] = chemical_step1[use_direct_only_conus_ag_aa +use_direct_only_conus_nonag_aa].sum(axis=1)
chemical_step1['NL48_Sum_Composites'] = chemical_step1[use_direct_only_nl48_ag_aa +use_direct_only_nl48_nonag_aa].sum(axis=1)

# Calculates factors that will be used for adjustment
# Ag factor : sum of ag uses/ ag composite; Non-Ag factor sum of non ag use/ non ag composite;
# Composite factor sum of Ag and Non Ag composite / AA
# Ag factor is applied to all Ag layer, Non-Ag and Composite factor is applied to JUST the Non-Ag uses - ESA team Summer/Fall 2017
# Ag factor was added to Step 2 in Spring of 2018

chemical_step1 ['CONUS_Ag_Ag_Factor'] = chemical_step1['CONUS_Sum_Ag'].div((chemical_step1[use_direct_only_conus_ag_aa[0]]).where(chemical_step1[use_direct_only_conus_ag_aa[0]]!= 0, np.nan), axis = 0)
chemical_step1 ['NL48_Ag_Ag_Factor'] = chemical_step1['NL48_Sum_Ag'].div((chemical_step1[use_direct_only_nl48_ag_aa[0]]).where(chemical_step1[use_direct_only_nl48_ag_aa[0]]!= 0, np.nan), axis = 0)
if not skip_non_ag_adjustment:
    chemical_step1 ['CONUS_NonAg_NonAg_Factor'] = chemical_step1['CONUS_Sum_NonAg'].div((chemical_step1[use_direct_only_conus_nonag_aa[0]]).where(chemical_step1[use_direct_only_conus_nonag_aa[0]]!= 0, np.nan), axis = 0)
    chemical_step1 ['NL48_NonAg_NonAg_Factor'] = chemical_step1['NL48_Sum_NonAg'].div((chemical_step1[use_direct_only_nl48_nonag_aa[0]]).where(chemical_step1[use_direct_only_nl48_nonag_aa[0]]!= 0, np.nan), axis = 0)


chemical_step1 ['CONUS_Composite_Factor'] = chemical_step1['CONUS_Sum_Composites'].div((chemical_step1[use_direct_only_conus_aa[0]]).where(chemical_step1[use_direct_only_conus_aa[0]]!= 0, np.nan), axis = 0)
chemical_step1 ['NL48_Composite_Factor'] = chemical_step1['NL48_Sum_Composites'].div((chemical_step1[use_direct_only_nl48_aa[0]]).where(chemical_step1[use_direct_only_nl48_aa[0]]!= 0, np.nan), axis = 0)
if not skip_non_ag_adjustment:
    chemical_step1.ix[:,['CONUS_Ag_Ag_Factor','CONUS_NonAg_NonAg_Factor','NL48_Ag_Ag_Factor','NL48_NonAg_NonAg_Factor','CONUS_Composite_Factor','NL48_Composite_Factor']]= chemical_step1.ix[:,['CONUS_Ag_Ag_Factor','CONUS_NonAg_NonAg_Factor','NL48_Ag_Ag_Factor','NL48_NonAg_NonAg_Factor','CONUS_Composite_Factor','NL48_Composite_Factor']].fillna(0)
else:
    chemical_step1.ix[:,['CONUS_Ag_Ag_Factor','NL48_Ag_Ag_Factor','CONUS_Composite_Factor','NL48_Composite_Factor']]= chemical_step1.ix[:,['CONUS_Ag_Ag_Factor','NL48_Ag_Ag_Factor','CONUS_Composite_Factor','NL48_Composite_Factor']].fillna(0)

# Applies the factor adjustments Ag use layer by Ag factor, Non Ag use layers by both NonAg factor and composite factor
chemical_step1.ix[:,use_direct_only_conus_ag] = chemical_step1.ix[:,use_direct_only_conus_ag].div((chemical_step1['CONUS_Ag_Ag_Factor']).where(chemical_step1['CONUS_Ag_Ag_Factor']!= 0, np.nan), axis = 0)
chemical_step1.ix[:,use_direct_only_nl48_ag] = chemical_step1.ix[:,use_direct_only_nl48_ag].div(chemical_step1['NL48_Ag_Ag_Factor'].where(chemical_step1['NL48_Ag_Ag_Factor']!= 0, np.nan), axis = 0)
chemical_step1.ix[:,use_direct_only_conus_nonag] = chemical_step1.ix[:,use_direct_only_conus_nonag].div(chemical_step1['CONUS_Composite_Factor'].where(chemical_step1['CONUS_Composite_Factor']!= 0, np.nan), axis = 0)
chemical_step1.ix[:,use_direct_only_nl48_nonag] = chemical_step1.ix[:,use_direct_only_nl48_nonag].div(chemical_step1['NL48_Composite_Factor'].where(chemical_step1['NL48_Composite_Factor']!= 0, np.nan), axis = 0)

if not skip_non_ag_adjustment:
    chemical_step1.ix[:,use_direct_only_conus_nonag] = chemical_step1.ix[:,use_direct_only_conus_nonag].div(chemical_step1['CONUS_NonAg_NonAg_Factor'].where(chemical_step1['CONUS_NonAg_NonAg_Factor']!= 0, np.nan), axis = 0)
    chemical_step1.ix[:,use_direct_only_nl48_nonag] = chemical_step1.ix[:,use_direct_only_nl48_nonag].div(chemical_step1['NL48_NonAg_NonAg_Factor'].where(chemical_step1['NL48_NonAg_NonAg_Factor']!= 0, np.nan), axis = 0)


# store an adjusted version of the 4 dataframes Ag/NonAg for both the CONUS and L48
adjusted_conus_ag = chemical_step1[['EntityID'] + use_direct_only_conus_ag].copy()
adjusted_conus_nonag = chemical_step1[['EntityID'] + use_direct_only_conus_nonag].copy()
adjusted_nl48_ag = chemical_step1[['EntityID'] + use_direct_only_nl48_ag].copy()
adjusted_nl48_nonag = chemical_step1[['EntityID'] + use_direct_only_nl48_nonag].copy()

# Calculated the difference seen in the adjusted overlap, this difference is removed fromt he summarize
# ground and aerial values
difference_conus_ag = unadjusted_conus_ag.set_index('EntityID').subtract(adjusted_conus_ag.set_index('EntityID'))
difference_conus_nonag = unadjusted_conus_nonag.set_index('EntityID').subtract(adjusted_conus_nonag.set_index('EntityID'))
difference_nl48_ag = unadjusted_nl48_ag.set_index('EntityID').subtract(adjusted_nl48_ag.set_index('EntityID'))
difference_nl48_nonag = unadjusted_nl48_nonag.set_index('EntityID').subtract(adjusted_nl48_nonag.set_index('EntityID'))

# get col header for drift columnm by replacing the 0 with the standard values of 305 for ground and 765 for aerial
columns_conus_ground_ag = [x.replace("_0", "_305") for x in difference_conus_ag.columns.values.tolist()]
columns_conus_ground_nonag = [x.replace("_0", "_305") for x in difference_conus_nonag.columns.values.tolist()]
columns_conus_aerial_ag= [x.replace("_0", "_765") for x in difference_conus_ag.columns.values.tolist()]
columns_conus_aerial_nonag = [x.replace("_0", "_765") for x in difference_conus_nonag.columns.values.tolist()]

columns_nl48_ground_ag = [x.replace("_0", "_305") for x in difference_nl48_ag.columns.values.tolist()]
columns_nl48_ground_nonag = [x.replace("_0", "_305") for x in difference_nl48_nonag.columns.values.tolist()]
columns_nl48_aerial_ag= [x.replace("_0", "_765") for x in difference_nl48_ag.columns.values.tolist()]
columns_nl48_aerial_nonag = [x.replace("_0", "_765") for x in difference_nl48_nonag.columns.values.tolist()]

#Adjust ground
difference_conus_ag.columns = columns_conus_ground_ag
difference_conus_nonag.columns = columns_conus_ground_nonag
difference_nl48_ag.columns = columns_nl48_ground_ag
difference_nl48_nonag.columns = columns_nl48_ground_nonag
adjusted_conus_ag_ground = chemical_step1.ix[: , ['EntityID']+use_direct_only_conus_ag_ground].set_index('EntityID').subtract(difference_conus_ag)
adjusted_conus_nonag_ground = chemical_step1.ix[: , ['EntityID']+use_direct_only_conus_nonag_ground].set_index('EntityID').subtract(difference_conus_nonag)
adjusted_nl48_ag_ground = chemical_step1.ix[: , ['EntityID']+use_direct_only_nl48_ag_ground].set_index('EntityID').subtract(difference_nl48_ag)
adjusted_nl48_nonag_ground = chemical_step1.ix[: , ['EntityID']+use_direct_only_nl48_nonag_ground].set_index('EntityID').subtract(difference_nl48_nonag)

#Adjust aerial
difference_conus_ag.columns = columns_conus_aerial_ag
difference_conus_nonag.columns = columns_conus_aerial_nonag
difference_nl48_ag.columns = columns_nl48_aerial_ag
difference_nl48_nonag.columns = columns_nl48_aerial_nonag
adjusted_conus_ag_aerial = chemical_step1.ix[: , ['EntityID']+use_direct_only_conus_ag_aerial].set_index('EntityID').subtract(difference_conus_ag)
adjusted_conus_nonag_aerial = chemical_step1.ix[: , ['EntityID']+use_direct_only_conus_nonag_aerial].set_index('EntityID').subtract(difference_conus_nonag)
adjusted_nl48_ag_aerial = chemical_step1.ix[: , ['EntityID']+use_direct_only_nl48_ag_aerial].set_index('EntityID').subtract(difference_nl48_ag)
adjusted_nl48_nonag_aerial = chemical_step1.ix[: , ['EntityID']+use_direct_only_nl48_nonag_aerial].set_index('EntityID').subtract(difference_nl48_nonag)

# reset index to move entitid back to column for update to the master df
adjusted_conus_ag_ground = adjusted_conus_ag_ground.reset_index()
adjusted_conus_nonag_ground = adjusted_conus_nonag_ground.reset_index()
adjusted_conus_ag_aerial = adjusted_conus_ag_aerial.reset_index()
adjusted_conus_nonag_aerial = adjusted_conus_nonag_aerial.reset_index()
adjusted_nl48_ag_ground = adjusted_nl48_ag_ground.reset_index()
adjusted_nl48nonag_ground = adjusted_nl48_nonag_ground.reset_index()
adjusted_nl48ag_aerial = adjusted_nl48_ag_aerial.reset_index()
adjusted_nl48nonag_aerial = adjusted_nl48_nonag_aerial.reset_index()

# updates the drift columns of the master data frame
chemical_step1.ix[:,adjusted_conus_ag_ground.columns.tolist()] =  adjusted_conus_ag_ground.ix[:,adjusted_conus_ag_ground.columns.tolist()]
chemical_step1.ix[:,adjusted_conus_nonag_ground.columns.tolist()] =  adjusted_conus_nonag_ground.ix[:,adjusted_conus_nonag_ground.columns.tolist()]
chemical_step1.ix[:,adjusted_conus_ag_aerial.columns.tolist()] =  adjusted_conus_ag_aerial.ix[:,adjusted_conus_ag_aerial.columns.tolist()]
chemical_step1.ix[:,adjusted_conus_nonag_aerial.columns.tolist()] =  adjusted_conus_nonag_aerial.ix[:,adjusted_conus_nonag_aerial.columns.tolist()]

chemical_step1.ix[:,adjusted_nl48_ag_ground.columns.tolist()] =  adjusted_nl48_ag_ground.ix[:,adjusted_nl48_ag_ground.columns.tolist()]
chemical_step1.ix[:,adjusted_nl48_nonag_ground.columns.tolist()] =  adjusted_nl48_nonag_ground.ix[:,adjusted_nl48_nonag_ground.columns.tolist()]
chemical_step1.ix[:,adjusted_nl48_ag_aerial.columns.tolist()] =  adjusted_nl48_ag_aerial.ix[:,adjusted_nl48_ag_aerial.columns.tolist()]
chemical_step1.ix[:,adjusted_nl48_nonag_aerial.columns.tolist()] =  adjusted_nl48_nonag_aerial.ix[:,adjusted_nl48_nonag_aerial.columns.tolist()]

# Applies the effects determinations based on just redundancy and saves the table
chemical_step1.fillna(0, inplace=True)
chemical_step1['Step 2 ED Comment'] = chemical_step1.apply(lambda row: step_1_ED(row, 'CONUS_' + chemical_name + " AA""_" + max_drift),axis=1)
chemical_step1['Step 2 ED Comment'] = chemical_step1.apply(lambda row: NLAA_overlap(row, col_selection_aa, cols_w_overlap),axis=1)
chemical_step1.to_csv(out_path_redundancy  + os.sep + 'GIS_Step2_' + file_type + chemical_name + '.csv')
# Export tables for the CONUS and NL48

conus_df_step1 = chemical_step1[conus_cols]
nl48_df_step1 = chemical_step1[nl48_cols_f]
conus_df_step1.to_csv(out_path_redundancy  + os.sep + 'CONUS_Step2_' + file_type + chemical_name + '.csv')
nl48_df_step1.to_csv(out_path_redundancy   + os.sep + 'NL48_Step2_' + file_type + chemical_name + '.csv')

# on off field adjustments- direct overlap is set to zero if for species identified as not be present on the on/off
# habitat group types  - decided to set to zero rather than re-calucating the overlap dut to the impact of drift in
# for the whole range; if pixels are remove a new acre value for the species would need to be calculated and drift
# overlap would be greater than 100 unless the drift pixel are also removed - it is possible to do this but not in the
# time frame provided so it was decided to set the direct overlap to zero Fall 2017

out_path_on_off = out_path+ os.sep + 'On_Off_Field'
create_directory(out_path_on_off)
chemical_step1['EntityID'] = chemical_step1['EntityID'].map(lambda x: x).astype(str)
chemical_step1.apply(lambda row: on_off_field(row, cult_use_cols, chemical_step1, on_off_cult_species), axis=1)
chemical_step1.apply(lambda row: on_off_field(row, pasture_use_cols, chemical_step1, on_off_pasture_species), axis=1)

out_path_on_off = out_path+ os.sep + 'On_Off_Field'
chemical_step1['Step 2 ED Comment'] = chemical_step1.apply(lambda row: step_1_ED(row, 'CONUS_' + chemical_name + " AA""_" + max_drift),axis=1)
chemical_step1['Step 2 ED Comment'] = chemical_step1.apply(lambda row: NLAA_overlap(row, col_selection_aa, cols_w_overlap),axis=1)
chemical_step1.fillna(0, inplace=True)
chemical_step1.to_csv(out_path_on_off + os.sep + 'GIS_Step2_' + file_type + chemical_name + '.csv')

conus_df_step1 = chemical_step1[conus_cols]
nl48_df_step1 = chemical_step1[nl48_cols_f]

conus_df_step1.to_csv(out_path_on_off + os.sep + 'CONUS_Step2_' + file_type + chemical_name + '.csv')
nl48_df_step1.to_csv(out_path_on_off  + os.sep + 'NL48_Step2_' + file_type + chemical_name + '.csv')


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
