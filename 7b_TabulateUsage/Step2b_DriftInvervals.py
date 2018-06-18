import pandas as pd
import os
import datetime
import numpy as np

# TODO Clean up on/off field - move to functions
# TODO FILTER NE/NLAAs

chemical_name = 'Carbaryl'
chemical_name = 'Methomyl'
use_lookup = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
             r'\SupportingTables' + os.sep + chemical_name + "_Uses_lookup_20180430.csv"

on_off_excel = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
               r'\_ExternalDrive\_CurrentSupportingTables\Species Collection_cmr_jc.xlsx'
max_drift = '765'
l48_BE_interval = r'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
                  r'\Endangered Species Pilot Assessments - OverlapTables\SupportingTables\ParentTables' \
                  r'\R_SprayInterval_20180522_Region.csv'

nl48_BE_interval = 'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
                   '\Endangered Species Pilot Assessments - OverlapTables\SupportingTables\ParentTables' \
                   '\R_SprayInterval_20180522_NL48Range.csv'

master_list = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'

col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']

out_location = 'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
               '\Endangered Species Pilot Assessments - OverlapTables\ChemicalTables'

find_file_type = os.path.basename(l48_BE_interval)
if find_file_type.startswith('R'):
    file_type = 'R_'
else:
    file_type = 'CH_'


today = datetime.datetime.today()
date = today.strftime('%Y%m%d')


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


def on_off_field(row, cols, df, on_off_species):
    ent_id = str(row['EntityID'])
    col = [v for v in cols if v.endswith("_0")]
    if ent_id in on_off_species:
        for i in col:
            df.loc[df['EntityID'] == ent_id, i] = 0


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# set up directory
create_directory(out_location + os.sep + chemical_name)
out_path = out_location + os.sep + chemical_name

# load input files
use_lookup_df = pd.read_csv(use_lookup, dtype=object)
l48_df = pd.read_csv(l48_BE_interval, dtype=object)
nl48_df = pd.read_csv(nl48_BE_interval,dtype=object)

# ser entity id to str
l48_df.ix[:, 'EntityID'] = l48_df['EntityID'].map(lambda x: x).astype(str)
l48_df.ix[:, 'EntityID'] = l48_df['EntityID'].map(lambda x: x).astype(str)

# Extract columns that will be adjusted for redundancy from input tables
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

on_off_cult_cols = [x if x.split("_")[0] == 'CONUS' else 'NL48_' + x.split("_")[1] for x in on_off_cult_cols]
on_off_cult_cols = list(set(on_off_cult_cols))

on_off_pasture_cols = on_off_pasture['FinalColHeader'].values.tolist()
on_off_pasture_cols = [x if x.split("_")[0] == 'CONUS' else 'NL48_' + x.split("_")[1] for x in on_off_pasture_cols]
on_off_pasture_cols = list(set(on_off_pasture_cols))

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
base_sp_df.ix[:, 'EntityID'] = base_sp_df['EntityID'].map(lambda x: x).astype(str)


# STEP 1 :     ##Filter L48 AA
out_path_no_adjustment = out_path + os.sep + 'No Adjustment'
create_directory(out_path_no_adjustment)

# Extact column head that are need for chemical based on input file
aa_layers_CONUS = use_lookup_df.loc[(use_lookup_df['Included AA'] == 'x') & (use_lookup_df['Region'] == 'CONUS')]
col_prefix_CONUS = aa_layers_CONUS['FinalColHeader'].values.tolist()

# trandorm input table for nl48 so it can be filtered  by use and set header - remove index row
df_t = l48_df.T.reset_index()
new_header = df_t.iloc[1]
df_t.columns= new_header

# add column that extract the col_prefix from use name with interval that is in the first column after the
# transformation, header is Entity ID
df_t ['FinalColHeader'] = df_t['EntityID'].map(lambda x: x.split("_")[0]+"_"+x.split("_")[1] if len (x.split("_")) >1 else 0).astype(str)
# filter table to just those col_prefix needed for chemical
filter_df_t = df_t[(df_t['FinalColHeader'].isin(col_prefix_CONUS))]
# filter species info from table
species_info = df_t.iloc[0:5]
# concats the two filtered tables
m_df = pd.concat([species_info, filter_df_t ], axis =0).reset_index(drop=True)
m_df.drop('FinalColHeader', axis=1, inplace=True)
# transforms tables so it has the use intervals for columns and species as rows again, set col header, remove old index
# rows and drops extra columns
aa_l48 = m_df.T
new_header = aa_l48.iloc[0]
aa_l48.columns= new_header
aa_l48 = aa_l48.iloc[1:]
aa_l48['EntityID'] = aa_l48.index
aa_l48 = aa_l48.reset_index(drop=True)
[aa_l48.drop(m, axis=1, inplace=True) for m in aa_l48.columns.values.tolist() if not m.startswith('CONUS') and m != 'EntityID']

# list of columns need for the conus tables - used in other steps
conus_cols = aa_l48.columns.values.tolist()

# merges table to base species informations and exports to csv
out_aa_l48 = pd.merge(base_sp_df, aa_l48, on='EntityID', how='left')
out_aa_l48.to_csv(out_path_no_adjustment + os.sep + file_type + 'CONUS_Step2_Intervals_' + chemical_name + '.csv')

# STEP 1B  : # ##Filter NL48 AA
# Extact column head that are need for chemical based on input file
aa_layers_NL48 = use_lookup_df.loc[(use_lookup_df['Included AA'] == 'x') & (use_lookup_df['Region'] != 'CONUS')]
col_prefix_NL48 = aa_layers_NL48['FinalColHeader'].values.tolist()
col_prefix_NL48_noregion = [x if x.split("_")[0] == 'CONUS' else 'NL48_' + x.split("_")[1] for x in col_prefix_NL48]
cols_all_uses_nl48 = nl48_df.columns.values.tolist()

# generate the list cols needed for the NL48 - TODO update to the transoform and filter done for CONUS
cols_aa_nl48 = []

for v in col_prefix_NL48:
    for col in cols_all_uses_nl48:
        if col.startswith(v):
            cols_aa_nl48.append(col)
        else:
            pass
cols_aa_nl48.insert(0, 'EntityID')

# sums the values from each individual region into the the value for combined NL48 region based on common use intervals
# ie AK _0 + AS_0 + HI_0 ...
intervals = []
for i in cols_aa_nl48:
    int_bin = i.split("_")[len(i.split("_")) - 1]
    if int_bin not in intervals and int_bin != 'EntityID':
        intervals.append(int_bin)

for t in intervals:
    binned_intervals = [x for x in cols_aa_nl48 if x.endswith("_" + t)]
    binned_uses_list = [x.split("_")[1] for x in binned_intervals]
    binned_uses_list = list(set(binned_uses_list))

for z in binned_uses_list:
    for t in intervals:
        binned_use = [p for p in binned_intervals if p.split("_")[1] == z]
        out_nl48_col = 'NL48_' + z + "_" + t
        nl48_df.ix[:,binned_use] = nl48_df.ix[:,binned_use].apply(pd.to_numeric, errors='coerce')
        nl48_df[out_nl48_col] = nl48_df[binned_use].sum(axis=1)
# list of columns need for the nl48 tables - used to filter here and in other steps
out_cols = nl48_df.columns.values.tolist()
nl48_cols_f = [k for k in out_cols if k.startswith('NL48')]
nl48_cols_f.insert(0, 'EntityID')

# filter to just the cols needed for  nl48
aa_nl48 = nl48_df.ix[:, nl48_cols_f]

# merges table to base species informations and exports to csv
out_aa_nl48 = pd.merge(base_sp_df, aa_nl48, on='EntityID', how='left')
out_aa_nl48.to_csv(out_path_no_adjustment + os.sep + file_type + 'NL48_Step2_Intervals_' + chemical_name + '.csv')

# STEP 2  : Adjusted for redundancy
# redundancy adjustments - adjusts the direct overlap based on the ag, nonag and composite factors that are calculated
# Merges nl48 and conus into one df so that routine is the same as the 2a_Tables_overlap
#  TODO move this routine to a function or class object so both can call the same code
chemical_step1 = pd.merge(aa_l48, aa_nl48, on='EntityID', how='left')
# ## set up out path
out_path_redundancy = out_path+ os.sep + 'Redundancy_only'
create_directory(out_path_redundancy)
chemical_step1.drop_duplicates()
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


# Confirms all number are set to a numeric data type
chemical_step1.ix[:, use_direct_only_conus_ag] = chemical_step1.ix[:,use_direct_only_conus_ag].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:, use_direct_only_conus_nonag] = chemical_step1.ix[:,use_direct_only_conus_nonag].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:, use_direct_only_nl48_ag] = chemical_step1.ix[:,use_direct_only_nl48_ag].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:, use_direct_only_nl48_nonag] = chemical_step1.ix[:,use_direct_only_nl48_nonag].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:, use_direct_only_conus_ag_aa + use_direct_only_conus_nonag_aa] = chemical_step1.ix[:, use_direct_only_conus_ag_aa + use_direct_only_conus_nonag_aa].apply(pd.to_numeric, errors='coerce')
chemical_step1.ix[:, use_direct_only_nl48_ag_aa + use_direct_only_nl48_nonag_aa] = chemical_step1.ix[:, use_direct_only_nl48_ag_aa + use_direct_only_nl48_nonag_aa].apply(pd.to_numeric, errors='coerce')
if not skip_non_ag_adjustment:
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
# Ag factor is applied to all Ag layer, Non-Ag and Composite factor is applied to JUST the Non-Ag uses
# Parameterr set by ESA team Summer/Fall 2017
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
    chemical_step1.ix[:,['CONUS_Ag_Ag_Factor','NL48_Ag_Ag_Factor']]= chemical_step1.ix[:,['CONUS_Ag_Ag_Factor','NL48_Ag_Ag_Factor']].fillna(0)

# Applies the factor adjustments Ag use layer by Ag factor, Non Ag use layers by both NonAg factor and composite factor
chemical_step1.ix[:,use_direct_only_conus_ag] = chemical_step1.ix[:,use_direct_only_conus_ag].div((chemical_step1['CONUS_Ag_Ag_Factor']).where(chemical_step1['CONUS_Ag_Ag_Factor']!= 0, np.nan), axis = 0)
chemical_step1.ix[:,use_direct_only_nl48_ag] = chemical_step1.ix[:,use_direct_only_nl48_ag].div(chemical_step1['NL48_Ag_Ag_Factor'].where(chemical_step1['NL48_Ag_Ag_Factor']!= 0, np.nan), axis = 0)
if not skip_non_ag_adjustment:
    chemical_step1.ix[:,use_direct_only_conus_nonag] = chemical_step1.ix[:,use_direct_only_conus_nonag].div(chemical_step1['CONUS_NonAg_NonAg_Factor'].where(chemical_step1['CONUS_NonAg_NonAg_Factor']!= 0, np.nan), axis = 0)
    chemical_step1.ix[:,use_direct_only_nl48_nonag] = chemical_step1.ix[:,use_direct_only_nl48_nonag].div(chemical_step1['NL48_NonAg_NonAg_Factor'].where(chemical_step1['NL48_NonAg_NonAg_Factor']!= 0, np.nan), axis = 0)
    chemical_step1.ix[:,use_direct_only_conus_nonag] = chemical_step1.ix[:,use_direct_only_conus_nonag].div(chemical_step1['CONUS_Composite_Factor'].where(chemical_step1['CONUS_Composite_Factor']!= 0, np.nan), axis = 0)
    chemical_step1.ix[:,use_direct_only_nl48_nonag] = chemical_step1.ix[:,use_direct_only_nl48_nonag].div(chemical_step1['NL48_Composite_Factor'].where(chemical_step1['NL48_Composite_Factor']!= 0, np.nan), axis = 0)

chemical_step1 = pd.merge(base_sp_df, chemical_step1, on='EntityID', how='left')
chemical_step1.fillna(0, inplace=True)
chemical_step1['EntityID'] = chemical_step1['EntityID'].map(lambda x: x).astype(str)

aa_l48 = chemical_step1 [conus_cols]
aa_nl48 = chemical_step1[nl48_cols_f]
aa_l48.to_csv(out_path_redundancy + os.sep + 'CONUS_Step2_Intervals_' + file_type + chemical_name + '.csv')
aa_nl48.to_csv(out_path_redundancy + os.sep + 'NL48_Step2_Intervals_' + file_type + chemical_name + '.csv')


# STEP 3 - ON/ OFF ADJUSTMENTS

# on off field adjustments- direct overlap is set to zero if for species identified as not be present on the on/off
# habitat group types  - decided to set to zero rather than re-calucating the overlap dut to the impact of drift in
# for the whole range; if pixels are remove a new acre value for the species would need to be calculated and drift
# overlap would be greater than 100 unless the drift pixel are also removed - it is possible to do this but not in the
# time frame provided so it was decided to set the direct overlap to zero Fall 2017

# ##Filter L48 AA
out_path_on_off = out_path + os.sep + 'On_Off_Field'
create_directory(out_path_on_off)

cult_use_cols = []
for z in conus_cols:
    for p in on_off_cult_cols:
        if z.startswith(p):
            cult_use_cols.append(z)

pasture_use_cols = []
for z in conus_cols:
    for p in on_off_pasture_cols:
        if z.startswith(p):
            pasture_use_cols.append(z)

# orchard_use_cols = []
# for z in conus_cols:
#     for p in on_off_orchard_cols:
#         if z.startswith(p):
#           if z.endswith("_0"):
#               orchard_use_cols.append(z)


aa_l48.apply(lambda row: on_off_field(row, cult_use_cols, aa_l48, on_off_cult_species), axis=1)
aa_l48.apply(lambda row: on_off_field(row, pasture_use_cols, aa_l48, on_off_pasture_species), axis=1)
aa_l48 = pd.merge(base_sp_df, aa_l48, on='EntityID', how='left')
aa_l48.fillna(0, inplace=True)
aa_l48.to_csv(out_path_on_off + os.sep + file_type + 'CONUS_Step2_Intervals_' + chemical_name + '.csv')

# # ##Filter NL48 AA

cult_use_cols = []
for z in nl48_cols_f:
    for p in on_off_cult_cols:
        if z.startswith(p):
            cult_use_cols.append(z)

pasture_use_cols = []
for z in nl48_cols_f:
    for p in on_off_pasture_cols:
        if z.startswith(p):
            pasture_use_cols.append(z)

# orchard_use_cols = []
# for z in nl48_cols_f:
#     for p in on_off_orchard_cols:
#         if z.startswith(p):
#           if z.endswith("_0"):
#               orchard_use_cols.append(z)

aa_nl48.apply(lambda row: on_off_field(row, cult_use_cols, aa_nl48, on_off_cult_species), axis=1)
aa_nl48.apply(lambda row: on_off_field(row, pasture_use_cols, aa_nl48, on_off_pasture_species), axis=1)
aa_nl48 = pd.merge(base_sp_df, aa_nl48, on='EntityID', how='left')
aa_l48.fillna(0, inplace=True)
# removed date sharepoint
aa_nl48.to_csv(out_path_on_off + os.sep + file_type + 'NL48_Step2_Intervals_' + chemical_name + '.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
