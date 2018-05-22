import pandas as pd
import os
import datetime


# Be sure on off field is accounted for

chemical_name = 'Methomyl'
#chemical_name = 'Carbaryl'
use_lookup = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
             r'\SupportingTables' + os.sep + chemical_name + "_Step1_Uses_lookup_20180430.csv"

max_drift = '765'
l48_BE_sum = r'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
             r'\Endangered Species Pilot Assessments - OverlapTables\SupportingTables\ParentTables' \
             r'\R_AllUses_BE_L48_20180522.csv'

nl48_BE_sum = r'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
              r'\Endangered Species Pilot Assessments - OverlapTables\SupportingTables\ParentTables' \
              '\R_AllUses_BE_NL48_20180522.csv'

master_list = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'

col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']

out_location = 'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\ChemicalTables'

on_off_species =[]

find_file_type = os.path.basename(l48_BE_sum)
if find_file_type.startswith('R'):
    file_type = 'R_'
else:
    file_type = 'CH_'

def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)

def step_1_ED(row, col_l48):
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
    return value

def on_off_field(row, cols, df):
    ent_id = row['EntityID']
    if ent_id in on_off_species:
        col = [v for v in cols if v.endswith("_0")]
        direct_over = row[col[0]]
        df.loc[df['EntityID'] == ent_id , [col[0]]]= 0
        for other_col in cols:
            if other_col == col[0]:
                pass
            else:
                value = row[other_col]
                df.loc[df['EntityID'] == ent_id , [other_col]]= value - direct_over
    else:
        pass

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_directory(out_location + os.sep + chemical_name)
out_path = out_location + os.sep + chemical_name
use_lookup_df = pd.read_csv(use_lookup)
l48_df = pd.read_csv(l48_BE_sum)
nl48_df = pd.read_csv(nl48_BE_sum)
list_final_uses = list(set(use_lookup_df['FinalUseHeader'].values.tolist()))
collapsed_dict = {}

# ## Species info from master list
species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]
base_sp_df['EntityID'] = base_sp_df['EntityID'].map(lambda x: x).astype(str)

# ##Filter L48 AA
aa_layers_CONUS = use_lookup_df.loc[((use_lookup_df['Action Area'] == 'x') | (use_lookup_df['other layer'] == 'x')) & (
    use_lookup_df['Region'] == 'CONUS')]
col_prefix_CONUS = aa_layers_CONUS['FinalColHeader'].values.tolist()
col_selection_aa = ['EntityID']
for col in col_prefix_CONUS:
    col_selection_aa.append(col + "_0")
    if len(aa_layers_CONUS.loc[(aa_layers_CONUS['FinalColHeader'] == col) & (aa_layers_CONUS['ground'] == 'x')]) > 0:
        col_selection_aa.append(col + "_305")
    if len(aa_layers_CONUS.loc[(aa_layers_CONUS['FinalColHeader'] == col) & (aa_layers_CONUS['aerial'] == 'x')]) > 0:
        col_selection_aa.append(col + "_765")

chemical_step1 = l48_df[col_selection_aa]
final_use = aa_layers_CONUS.loc[(aa_layers_CONUS['Action Area'] == 'x') & (aa_layers_CONUS['aerial'] == 'x')], [
    'FinalColHeader']
if len(aa_layers_CONUS.loc[(aa_layers_CONUS['Action Area'] == 'x') & (aa_layers_CONUS['aerial'] == 'x')]) > 0:
    final_use = str(final_use) + '_765'
else:
    final_use = str(final_use) + '_305'

chemical_step1 = pd.merge(base_sp_df, chemical_step1, on='EntityID', how='left')

# ##Filter NL48 AA
aa_layers_NL48 = use_lookup_df.loc[((use_lookup_df['Action Area'] == 'x') | (use_lookup_df['other layer'] == 'x')) & (
use_lookup_df['Region'] != 'CONUS')]
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

# on/off not part of step 1
# use_cols =[z for z in chemical_step1.columns.values.tolist() if z not in col_include_output]
# conus_cols = [p for p in use_cols if p.startswith('CONUS')]
# conus_cols.remove('CONUS_Federal Lands_0')
# nl48_cols = [p for p in use_cols if p.startswith('NL48')]
#
# chemical_step1.apply(lambda row: on_off_field(row, conus_cols, chemical_step1), axis=1)
# chemical_step1.apply(lambda row: on_off_field(row, nl48_cols, chemical_step1), axis=1)

chemical_step1 ['Step 1 ED Comment'] = chemical_step1 .apply(lambda row: step_1_ED(row, 'CONUS_'+ chemical_name +" AA"
                                                                                   "_"+max_drift), axis=1)

chemical_step1.to_csv(out_path + os.sep + 'GIS_Step1_' + file_type + chemical_name + '.csv')
conus_cols = [v for v in chemical_step1.columns.values.tolist() if v.startswith('CONUS') or v in col_include_output]
nl48_cols_f = [v for v in chemical_step1.columns.values.tolist() if v.startswith('NL48') or v in col_include_output]

conus_df_step1 = chemical_step1[conus_cols]
nl48_df_step1 = chemical_step1[nl48_cols_f]

conus_df_step1.to_csv(out_path + os.sep + 'CONUS_Step1_' + file_type + chemical_name + '.csv')
nl48_df_step1.to_csv(out_path + os.sep + 'NL48_Step1_' + file_type + chemical_name + '.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
