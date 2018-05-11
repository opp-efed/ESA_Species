import pandas as pd
import os
import datetime

# Be sure on off field is accounted for

chemical_name = 'Diazinon'

use_lookup = r'L:\ESA\Results\diazinon\RangeUses_lookup.csv'
max_drift = '765'
l48_BE_sum = r'L:\ESA\Results\diazinon\out_tables\ParentTables\no usage\SprayInterval_IntStep_30_MaxDistance_1501' \
             r'\R_AllUses_BE_L48_20180212.csv'

master_list = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'

col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country','Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']


out_location = r'L:\ESA\Results\diaz_example\outtables'

on_off_species = []

find_file_type = os.path.basename(l48_BE_sum)
if find_file_type.startswith('R'):
    file_type = 'R_'
else:
    file_type = 'CH_'


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


def step_1_ED(row, col_l48):
    if row['CONUS_Federal Lands_0'] >= 98.5:
        return 'No Effect'
    if row[col_l48] >= 0.5:
        return 'May Affect'
    elif row[col_l48] < 0.5:
        return 'No Effect'


def on_off_field(row, cols, df):
    ent_id = row['EntityID']
    if ent_id in on_off_species:
        col = [v for v in cols if v.endswith("_0")]
        direct_over = row[col[0]]
        df.loc[df['EntityID'] == ent_id, [col[0]]] = 0
        for other_col in cols:
            if other_col == col[0]:
                pass
            else:
                value = row[other_col]
                df.loc[df['EntityID'] == ent_id, [other_col]] = value - direct_over
    else:
        pass


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_directory(out_location + os.sep + chemical_name)
out_path = out_location + os.sep + chemical_name
use_lookup_df = pd.read_csv(use_lookup)
l48_df = pd.read_csv(l48_BE_sum)

list_final_uses = list(set(use_lookup_df['FinalUseHeader'].values.tolist()))
collapsed_dict = {}

# ## Species info from master list
species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]
base_sp_df['EntityID'] = base_sp_df['EntityID'].map(lambda x: x).astype(str)

# ##Filter L48 AA
aa_layers_CONUS = use_lookup_df.loc[((use_lookup_df['Action Area'] == 'x'))]
col_prefix_CONUS = aa_layers_CONUS['FinalColHeader'].values.tolist()
print col_prefix_CONUS
col_selection_aa = ['EntityID']
for col in col_prefix_CONUS:
    col_selection_aa.append(col + "_0")
    if len(aa_layers_CONUS.loc[(aa_layers_CONUS['FinalColHeader'] == col) & (aa_layers_CONUS['ground'] == 'x')]) > 0:
        col_selection_aa.append(col + "_305")
    if len(aa_layers_CONUS.loc[(aa_layers_CONUS['FinalColHeader'] == col) & (aa_layers_CONUS['aerial'] == 'x')]) > 0:
        col_selection_aa.append(col + "_765")

col_selection_aa.append('CONUS_Federal Lands_0')
chemical_step1 = l48_df[col_selection_aa]
final_use = aa_layers_CONUS.loc[(aa_layers_CONUS['Action Area'] == 'x') & (aa_layers_CONUS['aerial'] == 'x')], [
    'FinalColHeader']
if len(aa_layers_CONUS.loc[(aa_layers_CONUS['Action Area'] == 'x') & (aa_layers_CONUS['aerial'] == 'x')]) > 0:
    final_use = str(final_use) + '_765'
else:
    final_use = str(final_use) + '_305'

chemical_step1 = pd.merge(base_sp_df, chemical_step1, on='EntityID', how='left')

chemical_step1['Step 1 ED Comment'] = chemical_step1.apply(lambda row: step_1_ED(row, 'CONUS_Diazinon AA_' + max_drift),
                                                           axis=1)

chemical_step1.to_csv(out_path + os.sep + 'GIS_Step1_' + file_type + chemical_name + '.csv')
conus_cols = [v for v in chemical_step1.columns.values.tolist() if v.startswith('CONUS') or v in col_include_output]

conus_df_step1 = chemical_step1[conus_cols]

conus_df_step1.to_csv(out_path + os.sep + 'CONUS_Step1_' + file_type + chemical_name + '.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
