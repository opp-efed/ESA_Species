import pandas as pd
import os
import datetime

import pandas as pd
import os
import datetime

# TODO FILTER NE/NLAAs
chemical_name = 'Diazinon'
use_lookup = r'L:\ESA\Results\diazinon\RangeUses_lookup.csv'

max_drift = '765'
l48_BE_interval = r'L:\ESA\Results\diazinon\out_tables\ParentTables\no usage\SprayInterval_IntStep_30_MaxDistance_1501' \
                  r'\R_SprayInterval_20180212_Region.csv'

master_list = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'

col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']

out_location = r'L:\ESA\Results\diaz_example\outtables'

find_file_type = os.path.basename(l48_BE_interval)
if find_file_type.startswith('R'):
    file_type = 'R_'
else:
    file_type = 'CH_'

on_off_species = []


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


def on_off_field(row, cols, df):
    ent_id = row['EntityID']
    if ent_id in on_off_species:
        for col in cols:
            direct_over = row[col]
            df.loc[df['EntityID'] == ent_id, [col]] = 0
    else:
        pass


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_directory(out_location + os.sep + chemical_name)
out_path = out_location + os.sep + chemical_name
use_lookup_df = pd.read_csv(use_lookup)
l48_df = pd.read_csv(l48_BE_interval)

list_final_uses = list(set(use_lookup_df['FinalUseHeader'].values.tolist()))
collapsed_dict = {}

# ## Species info from master list
species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]
base_sp_df['EntityID'] = base_sp_df['EntityID'].map(lambda x: x).astype(str)

# ##Filter L48 AA
aa_layers_CONUS = use_lookup_df.loc[(use_lookup_df['Action Area'] == 'x') & (use_lookup_df['Region'] == 'CONUS')]
col_prefix_CONUS = aa_layers_CONUS['FinalColHeader'].values.tolist()
cols_all_uses = l48_df.columns.values.tolist()
cols_aa_l48 = [col for col in cols_all_uses if col.startswith(col_prefix_CONUS[0])]
cols_aa_l48.insert(0, 'EntityID')
aa_l48 = l48_df.ix[:, cols_aa_l48]
direct_overlap_col = aa_l48.columns.values.tolist()

# direct_overlap_col = [p for p in direct_overlap_col if p.endswith("_0")]
# aa_l48.apply(lambda row: on_off_field(row, direct_overlap_col, aa_l48), axis=1)

aa_l48 = pd.merge(base_sp_df, aa_l48, on='EntityID', how='left')
aa_l48.to_csv(out_path + os.sep + file_type + 'CONUS_Step1_Intervals_' + chemical_name + '.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
