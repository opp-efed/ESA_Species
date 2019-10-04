import pandas as pd
import os
import datetime

import pandas as pd
import os
import datetime

# TODO FILTER NE/NLAAs

chemical_name = '' # chemical_name = 'Carbaryl', Methomyl
file_type = 'Range'  # 'Range or CriticalHabitat

# use look up
use_lookup = r'path' \
             r'\SupportingTables' + os.sep + chemical_name + "_Step1_Uses_lookup_20190409.csv"

max_drift = '792'

# root path directory
root_path = r'path/tabulated'

# Tables directory  one level done from chemical
# No adjustment
folder_path = r'SprayInterval_IntStep_30_MaxDistance_1501\noadjust'

l48_BE_interval_table = "R_UnAdjusted_SprayInterval_noadjust_Full Range_20190626.csv" # example table
nl48_BE_interval_table = "R_UnAdjusted_SprayInterval_noadjust_Full Range_20190626.csv"  #example table

master_list = r"\MasterListESA_Feb2017_20190130.csv"
# columns from master to include
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']

out_location =  root_path

l48_BE_interval = root_path + os.sep + chemical_name + os.sep+file_type + os.sep + folder_path + os.sep + l48_BE_interval_table
nl48_BE_interval = root_path + os.sep + chemical_name + os.sep+file_type + os.sep + folder_path + os.sep + nl48_BE_interval_table

file_type_marker = os.path.basename(l48_BE_interval).split("_")[0] + "_"
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

create_directory(out_location + os.sep + chemical_name + os.sep + 'Summarized Tables')
out_path = out_location + os.sep + chemical_name + os.sep + 'Summarized Tables' + os.sep + 'Step 1'
create_directory(out_path)
use_lookup_df = pd.read_csv(use_lookup)
l48_df = pd.read_csv(l48_BE_interval, dtype=object)
nl48_df = pd.read_csv(nl48_BE_interval, dtype=object)
nl48_df['EntityID'] = nl48_df['EntityID'].map(lambda x: x).astype(str)
nl48_df['EntityID'] = nl48_df['EntityID'].map(lambda x: x).astype(str)
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
# print col_prefix_CONUS
cols_all_uses = l48_df.columns.values.tolist()

cols_aa_l48 = [col for col in cols_all_uses if col.startswith(col_prefix_CONUS[0])]
# print cols_aa_l48
cols_aa_l48.insert(0, 'EntityID')
aa_l48 = l48_df.ix[:, cols_aa_l48]
direct_overlap_col = aa_l48.columns.values.tolist()

# direct_overlap_col = [p for p in direct_overlap_col if p.endswith("_0")]
# aa_l48.apply(lambda row: on_off_field(row, direct_overlap_col, aa_l48), axis=1)

aa_l48 = pd.merge(base_sp_df, aa_l48, on='EntityID', how='left')
aa_l48.to_csv(out_path + os.sep + file_type_marker + 'CONUS_Step1_Intervals_' + chemical_name + '.csv')

# ##Filter NL48 AA
aa_layers_NL48 = use_lookup_df.loc[(use_lookup_df['Action Area'] == 'x') & (use_lookup_df['Region'] != 'CONUS')]
col_prefix_NL48 = aa_layers_NL48['FinalColHeader'].values.tolist()
print col_prefix_NL48
cols_all_uses_nl48 = nl48_df.columns.values.tolist()
print col_prefix_NL48
cols_aa_nl48 = []

for v in col_prefix_NL48:
    for col in cols_all_uses_nl48:
        if col.startswith(v +"_"):
            cols_aa_nl48.append(col)
        else:
            pass
cols_aa_nl48.insert(0, 'EntityID')
intervals = []

for i in cols_aa_nl48:
    int_bin = i.split("_")[len(i.split("_")) - 1]
    if int_bin not in intervals and int_bin != 'EntityID':
        intervals.append(int_bin)

for t in intervals:
    binned_use = [x for x in cols_aa_nl48 if x.endswith("_" + t)]
    print binned_use
    nl48_df.ix[:, binned_use] = nl48_df.ix[:, binned_use].apply(pd.to_numeric, errors='coerce')
    out_nl48_col = 'NL48_' + binned_use[0].split("_")[1] + "_" + binned_use[0].split("_")[2]
    nl48_df[out_nl48_col] = nl48_df[binned_use].sum(axis=1)

out_cols = nl48_df.columns.values.tolist()
out_cols = [k for k in out_cols if k.startswith('NL48')]
out_cols.insert(0, 'EntityID')
aa_nl48 = nl48_df.ix[:, out_cols]

# direct_overlap_col = aa_nl48.columns.values.tolist()
# direct_overlap_col = [p for p in direct_overlap_col if p.endswith("_0")]
aa_nl48.apply(lambda row: on_off_field(row, direct_overlap_col, aa_nl48), axis=1)
aa_nl48 = pd.merge(base_sp_df, aa_nl48, on='EntityID', how='left')

aa_nl48.to_csv(out_path + os.sep + file_type_marker + 'NL48_Step1_Intervals_' + chemical_name + '.csv')
print out_path + os.sep + file_type_marker + 'NL48_Step1_Intervals_' + chemical_name + '.csv'
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
