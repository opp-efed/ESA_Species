import datetime
import os

import pandas as pd


# Title - Transforms out results by zone and summarize totals by species - final output is a master sum table of results
# by use and interval for each species

# TODO set up separate script that will spit out chem specific table with different interval include aerial and group
# TODO need to decide if Ag should be add back into each of the uses for species in regions w/o all use layers

# ###############user input
chemical_name = 'AllUses'
in_table = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
           r'\_ED_results\Tabulated_Jan2018\L48\CH\Agg_Layers\SprayInterval_IntStep_30_MaxDistance_1501' \
           r'\CH_AllUses_BE_L48_20180128.csv'
look_up_use = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
                  r'\_ExternalDrive\_CurrentSupportingTables\RangeUses_lookup.csv'

find_file_type = in_table.split(os.sep)
if 'L48' in find_file_type:
    regions = ['CONUS']
    p_region = 'L48'
elif 'Range' in find_file_type:
    regions =['AK', 'GU', 'HI',  'PR', 'VI', 'CNMI', 'AS']
    p_region = 'NL48'
else:
   regions = [ 'GU', 'HI',  'PR', 'VI', 'CNMI', ]  # no ch in AK and AS
   p_region = 'NL48'

none_spatial_uses = []  # ['Mosquito Control', 'Wide Area Use'] uses that should 100%

# #############Static Variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

path_intable, in_table_name = os.path.split(in_table)
file_type = in_table_name.split("_")[0]
temp_folder = path_intable
out_csv = temp_folder + os.sep + file_type + "_"+chemical_name+"_"+ p_region+ '_Collapsed_WoE_' + date + '.csv'

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Sets up the intervals that are of interests for each of the uses
collapses_dict = {}
use_lookup = pd.read_csv(look_up_use)
use_lookup['FinalColHeader'].fillna('none', inplace=True)
use_lookup['FinalUseHeader'].fillna('none', inplace=True)

#region_lookup = use_lookup.loc[(use_lookup['Region'].isin(regions) & use_lookup['Included AA'] =='yes')]
region_lookup = use_lookup.loc[(use_lookup['Included AA'] == 'yes') & use_lookup['Region'].isin(regions)]

list_regional_uses = list(set(region_lookup ['FinalColHeader'].values.tolist()))
list_final_uses = list(set(region_lookup ['FinalUseHeader'].values.tolist()))

list_final_uses.remove('none') if 'none' in list_final_uses else list_final_uses
list_regional_uses.remove('none') if 'none' in list_regional_uses else list_regional_uses

for v in none_spatial_uses:
    list_final_uses.append(v)

for f_use in list_final_uses:

    regional_uses = [u for u in list_regional_uses if u.split("_")[1] == f_use]
    direct_overlap = [u + "_0" for u in regional_uses]
    collapses_dict[f_use] = direct_overlap

sp_table_df = pd.read_csv(in_table, dtype=object)
c_columns = sp_table_df.columns.values.tolist()
[sp_table_df.drop(m, axis=1, inplace=True) for m in sp_table_df.columns.values.tolist() if m.startswith('Unnamed')]
columns_uses = [t for t in sp_table_df.columns.values.tolist() if t.split("_")[0] in regions]
columns_species = [t for t in sp_table_df.columns.values.tolist() if t.split("_")[0] not in regions]
sp_info_df = sp_table_df.loc[:, columns_species]
use_df = sp_table_df.loc[:, columns_uses]

col_reindex = columns_species + list_final_uses

collapsed_df = pd.DataFrame(data=sp_info_df)

no_overlap_uses = []
for use in list_final_uses:
    print use
    binned_col = list(collapses_dict[use])
    [no_overlap_uses.append(v) for v in binned_col if v not in c_columns]
    binned_col = [i for i in binned_col if i not in no_overlap_uses]

    if use in none_spatial_uses:
        collapsed_df.ix[:, str(use)] = 100
    else:
        binned_df = use_df[binned_col]
        use_results_df = binned_df.apply(pd.to_numeric, errors='coerce')
        collapsed_df[(str(use))] = use_results_df.sum(axis=1)

final_df = collapsed_df.reindex(columns=col_reindex)
final_df = final_df.fillna(0)
final_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
print "Uses coming up as not overlap for all species in regions {0}".format(no_overlap_uses)
