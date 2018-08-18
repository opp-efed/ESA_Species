import datetime
import os

import pandas as pd


# Title - Transforms out results by zone and summarize totals by species - final output is a master sum table of results
# by use and interval for each species

# TODO set up separate script that will spit out chem specific table with different interval include aerial and group
# TODO need to decide if Ag should be add back into each of the uses for species in regions w/o all use layers

# ###############user input variables
in_table = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\Range_test\SprayInterval_IntStep_30_MaxDistance_1501\R_AllUses_BE_20170816.csv'
look_up_use = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\RangeUses_lookup.csv'
# master list

regions = ['CONUS', 'AK', 'GU', 'HI', 'AS', 'PR', 'VI', 'CNMI', 'AS']
none_spatial_uses = ['Mosquito Control', 'Wide Area Use']

# #############Static Variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

path_intable, in_table_name = os.path.split(in_table)
file_type = in_table_name.split("_")[0]
temp_folder = path_intable
out_csv = temp_folder + os.sep + file_type + '_Collapsed_WoE_' + date + '.csv'

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Sets up the intervals that are of interests for each of the uses
collapses_dict = {}
use_lookup = pd.read_csv(look_up_use)
list_regional_uses = use_lookup['FinalColHeader'].values.tolist()
list_final_uses = list(set(use_lookup['FinalUseHeader'].values.tolist()))

print list_regional_uses
print list_final_uses
for v in none_spatial_uses:
    list_final_uses.append(v)

for f_use in list_final_uses:
    regional_uses = [u for u in list_regional_uses if u.split("_")[1] == f_use]
    direct_overlap = [u + "_0" for u in regional_uses]
    collapses_dict[f_use] = direct_overlap

sp_table_df = pd.read_csv(in_table, dtype=object)
[sp_table_df.drop(m, axis=1, inplace=True) for m in sp_table_df.columns.values.tolist() if m.startswith('Unnamed')]
columns_uses = [t for t in sp_table_df.columns.values.tolist() if t.split("_")[0] in regions]
columns_species = [t for t in sp_table_df.columns.values.tolist() if t.split("_")[0] not in regions]
sp_info_df = sp_table_df.loc[:, columns_species]
use_df = sp_table_df.loc[:, columns_uses]

col_reindex = columns_species + list_final_uses

collapsed_df = pd.DataFrame(data=sp_info_df)

for use in list_final_uses:
    print use
    binned_col = list(collapses_dict[use])

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
