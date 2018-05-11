import datetime
import os

import pandas as pd

# Title- Generate BE summary table from master percent overlap tables - this script is interchangeable with the on
# under GAP_Species but kept  separate
#               1) Generates BE summary table at 0, 305 and 765 interval; for aggregated layers, AA, Ag and NonAG
#                   1a) NOTE these interval included the  lower interval; this can be change within script
# Static variables are updated once per update; user input variables update each  run

# ASSUMPTIONS
# col in UsesLook up that represents the Final Use Header - values do not have


# TODO set up bool variable to run interveral inclusive of each other and exclusive of each other see title 1a
# TODO set up separate script so that it will check for missing runs, right now if there is not datat in the master tables

# ###############user input variables
full_impact = True  # if drift values should include use + drift True if direct use and drift should be separate false
in_table = r'L:\ESA\Results\diazinon\Tabulated_usage\Diazinon\example\SprayInterval_IntStep_30_MaxDistance_1501' \
           r'\Upper_SprayInterval_20180221.csv'

# in_table = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
#            r'\_ED_results\Tabulated_Jan2018\L48\CH\Agg_Layers\SprayInterval_IntStep_30_MaxDistance_1501' \
#            r'\CH_SprayInterval_20180201_Region.csv'


col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country','Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']

# look_up_use = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
#                   r'\_ExternalDrive\_CurrentSupportingTables\RangeUses_lookup.csv'

look_up_use = r'L:\ESA\Results\diazinon\RangeUses_lookup.csv'

# #############Static Variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

find_file_type = in_table.split(os.sep)
if 'L48' in find_file_type:
    p_region = 'L48'  # can be L48 or
    regions = ['CONUS']
else:
    p_region = 'NL48'
    regions = ['AK', 'GU', 'HI', 'AS', 'PR', 'VI', 'CNMI', 'AS']
path_intable, in_table_name = os.path.split(in_table)
file_type = in_table_name.split("_")[0]
temp_folder = path_intable


out_csv = temp_folder + os.sep + file_type + '_AllUses_BE_' +p_region +"_"+date+ '.csv'


bins = [0, 305, 765]  # meter conversion of 1000 and 2500 foot buffer round up to the nearest 5

use_lookup = pd.read_csv(look_up_use)
use_lookup['FinalColHeader'].fillna('none', inplace=True)
region_lookup = use_lookup.loc[(use_lookup['Included AA'] == 'x')]


list_regional_uses = list(set(region_lookup ['FinalColHeader'].values.tolist()))

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Sets up the intervals that are of interests for each of the uses

sp_table_df = pd.read_csv(in_table, dtype=object)
[sp_table_df.drop(m, axis=1, inplace=True) for m in sp_table_df.columns.values.tolist() if m.startswith('Unnamed')]
columns_uses =[t for t in sp_table_df.columns.values.tolist() if t.split("_")[0] == 'CONUS']

columns_species = [t for t in sp_table_df.columns.values.tolist() if t.split("_")[0]!= 'CONUS']
# print sp_table_df
sp_info_df = sp_table_df.loc[:, columns_species]
use_df = sp_table_df.loc[:, columns_uses]

use_list = use_df.columns.values.tolist()


uses = []
for x in use_list:
    split_value = x.split("_")
    interval_value = split_value[len(split_value) - 1]
    use_nm = split_value[1]
    for v in split_value:
        if v in regions or v == use_nm or v == interval_value:
            pass
        else:
            use_nm = use_nm + v
    uses.append(use_nm)

collapsed_df = pd.DataFrame()

for i in list_regional_uses:

        # NOTES SEE ASSUMPTION ABOUT NOT HAVING ANY "_" in the use name part of final use col headers;
        # ie [region]_[use name]_[intervalvalue] list of current group of column will not populate correctly
        break_use = i.split("_")
        use_group = break_use[0] + "_" + break_use[1]

        current_group = [use for use in use_list if (use.split("_")[0] + "_" + use.split("_")[1]) == use_group]

        if len(current_group) == 0:
            continue
        grouped_use = use_df.loc[:, current_group]

        current_cols = grouped_use.columns.values.tolist()
        previous_col = []

        for value in bins:

            new_df = pd.DataFrame()
            binned_col = []
            if full_impact:  # direct use is included is drift calculations
                for col in current_cols:
                    get_interval = col.split('_')
                    interval = int(get_interval[(len(get_interval) - 1)])
                    if interval <= value and col not in previous_col:
                        binned_col.append(col)

            elif not full_impact:  # direct use is not included is drift calculations

                if value == bins[0]:
                    for col in current_cols:
                        get_interval = col.split('_')
                        interval = int(get_interval[(len(get_interval) - 1)])
                        if interval == bins[0]:

                            binned_col.append(col)
                else:

                    for col in current_cols:
                        get_interval = col.split('_')
                        interval = int(get_interval[(len(get_interval) - 1)])
                        if interval == 0:
                            continue
                        else:
                            if interval <= value and col not in previous_col:
                                binned_col.append(col)

                for p in binned_col:
                    if p in previous_col:
                        binned_col.remove(p)

                    previous_col.append(p)

            binned_df = grouped_use[binned_col]

            use_results_df = binned_df.apply(pd.to_numeric, errors='coerce')
            new_df[(str(use_group) + '_' + str(value))] = use_results_df.sum(axis=1)
            collapsed_df = pd.concat([collapsed_df, new_df], axis=1)


final_df = pd.concat([sp_info_df, collapsed_df], axis=1)
col_final = collapsed_df.columns.values.tolist()
master_col = col_include_output
for i in col_final:
    master_col.append(i)

# final_df = final_df.reindex(columns=master_col)
final_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
