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


# TODO set up bool variable to run interval inclusive of each other and exclusive of each other see title 1a
# TODO set up separate script so that it will check for missing runs, right now if there is not datat in the master tables

# ###############user input variables
full_impact = True  # if drift values should include use + drift True if direct use and drift should be separate false


in_table = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\Enlist Duo\Request_20190906\Overlap_byCounties_Merge'
out_location = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\Enlist Duo\Request_20190906\AllUses'
id_value = 'R_Enlist'
regions = ['CONUS']
st_or_cnty = ['GEOID', 'STUSPS', 'Acres_CONUS']  # if state STUSPS always include the acres col


col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']

look_up_use = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ED_results\_CurrentSupportingTables\Uses_lookup_20180430.csv'

# #############Static Variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

# meter conversion of 1000 and 2500 foot buffer round up to the nearest 5 per group discussion Fall 2016
# Limits for AgDrift for ground and aerial
bins = [0, 30,42,60,67,84 ,90,94,108,120,123,127,134,150, 305, 792]
bins = [0, 30]

use_lookup = pd.read_csv(look_up_use)
use_lookup['FinalColHeader'].fillna('none', inplace=True)
region_lookup = use_lookup.loc[use_lookup['Region'].isin(regions)]


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Sets up the intervals that are of interests for each of the uses
list_table = os.listdir(in_table)
list_table = [csv for csv in list_table if csv.endswith('.csv')]

count =0
out_df = pd.DataFrame()
collapsed_df = pd.DataFrame()
for v in list_table:
    final_df =  pd.DataFrame()
    sp_table_df = pd.read_csv(in_table + os.sep+v, dtype=object)
    sp_base = sp_table_df[col_include_output]
    sp_table_df.fillna(0, inplace=True)
    [sp_table_df.drop(m, axis=1, inplace=True) for m in sp_table_df.columns.values.tolist() if m.startswith('Unnamed')]
    sp_table_df ['GEOID'] = sp_table_df['GEOID'].map(lambda x: x).astype(str)
    use_list = [ v for v in sp_table_df.columns.values.tolist() if v.startswith('CONUS')]
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
        if use_nm not in uses:
            uses.append(use_nm)

    for i in uses:
        print "Working on {0}".format(i)

        # NOTES SEE ASSUMPTION ABOUT NOT HAVING ANY "_" in the use name part of final use col headers;
        # ie [region]_[use name]_[intervalvalue] list of current group of column will not populate correctly
        use_cols = [v for v in sp_table_df.columns.values.tolist() if v.startswith('CONUS_' + i)]

        grouped_use = sp_table_df.loc[:, ['EntityID'] + st_or_cnty + use_cols]
        grouped_use['GEOID'] = grouped_use['GEOID'].map(lambda x: x.split('.')[0]).astype(str)

        previous_col = []
        for value in bins:
            new_df = pd.DataFrame()
            binned_col = []
            if full_impact:  # direct use is included is drift calculations
                for col in use_cols:
                    get_interval = col.split('_')
                    interval = int(get_interval[(len(get_interval) - 1)])
                    if interval <= value and col not in previous_col:
                        binned_col.append(col)
            elif not full_impact:  # direct use is not included is drift calculations
                if value == bins[0]:
                    for col in use_cols:
                        get_interval = col.split('_')
                        interval = int(get_interval[(len(get_interval) - 1)])
                        if interval == bins[0]:
                            binned_col.append(col)
                else:
                    for col in use_cols:
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
            if len(collapsed_df) == 0:
                use_results_df = grouped_use[['EntityID'] + st_or_cnty + binned_col]
                use_results_df.loc[:, binned_col] = use_results_df.loc[:, binned_col].apply(pd.to_numeric,
                                                                                            errors='coerce')
                collapsed_df = pd.concat([collapsed_df, use_results_df], axis=1)

            else:
                use_results_df = grouped_use[['EntityID'] + st_or_cnty + binned_col]
                use_results_df.loc[:, binned_col] = use_results_df.loc[:, binned_col].apply(pd.to_numeric,
                                                                                            errors='coerce')
                use_results_df[(str('CONUS_' + i) + '_' + str(value))] = use_results_df[binned_col].sum(axis=1)
                binned_result = use_results_df[['EntityID'] + st_or_cnty + [(str('CONUS_' + i) + '_' + str(value))]]
                collapsed_df = pd.merge(collapsed_df, binned_result, how='outer', left_on=['EntityID'] + st_or_cnty,
                                        right_on=['EntityID'] + st_or_cnty)

out_csv = out_location + os.sep + 'AllUses_Cnty_' + id_value +"_"+date+ '.csv'
collapsed_df = pd.merge(sp_base,collapsed_df,how='right',on='EntityID')
collapsed_df.loc[:,['CONUS_Corn_0', 'CONUS_Corn_30', 'CONUS_Cotton_0', 'CONUS_Cotton_30', 'CONUS_Soybeans_0', 'CONUS_Soybeans_30']] = collapsed_df.loc[:,['CONUS_Corn_0', 'CONUS_Corn_30', 'CONUS_Cotton_0', 'CONUS_Cotton_30', 'CONUS_Soybeans_0', 'CONUS_Soybeans_30']].apply(pd.to_numeric, errors='coerce')
collapsed_df.drop_duplicates(inplace= True)
collapsed_df.fillna(0, inplace=True)
collapsed_df.to_csv(out_csv)

out_csv = out_location + os.sep + 'AllUses_State_' + id_value +"_"+date+ '.csv'
collapsed_df.drop(['GEOID'],axis=1, inplace=True)
collapsed_df =collapsed_df.groupby(col_include_output + ['STUSPS', 'Acres_CONUS']).sum().reset_index()

collapsed_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)