import datetime
import os

import pandas as pd

# Title - Transforms out results by zone and summarize totals by species - final output is a master sum table of results
# by use and interval for each species

# TODO set up separate script that will spit out chem specific table with different interval include aerial and group

# inlocation
filetype = 'R'
in_table = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\FinalTables_Range\BETables\R_AllUses_BE_20170209.csv'
master_col = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'CH_GIS']
# master list
temp_folder = r'C:\Users\JConno02\Documents\Projects\SmallProject\BPPD_overlap'
out_csv = temp_folder + os.sep + 'R_ESA_AllSpecies_20170209_bySpecies_20170515.csv'
sp_index_cols = 12

if filetype == 'R':
    collapses_dict = {
        'Corn_Collapsed': ['CONUS_Corn_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0',
                           'VI_Ag_0'],
        'Corn_CONUS': ['CONUS_Corn_0'],
        'Cotton_Collapsed': ['CONUS_Cotton_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0',
                             'VI_Ag_0'],
        'Cotton_CONUS': ['CONUS_Cotton_0'],
        'Soybean_Collapsed': ['CONUS_Soybeans_0', 'AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0',
                              'VI_Ag_0'],
        'Soyben_CONUS': ['CONUS_Soybeans_0'],
        'Ag_NL48': ['AK_Ag_0', 'AS_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0']}
    col_reindex = ['EntityID', 'comname', 'sciname', 'family', 'status_text', 'pop_abbrev', 'Group' 'Corn_Collapsed',
                   'Corn_CONUS', 'Cotton_Collapsed', 'Cotton_CONUS', 'Soybean_Collapsed', 'Soybean_CONUS', 'Ag_NL48']
else:
    collapses_dict = {
        'Corn_Collapsed': ['CONUS_Corn_0', 'AK_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0'],
        'Corn_CONUS': ['CONUS_Corn_0'],
        'Cotton_Collapsed': ['CONUS_Cotton_0', 'AK_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0'],
        'Cotton_CONUS': ['CONUS_Cotton_0'],
        'Soybean_Collapsed': ['CONUS_Soybeans_0', 'AK_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0'],
        'Soyben_CONUS': ['CONUS_Soybeans_0'],
        'Ag_NL48': ['AK_Ag_0', 'CNMI_Ag_0', 'GU_Ag_0', 'HI_Ag_0', 'PR_Ag_0', 'VI_Ag_0']}

    col_reindex = ['EntityID', 'comname', 'sciname', 'family', 'status_text', 'pop_abbrev', 'Group', 'Des_CH', 'CH_GIS',
                   'Corn_Collapsed', 'Corn_CONUS', 'Cotton_Collapsed', 'Cotton_CONUS', 'Soybean_Collapsed',
                   'Soybean_CONUS', 'Ag_NL48']

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
list_uses = collapses_dict.keys()
print list_uses
# Sets up the intervals that are of interests for each of the uses

sp_table_df = pd.read_csv(in_table, dtype=object)
# print sp_table_df
sp_info_df = sp_table_df.iloc[:, :sp_index_cols]
use_df = sp_table_df.iloc[:, sp_index_cols:]
# print use_df

collapsed_df = pd.DataFrame(data=sp_info_df)

for use in list_uses:
    print use
    binned_col = list(collapses_dict[use])

    if not use == 'Mosquito Control':
        if not use == 'Wide Area Use':
            binned_df = use_df[binned_col]
            # print binned_df
            use_results_df = binned_df.apply(pd.to_numeric, errors='coerce')
            collapsed_df[(str(use))] = use_results_df.sum(axis=1)
        else:
            collapsed_df.ix[:, str(use)] = 100
    else:
        collapsed_df.ix[:, str(use)] = 100

final_df = collapsed_df.reindex(columns=col_reindex)
final_df = final_df.fillna(0)
# print sorted(collapsed_df.columns.values.tolist())
# print (collapsed_df.columns.values.tolist())
final_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
