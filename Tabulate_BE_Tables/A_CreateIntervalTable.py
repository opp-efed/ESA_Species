import pandas as pd
import os
import datetime

in_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\NL48\Range\MergeByUse'
out_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\NL48\Range'

# in_folder = r'E:\Tabulated_NewComps\NL48\AG\CriticalHabitat\MergeByUse'
# out_location = 'E:\Tabulated_NewComps\NL48\AG\CriticalHabitat'
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

out_csv = out_location + os.sep + 'R_NL48_SprayInterval_'+ (date)+'_All.csv'

master_list = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\MasterListESA_June2016_20170216.xlsx'
col_included = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'CH_GIS','Source of Call final BE-Range','WoE Summary Group','Source of Call final BE-Range']


def extract_species_info(master_in_table, col_from_master):
    check_extention = (master_in_table.split('.'))[1]
    if check_extention == 'xlsx':
        master_list_df = pd.read_excel(master_in_table)
    else:
        master_list_df = pd.read_csv(master_in_table)
    master_list_df['EntityID'] = master_list_df['EntityID'].astype(str)
    ent_list_included = master_list_df['EntityID'].values.tolist()
    sp_info_df = pd.DataFrame(master_list_df, columns=col_from_master)
    print sp_info_df
    sp_info_header = sp_info_df.columns.values.tolist()

    return sp_info_df, sp_info_header, ent_list_included


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

list_csv = os.listdir(in_folder)
print list_csv
list_csv = [csv for csv in list_csv if csv.endswith('.csv')]
print csv

main_species_infoDF, main_sp_header, ent_list_master = extract_species_info(master_list, col_included)

out_df = main_species_infoDF

for csv in list_csv:
    print csv

    current_csv = in_folder + os.sep + csv
    in_df = pd.read_csv(current_csv, dtype=object)
    cols_in_df = in_df.columns.values.tolist()
    cols_unnamed = [i for i in cols_in_df if i.startswith('Unnamed')]
    for j in cols_unnamed:
        in_df.drop(j, axis=1, inplace=True)

    for v in col_included:
        if v == 'EntityID':
            continue
        else:
            try:
                in_df.drop(v, axis=1, inplace=True)
            except:
                pass  # column in list not in table

    out_df = pd.merge(out_df, in_df, on='EntityID', how='left')
    out_df  = out_df.fillna(0)

    # out_df= out_df.reindex(columns=col)
    out_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
