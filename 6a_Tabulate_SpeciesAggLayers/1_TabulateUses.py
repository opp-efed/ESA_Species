import datetime
import os

import pandas as pd

agg_layers = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ED_results\Results\L48\Range\Agg_Layers'
indv_years = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ED_results\Results\L48\Range\Indiv_Year_raw'
nl48 = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ED_results\Results\NL48\Range\Agg_Layers'
outlocation = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects' \
              r'\ESA\_ExternalDrive\_CurrentSupportingTables'
lowest_folders = ['Range', 'CriticalHabitat']
regions = ['AK', 'AS', 'CNMI', 'CONUS', 'GU', 'HI', 'PR', 'VI']

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

range_df = pd.DataFrame(
    columns=['FullName', 'Region', 'Use','Usage lookup' 'Type', 'Cell Size', 'Included AA', 'FinalUseHeader', 'FinalColHeader',
             'Chem Table FinalColHeader','Action Area', 'ground', 'aerial', 'other layer','On/Off_AG',
             'On/Off_Pasture', 'On/Off_Orchard_Plantation'])
ch_df = pd.DataFrame(
    columns=['FullName', 'Region', 'Use','Usage lookup' 'Type', 'Cell Size', 'Included AA', 'FinalUseHeader', 'FinalColHeader',
             'Chem Table FinalColHeader','Action Area', 'ground', 'aerial', 'other layer','On/Off_AG',
             'On/Off_Pasture', 'On/Off_Orchard_Plantation'])

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
dirfolder = os.path.dirname(os.path.dirname(agg_layers))
dirfolder_nl48 = os.path.dirname(os.path.dirname(nl48))
for v in lowest_folders:
    path_file_types = dirfolder + os.sep + v + os.sep + 'Agg_Layers'
    print path_file_types
    use_folders = os.listdir(path_file_types)
    for use in use_folders:
        if use.startswith('z'):
            pass
        else:
            region = use.split("_")[0]
            split_nm = use.split("_")

            use_nm = use.split("_")[1]
            for t in split_nm:
                if t == region or t == use_nm or t == 'euc':
                    pass
                else:
                    use_nm = use_nm + "_" + t
            if v == 'Range':
                range_df = range_df.append(
                    {'FullName': region + "_" + use_nm, 'Region': region, 'Use': use_nm, 'Type': '', 'Cell Size': ''},
                    ignore_index=True)
            else:
                ch_df = ch_df.append(
                    {'FullName': region + "_" + use_nm, 'Region': region, 'Use': use_nm, 'Type': '', 'Cell Size': ''},
                    ignore_index=True)

for v in lowest_folders:
    path_file_types = dirfolder + os.sep + v + os.sep + 'Indiv_Year_raw'

    print path_file_types
    region = 'CONUS'
    use_folders = os.listdir(path_file_types)
    for use in use_folders:
        if use.startswith('z'):
            pass
        else:
            split_nm = use.split("_")
            use_nm = use.split("_")[2]
            for t in split_nm:
                if t == 'r' or t == use_nm or t == 'rec' or split_nm[1]:
                    pass
                else:
                    use_nm = use_nm + "_" + t
            if v == 'Range':
                range_df = range_df.append(
                    {'FullName': region + "_" + use_nm, 'Region': region, 'Use': use_nm, 'Type': '', 'Cell Size': ''},
                    ignore_index=True)
            else:
                ch_df = ch_df.append(
                    {'FullName': region + "_" + use_nm, 'Region': region, 'Use': use_nm, 'Type': '', 'Cell Size': ''},
                    ignore_index=True)

for v in lowest_folders:
    path_file_types = dirfolder_nl48 + os.sep + v + os.sep + 'Agg_Layers'

    print path_file_types
    use_folders = os.listdir(path_file_types)
    for use in use_folders:
        if use.startswith('z'):
            pass
        else:
            region = use.split("_")[0]
            split_nm = use.split("_")
            print use
            use_nm = use.split("_")[1]
            for t in split_nm:
                if t == region or t == use_nm or t == 'euc':
                    pass
                else:
                    use_nm = use_nm + "_" + t
            if v == 'Range':
                range_df = range_df.append(
                    {'FullName': region + "_" + use_nm, 'Region': region, 'Use': use_nm, 'Type': '', 'Cell Size': ''},
                    ignore_index=True)
                ch_df = ch_df.append(
                    {'FullName': region + "_" + use_nm, 'Region': region, 'Use': use_nm, 'Type': '', 'Cell Size': ''},
                    ignore_index=True)

range_df.drop_duplicates(inplace=True)
ch_df.drop_duplicates(inplace=True)

range_df.to_csv(outlocation + os.sep + 'RangeUses_lookup_' + date + '.csv')
ch_df.to_csv(outlocation + os.sep + 'CH_Uses_lookup_' + date + '.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
