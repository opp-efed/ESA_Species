import os
import pandas as pd
import datetime


# Author J.Connolly
# Internal deliberative, do not cite or distribute

# Location where the all of the Agg Layer tables are - NOTE Both the CONUS and NL48 need to be completed for this
# Script to complete
in_location = r'path\Agg_Layers\Range'
# tables generated from Step A
in_huc_table = r'path\Range' # out path Step A
# chemical use lookup
use_lookup = r"path\[chemical]Uses_lookup_20190409.csv"

master_list =  r"\MasterListESA_Feb2017_20190130.csv"
# out columns to include
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']

# ##
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Get date
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]
base_sp_df['EntityID'] = base_sp_df['EntityID'].map(lambda x: x).astype(str)

# L48_df = base_sp_df.copy()
NL48_df = pd.DataFrame(columns=['EntityID'])
L48_df = pd.DataFrame(columns=['EntityID'])
# NL48_df = base_sp_df.copy()

use_lookup_df = pd.read_csv(use_lookup)
usage_lookup_df = use_lookup_df.ix[:, ['FullName', 'Usage lookup', 'FinalColHeader', 'Type', 'Cell Size']].copy()
usage_lookup_df['Filename'] = usage_lookup_df['FullName'].map(lambda x: str(x) + "_euc.csv").astype(str)


def huc_acres(cell_size, in_sum_df):
    msq_conversion = cell_size * cell_size
    in_sum_df.ix[:, ['COUNT']] = in_sum_df.ix[:, ['COUNT']].apply(pd.to_numeric, errors='coerce')

    # convert pixels to msq
    overlap = in_sum_df[['EntityID', 'STUSPS', 'HUC2_AB', 'COUNT']].copy()
    overlap.ix[:, ['COUNT']] *= msq_conversion

    # # convert msq to acres
    # overlap.ix[:, ['COUNT']] *= 0.000247
    overlap.columns = ['EntityID', 'STUSPS', 'HUC2_AB', 'MSQ']
    return overlap


list_folder = os.listdir(in_location)
list_huc_table = os.listdir(in_huc_table)
list_huc_table = [v for v in list_huc_table if v.endswith('.csv')]


for folder in list_folder:
    print folder
    region = folder.split("_")[0]
    final_col = usage_lookup_df.loc[(usage_lookup_df['FullName'] == folder.replace('_euc', '')), 'FinalColHeader'].iloc[0]
    if final_col.endswith('OnOff') and len(usage_lookup_df.loc[(usage_lookup_df['FullName'] == folder.replace('_euc', '')), 'FinalColHeader'])> 1:
        final_col = \
            usage_lookup_df.loc[(usage_lookup_df['FullName'] == folder.replace('_euc', '')), 'FinalColHeader'].iloc[1]
    type_use = usage_lookup_df.loc[(usage_lookup_df['FullName'] == folder.replace('_euc', '')), 'Type'].iloc[0]
    r_cell_size = usage_lookup_df.loc[(usage_lookup_df['FullName'] == folder.replace('_euc', '')), 'Cell Size'].iloc[0]
    c_huc = [huc for huc in list_huc_table if huc.startswith(region)]
    huc_table_df = pd.read_csv(in_huc_table + os.sep + c_huc[0])
    huc_acres_df = huc_acres(r_cell_size, huc_table_df)
    huc_acres_df ['EntityID'] = huc_acres_df ['EntityID'].map(lambda (n): n).astype(str)
    list_csv = os.listdir(in_location + os.sep + folder)
    list_csv = [csv for csv in list_csv if csv.endswith('HUC2AB.csv')]
    use_df = pd.DataFrame()
    for csv in list_csv:
        print ('  {0}'.format(csv))
        in_table = pd.read_csv(in_location + os.sep + folder + os.sep + csv)

        in_table ['EntityID'] = in_table ['EntityID'].map(lambda (n): n).astype(str)
        in_table ['VALUE_0'] = in_table ['VALUE_0'].map(lambda (n): n).astype(float)
        # in_table = in_table['VALUE_0']>0

        if len (in_table) >0:  # drops uses and HUC and/or uses where the direct overlap is 0
            filter_df = in_table[['EntityID', 'HUC2_AB', 'STUSPS', 'VALUE_0']].copy()


            join_acres = pd.merge(filter_df, huc_acres_df, how='left', left_on=['EntityID', 'HUC2_AB', 'STUSPS'],right_on=['EntityID', 'HUC2_AB', 'STUSPS'])

            # percent overlap for part of the range in the HUC/State Combo
            join_acres['VALUE_0'] = join_acres[['VALUE_0']].div(join_acres['MSQ'], axis=0)
            join_acres.ix[:, ['VALUE_0']] *= 100

            filter_df = join_acres[['EntityID', 'HUC2_AB', 'STUSPS', 'VALUE_0', 'MSQ']].copy()

            filter_df.columns = ['EntityID', 'HUC2_AB', 'STUSPS', final_col + '_0', 'MSQ_Species',]
            use_df = pd.concat([use_df, filter_df])
            use_df.fillna(0, inplace=True)
            use_df.drop_duplicates(use_df, inplace=True)
            use_df['EntityID'] = use_df['EntityID'].map(lambda (n): n).astype(str)
            use_df['HUC2_AB'] = use_df['HUC2_AB'].map(lambda (n): n).astype(str)



    if region == 'CONUS':
            if 'HUC2_AB' not in L48_df.columns.values.tolist():
                # L48_df = pd.merge(L48_df, use_df, how='outer', on=['EntityID'])
                L48_df = use_df
            else:
                L48_df = pd.merge(L48_df, use_df, how='outer', on=['EntityID','HUC2_AB', 'STUSPS'])

    else:
            if 'HUC2_AB' not in NL48_df.columns.values.tolist():
                # NL48_df = pd.merge(NL48_df, use_df, how='outer', on=['EntityID'])
                NL48_df = use_df

            else:
                # NL48_df['EntityID'] = NL48_df['EntityID'].map(lambda x: x).astype(str)
                NL48_df['HUC2_AB'] = NL48_df['HUC2_AB'].map(lambda x: x).astype(str)
                NL48_df = pd.merge(NL48_df, use_df, how='outer', on=['EntityID','HUC2_AB', 'STUSPS'])



L48_df.fillna(0, inplace=True)
NL48_df.fillna(0, inplace=True)
NL48_df = pd.merge(base_sp_df,NL48_df, how='outer', on=['EntityID'])
L48_df = pd.merge(base_sp_df,L48_df, how='outer', on=['EntityID'])

L48_df = L48_df.loc[:,~L48_df.columns.duplicated()]
total_area_sp_huc = L48_df[['EntityID', 'MSQ_Species_x']].copy()
sum_huc = total_area_sp_huc.groupby(['EntityID' ], as_index=False).sum()
sum_huc.drop_duplicates(inplace=True)


sum_huc.columns = ['EntityID','MSQ_Total']
L48_df = pd.merge(L48_df, sum_huc, how='left', on=['EntityID'])
L48_df['Percent of Range'] = L48_df[['MSQ_Species_x']].div(L48_df['MSQ_Total'], axis=0)
L48_df.ix[:, ['Percent of Range']] *= 100
L48_df.drop(['MSQ_Species_x','MSQ_Species_y'],axis=1, inplace=True)
L48_df.fillna(0, inplace=True)
L48_df.to_csv (in_huc_table+ os.sep + 'CONUS_HUC_Overlap_' + date + '.csv')
print ("Exported {0}".format(in_huc_table+ os.sep + 'CONUS_HUC_Overlap_' + date + '.csv'))

msq_cols = [v for v in NL48_df.columns.values.tolist() if v.startswith('MSQ_Species_x')]
NL48_df['MSQ_Species_x_all'] = NL48_df[msq_cols ].sum(axis=1)

NL48_df = NL48_df.loc[:,~NL48_df.columns.duplicated()]
total_area_sp_huc = NL48_df[['EntityID','MSQ_Species_x_all']].copy()

sum_huc = total_area_sp_huc.groupby(['EntityID', ], as_index=False).sum()
sum_huc.drop_duplicates(inplace=True)
sum_huc.drop_duplicates(inplace=True)
sum_huc.columns = ['EntityID','MSQ_Total']
sum_huc['EntityID'] = sum_huc['EntityID'].map(lambda x: x).astype(str)

NL48_df = pd.merge(NL48_df, sum_huc, how='left', on=['EntityID'])
NL48_df['Percent of Range'] = NL48_df[['MSQ_Species_x_all']].div(NL48_df['MSQ_Total'], axis=0)
NL48_df.ix[:, ['Percent of Range']] *= 100
NL48_df['HUC 2 surrogate'] = NL48_df['HUC2_AB'].map(lambda (n): '20' if str(n).startswith('22') else str(n)).astype(str)
NL48_df.drop(['MSQ_Species_x','MSQ_Species_y'],axis=1, inplace=True)
NL48_df.fillna(0, inplace=True)
NL48_df.to_csv (in_huc_table + os.sep + 'NL48_HUC_Overlap_' + date + '.csv')
print ("Exported {0}".format(in_huc_table + os.sep + 'NL48_HUC_Overlap_' + date + '.csv'))

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
