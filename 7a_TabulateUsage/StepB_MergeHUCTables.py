import os
import pandas as pd
import datetime
import numpy as np


# Author J.Connolly
# Internal deliberative, do not cite or distribute

# Location where the all of the Agg Layer tables are - NOTE Both the CONUS and NL48 need to be completed for this
# Script to complete
in_location = r'D:\Tabulated_HUCAB\Agg_Layers\CriticalHabitat'
# tables generated from Step A
in_huc_table = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\ChemicalTables\Aquatic\Herbicides 2020\CriticalHabitat' # out path Step A #\CriticalHabitat or Range
# chemical use lookup
use_lookup = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\Glyphosate\GLY_Uses_lookup_June2020_v2.csv"

master_list =  r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\MasterListESA_Dec2018_June2020.csv"
# out columns to include
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_YesNo', 'Migratory', 'Migratory_YesNo',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']

run_nl48 = True
# MP is CNMI
nl48_regions = ['AK', 'AS', 'CNMI', 'GU', 'HI', 'PR', 'VI', 'MP']

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
usage_lookup_df = use_lookup_df.ix[:, ['FullName', 'Usage lookup', 'Chem Table FinalColHeader', 'Type', 'Cell Size']].copy()
usage_lookup_df['Filename'] = usage_lookup_df['FullName'].map(lambda x: str(x) + "_euc.csv").astype(str)
usage_lookup_df['folder'] = usage_lookup_df['FullName'].map(lambda x: str(x) + "_euc").astype(str)
all_folders = usage_lookup_df['folder'].values.tolist()

def huc_acres(cell_size, in_sum_df):

    msq_conversion = cell_size * cell_size
    in_sum_df.ix[:, ['COUNT']] = in_sum_df.ix[:, ['COUNT']].apply(pd.to_numeric, errors='coerce')

    # convert pixels to msq
    overlap = in_sum_df[['EntityID', 'STUSPS', 'HUC2_AB', 'COUNT']].copy()
    overlap.ix[:, ['COUNT']] *= msq_conversion

    # # convert msq to acres
    # overlap.ix[:, ['COUNT']] *= 0.000247
    overlap.columns = ['EntityID', 'STUSPS', 'HUC2_AB', 'MSQ']
    overlap['EntityID'] = overlap['EntityID'].map(lambda x: x).astype(str)

    return overlap


list_folder = os.listdir(in_location)
list_huc_table = os.listdir(in_huc_table)
list_huc_table = [v for v in list_huc_table if v.endswith('.csv')]


for folder in list_folder:
    c_region = folder.split("_")[0]
    if folder not in all_folders:
        pass
    else:
        print folder
        region = folder.split("_")[0]
        try:
            final_col = usage_lookup_df.loc[(usage_lookup_df['FullName'] == folder.replace('_euc', '')), 'Chem Table FinalColHeader'].iloc[0]
            if final_col.endswith('OnOff') and len(usage_lookup_df.loc[(usage_lookup_df['FullName'] == folder.replace('_euc', '')), 'Chem Table FinalColHeader'])> 1:
                final_col = \
                    usage_lookup_df.loc[(usage_lookup_df['FullName'] == folder.replace('_euc', '')), 'Chem Table FinalColHeader'].iloc[1]
            type_use = usage_lookup_df.loc[(usage_lookup_df['FullName'] == folder.replace('_euc', '')), 'Type'].iloc[0]
            r_cell_size = usage_lookup_df.loc[(usage_lookup_df['FullName'] == folder.replace('_euc', '')), 'Cell Size'].iloc[0]
            c_huc = [huc for huc in list_huc_table if huc.startswith(region)]
            huc_table_df = pd.read_csv(in_huc_table + os.sep + c_huc[0])
            huc_acres_df = huc_acres(r_cell_size, huc_table_df)
            huc_acres_df ['EntityID'] = huc_acres_df ['EntityID'].map(lambda (n): n).astype(str)
            huc_acres_df['HUC2_AB'] = huc_acres_df ['HUC2_AB'].map(lambda (n): n if len(str(n))>1 else "0" +str(n)).astype(str)
            huc_acres_df['STUSPS'] = huc_acres_df ['STUSPS'].map(lambda (n): n).astype(str)
            list_csv = os.listdir(in_location + os.sep + folder)
            list_csv = [csv for csv in list_csv if csv.endswith('HUC2AB.csv')]
            use_df = pd.DataFrame()
            for csv in list_csv:
                print ('  {0} will be {1}'.format((in_location + os.sep + folder + os.sep + csv), final_col))
                in_table = pd.read_csv(in_location + os.sep + folder + os.sep + csv)
                if len(in_table) == 0:
                    continue
                in_table ['EntityID'] = in_table ['EntityID'].map(lambda (n): n).astype(str)
                in_table ['VALUE_0'] = in_table ['VALUE_0'].map(lambda (n): n).astype(float)
                in_table = in_table[in_table['VALUE_0']>0]

                if len (in_table) >0:  # drops uses and HUC and/or uses where the direct overlap is 0
                    filter_df = in_table[['EntityID', 'HUC2_AB', 'STUSPS', 'VALUE_0']].copy()
                    filter_df['EntityID'] = filter_df['EntityID'].map(lambda (n): n).astype(str)
                    filter_df['HUC2_AB'] = filter_df['HUC2_AB'].map(lambda (n): n if len(str(n))>1 else "0" +str(n)).astype(str)
                    filter_df['STUSPS'] = filter_df['STUSPS'].map(lambda (n): n).astype(str)
                    common_col = [col for col in huc_acres_df.columns.values.tolist() if
                                  col in filter_df.columns.values.tolist()]
                    join_acres = pd.merge(filter_df, huc_acres_df, how='left', left_on=common_col, right_on=common_col)

                    # percent overlap for part of the range in the HUC/State Combo
                    join_acres['VALUE_0'] = join_acres[['VALUE_0']].div(join_acres['MSQ'], axis=0)
                    join_acres.ix[:, ['VALUE_0']] *= 100

                    filter_df = join_acres[['EntityID', 'HUC2_AB', 'STUSPS', 'VALUE_0', 'MSQ']].copy()
                    filter_df.columns = ['EntityID', 'HUC2_AB', 'STUSPS', final_col + '_0', 'MSQ_Species']
                    # print filter_df.head()

                    if len(use_df) == 0:
                        use_df = filter_df.copy()
                    else:
                        # print use_df.columns.values.tolist()
                        common_col = [col for col in use_df.columns.values.tolist() if
                                      col in filter_df.columns.values.tolist()]

                        # use_df = pd.merge(use_df, filter_df, how='outer',left_on=['EntityID', 'HUC2_AB', 'STUSPS', final_col + '_0'],right_on=['EntityID', 'HUC2_AB', 'STUSPS', final_col + '_0'])

                        use_df = pd.merge(use_df, filter_df, how='outer',left_on=common_col,right_on=common_col)

                    use_df.fillna(0, inplace=True)

                    use_df['EntityID'] = use_df['EntityID'].map(lambda (n): n).astype(str)
                    use_df['HUC2_AB'] = use_df['HUC2_AB'].map(lambda (n): n if len(str(n))>1 else "0" +str(n)).astype(str)

                    # print use_df.head()

        except IndexError:  # previously run use is not longer being tracked in the use look table
            pass
        # print use_df.head()
        # print use_df.columns.values.tolist()
        if len(use_df) > 0:
            if region == 'CONUS':
                if 'MSQ_Species_final' in use_df.columns.values.tolist() and 'MSQ_Species' in use_df.columns.values.tolist():
                    use_df['Compare MSQ'] = use_df['MSQ_Species_final'].ne(use_df['MSQ_Species'], axis=0)
                    if len(use_df[use_df['Compare MSQ'] =='TRUE']) >0:
                        use_df['MSQ_Species_final'] = use_df [['MSQ_Species_final', 'MSQ_Species']].max(axis=1)
                    else:
                        pass
                    use_df = use_df.drop(['MSQ_Species','Compare MSQ'], axis=1)
                elif 'MSQ_Species' in use_df.columns.tolist():
                    use_df['MSQ_Species_final'] = use_df['MSQ_Species']
                    use_df = use_df.drop('MSQ_Species', axis=1)
                else:
                    use_df['MSQ_Species_final'] = use_df['MSQ_Species_final']
                if 'HUC2_AB' not in L48_df.columns.values.tolist():
                    # L48_df = pd.merge(L48_df, use_df, how='outer', on=['EntityID'])
                    L48_df = use_df
                else:
                    common_col = [col for col in L48_df.columns.values.tolist() if
                                  col in use_df.columns.values.tolist()]
                    if len(use_df)>0:  # confirms there is overlap for use
                        L48_df = pd.merge(L48_df, use_df, how='outer', on=common_col)

                L48_df.fillna(0, inplace=True)

            else:

                if 'MSQ_Species_final' in use_df.columns.values.tolist() and 'MSQ_Species' in use_df.columns.values.tolist():
                    # use_df['MSQ_Species_final'] = np.where((use_df['MSQ_Species_final'] == use_df['MSQ_Species']),
                    #                                        use_df [['MSQ_Species_final', 'MSQ_Species']].max(axis=1),use_df [['MSQ_Species_final', 'MSQ_Species']].sum(axis=1))
                    # use_df['MSQ_Species_final'] = use_df [['MSQ_Species_final', 'MSQ_Species']].max(axis=1)
                    use_df['Compare MSQ'] = use_df['MSQ_Species_final'].ne(use_df['MSQ_Species'], axis=0)
                    if len(use_df[use_df['Compare MSQ'] =='TRUE']) >0:
                        use_df['MSQ_Species_final'] = use_df [['MSQ_Species_final', 'MSQ_Species']].sum(axis=1)
                    else:
                        pass
                    use_df = use_df.drop(['MSQ_Species','Compare MSQ'], axis=1)
                elif 'MSQ_Species' in use_df.columns.tolist():
                    use_df['MSQ_Species_final'] =  use_df['MSQ_Species']
                    use_df = use_df.drop('MSQ_Species', axis=1)
                else:
                    use_df['MSQ_Species_final'] = use_df['MSQ_Species_final']
                if 'HUC2_AB' not in NL48_df.columns.values.tolist():
                    # NL48_df = pd.merge(NL48_df, use_df, how='outer', on=['EntityID'])
                    NL48_df = use_df


                else:

                    common_col = [col for col in NL48_df.columns.values.tolist() if
                                  col in use_df.columns.values.tolist()]
                    if final_col +"_0" in common_col:
                        common_col.remove(final_col +"_0")
                    # use_df['combined'] = df[cols].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)

                    NL48_df['EntityID'] = NL48_df['EntityID'].map(lambda x: x).astype(str)
                    NL48_df['HUC2_AB'] = NL48_df['HUC2_AB'].map(lambda (n): str(n)).astype(str)
                    if len(use_df)> 0: # confirms there is overlap for use
                        print common_col
                        NL48_df = pd.merge(NL48_df, use_df, how='outer', on=common_col, suffixes=('_left'+'_'+c_region, '_'+c_region))


L48_df.fillna(0, inplace=True)
NL48_df.fillna(0, inplace=True)
NL48_df = pd.merge(base_sp_df,NL48_df, how='outer', on=['EntityID'])
L48_df = pd.merge(base_sp_df,L48_df, how='outer', on=['EntityID'])
NL48_df.to_csv (in_huc_table + os.sep + 'Test2_NL48_HUC_Overlap_' + date + '.csv')

L48_df = L48_df.loc[:,~L48_df.columns.duplicated()]
total_area_sp_huc = L48_df[['EntityID', 'MSQ_Species_final']].copy()
sum_huc = total_area_sp_huc.groupby(['EntityID' ], as_index=False).sum()
sum_huc.drop_duplicates(inplace=True)
sum_huc.columns = ['EntityID','MSQ_Total']

L48_df = pd.merge(L48_df, sum_huc, how='left', on=['EntityID'])
L48_df['Percent of Range'] = L48_df[['MSQ_Species_final']].div(L48_df['MSQ_Total'], axis=0)
L48_df.ix[:, ['Percent of Range']] *= 100
L48_df.fillna(0, inplace=True)
# print L48_df.head()
L48_df = L48_df[(L48_df['L48/NL48']=='CONUS')|(L48_df['L48/NL48']=='Both')]
L48_df.to_csv (in_huc_table+ os.sep + 'CONUS_HUC_Overlap_' + date + '.csv')
print ("Exported {0}".format(in_huc_table+ os.sep + 'CONUS_HUC_Overlap_' + date + '.csv'))


if run_nl48:
    # NL48_df = NL48_df.loc[:,~NL48_df.columns.duplicated()]
    NL48_df.to_csv (in_huc_table + os.sep + 'Test_NL48_HUC_Overlap_' + date + '.csv')
    # print NL48_df.columns.values.tolist()
    NL48_df = NL48_df.loc[:,~NL48_df.columns.duplicated()]
    all_nl48 = [v for v in NL48_df.columns.values.tolist() if v.startswith('NL48') ]
    nl48_cols =list(set([col.split("_")[0] +"_"+col.split("_")[1] for col in all_nl48]))
    print nl48_cols
    for i in nl48_cols:
        c_columns = [v for v in NL48_df.columns.values.tolist() if v.startswith(i+"_0")]
        print i, c_columns
        NL48_df[i +"_0_final"] = NL48_df[c_columns].sum(axis=1)
        # c_columns.remove(i +"_0")
        NL48_df.drop(c_columns, axis=1,inplace=True)

    total_area_sp_huc = NL48_df[['EntityID','MSQ_Species_final']].copy()
    sum_huc = total_area_sp_huc.groupby(['EntityID', ], as_index=False).sum()
    sum_huc.drop_duplicates(inplace=True)
    sum_huc.columns = ['EntityID','MSQ_Total']
    sum_huc['EntityID'] = sum_huc['EntityID'].map(lambda x: x).astype(str)

    NL48_df = pd.merge(NL48_df, sum_huc, how='left', on=['EntityID'])
    NL48_df['Percent of Range'] = NL48_df['MSQ_Species_final'].div(NL48_df['MSQ_Total'].where(NL48_df['MSQ_Species_final']> 0, np.nan), axis=0)
    NL48_df.ix[:, ['Percent of Range']] *= 100
    NL48_df['HUC 2 surrogate'] = NL48_df['HUC2_AB'].map(lambda (n): '20' if str(n).startswith('22') else str(n)).astype(str)

    NL48_df.fillna(0, inplace=True)
    # print NL48_df.head()
    NL48_df = NL48_df[(NL48_df['STUSPS'].isin(nl48_regions))|(NL48_df['L48/NL48']=='Both')]

    NL48_df.to_csv (in_huc_table + os.sep + 'NL48_HUC_Overlap_' + date + '.csv')
    print ("Exported {0}".format(in_huc_table + os.sep + 'NL48_HUC_Overlap_' + date + '.csv'))

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)

