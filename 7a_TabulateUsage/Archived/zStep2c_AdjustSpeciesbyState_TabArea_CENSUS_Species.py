import pandas as pd
import os
import datetime
import sys



chemical_name = 'Malathion_Census'

st_cnty = 'County'  # if running on cnty change to County, State if using state
range_ch = 'ch'  # ch for critical habitat
# use_lookup = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
#              r'\SupportingTables' + os.sep + chemical_name + "_RangeUses_lookup.csv"
# suffixes = ['noadjust', 'adjEle','_adjEleHab','_adjHab']  #TODO Change the second if to else when finished
suffixes = ['noadjust']

use_lookup = r'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
             r'\Endangered Species Pilot Assessments - OverlapTables\SupportingTables' \
             r'\Malathion_Uses_lookup_20181106_test.csv'

state_fp_lookup = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
                  r'\_ExternalDrive\_CurrentSupportingTables\Usage\ForOverlap\STATEFP_lookup.csv'

pct_table_directory = r'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
                      r'\Endangered Species Pilot Assessments - OverlapTables\SupportingTables\PCT\Malathion' \
                      r'\unsurvey_100'

presence_absence_cnty = 'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects' \
                        '\ESA\_ExternalDrive\_CurrentSupportingTables\Usage\cnty_pre_abs.csv'

out_location = r'L:\Workspace\StreamLine\ESA\Tabulated_TabArea_HUCAB'
# species overlap with uses include the state or cnty breaks
if st_cnty == 'State':
    in_location_species = r'L:\Workspace\StreamLine\ESA\Tabulated_TabArea_HUCAB\PolBoundaries\States'

else:
    in_location_species = r'L:\Workspace\StreamLine\ESA\Tabulated_TabArea_HUCAB\PolBoundaries\Counties'
# totals for states actual overlap for the political boundary
in_locations_states = 'L:\Workspace\StreamLine\ESA\Results_Usage\PolBoundaries\Agg_layers'
regions = ['AK', 'AS', 'CNMI', 'CONUS', 'GU', 'HI', 'PR', 'VI']


if not os.path.exists(out_location):
    os.mkdir(out_location)

if range_ch == 'r':
    file_type = 'Range'
else:
    file_type = 'CriticalHabitat'

out_location = out_location + os.sep + chemical_name
if not os.path.exists(out_location):
    os.mkdir(out_location)

out_location = out_location + os.sep + file_type

if not os.path.exists(out_location):
    os.mkdir(out_location)


def state_pct(row, msq_state_col, pct_col):
    adjust_value = row[msq_state_col] * row[pct_col]

    return adjust_value


def outside_range(row, state, species_state):
    adjust_value = row[state] - row[species_state]
    return adjust_value


def min_range(row, total, adjust):
    total_outside = row[total]
    st_adjust = row[adjust]

    if total_outside > st_adjust:
        return 0
    else:
        return st_adjust - total_outside


def max_range(row, total, adjust, ):
    st_adjust = row[adjust]
    sp_total = row[total]
    if sp_total < st_adjust:
        return sp_total
    else:
        return st_adjust


def uniform_spe(row, pct, sp_total_col):
    return row[sp_total_col] * row[pct]


def adjust_drift(df, pct):
    # Distance 0 adjusted separately

    cols = [v for v in df.columns.values.tolist() if v.startswith("VALUE_")]
    df.ix[:, cols] = df.ix[:, cols].apply(pd.to_numeric, errors='coerce')
    df.ix[:, cols] = df.ix[:, cols].multiply(df[pct], axis=0)
    return df


def apply_pct(pol_id_col, sp_df, fil_pct, fil_state, use_value):
    # merge the pct to species_df to pct then merge to filtereed state
    merged_species = pd.merge(sp_df, fil_pct, on=pol_id_col, how='left')
    merged_species_state = pd.merge(merged_species, fil_state, on=pol_id_col, how='left')
    merged_species_state['Drift_PCT'] = merged_species_state['PCT_' + use_value].map(
        lambda x: 0 if x == 0 else 1)

    # PCT Adjustments
    merged_species_state['State msq adjusted by PCT'] = \
        merged_species_state.apply(
            lambda row: state_pct(row, 'State direct msq', 'PCT_' + use_lookup_df_value), axis=1)
    merged_species_state['Total outside species range'] = \
        merged_species_state.apply(lambda row: outside_range(row, 'State direct msq', 'VALUE_0'),
                                   axis=1)
    merged_species_state['Total outside species range'] = \
        merged_species_state.apply(lambda row: outside_range(row, 'State direct msq', 'VALUE_0'),
                                   axis=1)
    merged_species_state['Total outside species range'] = \
        merged_species_state.apply(lambda row: outside_range(row, 'State direct msq', 'VALUE_0'),
                                   axis=1)
    merged_species_state['Min in Species range'] = \
        merged_species_state.apply(
            lambda row: min_range(row, 'Total outside species range', 'State msq adjusted by PCT'),
            axis=1)
    merged_species_state['Max in species range'] = merged_species_state.apply(
        lambda row: max_range(row, 'VALUE_0', 'State msq adjusted by PCT'), axis=1)
    merged_species_state['Uniform'] = merged_species_state.apply(
        lambda row: uniform_spe(row, 'PCT_' + use_lookup_df_value, 'VALUE_0'), axis=1)

    if pol_id_col == 'GEOID':
        merged_species_state = (merged_species_state.groupby(['EntityID', 'HUC2_AB', 'STUSPS']).sum()).reset_index()
        merged_species_state.drop('GEOID',axis=1, inplace=True)

    merged_species_state = adjust_drift(merged_species_state, 'Drift_PCT')
    csv_out = csv.replace('.csv', "_" + os.path.basename(pct_table[0]).split("_")[1] + '.csv')
    print csv_out
    merged_species_state.to_csv(out_path + os.sep + csv_out)
    print 'Table can be found at {0}\n'.format(out_path + os.sep + csv_out)
# ##
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Get date
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
presence_absence_df= pd.read_csv(presence_absence_cnty)
presence_absence_df['GEOID'] = presence_absence_df['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
state_fp = pd.read_csv(state_fp_lookup)
state_fp['STATEFP'] = state_fp['STATEFP'].map(lambda x: str(x) if len(str(x)) == 2 else '0' + str(x)).astype(str)
state_fp['GEOID'] = state_fp['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
state_fp['STATE_Upper'] = state_fp['STATE'].map(lambda x: str(x).upper()).astype(str)

list_pct_table = [v for v in os.listdir(pct_table_directory) if v.endswith('.csv')]

for group in ['max'] :  #['min', 'max', 'avg']
    # assumes identifier is in this postion
    pct_table = [v for v in list_pct_table if v.split("_")[1] == group] [0]
    pct_df = pd.read_csv(pct_table_directory + os.sep + pct_table)
    use_lookup_df = pd.read_csv(use_lookup)
    usage_lookup_df = use_lookup_df.ix[:, ['FullName', 'Usage lookup', 'Included AA']]
    usage_lookup_df['Filename'] = usage_lookup_df['FullName'].map(lambda x: str(x) + "_euc").astype(str)
    # limit look-up to just those uses in the current chemical
    usage_lookup_df = usage_lookup_df.loc[usage_lookup_df['Included AA'] == 'x']
    print usage_lookup_df
    list_use_folder= usage_lookup_df['Filename'].values.tolist()
    out_path = out_location +  os.sep + group
    print out_path

    if not os.path.exists(os.path.dirname(out_path)):
        os.mkdir(os.path.dirname(out_path))
    if not os.path.exists(out_path):
        os.mkdir(out_path)
    if not os.path.exists(out_location + os.sep + 'no adjustment'):
        os.mkdir(out_location + os.sep + 'no adjustment')

    if os.path.basename(in_location_species) == 'State':
        pol_id = 'STATEFP'
        pct_header = 'State'
    else:
        pol_id = 'GEOID'
        pct_header = 'GEOID'

    t_pct = pct_df.T
    t_pct = t_pct.reset_index()
    update_cols = t_pct.iloc[0].values.tolist()
    update_cols[0] = 'STATE_Upper'
    t_pct.columns = update_cols
    t_pct = t_pct.reindex(t_pct.index.drop(0))
    t_pct = pd.merge(t_pct, state_fp,  on= 'STATE_Upper', how='left')

    list_csv = os.listdir(in_location_species)

    for value in suffixes:
        if value == 'noadjust':
            c_list = [v for v in list_csv if v.endswith(value+'.csv')]
            for csv in c_list:
                cnty_csv = csv.replace("_"+value +'.csv', "_"+st_cnty+".csv")
                remove_sp_abb = cnty_csv.split("_")[0] +"_"+ cnty_csv.split("_")[1]+"_"
                cnty_csv = cnty_csv.replace(remove_sp_abb,'')

                cnty_folder = cnty_csv.replace( "_"+st_cnty+".csv","")
                if cnty_folder not in list_use_folder:
                    continue

                if not os.path.exists(out_location +os.sep+'no adjustment'+os.sep+csv):
                    csv_out = csv.replace('.csv',"_"+os.path.basename(pct_table).split("_")[1]+'.csv')
                    species_df = pd.read_csv(in_location_species + os.sep +csv, low_memory=False)
                    species_df.to_csv(out_location +os.sep+'no adjustment'+os.sep+csv)

            c_list = [v for v in list_csv if v.endswith(value+'.csv')]  # break csv into suffix groups

        if value == 'noadjust': # TODO removed if and change to elseafter completing update for elevation and habitat
            for csv in c_list:

                cnty_csv = csv.replace("_"+value +'.csv', "_"+st_cnty+".csv")
                remove_sp_abb = cnty_csv.split("_")[0] +"_"+ cnty_csv.split("_")[1]+"_"
                cnty_csv = cnty_csv.replace(remove_sp_abb,'')
                cnty_folder = cnty_csv.replace( "_"+st_cnty+".csv","")
                if cnty_folder not in list_use_folder:
                    continue
                else:
                    if not os.path.exists( out_path+os.sep+csv.replace('.csv',"_"+group+'.csv')):
                        print csv
                        usage_col_header = usage_lookup_df.loc[usage_lookup_df['Filename'] == cnty_folder].iloc[0]
                        cnty_df = pd.read_csv(in_locations_states + os.sep + cnty_folder  + os.sep + cnty_csv)
                        cnty_df ['GEOID'] = cnty_df ['GEOID'] .map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(
                            str)

                        cnty_w_p_a = pd.merge(cnty_df, presence_absence_df, on= 'GEOID', how='left')
                        cnty_w_p_a ['STATEFP'] = cnty_w_p_a ['GEOID'].map(lambda (n): str(n)[:2] if len(str(n)) == 5 else '0' + str(n)[:1]).astype(str)
                        usage_col_header = usage_lookup_df.loc[usage_lookup_df['Filename'] == cnty_folder, 'Usage lookup'].iloc[0]
                        cnty_w_p_a_use= cnty_w_p_a[cnty_df.columns.values.tolist()+[usage_col_header, 'STATEFP']]
                        print cnty_w_p_a_use.head()
                        # TODO Check why some of the PCTS are coming in as NaN is this because we are just running FLA?
                        cnty_w_p_a_use[usage_col_header].fillna(1, inplace=True)
                        # If we want to remove values in the drift are for crops not found in census
                        # cnty_w_p_a_use.ix[:,cnty_df.columns.values.tolist()] = cnty_w_p_a_use.ix[:,cnty_df.columns.values.tolist()] .multiply(cnty_w_p_a_use[usage_col_header], axis=0)

                        # this .loc and multople is causing a SettingWithCopyWarning:
                        # A value is trying to be set on a copy of a slice from a DataFrame
                        cnty_w_p_a_use.loc[:,['VALUE_0']] = cnty_w_p_a_use.loc[:,['VALUE_0']].multiply(cnty_w_p_a_use[usage_col_header], axis=0)
                        col_to_sum_state = ['STATEFP']+[v for v in cnty_df.columns.values.tolist() if v.startswith('VALUE_')]
                        # Copy causing a SettingWithCopyWarning:
                        # A value is trying to be set on a copy of a slice from a DataFrame
                        sum_df = cnty_w_p_a_use[col_to_sum_state].copy()

                        val_cols = [v for v in cnty_df.columns.values.tolist() if v.startswith('VALUE_')]
                        state_df  = sum_df.groupby(['STATEFP'])[val_cols].sum().reset_index()

                        # form verse w/o census
                        #state_df = pd.read_csv(in_locations_states + os.sep + state_folder + os.sep + state_csv)
                        state_df['STATEFP'] = state_df['STATEFP'].map(lambda x: str(x) if len(str(x)) == 2 else '0' + str(x)).astype(str)
                        filtered_state = state_df.ix[:, ['STATEFP',  'Acres', 'VALUE_0']].copy()
                        filtered_state.columns = ['STATEFP', 'Acres', 'State direct msq']

                        # Load species overlap result for state
                        species_df = pd.read_csv(in_location_species + os.sep +csv, low_memory=False)
                        species_df['STATEFP'] = species_df['STATEFP'].map(lambda x: str(x) if len(str(x)) == 2 else '0' + str(x)).astype(
                            str)

                        # determine the crop in pct table that applies to the current csv then filter the pct table to just that crop and
                        # other columns need to merge

                        use_lookup_df_value = usage_lookup_df.loc[usage_lookup_df['Filename'] == cnty_folder, 'Usage lookup'].iloc[0]
                        use_lookup_df_inc_chem = usage_lookup_df.loc[usage_lookup_df['Filename'] == cnty_folder, 'Included AA'].iloc[0]
                        if use_lookup_df_inc_chem == 'x':
                            filter_col = ['STATE', 'STATEFP', 'EntityID']  # ADD IN ENTITY ID SPECIES PCT
                            filter_col.append(use_lookup_df_value)
                            filtered_pct = t_pct.ix[:, filter_col].copy()
                            if len(filtered_pct.columns.values.tolist()) > 4:
                                print('Check the pct input tables for duplicate crop in the GenClass, there are too many '
                                      'columns from the filtered output for the state and PCT')
                                sys.exit()
                            filtered_pct.columns = ['STATE', 'STATEFP', 'EntityID','PCT_'+use_lookup_df_value]

                            # merge the pct to species_df to pct then merge to filtereed state
                            # HERE UPDATE PCT TABLE _ MERGE ON STATE AND ENTITY ID
                            merged_species = pd.merge(species_df, filtered_pct, on=['EntityID','STATEFP'], how='left')
                            merged_species_state = pd.merge(merged_species, filtered_state, on='STATEFP', how='left')
                            merged_species_state['Drift_PCT'] =  merged_species_state['PCT_'+use_lookup_df_value].map(lambda x: 0 if x == 0 else 1)

                            #TODO FOR SPECIES/CENSUS AG PCT - PCTS WILL NEED TO BE UPDATED TO SPECIES PCTS THIS CAN BE LOAD TO
                            # POL OVERLAP TABLE OR HERE WITH THE SPECIES OVERLAP
                            #TODO GENERATE SPECIES AGG PCTS THEN CHECK MEREGE TO SEE IF IT WORK
                            # PCT Adjustments
                            merged_species_state['State msq adjusted by PCT'] = \
                                merged_species_state.apply(lambda row: state_pct(row, 'State direct msq', 'PCT_'+use_lookup_df_value), axis=1)
                            merged_species_state['Total outside species range'] = \
                                merged_species_state.apply(lambda row: outside_range(row, 'State direct msq', 'VALUE_0'), axis=1)
                            merged_species_state['Total outside species range'] = \
                                merged_species_state.apply(lambda row: outside_range(row, 'State direct msq', 'VALUE_0'), axis=1)
                            merged_species_state['Total outside species range'] = \
                                merged_species_state.apply(lambda row: outside_range(row, 'State direct msq', 'VALUE_0'), axis=1)
                            merged_species_state['Min in Species range'] = \
                                merged_species_state.apply(lambda row: min_range(row, 'Total outside species range', 'State msq adjusted by PCT'), axis=1)
                            merged_species_state['Max in species range'] = merged_species_state.apply(lambda row: max_range(row, 'VALUE_0', 'State msq adjusted by PCT'), axis=1)
                            merged_species_state['Uniform'] = merged_species_state.apply(lambda row: uniform_spe (row, 'PCT_'+use_lookup_df_value, 'VALUE_0'), axis=1)
                            merged_species_state = adjust_drift (merged_species_state, 'Drift_PCT')
                            if value == 'noadjust':
                                csv_out = csv.replace('noadjust.csv',"census_species_"+group+'.csv')
                            else:
                                csv_out = csv.replace('.csv',"census_species_"+group+'.csv')
                                csv_out = csv.replace('.csv',"_"+group+'.csv')

                            merged_species_state.to_csv(out_path+os.sep+csv_out)
                            print 'Table can be found at {0}\n'.format(out_path+os.sep+csv_out)
                        else:
                            print ('Use {0} is not part of chemical\n'.format(use_lookup_df_value))

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)