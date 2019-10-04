import pandas as pd
import os
import datetime
import sys


# Author J.Connolly
# Internal deliberative, do not cite or distribute

# This script generates the species/use table for a chemical ie r_amphib_CONUS_CDL_1317_10x2_euc_census_max.csv
# generated in Step 2 it adds in the chemical information, PCT, min/max/uniform in range/ch; these tables are them
# summarize by use (3a).  The percent overlap calculated (3b)

chemical_name = ''

st_cnty = 'County'  # if running on cnty change to County, State if using state

suffixes = ['noadjust']  # 'noadjust', 'adjEle','adjEleHab','adjHab' result suffixes

state_fp_lookup = r'path\STATEFP_lookup.csv'
out_location = r'outlocation'

range_ch = 'r'  #r for range  ch for critical habitat

#NOTE PCTS FOR AA AND COMPOSITES MOST BE 1 FOR THE FACTOR ADJUSTMENTS
# PCT, pre/ab, and use look up table must use same use names - this is the Usage lookup values on the use lookup table
pct_table_directory = r'path\PCT' + os.sep + chemical_name

use_lookup = r"path"+ os.sep + chemical_name + "Uses_lookup_20190409.csv"

# chemical masks
if chemical_name == '':
    presence_absence_cnty = r'path\filename.csv'
elif chemical_name == '':
    presence_absence_cnty = r'path\filename.csv'
else:
    print('Check the presences absence table for the chemical')
    sys.exit()
# species overlap with uses include the state or cnty breaks
if st_cnty == 'State':
    in_location_species = out_location + os.sep +'PolBoundaries\States'

else:
    in_location_species = out_location + os.sep + 'PolBoundaries\Counties'

# totals for states; overlap for the political boundary
in_locations_states = 'pathP\PolBoundaries\Agg_layers'
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
''
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
presence_absence_df= pd.read_csv(presence_absence_cnty, low_memory=False)
presence_absence_df['GEOID'] = presence_absence_df['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
state_fp = pd.read_csv(state_fp_lookup)
state_fp['STATEFP'] = state_fp['STATEFP'].map(lambda x: str(x) if len(str(x)) == 2 else '0' + str(x)).astype(str)
state_fp['GEOID'] = state_fp['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
state_fp['STATE_Upper'] = state_fp['STATE'].map(lambda x: str(x).upper()).astype(str)

list_pct_table = [v for v in os.listdir(pct_table_directory) if v.endswith('.csv')]

for group in [ 'max','avg','min'] :  # pcts to include
    # assumes identifier is in this position
    print list_pct_table
    pct_table = [v for v in list_pct_table if v.split("_")[1] == group] [0]
    pct_df = pd.read_csv(pct_table_directory + os.sep + pct_table) # read pct table
    use_lookup_df = pd.read_csv(use_lookup) # reads use lookup tables
    usage_lookup_df = use_lookup_df.ix[:, ['FullName', 'Usage lookup', 'Included AA']] # filters tables
    usage_lookup_df['Filename'] = usage_lookup_df['FullName'].map(lambda x: str(x) + "_euc").astype(str) # set filename
    # limit look-up to just those uses in the current chemical
    usage_lookup_df = usage_lookup_df.loc[usage_lookup_df['Included AA'] == 'x'] # filter to just uses for chemial per lookups
    print use_lookup_df['Usage lookup'].values.tolist()
    list_use_folder= usage_lookup_df['Filename'].values.tolist()  # get a list of folders for chemical
    out_path = out_location + os.sep + group
    print out_path
    # adds directories if needed
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

    t_pct = pct_df.T  # transforms PCT table so uses are the headers
    t_pct = t_pct.reset_index()  # reset index
    update_cols = t_pct.iloc[0].values.tolist()  # col headers are in row 0
    update_cols[0] = 'STATE_Upper'  # sets col for index position 0
    t_pct.columns = update_cols # updates col heading
    t_pct = t_pct.reindex(t_pct.index.drop(0))  # drops row with col headers
    t_pct = pd.merge(t_pct, state_fp,  on= 'STATE_Upper', how='left')  # merges to pol bound lookup

    list_csv = os.listdir(in_location_species)  # get a lists of files
    list_csv = [c for c in list_csv if c.startswith(range_ch)]  # filters list

    for value in suffixes:
        if value == 'noadjust':
            c_list = [v for v in list_csv if v.endswith(value+'.csv')]
            for csv in c_list:
                state_csv = csv.replace("_"+value +'.csv', "_"+'State'+".csv")
                remove_sp_abb = state_csv.split("_")[0] +"_"+state_csv.split("_")[1]+"_"
                state_csv = state_csv.replace(remove_sp_abb,'')
                state_folder = state_csv.replace( "_"+'State'+".csv","")
                if state_folder.replace('_State.csv','')  not in list_use_folder:
                    continue
                if not os.path.exists(out_location +os.sep+'no adjustment'+os.sep+csv):
                    csv_out = csv.replace('.csv',"_"+os.path.basename(pct_table).split("_")[1]+'.csv')
                    species_df = pd.read_csv(in_location_species + os.sep +csv)
                    species_df.to_csv(out_location +os.sep+'no adjustment'+os.sep+csv)

        c_list = [v for v in list_csv if v.endswith(value+'.csv')]  # break csv into suffix groups

        if value in suffixes: # TODO removed if and change to elseafter completing update for elevation and habitat
            for csv in c_list:
                print csv
                # loading county file so we can apply the pre/ab mask for non registered crops; then  sum to state
                state_csv = csv.replace("_"+value +'.csv', "_"+'County'+".csv")
                remove_sp_abb = state_csv.split("_")[0] +"_"+state_csv.split("_")[1]+"_"
                state_csv = state_csv.replace(remove_sp_abb,'')
                state_folder = state_csv.replace( "_"+'County'+".csv","")
                if state_folder.replace('_County.csv','')  not in list_use_folder:
                    continue
                else:
                    print out_path+os.sep+csv.replace('.csv',"_"+group+'.csv')
                    if value == 'noadjust':
                        csv_out = csv.replace('noadjust.csv',"census_"+group+'.csv')
                    else:
                        csv_out = csv.replace('.csv',"census_"+value+"_"+group+'.csv')

                    if not os.path.exists( out_path+os.sep+csv_out):
                        print csv
                        usage_col_header = usage_lookup_df.loc[usage_lookup_df['Filename'] == state_folder].iloc[0]

                        state_df = pd.read_csv(in_locations_states + os.sep + state_folder + os.sep + state_csv)
                        state_df['STATEFP'] = state_df['STATEFP'].map(lambda x: str(x) if len(str(x)) == 2 else '0' + str(x)).astype(
                            str)
                        state_df['GEOID'] = state_df['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
                        usage_col_header = usage_lookup_df.loc[usage_lookup_df['Filename'] == state_folder, 'Usage lookup'].iloc[0]
                        print use_lookup

                        filtered_state = state_df.ix[:, ['STATEFP','GEOID', 'Acres', 'VALUE_0']]
                        # Summed 2019: Team discussion - apply pres/ab to both the state and species before calc
                        # treated acres
                        # joins with pre/ab table
                        filtered_state = pd.merge(filtered_state, presence_absence_df, on= ['GEOID'], how='left')
                        # multiples the presences/ab by the state overlap making counties with no registered crops 0
                        filtered_state.loc[:,['VALUE_0']] = filtered_state.loc[:,['VALUE_0']].multiply(filtered_state[usage_col_header], axis=0)
                        filtered_state = filtered_state.ix[:, ['STATEFP', 'Acres', 'VALUE_0']]
                        filtered_state.columns = ['STATEFP', 'Acres', 'State direct msq']
                        # sums counties to state
                        filtered_state = filtered_state.groupby(['STATEFP'])[['Acres', 'State direct msq']].sum().reset_index()
                        # Load species overlap result for state
                        species_df = pd.read_csv(in_location_species + os.sep +csv, low_memory=False)
                        species_df['STATEFP'] = species_df['STATEFP'].map(lambda x: str(x) if len(str(x)) == 2 else '0' + str(x)).astype(
                            str)
                        species_df ['GEOID'] = species_df ['GEOID'] .map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(
                            str)
                        # joins with pre/ab table
                        cnty_w_p_a = pd.merge(species_df, presence_absence_df, on= ['GEOID'], how='left')
                        cnty_w_p_a.drop_duplicates(inplace=True)
                        # print statement to QC outputs
                        # cnty_w_p_a.to_csv (r'L:\Workspace\StreamLine\ESA\Tabulated_TabArea_HUCAB_Usage\Methomyl\test_census' + os.sep+ 'merge_' +csv)

                        cnty_w_p_a_use= cnty_w_p_a[species_df.columns.values.tolist()+[usage_col_header]]
                        cnty_w_p_a_use[usage_col_header].fillna(1, inplace=True)
                        # If we want to remove values in the drift are for crops not found in census
                        # cnty_w_p_a_use.ix[:,cnty_df.columns.values.tolist()] = cnty_w_p_a_use.ix[:,cnty_df.columns.values.tolist()] .multiply(cnty_w_p_a_use[usage_col_header], axis=0)

                        # multiples the presences/ab by the species overlap making counties with no registered crops 0
                        cnty_w_p_a_use.loc[:,['VALUE_0']] = cnty_w_p_a_use.loc[:,['VALUE_0']].multiply(cnty_w_p_a_use[usage_col_header], axis=0)
                        # print statement to QC outputs
                        # cnty_w_p_a_use.to_csv (r'L:\Workspace\StreamLine\ESA\Tabulated_TabArea_HUCAB_Usage\Methomyl\test_census'+os.sep + 'multiple_' +csv)

                        col_to_sum_state = ['EntityID','STATEFP']+[v for v in species_df.columns.values.tolist() if v.startswith('VALUE_')]
                        sum_df = cnty_w_p_a_use[col_to_sum_state].copy()

                        val_cols = [v for v in species_df.columns.values.tolist() if v.startswith('VALUE_')]
                        species_state_df  = sum_df.groupby(['EntityID','STATEFP'])[val_cols].sum().reset_index()
                        # print state to qc outputs
                        # species_state_df.to_csv (r'L:\Workspace\StreamLine\ESA\Tabulated_TabArea_HUCAB_Usage\Methomyl\test_census' + os.sep+'sum_' +csv)

                        # determine the crop in pct table that applies to the current csv then filter the pct table to just that crop and
                        # other columns need to merge
                        use_lookup_df_value = usage_lookup_df.loc[usage_lookup_df['Filename'] == state_folder, 'Usage lookup'].iloc[0]
                        use_lookup_df_inc_chem = usage_lookup_df.loc[usage_lookup_df['Filename'] == state_folder, 'Included AA'].iloc[0]
                        if use_lookup_df_inc_chem == 'x':
                            filter_col = ['STATE', 'STATEFP']
                            filter_col.append(use_lookup_df_value)
                            filtered_pct = t_pct.ix[:, filter_col].copy()

                            if len(filtered_pct.columns.values.tolist()) > 3:
                                print('Check the pct input tables for duplicate crop in the GenClass, there are too many '
                                      'columns from the filtered output for the state and PCT')
                                sys.exit()
                            filtered_pct.columns = ['STATE', 'STATEFP', 'PCT_'+use_lookup_df_value]

                            # If this no overlap the table will be empty and can't be joined
                            if 'STATEFP' not in species_state_df.columns.values.tolist() == 0:
                                pass
                            else:
                                # merge the pct to species_df to pct then merge to filtereed state
                                merged_species = pd.merge(species_state_df, filtered_pct, on='STATEFP', how='left')
                                merged_species.drop_duplicates(inplace=True)   # TODO FIND THE MERGE THAT CAUSE DUP ROWS
                                merged_species_state = pd.merge(merged_species, filtered_state, on='STATEFP', how='left')
                                merged_species_state.drop_duplicates(inplace=True)  # TODO FIND THE MERGE THAT CAUSE DUP ROWS
                                merged_species_state['Drift_PCT'] =  merged_species_state['PCT_'+use_lookup_df_value].map(lambda x: 0 if x == 0 else 1)

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
                                    csv_out = csv.replace('noadjust.csv',"census_"+group+'.csv')
                                else:
                                    csv_out = csv.replace('.csv',"census_"+value+"_"+group+'.csv')

                                print csv_out
                                merged_species_state.to_csv(out_path+os.sep+csv_out)
                                print 'Table can be found at {0}\n'.format(out_path+os.sep+csv_out)
                        else:
                            print ('Use {0} is not part of chemical\n'.format(use_lookup_df_value))

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)