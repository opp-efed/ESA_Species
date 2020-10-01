import pandas as pd
import os
import datetime
import sys

# Author J.Connolly USEPA

# Description: Applies the chemical specific information to the overlap results, this includes the Census of
# Agricultural (CoA) mask for counties represent exclusively by unregistered uses, and the aggregated PCTS (max, avg,
# min) for the chemical.

# This script has been approved for release by the U.S. Environmental Protection Agency (USEPA). Although
# the software has been subjected to rigorous review, the USEPA reserves the right to update the script as needed
# pursuant to further analysis and review. No warranty, expressed or implied, is made by the USEPA or the U.S.
# Government as to the functionality of the software and related material nor shall the fact of release constitute
# any such warranty. Furthermore, the software is released on condition that neither the USEPA nor the U.S. Government
# shall be held liable for any damages resulting from its authorized or unauthorized use.


# Variables set by user
chemical_name = 'Glyphosate'  # chemical name used for tracking
st_cnty = 'County'  # 'County' or 'State'

# flags to apply to the end of output file name for the different overlap scenarios.  This should always include
# 'noadjust' for the standard no adjustment and aggregated PCT runs; but user can had in additional flags to other
# overlap scenarios that included supplemental information such as as habitat
suffixes = ['noadjust']  # 'noadjust', 'adjHab' example result suffixes
pct_groups = ['max','min','avg']  # pcts to include in outputs

# Look up table with STATEFP, GEOID, and STATE
state_fp_lookup = r"C:\Users\JConno02\Environmental Protection Agency (EPA)" \
                  r"\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\STATEFP_lookup.csv"
# root path to the out location where the chemical independent results are saved
out_location = r'D:\Tabulated_HUCAB'

range_ch = 'ch'  # r for range  ch for critical habitat

# NOTE PCTS FOR AA AND COMPOSITES MOST BE 1 FOR THE FACTOR ADJUSTMENTS
# NOTE PCT, pre/ab, and use look up table must include the same same use names; identified in the Usage lookup col of
# use lookup table

# Folder with the max, avg, min aggregated PCT values for the chemical
# PCT tables must be in the format [chemical name]_[pct group].csv
# NOTES BEFORE RUNNING CONFIRM THE ACTION AREAS PCTS ARE 1
pct_table_directory = r'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
                      r'\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs' + os.sep + \
                      chemical_name+ os.sep+'PCT'
# input table identifying the uses and drift limits for the chemical
use_lookup = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs" \
             + os.sep + chemical_name + os.sep + "GLY_Uses_lookup_June2020_v2.csv"

# chemical masks based on the census of AG
# 1's identifies counties with a registered use in the UDL; 0's not registered use in the UDL
# NOTE be sure the input tables includes all counties, including counties not found in the Census of Ag
presence_absence_cnty = r'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
                      r'\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs' + os.sep + \
                        chemical_name + os.sep +"Cnty_Mask_20200810_GLY_1_0_cnty.csv"

# species overlap with uses include the state or cnty breaks; this points to chemical independent species results
# summarized to political boundaries; Confirm folder names
if st_cnty == 'State':
    in_location_species = out_location + os.sep +'Sp_PolBoundaries\States'
else:
    in_location_species = out_location + os.sep + 'Sp_PolBoundaries\Counties'


# overlap for the political boundary, state and county - no species info
in_locations_states = 'D:\Results_HUCAB\PolBoundaries\Agg_layers'

# Static variables no user input required
# sets output file directory set by script;
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


# Functions
def state_pct(row, msq_state_col, pct_col):
    # calc state treated area
    adjust_value = row[msq_state_col] * row[pct_col]
    return adjust_value


def outside_range(row, state, species_state):
    # calc use area outside species files
    adjust_value = row[state] - row[species_state]
    # address edge effect when there is more area in the species range in a state than in the whole state updated on
    # 7/30/20 lower limited of outside of the range but with in the state is 0; edge effect can result in negative
    # values
    if adjust_value < 0:
        adjust_value = 0
    return adjust_value


def min_range(row, total, adjust):
    # calc min use area in species files
    total_outside = row[total]
    st_adjust = row[adjust]
    if total_outside > st_adjust:
        return 0
    else:
        return st_adjust - total_outside


def max_range(row, total, adjust, ):
    # calc  max use area in species files
    st_adjust = row[adjust]
    sp_total = row[total]
    if sp_total < st_adjust:
        return sp_total
    else:
        return st_adjust


def uniform_spe(row, pct, sp_total_col):
    # calc uniform treated area in species files
    return row[sp_total_col] * row[pct]


def adjust_drift(df, pct):
    # Distance 0 adjusted separately
    cols = [v for v in df.columns.values.tolist() if v.startswith("VALUE_")]
    df.ix[:, cols] = df.ix[:, cols].apply(pd.to_numeric, errors='coerce')
    df.ix[:, cols] = df.ix[:, cols].multiply(df[pct], axis=0)
    return df


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Get date
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
# load presence/absence table
presence_absence_df= pd.read_csv(presence_absence_cnty, low_memory=False)
# confirm data type; and check for leading zeros that dropped
presence_absence_df['GEOID'] = presence_absence_df['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
# load state_fp_lookup table
state_fp = pd.read_csv(state_fp_lookup)
# confirm data type; and check for leading zeros that dropped
state_fp['STATEFP'] = state_fp['STATEFP'].map(lambda x: str(x) if len(str(x)) == 2 else '0' + str(x)).astype(str)
state_fp['GEOID'] = state_fp['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
# added state name in all upper case letters for merges
state_fp['STATE_Upper'] = state_fp['STATE'].map(lambda x: str(x).upper()).astype(str)
# get a list of PCT tables to run
list_pct_table = [v for v in os.listdir(pct_table_directory) if v.endswith('.csv')]

for group in pct_groups:  # pcts to include in output - loops over each group
    # HARD CODE - assumes PCT identifier is in position 1 of the file name
    print list_pct_table
    pct_table = [v for v in list_pct_table if v.split("_")[1] == group] [0]
    # read pct table for chemicals
    pct_df = pd.read_csv(pct_table_directory + os.sep + pct_table)
    # reads use lookup tables to determine which uses are applicable for chem
    use_lookup_df = pd.read_csv(use_lookup)
    usage_lookup_df = use_lookup_df.ix[:, ['FullName', 'Usage lookup', 'Included AA']]  # filters tables
    # Add the raster filename to the look-up tables by adding the suffix _euc
    usage_lookup_df['Filename'] = usage_lookup_df['FullName'].map(lambda x: str(x) + "_euc").astype(str)
    # limit look-up to just those uses applicable to the current chemical
    # HARD CODE-MUST use x in the use lookup to flag use for chemical
    usage_lookup_df = usage_lookup_df.loc[usage_lookup_df['Included AA'] == 'x']
    # print uses that will be include for users to see
    print list(set(use_lookup_df['Usage lookup'].values.tolist()))
    # get a list of folders with the individual use results
    list_use_folder= usage_lookup_df['Filename'].values.tolist()
    out_path = out_location + os.sep + group

    # adds output directories if needed to save the chemical specific output
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
    # prepare pct table for to merge with the species results based on the state name
    t_pct = pct_df.T  # transforms PCT table so uses are the headers
    t_pct = t_pct.reset_index()  # reset index
    update_cols = t_pct.iloc[0].values.tolist()  # col headers are in row 0
    update_cols[0] = 'STATE_Upper'  # sets col for index position 0
    t_pct.columns = update_cols # updates col heading
    t_pct = t_pct.reindex(t_pct.index.drop(0))  # drops row with col headers
    t_pct = pd.merge(t_pct, state_fp,  on= 'STATE_Upper', how='left')  # merges to pol bound lookup
    # get a list of the all of the chemical independent results - not all of these will be applicable
    list_csv = os.listdir(in_location_species)  # get a lists of files
    list_csv = [c for c in list_csv if c.startswith(range_ch)]  # filters list to just the range or ch
    # loops over the overlap scenarios user identified
    for value in suffixes:
        print value
        if value == 'noadjust':
            c_list = [v for v in list_csv if v.endswith(value+'.csv')]
            for csv in c_list:
                # striped the use name from file name to link the species and state overlap results together
                state_csv = csv.replace("_"+value +'.csv', "_"+'State'+".csv")
                remove_sp_abb = state_csv.split("_")[0] +"_"+state_csv.split("_")[1]+"_"
                state_csv = state_csv.replace(remove_sp_abb,'')
                state_folder = state_csv.replace( "_"+'State'+".csv","")
                if state_folder.replace('_State.csv','') not in list_use_folder:
                    continue
                if not os.path.exists(out_location +os.sep+'no adjustment'+os.sep+csv):
                    print ('Working on {0} noadjust'.format(csv))
                    csv_out = csv.replace('.csv',"_"+os.path.basename(pct_table).split("_")[1]+'.csv')
                    species_df = pd.read_csv(in_location_species + os.sep +csv)
                    species_df.to_csv(out_location +os.sep+'no adjustment'+os.sep+csv)

        # gets list of results tables specific to the current overlap scenario; identified using the suffix
        c_list = [v for v in list_csv if v.endswith(value+'.csv')]

        if value in suffixes:   # loops over the overlap scenarios user identified

            for csv in c_list:
                # loading county file to apply the pre/ab mask for non registered crops; then sums to state
                state_csv = csv.replace("_"+value + '.csv', "_"+'County'+".csv")
                # striped the use name from file name to link the species and pol_boundary overlap results together
                # NOTE files name structure set by scripts must be used
                # NOTE - script used the variable state to mean political boundaries - this can be the state or cnty
                remove_sp_abb = state_csv.split("_")[0] + "_" + state_csv.split("_")[1]+"_"
                state_csv = state_csv.replace(remove_sp_abb, '')
                state_folder = state_csv.replace("_" + 'County' + ".csv", "")
                if state_folder.replace('_County.csv', '') not in list_use_folder:
                    continue
                else:
                    # creates out_csv name
                    # after saving files without pct adjustment applies pcts and saves new name
                    if value == 'noadjust':
                        csv_out = csv.replace('noadjust.csv', "census_" + group + '.csv')
                    else:
                        csv_out = csv.replace('.csv', "census_" + value + "_" + group + '.csv')

                    if not os.path.exists(out_path+os.sep+csv_out):
                        print ('Working on {0}, {1}, {2}'.format(csv, value, group))
                        state_df = pd.read_csv(in_locations_states + os.sep + state_folder + os.sep + state_csv)
                        # confirm data type; and check for leading zeros
                        state_df['GEOID'] = state_df['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
                        state_df['STATEFP'] = state_df['GEOID'].str[:2]  # extracts StateFP from GeoID

                        # Get the use lookup value from the usage lookup table
                        # get the term for the usage column based on the use name used for the folder name
                        # this column header is used to link several of the lookup tables
                        usage_col_header = usage_lookup_df.loc[usage_lookup_df['Filename'] == state_folder, 'Usage lookup'].iloc[0]
                        print ('Value use to link to the usage tables is {0}'.format(usage_col_header))

                        filtered_state = state_df.ix[:, ['STATEFP', 'GEOID', 'Acres', 'VALUE_0']].copy()

                        # Summed 2019: apply pres/ab to both the state and species before calc treated area so that
                        # treated area is based on counties with registered uses
                        # joins political boundary table with pre/ab table
                        filtered_state = pd.merge(filtered_state, presence_absence_df, on= ['GEOID'], how='left')
                        # multiples the presences/ab by the state overlap making counties with no registered crops 0
                        # NOTE: the presence absence table must use the column headers found in the usage_col_header
                        # of the use look up table
                        filtered_state.loc[:,['VALUE_0']] = filtered_state.loc[:,['VALUE_0']].multiply(filtered_state[usage_col_header], axis=0)
                        # extracts the columns for the direct overlap by state after applying the pres/ab table by
                        # county
                        filtered_state = filtered_state.ix[:, ['STATEFP', 'Acres', 'VALUE_0']].copy()
                        # updated column headers
                        filtered_state.columns = ['STATEFP', 'Acres', 'State direct msq']
                        # sums counties to state
                        filtered_state = filtered_state.groupby(['STATEFP'])[['Acres', 'State direct msq']].sum().reset_index()
                        # Load species overlap result with state and county information
                        species_df = pd.read_csv(in_location_species + os.sep + csv, low_memory=False)
                        unnamed_cols = [v for v in species_df.columns.values.tolist() if v.startswith('Unnamed')]
                        species_df.drop(unnamed_cols,axis=1,inplace=True)
                        species_df['STATEFP'] = species_df['STATEFP'].map(
                            lambda x: str(x) if len(str(x)) == 2 else '0' + str(x)).astype(str)
                        species_df['GEOID'] = species_df ['GEOID'] .map(
                            lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
                        # joins species table with pre/ab table
                        # NOTE: the presence absence table must use the column headers found in the usage_col_header
                        # of the use look up table
                        cnty_w_p_a = pd.merge(species_df, presence_absence_df, on= ['GEOID'], how='left')
                        cnty_w_p_a.drop_duplicates(inplace=True)
                        cnty_w_p_a_use= cnty_w_p_a[species_df.columns.values.tolist()+[usage_col_header]].copy()
                        cnty_w_p_a_use[usage_col_header].fillna(1, inplace=True)
                        # multiples the presences/ab by the species overlap making counties with no registered crops 0
                        cnty_w_p_a_use.loc[:,['VALUE_0']] = cnty_w_p_a_use.loc[:,['VALUE_0']].multiply(cnty_w_p_a_use[usage_col_header], axis=0)
                        # extract the columns releated to species, states, and overlap
                        col_to_sum_state = ['EntityID','STATEFP']+[v for v in species_df.columns.values.tolist() if v.startswith('VALUE_')]
                        sum_df = cnty_w_p_a_use[col_to_sum_state].copy()  # makes a copy of the filtered df
                        # identifies the overlap and drift columns
                        val_cols = [v for v in species_df.columns.values.tolist() if v.startswith('VALUE_')]
                        # sum by species and state for all overlap  columns

                        species_state_df = sum_df.groupby(['EntityID','STATEFP'])[val_cols].sum().reset_index()
                        # loops over just the uses identified as part of the AA for the chemical based on the use
                        # look-up table
                        use_lookup_df_inc_chem = usage_lookup_df.loc[usage_lookup_df['Filename'] == state_folder, 'Included AA'].iloc[0]
                        if use_lookup_df_inc_chem == 'x':
                            filter_col = ['STATE', 'STATEFP', usage_col_header]
                            # extract the state and pct information for the current use from the PCT table
                            filtered_pct = t_pct.ix[:, filter_col].copy()
                            if len(filtered_pct.columns.values.tolist()) > 3:
                                print('Check the pct input tables for duplicate crop in the GenClass, there are too '
                                      'many columns from the filtered output for the state and PCT')
                                sys.exit()
                            # updates column headers
                            filtered_pct.columns = ['STATE', 'STATEFP', 'PCT_'+usage_col_header]
                            # If this no overlap the table will be empty and can't be joined
                            if 'STATEFP' not in species_state_df.columns.values.tolist() == 0:
                                # if overlap tables is blank, ie no overlap moves to the next table
                                pass
                            else:
                                # merge the pct to species_df to pct then merge to filtered state table to calc the
                                # state treated acres based on the PCT
                                merged_species = pd.merge(species_state_df, filtered_pct, on='STATEFP', how='left')
                                merged_species.drop_duplicates(inplace=True)
                                merged_species_state = pd.merge(merged_species, filtered_state, on='STATEFP', how='left')
                                merged_species_state.drop_duplicates(inplace=True)
                                merged_species_state['Drift_PCT'] =  merged_species_state['PCT_'+usage_col_header].map(lambda x: 0 if x == 0 else 1)

                                # CONFIRM DATA TYPE of results to be adjusted by PCT
                                merged_species_state['State direct msq'] = merged_species_state['State direct msq'].map(lambda x: x).astype(float)
                                merged_species_state['PCT_'+usage_col_header] = merged_species_state['PCT_'+usage_col_header].map(lambda x: x).astype(float)
                                merged_species_state['VALUE_0'] = merged_species_state['VALUE_0'].map(lambda x: x).astype(float)

                                # PCT Adjustments by treated area bounding scenario
                                # calculates the state treated area
                                merged_species_state['State msq adjusted by PCT'] = \
                                    merged_species_state.apply(lambda row: state_pct(row, 'State direct msq', 'PCT_'+usage_col_header), axis=1)
                                # calculated the total use outside the species range, min use in range (lower bound
                                # distribution of treated areas); max in species range (upper bound distribution of
                                # treated areas) and the uniform distribution of treated areas
                                merged_species_state['Total outside species range'] = merged_species_state.apply(lambda row: outside_range(row, 'State direct msq', 'VALUE_0'), axis=1)
                                merged_species_state['Lower in species range'] = merged_species_state.apply(lambda row: min_range(row, 'Total outside species range', 'State msq adjusted by PCT'), axis=1)
                                merged_species_state['Upper in species range'] = merged_species_state.apply(lambda row: max_range(row, 'VALUE_0', 'State msq adjusted by PCT'), axis=1)
                                # calculates the uniform treated area by applying the PCT to species results
                                merged_species_state['Uniform in species range'] = merged_species_state.apply(lambda row: uniform_spe (row, 'PCT_'+usage_col_header, 'VALUE_0'), axis=1)
                                # If there is not usage drift values adjusted to 0
                                merged_species_state = adjust_drift (merged_species_state, 'Drift_PCT')

                                # set the output tables with overlap scenario flags
                                if value == 'noadjust':
                                    csv_out = csv.replace('noadjust.csv',"census_"+group+'.csv')
                                else:
                                    csv_out = csv.replace('.csv',"census_"+value+"_"+group+'.csv')
                                merged_species_state.to_csv(out_path+os.sep+csv_out)
                                print 'Table can be found at {0}\n'.format(out_path+os.sep+csv_out)
                        else:
                            print ('Use {0} is not part of chemical\n'.format(usage_col_header))

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
