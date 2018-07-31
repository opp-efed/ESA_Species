import pandas as pd
import os

chemical_name = 'Carbaryl'
st_cnty = 'States'  # if running on cnty change to Counties
# use_lookup = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
#              r'\SupportingTables' + os.sep + chemical_name + "_RangeUses_lookup.csv"
suffixes = ['noadjust', 'adjEle','_adjEleHab','_adjHab']
use_lookup = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\SupportingTables\Carbaryl_Uses_lookup_20180430.csv'

state_fp_lookup = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
                  r'\_ExternalDrive\_CurrentSupportingTables\Usage\ForOverlap\STATEFP_lookup.csv'
pct_table_directory = r'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
            r'\Endangered Species Pilot Assessments - OverlapTables\SupportingTables\PCT\Carbaryl'

out_location = r'L:\ESA\Tabulates_Usage'
# species overlap with uses include the state or cnty breaks
in_location_species = r'L:\ESA\Test_usage\Agg_Layers\ByPolBoundary\PolBoundaries\States'
# totals for states actual overlap for the political boundary
in_locations_states = 'L:\ESA\Tabulated_PolBoundaries\PoliticalBoundaries\Agg_Layers\States'
regions = ['AK', 'AS', 'CNMI', 'CONUS', 'GU', 'HI', 'PR', 'VI']
# state_df = pd.read_csv(r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects"
#                        r"\ESA\_ED_results\Tabulated_Jan2018\PolticalBoundaries\PolticalBoundaries\Agg_Layers\States")
# species_df = pd.read_csv(
#     r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ED_results\Tabulated_Jan2018\PolticalBoundaries\PolticalBoundaries\Agg_Layers\States\CONUS_CDL_1016_10x2_euc.csv")


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

def uniform_spe (row, pct, sp_total_col):
    return row[sp_total_col] * row[pct]

def adjust_drift (df, pct):
    # Distance 0 adjusted separately

    cols = [v for v in df.columns.values.tolist() if v.startswith("VALUE_")]
    df.ix[:,cols] = df.ix[:, cols].apply(pd.to_numeric, errors='coerce')
    df.ix[:,cols] = df.ix[:,cols].multiply(df[pct], axis=0)
    return df
# ##
list_pct_table = [v for v in os.listdir(pct_table_directory) if v.endswith('.csv')]
for group in ['min','max','avg']:
    pct_table =  [v for v in list_pct_table if v.split("_")[1] == group]
    state_fp = pd.read_csv(state_fp_lookup)
    state_fp['STATEFP'] = state_fp['STATEFP'].map(lambda x: str(x) if len(str(x)) == 2 else '0' + str(x)).astype(str)
    pct_df = pd.read_csv(pct_table_directory +os.sep + pct_table[0])

    use_lookup_df = pd.read_csv(use_lookup)
    usage_lookup_df = use_lookup_df.ix[:, ['FullName', 'Usage lookup']]
    usage_lookup_df['Filename'] = usage_lookup_df['FullName'].map(lambda x: str(x) + "_euc.csv").astype(str)
    # assumes identifier is in this postion
    out_path = out_location +os.sep+chemical_name+os.sep+os.path.basename(pct_table[0]).split("_")[1]

    if not os.path.exists(os.path.dirname(out_path)):
        os.mkdir(os.path.dirname(out_path))
    if not os.path.exists(out_path):
        os.mkdir(out_path)

    t_pct = pct_df.T
    t_pct = t_pct.reset_index()
    update_cols = t_pct.iloc[0].values.tolist()
    update_cols[0] = 'STATE'
    t_pct.columns = update_cols
    t_pct = t_pct.reindex(t_pct.index.drop(0))
    t_pct = pd.merge(t_pct, state_fp, on='STATE', how='left')

    list_csv = os.listdir(in_location_species)


    for value in suffixes:
            c_list = [v for v in list_csv if v.endswith(value+'.csv')]  # break csv into suffix groups
            # in_results_sp = in_location_species + os.sep +folder+ os.sep + st_cnty
            # Load state overlap result for state # NOTE file names must be the same as species filter to just direct overlap
            # and other important cols

            for csv in c_list:
                state_csv = csv.replace("_"+value +'.csv', ".csv")
                for v in  state_csv.split("_"):
                    if v not in regions:
                        state_csv = state_csv.replace(v+"_", "")
                    else:
                        break


                state_df = pd.read_csv(in_locations_states + os.sep + state_csv)
                state_df['STATEFP'] = state_df['STATEFP'].map(lambda x: str(x) if len(str(x)) == 2 else '0' + str(x)).astype(str)
                filtered_state = state_df.ix[:, ['STATEFP',  'Acres', 'VALUE_0']]
                filtered_state.columns = ['STATEFP', 'Acres', 'State direct msq']

                # Load species overlap result for state
                species_df = pd.read_csv(in_location_species + os.sep +csv)
                species_df['STATEFP'] = species_df['STATEFP'].map(lambda x: str(x) if len(str(x)) == 2 else '0' + str(x)).astype(
                    str)

                # determine the crop in pct table that applies to the current csv then filter the pct table to just that crop and
                # other columns need to merge

                use_lookup_df_value = usage_lookup_df.loc[(usage_lookup_df['Filename'] == state_csv), 'Usage lookup'].iloc[0]
                filter_col = ['STATE', 'STATEFP']
                filter_col.append(use_lookup_df_value)
                filtered_pct = t_pct.ix[:, filter_col]

                filtered_pct.columns = ['STATE', 'STATEFP', 'PCT_'+use_lookup_df_value]

                # merge the pct to species_df to pct then merge to filtereed state
                merged_species = pd.merge(species_df, filtered_pct, on='STATEFP', how='left')
                merged_species_state = pd.merge(merged_species, filtered_state, on='STATEFP', how='left')
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
                csv_out = csv.replace('.csv',"_"+os.path.basename(pct_table[0]).split("_")[1]+'.csv')
                print csv_out
                merged_species_state.to_csv(out_path+os.sep+csv_out)
                print 'Table can be found at {0}'.format(out_path+os.sep+csv_out)
