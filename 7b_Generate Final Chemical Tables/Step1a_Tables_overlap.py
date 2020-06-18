import datetime
import os

import pandas as pd

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# Be sure on off field is accounted for

chemical_name = 'Carbaryl'  # chemical_name = 'Carbaryl', Methomyl
file_type = 'Range'  # 'Range or CriticalHabitat

# input table identifying the uses and drift limits for the chemical
use_lookup = r"C:\Users\JConno02\Environmental Protection Agency (EPA)" \
             r"\Endangered Species Pilot Assessments - OverlapTables\SupportingTables" + os.sep + chemical_name + "_Step1_Uses_lookup_20191104.csv"
# Limit of aerial drift model
max_drift = '792'
nl48 = True

# root path directory
# out tabulated root path - ie Tabulated_[suffix] folder

# root_path  = r'E:\Workspace\StreamLine\ESA\Tabulated_UsageHUCAB'
root_path = r'E:\Workspace\StreamLine\ESA\Tabulated_UsageHUCABHabitat'

# Tables directory  one level done from chemical
# No adjustment
folder_path = r'SprayInterval_IntStep_30_MaxDistance_1501\noadjust'

# table names from previous steps found in folder above if you concatenate the path summarized into L48/NL48
l48_BE_sum_table = "R_UnAdjusted_Full Range_AllUses_BE_L48_SprayInterval_noadjust_20191127.csv"
nl48_BE_sum_table = "R_UnAdjusted_Full Range_AllUses_BE_NL48_SprayInterval_noadjust_20191127.csv"
#
# l48_BE_sum_table = "CH_UnAdjusted_Full CH_AllUses_BE_L48_SprayInterval_noadjust_20191127.csv"
# nl48_BE_sum_table = "CH_UnAdjusted_Full CH_AllUses_BE_NL48_SprayInterval_noadjust_20191127.csv"

master_list = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\MasterListESA_Dec2018_20190130.csv"
# columns from master species list to include in the output tables
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']

# Static variables to set the output directory and identify the type os runs; Range (R_) Critical Habitat (CH_)
out_location = root_path
l48_BE_sum = root_path + os.sep + chemical_name + os.sep + file_type + os.sep + folder_path + os.sep + l48_BE_sum_table
if nl48:
    nl48_BE_sum = root_path + os.sep + chemical_name + os.sep + file_type + os.sep + folder_path + os.sep + nl48_BE_sum_table
on_off_species = []
# get the files type for the runs, range or critical habitat
find_file_type = os.path.basename(l48_BE_sum)
if find_file_type.startswith('R'):
    file_type_marker = 'R_'
else:
    file_type_marker = 'CH_'

# Functions
def create_directory(dbf_dir):
    # Create output directories
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


def step_1_ED(row, col_l48):
    value = ''
    col_nl48 = col_l48.replace('CONUS', 'NL48')
    try:
        if row[col_l48] < 0.44 and row[col_nl48] < 0.44:
            value = 'No Effect - Overlap'
        elif row[col_l48] <= 4.45 and row[col_nl48] < 4.45:
            value = 'NLAA - Overlap - 5percent'

        elif row['CONUS_Federal Lands_0'] >= 98.5 or row['NL48_Federal Lands_0'] >= 99:
            value = 'No Effect - Federal Land'
        elif row['CONUS_Federal Lands_0'] >= 94.5 or row['NL48_Federal Lands_0'] >= 94.5:
            value = 'NLAA - Federal Land'

        elif row[col_l48] >= 0.45 or row[col_nl48] >= 0.45:
            value = 'May Affect'

        if file_type_marker == 'CH_':
            if row['Source of Call final BE-Critical Habitat'] != 'Terr WoE' and \
                    row['Source of Call final BE-Critical Habitat'] != 'Aqua WoE' and \
                    row['Source of Call final BE-Critical Habitat'] != 'Terr and Aqua WoE':
                value = 'No CritHab'
            if str(row['Source of Call final BE-Critical Habitat']).startswith('Qual'):
                value = 'Qualitative'

        if file_type_marker == 'R_':

            if str(row['Source of Call final BE-Range']).startswith('Qu'):
                value = 'Qualitative'
    except KeyError:  # if one of the tracking cols in this function is missing comment can't be added
        value = 'Tracking Col not completed'

    return value


def on_off_field(row, cols, df):
    ent_id = row['EntityID']
    if ent_id in on_off_species:
        col = [v for v in cols if v.endswith("_0")]
        direct_over = row[col[0]]
        df.loc[df['EntityID'] == ent_id, [col[0]]] = 0
        for other_col in cols:
            if other_col == col[0]:
                pass
            else:
                value = row[other_col]
                df.loc[df['EntityID'] == ent_id, [other_col]] = value - direct_over
    else:
        pass


def track_parameters (chem_name, f_type, use_lkup, masterlist, l48_be, nl48_be, out_loc,  c_date):
    # Export a table containing the parameters used to generated the Step 1 tables
    parameters_used = pd.DataFrame(columns=['Chemical Name', 'File Type', 'Use Lookup', 'Species List',
                                            'In Location L48', 'In Location NL48', 'Out file', 'Out Base Location'])
    parameters_used.loc[0, 'Chemical Name'] = chem_name
    parameters_used.loc[0, 'File Type'] = f_type
    parameters_used.loc[0, 'Use Lookup'] = use_lkup
    parameters_used.loc[0, 'Species List'] = masterlist
    parameters_used.loc[0, 'In Location L48'] = l48_be
    parameters_used.loc[0, 'In Location NL48'] = nl48_be
    parameters_used.loc[0, 'Out file'] = 'Step 1 Summarized'
    parameters_used.loc[0, 'Out Base Location'] = out_loc + os.sep + chem_name + os.sep + 'Summarized Tables'
    parameters_used.to_csv(out_loc + os.sep + chem_name + os.sep + 'Summarized Tables' + os.sep + 'Parameters_used_'
                           + f_type + "_Step 1 Summarized_" + c_date + '.csv')
    print("Parameter file can be found at {0}".format(out_loc + os.sep + chem_name + os.sep + 'Summarized Tables'
                                                      + os.sep + 'Parameters_used_' + f_type + "Step 1 Summarized_"
                                                      + c_date + '.csv'))
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
# Get the data
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
# create output directories
create_directory(out_location + os.sep + chemical_name + os.sep + 'Summarized Tables')
out_path = out_location + os.sep + chemical_name + os.sep + 'Summarized Tables' + os.sep + 'Step 1'
create_directory(out_path)
# load input tables
use_lookup_df = pd.read_csv(use_lookup)

# Load data tables and set EntityID column as a string
l48_df = pd.read_csv(l48_BE_sum)
l48_df['EntityID'] = l48_df['EntityID'].map(lambda p: str(p).split('.')[0]).astype(str)
if nl48:
    nl48_df = pd.read_csv(nl48_BE_sum)
    nl48_df['EntityID'] = nl48_df['EntityID'].map(lambda p: str(p).split('.')[0]).astype(str)

# ## Species info from master list
species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]
# confirm input tables have EntityID column set as string for merges
base_sp_df['EntityID'] = base_sp_df['EntityID'].map(lambda p: str(p).split('.')[0]).astype(str)

# ##Filter L48 AA information - uses start with CONUS
aa_layers_CONUS = use_lookup_df.loc[((use_lookup_df['Action Area'] == 'x') | (use_lookup_df['other layer'] == 'x')) & (
        use_lookup_df['Region'] == 'CONUS')]
col_prefix_CONUS = aa_layers_CONUS['FinalColHeader'].values.tolist()
col_selection_aa = ['EntityID']
for col in col_prefix_CONUS:
    col_selection_aa.append(col + "_0")
    if len(aa_layers_CONUS.loc[(aa_layers_CONUS['FinalColHeader'] == col) & (aa_layers_CONUS['ground'] == 'x')]) > 0:
        col_selection_aa.append(col + "_305")
    if len(aa_layers_CONUS.loc[(aa_layers_CONUS['FinalColHeader'] == col) & (aa_layers_CONUS['aerial'] == 'x')]) > 0:
        col_selection_aa.append(col + "_792")
chemical_step1 = l48_df[col_selection_aa]
final_use = aa_layers_CONUS.loc[(aa_layers_CONUS['Action Area'] == 'x') & (aa_layers_CONUS['aerial'] == 'x')], [
    'FinalColHeader']
if len(aa_layers_CONUS.loc[(aa_layers_CONUS['Action Area'] == 'x') & (aa_layers_CONUS['aerial'] == 'x')]) > 0:
    final_use = str(final_use) + '_792'
else:
    final_use = str(final_use) + '_305'
# print chemical_step1.head()
# Merges the species information with the CONUS uses extracted
chemical_step1 = pd.merge(base_sp_df, chemical_step1, on='EntityID', how='left')
# print chemical_step1.head()

# ##Filter NL48 AA - uses start with regional abbreviations
if nl48:
    aa_layers_NL48 = use_lookup_df.loc[((use_lookup_df['Action Area'] == 'x') | (use_lookup_df['other layer'] == 'x')) & (
            use_lookup_df['Region'] != 'CONUS')]
    col_prefix_NL48 = aa_layers_NL48['FinalColHeader'].values.tolist()
    col_selection_nl48 = ['EntityID']

    for col in col_prefix_NL48:
        col_selection_nl48.append(col + "_0")
        if len(aa_layers_NL48.loc[(aa_layers_NL48['FinalColHeader'] == col) & (aa_layers_NL48['ground'] == 'x')]) > 0:
            col_selection_nl48.append(col + "_305")
        if len(aa_layers_NL48.loc[(aa_layers_NL48['FinalColHeader'] == col) & (aa_layers_NL48['aerial'] == 'x')]) > 0:
            col_selection_nl48.append(col + "_792")

    cols_w_overlap = [v for v in nl48_df.columns.values.tolist() if v in col_selection_nl48]
    nl48_df = nl48_df[cols_w_overlap]
    nl48_df = nl48_df.reindex(columns=col_selection_nl48)

    binned_use = []  # Empty list for combining uses across the NL48 regions
    # combine regional overlap into a summarized value represented by NL48_Use
    for x in cols_w_overlap:
        if x in col_include_output:
            pass
        else:
            if x.split("_")[1] not in binned_use:
                binned_use.append(x.split("_")[1])

    for bin_col in binned_use:
        list_col = [v for v in nl48_df.columns.values.tolist() if bin_col in v.split("_")]
        interval_list = list(set([t.split("_")[2] for t in list_col]))
        for interval in interval_list:
            list_col_interval = [z for z in list_col if z.endswith(interval)]
            # use_results_df = binned_df.apply(pd.to_numeric, errors='coerce')
            nl48_df['NL48_' + list_col_interval[0].split("_")[1] + "_" + list_col_interval[0].split("_")[2]] = nl48_df[
                list_col_interval].sum(axis=1)

    out_col = [v for v in nl48_df.columns.values.tolist() if v.startswith('NL48_')]
    out_col.insert(0, 'EntityID')
    out_nl48_df = nl48_df[out_col]

    # Merges the species/CONUS information with the NL48 uses extracted
    chemical_step1 = pd.merge(chemical_step1, out_nl48_df, on='EntityID', how='left')

chemical_step1['Step 1 ED Comment'] = chemical_step1.apply(lambda row: step_1_ED(row, 'CONUS_' + chemical_name + " AA" + "_" + max_drift), axis=1)

# Export tables with both CONUS and NL48 uses
chemical_step1.to_csv(out_path + os.sep + 'GIS_Step1_' + file_type_marker + chemical_name + '.csv')
conus_cols = [v for v in chemical_step1.columns.values.tolist() if v.startswith('CONUS') or v in col_include_output]
if nl48:
    nl48_cols_f = [v for v in chemical_step1.columns.values.tolist() if v.startswith('NL48') or v in col_include_output]

# Filter tables to just CONUS and NL48 uses and export tables specific to CONUS and the NL48 region
conus_df_step1 = chemical_step1[conus_cols]
conus_df_step1.to_csv(out_path + os.sep + 'CONUS_Step1_' + file_type_marker + chemical_name + '.csv')
# print out_path + os.sep + 'CONUS_Step1_' + file_type_marker + chemical_name + '.csv'
if nl48:
    nl48_df_step1 = chemical_step1[nl48_cols_f]
    nl48_df_step1.to_csv(out_path + os.sep + 'NL48_Step1_' + file_type_marker + chemical_name + '.csv')

# exports the parameters used to generate the tables
track_parameters(chemical_name, file_type, use_lookup, master_list, l48_BE_sum, nl48_BE_sum, out_location, date)
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
