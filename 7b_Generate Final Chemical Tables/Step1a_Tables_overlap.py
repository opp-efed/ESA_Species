import datetime
import os

import pandas as pd

# Author J.Connolly USEPA

# Description: Generates summary overlap tables that includes the action area results used for Step 1. Output includes
# the direct overlap and drift overlap for ground, aerial or both as identified by the use look-up table.

# This script has been approved for release by the U.S. Environmental Protection Agency (USEPA). Although
# the script has been subjected to rigorous review, the USEPA reserves the right to update the script as needed
# pursuant to further analysis and review. No warranty, expressed or implied, is made by the USEPA or the U.S.
# Government as to the functionality of the script and related material nor shall the fact of release constitute
# any such warranty. Furthermore, the script is released on condition that neither the USEPA nor the U.S. Government
# shall be held liable for any damages resulting from its authorized or unauthorized use.


# User input variables
chemical_name = 'Glyphosate'  # chemical name used for tracking
file_type = 'Range'  # 'Range or CriticalHabitat

# input table identifying the uses and drift limits for the chemical for step 1
use_lookup = r"C:\Users\JConno02\Environmental Protection Agency (EPA)" \
             r"\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs" + os.sep + chemical_name + \
             os.sep + "GLY_Step1_Uses_lookup_June2020_v2.csv"
# Limit of aerial drift model
max_drift = '792'
nl48 = True  # boolean True = include the NL48 False = Excludes the NL48

# root path directory
# tabulated root path - ie Tabulated_[suffix] folder
root_path = r'D:\Tabulated_Habitat'

# Tables directory  where the working/intermediate tables are found; this will start:
# SprayInterval_IntStep_30_MaxDistance_1501
# When combined the root_path + chemical + file_type + folder_path will reach the folder with the working tables
# identified in l48_BE_sum_table and nl48_BE_sum_table
# folder with result table that have no chemical specific adjustments
folder_path = r'SprayInterval_IntStep_30_MaxDistance_1501\noadjust'

# table names from previous steps found in folder above if you concatenate the path summarized into L48/NL48
# l48_BE_sum_table = "R_UnAdjusted_Full Range_AllUses_BE_L48_SprayInterval_noadjust_20200720.csv"
# l48_BE_sum_table = "CH_UnAdjusted_Full CH_AllUses_BE_L48_SprayInterval_noadjust_20200812.csv"
# nl48_BE_sum_table = "CH_UnAdjusted_Full CH_AllUses_BE_NL48_SprayInterval_noadjust_20200812.csv"
l48_BE_sum_table = "R_UnAdjusted_Full Range_AllUses_BE_L48_SprayInterval_noadjust_20200813.csv"
nl48_BE_sum_table = "R_UnAdjusted_Full Range_AllUses_BE_NL48_SprayInterval_noadjust_20200813.csv"

# master species list
master_list = "C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\MasterListESA_Dec2018_June2020.csv"
# columns from master species list to include in the output tables
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS', 'WoE Summary Group', 'Critical_Habitat_YesNo', 'Migratory',
                      'Migratory_YesNo', 'CH_Filename', 'Range_Filename', 'L48/NL48']

# Static variables to set the output directory and identify the run type; Range (R_) Critical Habitat (CH_)
out_location = root_path
l48_BE_sum = root_path + os.sep + chemical_name + os.sep + file_type + os.sep + folder_path + os.sep + l48_BE_sum_table
if nl48:
    nl48_BE_sum = root_path + os.sep + chemical_name + os.sep + file_type + os.sep + folder_path + os.sep + \
                  nl48_BE_sum_table
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


def track_parameters(chem_name, f_type, use_lkup, masterlist, l48_be, nl48_be, out_loc, c_date):
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
    parameters_used.to_csv(
        out_loc + os.sep + chem_name + os.sep + 'Summarized Tables' + os.sep + 'Parameters_used_' + f_type
        + "_Step 1 Summarized_" + c_date + '.csv')
    print("Parameter file can be found at {0}".format(out_loc + os.sep + chem_name + os.sep + 'Summarized Tables'
                                                      + os.sep + 'Parameters_used_' + f_type + "Step 1 Summarized_"
                                                      + c_date + '.csv'))


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Get the date
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
# create output directories
create_directory(out_location + os.sep + chemical_name + os.sep + 'Summarized Tables')
out_path = out_location + os.sep + chemical_name + os.sep + 'Summarized Tables' + os.sep + 'Step 1'
create_directory(out_path)
# load input tables and confirm input tables have EntityID column set as string for merges
use_lookup_df = pd.read_csv(use_lookup)
l48_df = pd.read_csv(l48_BE_sum)
l48_df['EntityID'] = l48_df['EntityID'].map(lambda h: str(h).split('.')[0]).astype(str)
if nl48:
    nl48_df = pd.read_csv(nl48_BE_sum)
    nl48_df['EntityID'] = nl48_df['EntityID'].map(lambda h: str(h).split('.')[0]).astype(str)

# ## Loads species info from master list
species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]
# confirm input tables have EntityID column set as string for merges
base_sp_df['EntityID'] = base_sp_df['EntityID'].map(lambda h: str(h).split('.')[0]).astype(str)

# ##Filter L48 AA information from use lookup table- uses start with CONUS
aa_layers_CONUS = use_lookup_df.loc[((use_lookup_df['Action Area'] == 'x') | (use_lookup_df['other layer'] == 'x')) & (
        use_lookup_df['Region'] == 'CONUS')]
col_prefix_CONUS = aa_layers_CONUS['FinalColHeader'].values.tolist()
col_selection_aa = ['EntityID']
for col in col_prefix_CONUS:
    col_selection_aa.append(col + "_0")
    if len((aa_layers_CONUS.loc[(aa_layers_CONUS['FinalColHeader'] == col) & (aa_layers_CONUS['ground'] == 'x')])) > 0:
        col_selection_aa.append(col + "_305")
    if len((aa_layers_CONUS.loc[(aa_layers_CONUS['FinalColHeader'] == col) & (aa_layers_CONUS['aerial'] == 'x')])) > 0:
        col_selection_aa.append(col + "_792")

chemical_step1 = l48_df[col_selection_aa]
final_use = aa_layers_CONUS.loc[(aa_layers_CONUS['Action Area'] == 'x') & (aa_layers_CONUS['aerial'] == 'x')], [
    'FinalColHeader']
if len(aa_layers_CONUS.loc[(aa_layers_CONUS['Action Area'] == 'x') & (aa_layers_CONUS['aerial'] == 'x')]) > 0:
    final_use = str(final_use) + '_792'
else:
    final_use = str(final_use) + '_305'
# Merges the species information with the CONUS uses extracted
chemical_step1 = pd.merge(base_sp_df, chemical_step1, on='EntityID', how='left')

if nl48:
    # ##Filter NL48 AA from use lookup- uses start with regional abbreviations
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

# Merges the species information with L48 with the summarized NL48 info
if nl48:
    chemical_step1 = pd.merge(chemical_step1, out_nl48_df, on='EntityID', how='left')

# Export tables with both all uses in the same table
chemical_step1.to_csv(out_path + os.sep + 'GIS_Step1_' + file_type_marker + chemical_name + '.csv')
conus_cols = [v for v in chemical_step1.columns.values.tolist() if v.startswith('CONUS') or v in col_include_output]
nl48_cols_f = [v for v in chemical_step1.columns.values.tolist() if v.startswith('NL48') or v in col_include_output]

# Filter tables to just CONUS and NL48 uses and exports
conus_df_step1 = chemical_step1[conus_cols]
conus_df_step1.to_csv(out_path + os.sep + 'CONUS_Step1_' + file_type_marker + chemical_name + '.csv')
if nl48:
    nl48_df_step1 = chemical_step1[nl48_cols_f]
    nl48_df_step1.to_csv(out_path + os.sep + 'NL48_Step1_' + file_type_marker + chemical_name + '.csv')
# exports the parameters used to generate the tables
if nl48:
    track_parameters(chemical_name, file_type, use_lookup, master_list, l48_BE_sum, nl48_BE_sum, out_location, date)
else:
    track_parameters(chemical_name, file_type, use_lookup, master_list, l48_BE_sum, "", out_location, date)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
