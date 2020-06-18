import pandas as pd
import os
import datetime


# TODO FILTER NE/NLAAs

# Description: Generates drift tables that includes the action area results used for Step 1. Output	includes the
# overlap in each 30 meter drift interval

chemical_name = 'Carbaryl' # chemical_name = 'Carbaryl', Methomyl
file_type = 'Range'  # 'Range or CriticalHabitat

# use look up
use_lookup =  r"C:\Users\JConno02\Environmental Protection Agency (EPA)" \
              r"\Endangered Species Pilot Assessments - OverlapTables\SupportingTables" + os.sep + chemical_name + "_Uses_lookup_20191104.csv"
max_drift = '792'
nl48 = True

# root path directory
# tabulated root path - ie Tabulated_[suffix] folder

# root_path  = r'E:\Workspace\StreamLine\ESA\Tabulated_UsageHUCAB'
root_path  = r'E:\Workspace\StreamLine\ESA\Tabulated_UsageHUCABHabitat'

# Tables directory  where the working/intermediate tables are found; this will start:
# SprayInterval_IntStep_30_MaxDistance_1501
# When combined the root_path + chemical + file_type + folder_path will reach the folder with the working tables
# identified in l48_BE_sum_table and nl48_BE_sum_table
# folder with result table that have no chemical specific adjustmentsnt
folder_path = r'SprayInterval_IntStep_30_MaxDistance_1501\noadjust'

l48_BE_interval_table = "R_UnAdjusted_SprayInterval_noadjust_Full Range_20191127.csv" # example table
nl48_BE_interval_table = "R_UnAdjusted_SprayInterval_noadjust_Full Range_20191127.csv"  #example table
#
# l48_BE_interval_table = "CH_UnAdjusted_SprayInterval_noadjust_Full CH_20191127.csv" # example table
# nl48_BE_interval_table = "CH_UnAdjusted_SprayInterval_noadjust_Full CH_20191127.csv"  #example table

master_list = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\MasterListESA_Dec2018_20190130.csv"
# columns from master to include
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']
# Static variables to set the output directory and identify the run type; Range (R_) Critical Habitat (CH_)
out_location = root_path
l48_BE_interval = root_path + os.sep + chemical_name + os.sep+file_type + os.sep + folder_path + os.sep + l48_BE_interval_table
if nl48:
    nl48_BE_interval = root_path + os.sep + chemical_name + os.sep+file_type + os.sep + folder_path + os.sep + nl48_BE_interval_table
file_type_marker = os.path.basename(l48_BE_interval).split("_")[0] + "_"

on_off_species = []


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


def track_parameters(chem_name, f_type, use_lkup, masterlist, l48_be, nl48_be, out_loc, c_date):
    parameters_used = pd.DataFrame(columns =['Chemical Name', 'File Type', 'Use Lookup','Species List','In Location L48',
                                             'In Location NL48', 'Out file','Out Base Location'])
    parameters_used.loc[0, 'Chemical Name'] = chem_name
    parameters_used.loc[0, 'File Type'] = f_type
    parameters_used.loc[0, 'Use Lookup'] = use_lkup
    parameters_used.loc[0, 'Species List'] = masterlist
    parameters_used.loc[0, 'In Location L48'] = l48_be
    parameters_used.loc[0, 'In Location NL48'] = nl48_be
    parameters_used.loc[0, 'Out file'] = 'Step 1 Intervals'
    parameters_used.loc[0, 'Out Base Location'] = out_loc + os.sep + chem_name+ os.sep+'Summarized Tables'
    parameters_used.to_csv(out_loc + os.sep + chem_name+ os.sep+'Summarized Tables' + os.sep + 'Parameters_used_'
                           + f_type +"_Step 1 Intervals_"+c_date+'.csv')
    print("Parameter file can be found at {0}".format(out_loc + os.sep + chem_name+ os.sep+'Summarized Tables' + os.sep
                                                      + 'Parameters_used_' + f_type + "Step 1 Intervals_"+
                                                      c_date+'.csv'))

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
# Get the date
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

# create output directories
create_directory(out_location + os.sep + chemical_name + os.sep + 'Summarized Tables')
out_path = out_location + os.sep + chemical_name + os.sep + 'Summarized Tables' + os.sep + 'Step 1'
create_directory(out_path)

# Load look-up tables
use_lookup_df = pd.read_csv(use_lookup)

# Load data tables and set EntityID column as a string
l48_df = pd.read_csv(l48_BE_interval, dtype=object)
l48_df['EntityID'] = l48_df['EntityID'].map(lambda p: str(p).split('.')[0]).astype(str)
if nl48:
    nl48_df = pd.read_csv(nl48_BE_interval, dtype=object)
    nl48_df['EntityID'] = nl48_df['EntityID'].map(lambda p: str(p).split('.')[0]).astype(str)

# ## Species info from master list
species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]
base_sp_df['EntityID'] = base_sp_df['EntityID'].map(lambda p: str(p).split('.')[0]).astype(str)

# ##Filter L48 AA information from use lookup table- uses start with CONUS
aa_layers_CONUS = use_lookup_df.loc[(use_lookup_df['Action Area'] == 'x') & (use_lookup_df['Region'] == 'CONUS')]
col_prefix_CONUS = aa_layers_CONUS['FinalColHeader'].values.tolist()
cols_all_uses = l48_df.columns.values.tolist()

cols_aa_l48 = [col for col in cols_all_uses if col.startswith(col_prefix_CONUS[0])]
cols_aa_l48.insert(0, 'EntityID')
aa_l48 = l48_df.ix[:, cols_aa_l48]
direct_overlap_col = aa_l48.columns.values.tolist()

aa_l48 = pd.merge(base_sp_df, aa_l48, on='EntityID', how='left')
aa_l48.to_csv(out_path + os.sep + file_type_marker + 'CONUS_Step1_Intervals_' + chemical_name + '.csv')

if nl48:
    # ##Filter NL48 AA from use lookup- uses start with regional abbreviations
    aa_layers_NL48 = use_lookup_df.loc[(use_lookup_df['Action Area'] == 'x') & (use_lookup_df['Region'] != 'CONUS')]
    col_prefix_NL48 = aa_layers_NL48['FinalColHeader'].values.tolist()
    # print col_prefix_NL48
    cols_all_uses_nl48 = nl48_df.columns.values.tolist()
    # print col_prefix_NL48
    cols_aa_nl48 = []

    for v in col_prefix_NL48:
        for col in cols_all_uses_nl48:
            if col.startswith(v +"_"):
                cols_aa_nl48.append(col)
            else:
                pass
    cols_aa_nl48.insert(0, 'EntityID')
    intervals = []  # empty list to store interval values

    # get a list of intervals to combine use across all regions
    for i in cols_aa_nl48:
        int_bin = i.split("_")[len(i.split("_")) - 1]
        if int_bin not in intervals and int_bin != 'EntityID':
            intervals.append(int_bin)

    # combine regional overlap by interval represented by NL48_[Use]
    for t in intervals:
        binned_use = [x for x in cols_aa_nl48 if x.endswith("_" + t)]
        print binned_use
        nl48_df.ix[:, binned_use] = nl48_df.ix[:, binned_use].apply(pd.to_numeric, errors='coerce')
        out_nl48_col = 'NL48_' + binned_use[0].split("_")[1] + "_" + binned_use[0].split("_")[2]
        nl48_df[out_nl48_col] = nl48_df[binned_use].sum(axis=1)

    out_cols = nl48_df.columns.values.tolist()
    out_cols = [k for k in out_cols if k.startswith('NL48')]
    out_cols.insert(0, 'EntityID')
    aa_nl48 = nl48_df.ix[:, out_cols]

    # Merges the species information with the summarized NL48 info
    aa_nl48 = pd.merge(base_sp_df, aa_nl48, on='EntityID', how='left')
    # saves output
    aa_nl48.to_csv(out_path + os.sep + file_type_marker + 'NL48_Step1_Intervals_' + chemical_name + '.csv')
    print out_path + os.sep + file_type_marker + 'NL48_Step1_Intervals_' + chemical_name + '.csv'

    track_parameters(chemical_name, file_type, use_lookup, master_list, l48_BE_interval, nl48_BE_interval, out_location,
                     date)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
