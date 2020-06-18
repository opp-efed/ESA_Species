import pandas as pd
import os
import datetime
import numpy as np
import sys

# Author J.Connolly
# Internal deliberative, do not cite or distribute


# TODO FILTER NE/NLAAs

# Runtime 2.5 hours for carbaryl

chemical_name = 'Carbaryl'  # 'Carbaryl'  'Methomyl'
file_type = 'Range'  # CriticalHabitat, Range

# use look - chemicals
use_lookup = r"C:\Users\JConno02\Environmental Protection Agency (EPA)" \
             r"\Endangered Species Pilot Assessments - OverlapTables\SupportingTables" + os.sep + chemical_name + "_Uses_lookup_20191104.csv"

# table with on/off field call- format specific for table and extension
on_off_excel = r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA" \
               r"\_ED_results\_CurrentSupportingTables\OnOff\OnOff_WoE_ForOverlap.xlsx"
# acres_col = must load path to acres tables - path set on line 105/108
max_drift = '792'

# root path directory
# out tabulated root path - ie Tabulated_[suffix] folder
root_path = r'E:\Workspace\StreamLine\ESA\Tabulated_UsageHUCABHabitat'
# Tables directory  where the working/intermediate tables are found; this will start:
# SprayInterval_IntStep_30_MaxDistance_1501
# When combined the root_path + chemical + file_type + folder_path will reach the folder with the working tables
# folder with result table that have no chemical specific adjustments
folder_path = r'SprayInterval_IntStep_30_MaxDistance_1501\noadjust'

# Tables directory  where the working/intermediate tables are found; this will start:
# SprayInterval_IntStep_30_MaxDistance_1501
# When combined the root_path + chemical + file_type + folder_path_census  will reach the folder with the working tables
# that includes the masking from the census of AG and aggregated PCT
# When combined the root_path + chemical + file_type + folder_path_sup  will reach the folder with the working tables
# that includes the masking from the census of AG, aggregated PCT, and limits the overlap to the the extent of the
# supplemental GIS data, e.g Habitat suitability

# census, habitat adjusted, or elevation adjusted or both
folder_path_census = r'SprayInterval_IntStep_30_MaxDistance_1501\census'
folder_path_sup = r'SprayInterval_IntStep_30_MaxDistance_1501\adjHab'  # Set parameter to '' to skip
sup_key = 'HAB'  # term used to identify what the supplemental information is

# TABLE NAMES these are example names for the file name structure- script will loop through all pct groups and all
# treated acres bounding
# Unadjusted tables
BE_interval_table = r"R_UnAdjusted_SprayInterval_noadjust_Full Range_[date].csv"

# # PCT adjusted Tables for census- example tables loops over all tables
# # single example table will loop through all tables; will loop over the all the PCT types (min, max, avg) and the
# # treated acres distributions types for use (Upper, Lower, and Uniform) based on the example table you provide
be_pct_table = "R_Uniform_SprayInterval_Full Range_census_avg_[date].csv"

# #  PCT, On/Off Field adjusted Tables for census - example tables loops over all tables
# # If use doesn't want to include on/off use the same tables as the PCT adjusted
BE_sum_PCT_onoff_interval_table = "R_Uniform_SprayInterval_On OffField_census_avg_[date].csv"


# PCT, On/Off Field adjusted Tables for census and habitat- example tables loops over all tables
# If use doesn't want to include on/off use the same tables as the PCT adjusted; supplemental information
BE_sup_table = "R_Lower_SprayInterval_Full Range_adjHab_avg_[date].csv"
BE_onoff_sup_table = "R_Lower_SprayInterval_On OffField_adjHab_avg_[date].csv"
# Variables to use to skip over the supplemental data adjustments
# BE_sup_table =""
# BE_onoff_sup_table = ""

# On/Off Adjustments; True- adjusted the overlap for on/off use site group, False do not adjust the on/off use site
# group
cult_crop = True
orchards_crops = True
pastures = True
residential = True
forest = True
row = True

# master species list
master_list = r"path\MasterListESA_Dec2018_20190130.csv"
# columns from master species list to include in the output tables
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS',  'WoE Summary Group','Critical_Habitat_', 'Migratory',
                      'Migratory_', 'CH_Filename', 'Range_Filename', 'L48/NL48']


#  Static variables - set up path - this is done so the path to the chemical does point to a previous run - it is linked
# paths to tables
out_location = root_path
BE_interval = root_path + os.sep + chemical_name + os.sep + file_type + os.sep + folder_path + os.sep + BE_interval_table
BE_sum_PCT_path = root_path + os.sep + chemical_name + os.sep + file_type + os.sep + folder_path_census
BE_sum_PCT_path_sup = root_path + os.sep + chemical_name + os.sep + file_type + os.sep + folder_path_sup
# file type - Critical habitat or R- because R and critical habitat files are split is separate folders even if
# script is started without updating the critical hab and range path the script will **not** run because the files
# nape will not start with the correct file_type identifier
find_file_type = os.path.basename(BE_interval)
if find_file_type.startswith('R'):
    file_type_marker = 'R_'
    acres_col = r"C:\Users\JConno02\Environmental Protection Agency (EPA)" \
                r"\Endangered Species Pilot Assessments - OverlapTables\R_Acres_Pixels_20191114.csv"
else:
    file_type_marker = 'CH_'
    acres_col = "C:\Users\JConno02\Environmental Protection Agency (EPA)" \
                "\Endangered Species Pilot Assessments - OverlapTables\CH_Acres_Pixels_20191114.csv"
if BE_onoff_sup_table != '':
    table_loop = [BE_sum_PCT_onoff_interval_table, BE_onoff_sup_table]
else:
    table_loop = [BE_sum_PCT_onoff_interval_table]

# Notes overlap cutoffs Summer/Fall 2017:  As part of the ESA streamlining set on 1% cut-off for and NE call and 5%
# cut-off for an NLAA issue.  For the purposes of precision this was set as anything < x.5 would meet the cut-off
# and >=x.5 would not - the specifics of < vs >= were not discussed with the team and can be adjusted if the group feels
# it should be something else.

# The quality of spatial files received by the Services was also discuss when setting these cut-offs.  JC proposed
# binning the files based on how confident we are in the file but the team felt setting these criteria for confidence
# would be too difficult.  Also, as the data we received need to be considered as 'best available' we should accept the
# files for face value. There are several concerns with this  - see pilot bird/PUR from diazinon as an example of all
# three of these concerns
#   1) When range is an overlap underestimated may be diluted due to the large denominator
#   2) Overlap may be overestimate due to the additional areas with use overlap
#   3) The use driving concern may not be correct due to concerns 1 and 2
#   4) These concerns can be further compounded when applying usage -  added Feb 2018


def create_directory(dbf_dir):
    # Create output directories
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


def save_parameter(df, chemicalname, filetype, uselookup, masterlist, no_adjust_p, pct_p, pct_onff_p, outbase,
                   index_num,acres):
    # Function to save/track input parameters used
    df.loc[index_num, 'Chemical Name'] = chemicalname
    df.loc[index_num, 'File Type'] = filetype
    df.loc[index_num, 'Use Lookup'] = uselookup
    df.loc[index_num, 'Species List'] = masterlist
    df.loc[index_num, 'Acres'] = acres
    df.loc[index_num, 'In Location No Adjust'] = no_adjust_p
    df.loc[index_num, 'In Location PCT Adjust'] = pct_p
    df.loc[index_num, 'In Location PCT/On Off Adjust'] = pct_onff_p
    df.loc[index_num, 'Out file'] = 'Step 2 Interval'
    df.loc[index_num, 'Out Base Location'] = outbase
    return df


def on_off_field(cols, df, on_off_species, df_not_on_off, acres):
    # Adjust the drift summarized calls for th on/off site cols
    l48_cols = [p for p in cols if p.startswith('CONUS')]
    nl48_cols = [p for p in cols if not p.startswith('CONUS')]

    species_acres = acres.loc[acres['EntityID'].isin(on_off_species)]
    conus_acres = species_acres[["EntityID", "Acres_CONUS"]]
    nl48_acres = species_acres[["EntityID", "TotalAcresNL48"]]

    species_unadjusted = df_not_on_off.loc[df_not_on_off['EntityID'].isin(on_off_species)]
    # back out acres for use
    species_unadjusted.ix[:, cols] = species_unadjusted.ix[:, cols].div(100)
    # conus
    species_unadjusted = pd.merge(species_unadjusted, conus_acres, on='EntityID', how='left')
    species_unadjusted.ix[:, l48_cols] = species_unadjusted.ix[:, l48_cols].multiply(
        species_unadjusted["Acres_CONUS"].where(species_unadjusted["Acres_CONUS"] != 0, np.nan), axis=0)
    col_direct_conus = [v for v in l48_cols if v.endswith('_0')]
    # update the overlap numbers after removing the direct overlap from the range
    for col in col_direct_conus:
        species_unadjusted["CONUS_Adjusted"] = species_unadjusted["Acres_CONUS"].sub(
            species_unadjusted[col].where(species_unadjusted["Acres_CONUS"] != 0, np.nan), fill_value=0)
        drift_cols = [v for v in l48_cols if v.startswith(col.split("_")[0] + "_" + col.split("_")[1])]
        drift_cols = [v for v in drift_cols if not v.endswith('_0')]
        species_unadjusted.ix[:, drift_cols] = species_unadjusted.ix[:, drift_cols].div(
            species_unadjusted["CONUS_Adjusted"].where(species_unadjusted["Acres_CONUS"] != 0, np.nan), axis=0)
        species_unadjusted.ix[:, drift_cols] *= 100

    # nl48
    species_unadjusted.drop("Acres_CONUS", axis=1, inplace=True)
    species_unadjusted = pd.merge(species_unadjusted, nl48_acres, on='EntityID', how='left')
    species_unadjusted.ix[:, nl48_cols] = species_unadjusted.ix[:, nl48_cols].multiply(
        species_unadjusted["TotalAcresNL48"].where(species_unadjusted["TotalAcresNL48"] != 0, np.nan), axis=0)
    col_direct_nl48 = [v for v in nl48_cols if v.endswith('_0')]
    for col in col_direct_nl48:
        species_unadjusted["NL48_Adjusted"] = species_unadjusted["TotalAcresNL48"].sub(
            species_unadjusted[col].where(species_unadjusted["TotalAcresNL48"] != 0, np.nan), fill_value=0)
        drift_cols = [v for v in nl48_cols if v.startswith(col.split("_")[0] + "_" + col.split("_")[1])]
        drift_cols = [v for v in drift_cols if not v.endswith('_0')]
        species_unadjusted.ix[:, drift_cols] = species_unadjusted.ix[:, drift_cols].div(
            species_unadjusted["NL48_Adjusted"].where(species_unadjusted["TotalAcresNL48"] != 0, np.nan), axis=0)
        species_unadjusted.ix[:, drift_cols] *= 100

    df_all_drift = [v for v in cols if not v.endswith("_0")]
    drift_df = species_unadjusted[["EntityID"] + df_all_drift]
    drift_df.ix[:, 'EntityID'] = drift_df.ix[:, 'EntityID'].map(lambda r: str(r).split('.')[0]).astype(str)
    left_update = df.set_index('EntityID')
    right_update = drift_df.set_index('EntityID')

    res = left_update.reindex(columns=left_update.columns.union(right_update.columns))
    res.update(right_update)

    df = res.reset_index()

    return df


def extract_chemical_overlap(col_prefix, aa_layers, region, df, distance_bins, section):
    # Generated a list of columns that will be found in the overlap tables based on the grounds and aerial values from
    # the chemical use look up table - we can specify ground or aerial based on chemical labels; direct overlap is
    # always included
    global out_df
    col_selection = ['EntityID']
    for col in col_prefix:
        if region == 'CONUS':
            if col.split("_")[0] == 'CONUS':
                for distance in distance_bins:
                    col_selection.append(col + "_" + str(distance))
        else:
            if col.split("_")[0] != 'CONUS':
                for distance in distance_bins:
                    col_selection.append(col + "_" + str(distance))
    try:
        # Extracts the use columns from the overlap table
        df = df.ix[:, col_selection]
        df = df.reindex(columns=col_selection)
    except:
        print ('A column from the chemical use lookup table is not in the overlap table; check the use lookup table '
               'and restart; error in section {0}').format(section)
        print col_selection
        print df.columns.values.tolist()
        print ('Check that a use drop because all species has 0 overlap - this has happened in AK')
        sys.exit()
    # Summarizes the individual overlap in the NL48 regions to a summarized values for all NL48 regions

    # SUM INDIVIDUAL NL48 REGION TO ONE GROUPED VALUE FOR ALL NL48
    if region == 'CONUS':
        col_region = 'CONUS_'
    else:
        col_region = 'NL48_'
    if col_region == 'NL48_':
        binned_use = []
        for x in col_selection:
            if x in col_include_output:
                pass
            else:
                if x.split("_")[1] not in binned_use:
                    binned_use.append(x.split("_")[1])
        out_col = []
        for bin_col in binned_use:
            list_col = [v for v in df.columns.values.tolist() if bin_col in v.split("_")]
            for interval in distance_bins:
                # confirm it is _0 and not just endswith; everything ends with 0 so without the _ all distances will be
                # in the _0 result
                list_col_interval = [z for z in list_col if z.endswith("_" + str(interval))]
                # print interval, list_col_interval
                df = df.apply(pd.to_numeric, errors='coerce')
                df[col_region + list_col_interval[0].split("_")[1] + "_" + list_col_interval[0].split("_")[2]] = df[
                    list_col_interval].sum(axis=1)
            out_col = [v for v in df.columns.values.tolist() if v.startswith(col_region)]
            out_col.insert(0, 'EntityID')
            out_df = df.ix[:, out_col]
            out_df.ix[:, 'EntityID'] = out_df.ix[:, 'EntityID'].map(lambda r: str(r).split('.')[0]).astype(str)
    else:
        out_df = df.copy()

    return out_df, col_selection


def apply_factor(factor_df, pct_df, skip_non_ag_adjustment, use_direct_only_conus_ag, use_direct_only_nl48_ag,
                 use_direct_only_conus_ag_aa, use_direct_only_nl48_ag_aa, use_direct_only_conus_nonag,
                 use_direct_only_nl48_nonag, use_direct_only_conus_nonag_aa, use_direct_only_nl48_nonag_aa,
                 overlap_cols_conus, overlap_cols_nl48):
    # Load factors (calculated from the un-adjusted overlap) into the PCT overlap table
    if not skip_non_ag_adjustment:
        factors_df = factor_df.ix[:, ['EntityID', 'CONUS_Ag_Ag_Factor', 'CONUS_NonAg_NonAg_Factor', 'NL48_Ag_Ag_Factor',
                                      'NL48_NonAg_NonAg_Factor', 'CONUS_Composite_Factor',
                                      'NL48_Composite_Factor']].fillna(0)

    else:
        factors_df = factor_df.ix[:, ['EntityID', 'CONUS_Ag_Ag_Factor', 'NL48_Ag_Ag_Factor', 'CONUS_Composite_Factor',
                                      'NL48_Composite_Factor']].fillna(0)

    pct_df = pd.merge(pct_df, factors_df, on='EntityID', how='left')

    # # Applies the factor adjustments to the pct adjusted overlap
    # # APPLIES COMPOSITE FACTOR

    # To Ag uses layer
    pct_df.ix[:, use_direct_only_conus_ag] = pct_df.ix[:, use_direct_only_conus_ag].div(
        pct_df['CONUS_Composite_Factor'].where(pct_df['CONUS_Composite_Factor'] != 0, np.nan), axis=0)
    pct_df.ix[:, use_direct_only_nl48_ag] = pct_df.ix[:, use_direct_only_nl48_ag].div(
        pct_df['NL48_Composite_Factor'].where(pct_df['NL48_Composite_Factor'] != 0, np.nan), axis=0)
    # To composites
    pct_df.ix[:, use_direct_only_conus_ag_aa] = pct_df.ix[:, use_direct_only_conus_ag_aa].div(
        pct_df['CONUS_Composite_Factor'].where(pct_df['CONUS_Composite_Factor'] != 0, np.nan), axis=0)
    pct_df.ix[:, use_direct_only_nl48_ag_aa] = pct_df.ix[:, use_direct_only_nl48_ag_aa].div(
        pct_df['NL48_Composite_Factor'].where(pct_df['NL48_Composite_Factor'] != 0, np.nan), axis=0)

    # To Non Ag use layers if we have non-ag uses
    if not skip_non_ag_adjustment:
        pct_df.ix[:, use_direct_only_conus_nonag] = pct_df.ix[:, use_direct_only_conus_nonag].div(
            pct_df['CONUS_Composite_Factor'].where(pct_df['CONUS_Composite_Factor'] != 0, np.nan), axis=0)
    pct_df.ix[:, use_direct_only_nl48_nonag] = pct_df.ix[:, use_direct_only_nl48_nonag].div(
        pct_df['NL48_Composite_Factor'].where(pct_df['NL48_Composite_Factor'] != 0, np.nan), axis=0)
    # To composites
    pct_df.ix[:, use_direct_only_conus_nonag_aa] = pct_df.ix[:, use_direct_only_conus_nonag_aa].div(
        pct_df['CONUS_Composite_Factor'].where(pct_df['CONUS_Composite_Factor'] != 0, np.nan), axis=0)
    pct_df.ix[:, use_direct_only_nl48_nonag_aa] = pct_df.ix[:, use_direct_only_nl48_nonag_aa].div(
        pct_df['NL48_Composite_Factor'].where(pct_df['NL48_Composite_Factor'] != 0, np.nan), axis=0)

    # # APPLIES AG Factor to Ag use layers
    pct_df.ix[:, use_direct_only_conus_ag] = pct_df.ix[:, use_direct_only_conus_ag].div(
        (pct_df['CONUS_Ag_Ag_Factor']).where(pct_df['CONUS_Ag_Ag_Factor'] != 0, np.nan), axis=0)
    pct_df.ix[:, use_direct_only_nl48_ag] = pct_df.ix[:, use_direct_only_nl48_ag].div(
        pct_df['NL48_Ag_Ag_Factor'].where(pct_df['NL48_Ag_Ag_Factor'] != 0, np.nan), axis=0)
    # # Applies Non-Ag factor to non-ag use layer if there are non-ag use layers
    if not skip_non_ag_adjustment:
        pct_df.ix[:, use_direct_only_conus_nonag] = pct_df.ix[:, use_direct_only_conus_nonag].div(
            pct_df['CONUS_NonAg_NonAg_Factor'].where(pct_df['CONUS_NonAg_NonAg_Factor'] != 0, np.nan), axis=0)
        pct_df.ix[:, use_direct_only_nl48_nonag] = pct_df.ix[:, use_direct_only_nl48_nonag].div(
            pct_df['NL48_NonAg_NonAg_Factor'].where(pct_df['NL48_NonAg_NonAg_Factor'] != 0, np.nan), axis=0)

    # stores factor and pct adjusted values ag and non ag uses for conus and NL48 - uses to calculate the difference
    # between the pct adjusted and the factor and redundancy adjusted values -impact of the redundancy adjustment and
    # adjust the drift values
    # Confirms values are in numeric format
    assert isinstance(pct_df, object)
    pct_df.ix[:, overlap_cols_conus] = pct_df.ix[:, overlap_cols_conus].apply(pd.to_numeric, errors='coerce')
    pct_df.ix[:, overlap_cols_nl48] = pct_df.ix[:, overlap_cols_nl48].apply(pd.to_numeric, errors='coerce')

    return pct_df


def apply_onoff(use_look_df, col_header_on_off, spe_on_off, working_df, use_cols, adjust_df, acres_df, pre_df):
    # type: (object, object, object, object, object, object, object, object) -> object
    # Load factors (calculated from the un-adjusted overlap) into the PCT overlap table and applies them
    cult_use_cols = []

    on_off_cult = use_look_df.loc[(use_look_df[col_header_on_off] == 'x')]
    on_off_cult_cols = list(set(on_off_cult['Chem Table FinalColHeader'].values.tolist()))
    on_off_cult_df = spe_on_off.loc[(spe_on_off[col_header_on_off] == 'OFF')]
    on_off_cult_species = on_off_cult_df['EntityID'].values.tolist()
    for z in working_df.columns.values.tolist():
        for p in on_off_cult_cols:
            if z.startswith(p):
                cult_use_cols.append(z)
    # if species is off site make direct overlap 0
    direct_cult = [v for v in cult_use_cols if v.endswith("_0")]
    species_cult_df = working_df.loc[(working_df['EntityID'].isin(on_off_cult_species))]
    for col in direct_cult:
        species_cult_df[col].values[:] = 0
    # moves 0 values to working df using update
    left_update = working_df.set_index('EntityID')
    right_update = species_cult_df.set_index('EntityID')
    res = left_update.reindex(columns=left_update.columns.union(right_update.columns))
    res.update(right_update)
    working_df = res.reset_index()
    # adjusted the drift intervals - removing the direct overlap from the species range area

    working_df = on_off_field(cult_use_cols, working_df, on_off_cult_species, adjust_df, acres_df)
    # any species not part of the on/off site confirmed they were not altered
    working_df.update(pre_df.loc[~(pre_df['EntityID'].isin(on_off_cult_species)), cult_use_cols])
    use_cols = use_cols + cult_use_cols
    return working_df, use_cols


def no_adjust_tables(interval_df, outpath, conus_cols_load, nl48_cols_load, col_prefix_conus, aa_layers_CONUS,
                     col_prefix_NL48, aa_layers_NL48, base_sp_df):
    # Overlap Scenario 1- no adjustment
    print("Working on No Adjustment Tables")
    out_path_no_adjustment = outpath + os.sep + '1_No Adjustment'  # out locations
    create_directory(out_path_no_adjustment)  # create directory

    # breaks out conus/nl48 cols
    l48_df = interval_df.ix[:, conus_cols_load]
    nl48_df = interval_df.ix[:, nl48_cols_load]

    bins = []  # empty list to hold of distance bins
    for v in l48_df.columns.values.tolist():
        if v in col_include_output:
            pass
        elif v.split("_")[1] == col_prefix_conus[0].split("_")[1]:
            bins.append(int(v.split("_")[2]))
    # Loads chemical information needed from no adjustment tables and merges to species list then merges CONUS and
    # NL48 into 1
    chemical_noadjustment, col_selection_conus = extract_chemical_overlap(col_prefix_conus, aa_layers_CONUS, 'CONUS',
                                                                          l48_df, bins, 'no adjustment - CONUS')
    out_nl48_df, col_selection_nl48 = extract_chemical_overlap(col_prefix_NL48, aa_layers_NL48, 'NL48', nl48_df, bins,
                                                               'no adjustment - NL48')
    chemical_noadjustment = pd.merge(base_sp_df, chemical_noadjustment, on='EntityID', how='left')
    # merges the CONUS and NL48 tables
    chemical_noadjustment = pd.merge(chemical_noadjustment, out_nl48_df, on='EntityID', how='left')
    # extracts conus and nl48 cols
    conus_cols_f = [v for v in chemical_noadjustment.columns.values.tolist() if
                    v.startswith('CONUS') or v in col_include_output]
    nl48_cols_f = [v for v in chemical_noadjustment.columns.values.tolist() if
                   v.startswith('NL48') or v in col_include_output]
    conus_unadj_df = chemical_noadjustment[conus_cols_f]  # regions df
    nl48_unadj_df = chemical_noadjustment[nl48_cols_f]  # region df
    # saves output
    conus_unadj_df.to_csv(
        out_path_no_adjustment + os.sep + file_type_marker + 'CONUS_Step2_Intervals_No Adjustment_' + chemical_name + '.csv')
    nl48_unadj_df.to_csv(
        out_path_no_adjustment + os.sep + file_type_marker + 'NL48_Step2_Intervals_No Adjustment_' + chemical_name + '.csv')
    chemical_noadjustment.to_csv(
        out_path_no_adjustment + os.sep + file_type_marker + 'Step2_Intervals_No Adjustment_' + chemical_name + '.csv')
    print ("Exported No Adjustment tables, found at: {0}".format(out_path_no_adjustment))
    return chemical_noadjustment, out_path_no_adjustment, conus_cols_f, nl48_cols_f, bins


def pct_table(be_sum_pct_interval, conus_cols_load, nl48_cols_load, out_path, group, bound, col_prefix_CONUS,
              aa_layers_CONUS, bins, col_prefix_NL48, aa_layers_NL48, base_sp_df, conus_cols_f, nl48_cols_f,
              count_on_off):
    # Overlap Scenario 2- PCT
    out_path_pct = out_path + os.sep + '2_PCT'  # set out path
    create_directory(out_path_pct)  # create directory
    create_directory(out_path_pct + os.sep + group)  # create directory
    out_path_pct = out_path_pct + os.sep + group  # out path
    create_directory(out_path_pct + os.sep + bound)  # create directory
    out_path_pct = out_path_pct + os.sep + bound  # out path

    # reads in PCT and Census Adjusted tables, makes sure entity id is str and drops old index cols
    interval_df_pct = pd.read_csv(be_sum_pct_interval, low_memory=False)
    interval_df_pct['EntityID'] = interval_df_pct['EntityID'].map(lambda r: str(r).split('.')[0]).astype(str)
    drop_col = [m for m in interval_df_pct.columns.values.tolist() if m.startswith('Unnamed')]
    interval_df_pct.drop(drop_col, axis=1, inplace=True)
    # splits into regional tables
    l48_df_pct = interval_df_pct.ix[:, conus_cols_load]
    nl48_df_pct = interval_df_pct.ix[:, nl48_cols_load]
    # Loads chemical information needed from no adjustment tables and merges to species list then merges CONUS and
    # NL48 into 1
    chemical_pct, col_selection_conus = extract_chemical_overlap(col_prefix_CONUS, aa_layers_CONUS, 'CONUS', l48_df_pct,
                                                                 bins, 'pct -conus')
    out_nl48_df_pct, col_selection_nl48 = extract_chemical_overlap(col_prefix_NL48, aa_layers_NL48, 'NL48', nl48_df_pct,
                                                                   bins, 'pct-nl48')
    chemical_pct = pd.merge(base_sp_df, chemical_pct, on='EntityID', how='left')
    # merges the CONUS and NL48 tables
    chemical_pct = pd.merge(chemical_pct, out_nl48_df_pct, on='EntityID', how='left')
    chemical_pct.fillna(0, inplace=True)  # fills empty cells with 0
    # regional tables
    conus_pct_df = chemical_pct[conus_cols_f]
    nl48_pct_df = chemical_pct[nl48_cols_f]
    if count_on_off == 0:
        # saves output
        conus_pct_df.to_csv(
            out_path_pct + os.sep + file_type_marker + 'CONUS_Step2_Intervals_PCT_' + chemical_name + '.csv')
        nl48_pct_df.to_csv(out_path_pct + os.sep + file_type_marker + 'NL48_Step2_Intervals_PCT_' + chemical_name + '.csv')
        chemical_pct.to_csv(out_path_pct + os.sep + file_type_marker + 'Step2_Intervals_PCT_' + chemical_name + '.csv')
        print ("  Exported PCT adjusted table for PCT {0} and bounding scenario {1}: found at: {2}".format(group, bound,
                                                                                                           out_path_pct))
    else:
        pass  # Don't save the habitat adjusted on/off just need the DF to confirm that cols not part of on/off are
    return chemical_pct, out_path_pct


def redundancy_tables(out_path, group, bound, chemical_pct, conus_cols_f, nl48_cols_f, other_layer_cols, ag_cols,
                      nonag_cols, chemical_noadjustment, skip_non_ag_adjustment):
    # Overlap Scenario 3- PCT and redundancy
    out_path_redundancy = out_path + os.sep + '3_Redundancy'  # set out path
    create_directory(out_path_redundancy)  # create folder

    create_directory(out_path_redundancy + os.sep + group)  # create folder
    out_path_redundancy = out_path_redundancy + os.sep + group  # set out path
    create_directory(out_path_redundancy + os.sep + bound)  # create folder
    out_path_redundancy = out_path_redundancy + os.sep + bound  # set out path
    # creates a copy of the PCT table to be adjusted further for redundancy
    chemical_pct_redundancy = chemical_pct.copy()  # copy of pct adjusted tables

    # COL FILTERS USED TO APPLY ADJUSTMENTS
    # list of columns that are just use sites that apply to conus vs nl48, aa, composites, use, ag and non-ag
    # conus/nl48
    overlap_cols_conus = [x for x in conus_cols_f if x not in col_include_output]
    overlap_cols_nl48 = [x for x in nl48_cols_f if x not in col_include_output]
    # layer flag as other that should not be adjusted ie Federal Lands
    other_layer_header = [x for x in other_layer_cols]

    # AA and composite columns - HARD CODE composite and actions areas must have AA in the use name
    aa_col_conus = [x for x in overlap_cols_conus if 'AA' in x.split('_')[1].split(' ')]
    aa_col_nl48 = [x for x in overlap_cols_nl48 if 'AA' in x.split('_')[1].split(' ')]

    # Use columns removes the Federal Lands AA, Ag and NonAg Composites from list
    use_col_conus = [x for x in overlap_cols_conus if not 'AA' in x.split('_')[1].split(' ') and (
            x.split('_')[0] + "_" + x.split('_')[1]) not in other_layer_cols]
    use_col_nl48 = [x for x in overlap_cols_nl48 if not 'AA' in x.split('_')[1].split(' ') and (
            x.split('_')[0] + "_" + x.split('_')[1]) not in other_layer_cols]

    # col filters list for direct overlap for ag and non ag uses represented by an _0 at the end of the column 
    # header
    use_direct_only_conus_ag = [x for x in use_col_conus if
                                x.endswith('_0') and (x.split('_')[0] + "_" + x.split('_')[1]) in ag_cols]
    use_direct_only_conus_nonag = [x for x in use_col_conus if
                                   x.endswith('_0') and (x.split('_')[0] + "_" + x.split('_')[1]) in nonag_cols]
    use_direct_only_nl48_ag = [x for x in use_col_nl48 if
                               x.endswith('_0') and (x.split('_')[0] + "_" + x.split('_')[1]) in ag_cols]
    use_direct_only_nl48_nonag = [x for x in use_col_nl48 if
                                  x.endswith('_0') and (x.split('_')[0] + "_" + x.split('_')[1]) in nonag_cols]

    # col filter direct overlap for aa, ag and non ag composites
    use_direct_only_conus_ag_aa = [x for x in aa_col_conus if x.endswith('_0') and 'Ag' in x.split("_")[1].split(" ")]
    use_direct_only_conus_nonag_aa = [x for x in aa_col_conus if
                                      x.endswith('_0') and 'NonAg' in x.split("_")[1].split(" ")]
    use_direct_only_conus_aa = [x for x in aa_col_conus if
                                x.endswith('_0') and 'Ag' not in x.split("_")[1].split(" ") and 'NonAg' not in
                                x.split("_")[1].split(" ")]
    print ("  Column representing direct overlap for AA is: {0}".format(use_direct_only_conus_aa)[0])

    use_direct_only_nl48_ag_aa = [x for x in aa_col_nl48 if x.endswith('_0') and 'Ag' in x.split("_")[1].split(" ")]
    use_direct_only_nl48_nonag_aa = [x for x in aa_col_nl48 if
                                     x.endswith('_0') and 'NonAg' in x.split("_")[1].split(" ")]
    use_direct_only_nl48_aa = [x for x in aa_col_nl48 if
                               x.endswith('_0') and 'Ag' not in x.split("_")[1].split(" ") and 'NonAg' not in
                               x.split("_")[1].split(" ")]

    # Stores unadjusted values ag and non ag uses for conus and NL48 - used for factor adjustment
    chemical_noadjustment.ix[:, overlap_cols_conus] = chemical_noadjustment.ix[:, overlap_cols_conus].apply(
        pd.to_numeric, errors='coerce')
    chemical_noadjustment.ix[:, overlap_cols_nl48] = chemical_noadjustment.ix[:, overlap_cols_nl48].apply(pd.to_numeric,
                                                                                                          errors='coerce')
    unadjusted_conus_ag = chemical_noadjustment[['EntityID'] + use_direct_only_conus_ag].copy()
    unadjusted_conus_nonag = chemical_noadjustment[['EntityID'] + use_direct_only_conus_nonag].copy()
    unadjusted_nl48_ag = chemical_noadjustment[['EntityID'] + use_direct_only_nl48_ag].copy()
    unadjusted_nl48_nonag = chemical_noadjustment[['EntityID'] + use_direct_only_nl48_nonag].copy()

    # stores values that are adjusts by PCT only and but adjusted for redundancy - used to calc the impact of the
    # redundancy scaling adjustment
    # Confirms all number are set to a numeric data type to start calculations for factor adjustments
    chemical_pct.ix[:, overlap_cols_conus] = chemical_pct.ix[:, overlap_cols_conus].apply(pd.to_numeric,
                                                                                          errors='coerce')
    chemical_pct.ix[:, overlap_cols_nl48] = chemical_pct.ix[:, overlap_cols_nl48].apply(pd.to_numeric, errors='coerce')
    pct_adjusted_conus_ag = chemical_pct[['EntityID'] + use_direct_only_conus_ag].copy()
    pct_adjusted_conus_nonag = chemical_pct[['EntityID'] + use_direct_only_conus_nonag].copy()
    pct_adjusted_nl48_ag = chemical_pct[['EntityID'] + use_direct_only_nl48_ag].copy()
    pct_adjusted_nl48_nonag = chemical_pct[['EntityID'] + use_direct_only_nl48_nonag].copy()

    # # Sum uses for factor adjustment groups ag uses, non ag uses and composites for both CONUS and NL48 - used for
    # factor adjustment
    chemical_noadjustment['CONUS_Sum_Ag'] = chemical_noadjustment[use_direct_only_conus_ag].sum(axis=1)
    chemical_noadjustment['CONUS_Sum_NonAg'] = chemical_noadjustment[use_direct_only_conus_nonag].sum(axis=1)
    chemical_noadjustment['NL48_Sum_Ag'] = chemical_noadjustment[use_direct_only_nl48_ag].sum(axis=1)

    chemical_noadjustment['NL48_Sum_NonAg'] = chemical_noadjustment[use_direct_only_nl48_nonag].sum(axis=1)
    if not skip_non_ag_adjustment:
        chemical_noadjustment['CONUS_Sum_Composites'] = chemical_noadjustment[
            use_direct_only_conus_ag_aa + use_direct_only_conus_nonag_aa].sum(axis=1)
        chemical_noadjustment['NL48_Sum_Composites'] = chemical_noadjustment[
            use_direct_only_nl48_ag_aa + use_direct_only_nl48_nonag_aa].sum(axis=1)
    else:
        chemical_noadjustment['CONUS_Sum_Composites'] = chemical_noadjustment[use_direct_only_conus_ag_aa].sum(axis=1)
        chemical_noadjustment['NL48_Sum_Composites'] = chemical_noadjustment[use_direct_only_nl48_ag_aa].sum(axis=1)

    # Calculates factors that will be used for adjustment based on the above values
    # Ag factor and composite factor calculated from the AA and AG Composites then applied to all individual Ag
    # layers, Non-Ag factor and composite factor calculated from the AA And Non-Ag Composites is applied to all of
    # the Non-Ag uses - Updated Winter 2018 based on QC before QC - the composite factor was not applied
    # to the AG use layers.  Both QCers indicated the composite factor need to be applied the to individual ag
    # layers (ESA team Fall 2017- BE Streamlining meeting lead CMR) QC of this process was conducted my CMR and CP
    # early summer 2018 (may/june see files Copy of Carbaryl_QC_FactorAdjustments_CP.xlsx and
    # Carbaryl_QC_FactorAdjustments.xlsx)

    # 5/16/19 Per discussion with CMR added the adjustment to the composite layers - this applies the composite
    # factor to both the Ag and non-ag composites

    # Ag factor = sum of ag uses/ ag composite - CONUS_Sum_Ag/ direct overlap of the ag composite
    # Non-Ag factor sum of non ag use/ non ag composite; - CONUS_Sum_NonAg /direct overlap of the non ag composite
    # Composite factor sum of Ag and Non Ag composite / AA - CONUS_Sum_Composites/ direct overlap of the AA

    # ### Calculated scaling factors
    # calculates the Ag Factor for CONUS and NL48 = where clause removed the rows where the  Ag composite 
    # is equal to 0
    chemical_noadjustment['CONUS_Ag_Ag_Factor'] = chemical_noadjustment['CONUS_Sum_Ag'].div(
        (chemical_noadjustment[use_direct_only_conus_ag_aa[0]]).where(
            chemical_noadjustment[use_direct_only_conus_ag_aa[0]] != 0, np.nan), axis=0)
    chemical_noadjustment['NL48_Ag_Ag_Factor'] = chemical_noadjustment['NL48_Sum_Ag'].div(
        (chemical_noadjustment[use_direct_only_nl48_ag_aa[0]]).where(
            chemical_noadjustment[use_direct_only_nl48_ag_aa[0]] != 0, np.nan), axis=0)
    # calculated the Non Ag factors for CONUS and NL48 = where clause removed the rows where the  Ag composite is equal to 0
    if not skip_non_ag_adjustment:
        chemical_noadjustment['CONUS_NonAg_NonAg_Factor'] = chemical_noadjustment['CONUS_Sum_NonAg'].div(
            (chemical_noadjustment[use_direct_only_conus_nonag_aa[0]]).where(
                chemical_noadjustment[use_direct_only_conus_nonag_aa[0]] != 0, np.nan), axis=0)
        chemical_noadjustment['NL48_NonAg_NonAg_Factor'] = chemical_noadjustment['NL48_Sum_NonAg'].div(
            (chemical_noadjustment[use_direct_only_nl48_nonag_aa[0]]).where(
                chemical_noadjustment[use_direct_only_nl48_nonag_aa[0]] != 0, np.nan), axis=0)
    # calculated the Composite factors for CONUS and NL48 = where clause removed the rows where the  Ag composite 
    # is equal to 0
    # To change this to use the un altered action area change to index position 1; to use usage action area index 
    # position 0
    chemical_noadjustment['CONUS_Composite_Factor'] = chemical_noadjustment['CONUS_Sum_Composites'].div(
        (chemical_noadjustment[use_direct_only_conus_aa[0]]).where(
            chemical_noadjustment[use_direct_only_conus_aa[0]] != 0, np.nan), axis=0)
    chemical_noadjustment['NL48_Composite_Factor'] = chemical_noadjustment['NL48_Sum_Composites'].div(
        (chemical_noadjustment[use_direct_only_nl48_aa[0]]).where(
            chemical_noadjustment[use_direct_only_nl48_aa[0]] != 0, np.nan), axis=0)

    chemical_pct_redundancy = apply_factor(chemical_noadjustment, chemical_pct_redundancy, skip_non_ag_adjustment,
                                           use_direct_only_conus_ag, use_direct_only_nl48_ag,
                                           use_direct_only_conus_ag_aa, use_direct_only_nl48_ag_aa,
                                           use_direct_only_conus_nonag, use_direct_only_nl48_nonag,
                                           use_direct_only_conus_nonag_aa, use_direct_only_nl48_nonag_aa,
                                           overlap_cols_conus, overlap_cols_nl48)  # apply factor

    chemical_pct_redundancy.fillna(0, inplace=True)  # fill blanks with 0s
    # regional tables
    conus_pct_red_df = chemical_pct_redundancy[conus_cols_f]
    nl48_pct_red_df = chemical_pct_redundancy[nl48_cols_f]
    # Export tables that are only PCT and census adjusted
    # with L48 and NL48 in one table, then individual files
    conus_pct_red_df.to_csv(
        out_path_redundancy + os.sep + file_type_marker + 'CONUS_Step2_Intervals_Redundancy_' + chemical_name + '.csv')
    nl48_pct_red_df.to_csv(
        out_path_redundancy + os.sep + file_type_marker + 'NL48_Step2_Intervals_Redundancy_' + chemical_name + '.csv')
    chemical_pct_redundancy.to_csv(
        out_path_redundancy + os.sep + file_type_marker + 'Step2_Intervals_Redundancy_' + chemical_name + '.csv')
    print ("  Exported PCT/ Redundancy adjusted table for PCT {0} and bounding scenario {1}: found at: {2}".format(
        group, bound, out_path_redundancy))
    return out_path_redundancy, chemical_pct_redundancy, skip_non_ag_adjustment, use_direct_only_conus_ag, \
           use_direct_only_nl48_ag, use_direct_only_conus_ag_aa, use_direct_only_nl48_ag_aa, \
           use_direct_only_conus_nonag, use_direct_only_nl48_nonag, use_direct_only_conus_nonag_aa, \
           use_direct_only_nl48_nonag_aa, overlap_cols_conus, overlap_cols_nl48, aa_col_conus,aa_col_nl48


def on_off_tables(count_on_off, out_path, group, bound, be_onoff_sup_table, be_sum_PCT_path_sup, be_sum_PCT_path,
                  be_sum_PCT_onoff_interval_table, parameters_used, count, chemical_noadjustment,
                  chemical_pct_redundancy, col_prefix_CONUS, aa_layers_CONUS, bins, col_prefix_NL48, aa_layers_NL48,
                  base_sp_df, conus_cols_f, nl48_cols_f, conus_cols_load, nl48_cols_load, use_lookup_df, on_off_df,
                  acres_df, BE_sum_PCT_interval, skip_non_ag_adjustment, use_direct_only_conus_ag,
                  use_direct_only_nl48_ag, use_direct_only_conus_ag_aa, use_direct_only_nl48_ag_aa,
                  use_direct_only_conus_nonag, use_direct_only_nl48_nonag, use_direct_only_conus_nonag_aa,
                  use_direct_only_nl48_nonag_aa, overlap_cols_conus, overlap_cols_nl48, aa_col_conus, aa_col_nl48):
    # Overlap Scenario 4 and 5- PCT, Redundancy, and On/Off site adjusted tables; includes supplemented
    # information export if parameters set by users
    if count_on_off == 1:  # if table includes supplemental data
        out_path_on_off = out_path + os.sep + '5_On_Off_Field_' + sup_key  # set out path
        # update variables to the supplemental data tables and paths
        onoff_table = be_onoff_sup_table
        in_path_table = be_sum_PCT_path_sup
    else:  # standard species data including census masking
        in_path_table = be_sum_PCT_path
        onoff_table = be_sum_PCT_onoff_interval_table
        out_path_on_off = out_path + os.sep + '4_On_Off_Field'  # set out path

    create_directory(out_path_on_off)  # create folder
    create_directory(out_path_on_off + os.sep + group)  # create folder
    out_path_on_off = out_path_on_off + os.sep + group  # out path
    create_directory(out_path_on_off + os.sep + bound)  # create folder
    out_path_on_off = out_path_on_off + os.sep + bound  # out path
    # print in_path_table
    # print out_path_on_off
    # load on/off site tables adjusted; can include supplemental data
    table_onoff = onoff_table.split("_")[0] + "_" + bound + "_" + onoff_table.split("_")[2] + "_" + \
                  onoff_table.split("_")[3] + "_" + onoff_table.split("_")[4] + "_" + group + "_" + \
                  onoff_table.split("_")[6]
    be_sum_pct_onoff_interval = in_path_table + os.sep + table_onoff
    # load tables
    df_pct_onoff = pd.read_csv(be_sum_pct_onoff_interval, low_memory=False)
    df_pct_onoff['EntityID'] = df_pct_onoff['EntityID'].map(lambda r: str(r).split('.')[0]).astype(str)
    # split into regional table
    l48_df_pct_onoff = df_pct_onoff.ix[:, conus_cols_load]
    nl48_df_pct_onoff = df_pct_onoff.ix[:, nl48_cols_load]
    # Loads chemical information needed from no adjustment tables and merges to species list then merges CONUS and
    # NL48 into 1
    chemical_pct_onoff, col_selection_conus = extract_chemical_overlap(col_prefix_CONUS, aa_layers_CONUS, 'CONUS',
                                                                       l48_df_pct_onoff, bins, 'pct -conus')
    out_nl48_df_pct_onoff, col_selection_nl48 = extract_chemical_overlap(col_prefix_NL48, aa_layers_NL48, 'NL48',
                                                                         nl48_df_pct_onoff, bins, 'pct-nl48')
    chemical_pct_onoff = pd.merge(base_sp_df, chemical_pct_onoff, on='EntityID', how='left')
    # merges the CONUS and NL48 tables
    chemical_on_off = pd.merge(chemical_pct_onoff, out_nl48_df_pct_onoff, on='EntityID', how='left')
    # Apply scaling factor
    chemical_on_off = apply_factor(chemical_noadjustment, chemical_on_off, skip_non_ag_adjustment,
                                   use_direct_only_conus_ag, use_direct_only_nl48_ag, use_direct_only_conus_ag_aa,
                                   use_direct_only_nl48_ag_aa, use_direct_only_conus_nonag,
                                   use_direct_only_nl48_nonag, use_direct_only_conus_nonag_aa,
                                   use_direct_only_nl48_nonag_aa, overlap_cols_conus, overlap_cols_nl48)
    # Copy of redundancy table from previous step
    chemical_pct_redundancy_on_off = chemical_pct_redundancy.copy()
    adjust_df = chemical_pct_redundancy.copy()
    # ## Extract use and columns from lookup table that will be adjusted for the on/off site, cultivated crops,
    # pastures, orchards - can add others
    # Also load the species to be adjust for each group

    use_cols = []  # empty list to track columns adjusted for on/off
    # applies the on/off adjustment for direct overlap (ie updated to 0) and updates the summarized drift cols

    if cult_crop:
        chemical_pct_redundancy_on_off, use_cols = apply_onoff(use_lookup_df, 'On/Off_AG', on_off_df,
                                                               chemical_pct_redundancy_on_off, use_cols, adjust_df,
                                                               acres_df, chemical_pct_redundancy)

    if pastures:
        chemical_pct_redundancy_on_off, use_cols = apply_onoff(use_lookup_df, 'On/Off_Pasture', on_off_df,
                                                               chemical_pct_redundancy_on_off, use_cols, adjust_df,
                                                               acres_df, chemical_pct_redundancy)

    if orchards_crops:
        chemical_pct_redundancy_on_off, use_cols = apply_onoff(use_lookup_df, 'On/Off_Orchard_Plantation',
                                                               on_off_df, chemical_pct_redundancy_on_off, use_cols,
                                                               adjust_df, acres_df, chemical_pct_redundancy)

    if residential:
        chemical_pct_redundancy_on_off, use_cols = apply_onoff(use_lookup_df, 'On/Off_Residential', on_off_df,
                                                               chemical_pct_redundancy_on_off, use_cols, adjust_df,
                                                               acres_df, chemical_pct_redundancy)
    if forest:
        chemical_pct_redundancy_on_off, use_cols = apply_onoff(use_lookup_df, 'On/Off_Forest', on_off_df,
                                                               chemical_pct_redundancy_on_off, use_cols, adjust_df,
                                                               acres_df, chemical_pct_redundancy)

    if row:
        chemical_pct_redundancy_on_off, use_cols = apply_onoff(use_lookup_df, 'On/Off_ROW', on_off_df,
                                                               chemical_pct_redundancy_on_off, use_cols, adjust_df,
                                                               acres_df, chemical_pct_redundancy)

    cols_not_on_off = []  # cols that the are not part of the on/off adjusted and should not be adjusted
    for col in chemical_pct_redundancy_on_off:
        if col not in use_cols and 'AA' not in col.split(" ") \
                and "AA_0" not in col.split(" ") and 'Federal Lands' not in col.split("_"):
            cols_not_on_off.append(col)
    # cols that weren't part of the off off shouldn't have been impacted to confirm they are update from the df
    # in the previous step; based on full range
    chemical_pct_redundancy_on_off.ix[:, cols_not_on_off] = chemical_pct_redundancy.ix[:, cols_not_on_off]
    # confirms AA cols matches the values before the on/off
    chemical_pct_redundancy_on_off.ix[:, aa_col_conus + aa_col_nl48] = chemical_pct_redundancy.ix[:, aa_col_conus + aa_col_nl48]
    # direct cols with no on/off cols should have a species area denominator that is adjusted to remove areas
    # only found in the 'off' locations; loaded from the direct overlap in the on/off table which was use the
    # adjusted species area for the denominator
    cols_not_on_off_direct = [v for v in cols_not_on_off if v.endswith("_0")]
    for col in cols_not_on_off_direct:
        chemical_pct_redundancy_on_off.ix[:, col] = chemical_on_off.ix[:, col]

    # sets the col order that is altered during the pd.df.updates
    chemical_pct_redundancy_on_off = chemical_pct_redundancy_on_off.reindex(
        columns=chemical_noadjustment.columns.values.tolist())
    chemical_pct_redundancy_on_off.fillna(0, inplace=True)  # fill empty cells with 0s
    # regional tables
    conus_pct_onoff_df = chemical_pct_redundancy_on_off[conus_cols_f]
    nl48_pct_onoff_df = chemical_pct_redundancy_on_off[nl48_cols_f]
    # Export tables for the CONUS and NL48; selects the cols for CONUS and NL48 for individual tables; export
    # location is based on the which table is being loaded; on/off or on/off w supplemental data
    conus_pct_onoff_df.to_csv(
        out_path_on_off + os.sep + file_type_marker + 'CONUS_Step2_Intervals_On_Off_Field' + chemical_name + '.csv')
    nl48_pct_onoff_df.to_csv(
        out_path_on_off + os.sep + file_type_marker + 'NL48_Step2_Intervals_On_Off_Field' + chemical_name + '.csv')
    chemical_pct_redundancy_on_off.to_csv(
        out_path_on_off + os.sep + file_type_marker + 'Step2_Intervals_On_Off_Field' + chemical_name + '.csv')

    print ("  Exported PCT/ Redundancy/ On Off adjusted table for PCT {0} and bounding scenario {1}: found at: {2}".format(group, bound, out_path_on_off))

    parameters_used = save_parameter(parameters_used, chemical_name, file_type, use_lookup, master_list,
                                     BE_interval,
                                     BE_sum_PCT_interval, be_sum_pct_onoff_interval, out_path_on_off, count, acres_col)
    count += 1

    return out_path_on_off, parameters_used, count


def load_inputs(use_lookup, acres_col, on_off_excel):
    # Loads input values from lookup tables
    use_lookup_df = pd.read_csv(use_lookup)
    # col headers for final tables

    # cols that are "other" cols like federal lands
    other_layer = use_lookup_df.loc[(use_lookup_df['other layer'] == 'x')]
    other_layer_cols = other_layer['Chem Table FinalColHeader'].values.tolist()
    # acres df
    acres_df = pd.read_csv(acres_col)
    acres_df['EntityID'] = acres_df['EntityID'].map(lambda r: str(r).split('.')[0]).astype(str)

    # identify ag and non cols; columns that will be scaled for redundancy
    ag_headers = use_lookup_df.loc[(use_lookup_df['Included AA Ag'] == 'x')]
    ag_cols = ag_headers['Chem Table FinalColHeader'].values.tolist()

    # Try except loop to set a boolean variable to skip non ag adjustment if there are no non ag uses
    try:
        non_ag_headers = use_lookup_df.loc[(use_lookup_df['Included AA NonAg'] == 'x')]
        nonag_cols = non_ag_headers['Chem Table FinalColHeader'].values.tolist()
        if len(non_ag_headers) == 0:
            skip_non_ag_adjustment = True
        else:
            skip_non_ag_adjustment = False
    except TypeError:
        non_ag_headers = []
        nonag_cols = []
        skip_non_ag_adjustment = True

    # ##Extracts columns from lookup tables that should be included for chemical in the AA and the col header to use 
    # in the final tables for CONUS
    aa_layers_CONUS = use_lookup_df.loc[
        ((use_lookup_df['Included AA'] == 'x') | (use_lookup_df['other layer'] == 'x')) & (
                use_lookup_df['Region'] == 'CONUS')]
    col_prefix_CONUS = aa_layers_CONUS['FinalColHeader'].values.tolist()

    # Extracts columns from lookup tables that should be included for chemical in the AA and the col header to use in 
    # the final tables for NL48 CONUS
    aa_layers_NL48 = use_lookup_df.loc[
        ((use_lookup_df['Action Area'] == 'x') | (use_lookup_df['other layer'] == 'x') | (
                use_lookup_df['Included AA'] == 'x')) & (use_lookup_df['Region'] != 'CONUS')]
    col_prefix_NL48 = aa_layers_NL48['FinalColHeader'].values.tolist()

    # ##Load species that should be adjusted for on/off calls from master list
    on_off_df = pd.read_excel(on_off_excel)
    on_off_df['EntityID'] = on_off_df['EntityID'].map(lambda r: str(r).split('.')[0]).astype(str)

    # ## Species info from master list
    species_df = pd.read_csv(master_list, dtype=object)
    [species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
    base_sp_df = species_df.loc[:, col_include_output]
    base_sp_df['EntityID'] = base_sp_df['EntityID'].map(lambda r: str(r).split('.')[0]).astype(str)

    return use_lookup_df, acres_df, on_off_df, base_sp_df, col_prefix_CONUS, aa_layers_CONUS, col_prefix_NL48, \
           aa_layers_NL48, skip_non_ag_adjustment, other_layer_cols, ag_cols, nonag_cols


def main():
    start_time = datetime.datetime.now()
    print "Start Time: " + start_time.ctime()
    today = datetime.datetime.today()
    date = today.strftime('%Y%m%d')

    adjustment_type = os.path.basename(folder_path_census)  # tracking for output
    # df to track parameters used to generate these tables
    parameters_used = pd.DataFrame(
        columns=['Chemical Name', 'File Type', 'Use Lookup', 'Species List','Acres', 'In Location No Adjust',
                 'In Location PCT Adjust',
                 'In Location PCT NL48', 'In Location PCT/On Off L48',
                 'Out file', 'Out Base Location'])

    # Create out location
    create_directory(out_location + os.sep + chemical_name + os.sep + "Summarized Tables")
    out_path = out_location + os.sep + chemical_name + os.sep + "Summarized Tables"

    # load input tables
    use_lookup_df, acres_df, on_off_df, base_sp_df, col_prefix_conus, aa_layers_CONUS, col_prefix_NL48, aa_layers_NL48, skip_non_ag_adjustment, other_layer_cols, ag_cols, nonag_cols = load_inputs(
        use_lookup, acres_col, on_off_excel)

    # Overlap Scenario 1 - No adjustments to overlap
    # No adjustment tables
    interval_df = pd.read_csv(BE_interval, low_memory=False)
    # Confirms Entity ID is a string
    interval_df['EntityID'] = interval_df['EntityID'].map(lambda r: str(r).split('.')[0]).astype(str)
    # drop previously save index columns
    drop_col = [m for m in interval_df.columns.values.tolist() if m.startswith('Unnamed')]
    interval_df.drop(drop_col, axis=1, inplace=True)
    # identifies CONUS and NL48 columns
    conus_cols_load = [v for v in interval_df.columns.values.tolist() if
                       v.startswith('CONUS') or v in col_include_output]
    nl48_cols_load = [v for v in interval_df.columns.values.tolist() if
                      not v.startswith('CONUS') or v in col_include_output]

    chemical_noadjustment, out_path_no_adjustment, conus_cols_f, nl48_cols_f, bins = no_adjust_tables(interval_df,
                                                                                                      out_path,
                                                                                                      conus_cols_load,
                                                                                                      nl48_cols_load,
                                                                                                      col_prefix_conus,
                                                                                                      aa_layers_CONUS,
                                                                                                      col_prefix_NL48,
                                                                                                      aa_layers_NL48,
                                                                                                      base_sp_df)
    # updates parameter tracker
    parameters_used = save_parameter(parameters_used, chemical_name, file_type, use_lookup, master_list, BE_interval,
                                     '', '', out_path_no_adjustment, 0, acres_col)

    count = 1  # counter for updating parameter tables
    # loop over PCT and bounding scenarios
    count_on_off = 0     # loops over on/off and on/off with supplemental data

    for in_table in table_loop:
        print in_table, table_loop, count_on_off
        for group in ['avg', 'min', 'max']:  # pct groups
            for bound in ['Lower', 'Upper', 'Uniform']:  # types of bounding scenarios
                print("\nWorking on PCT group {0} and bounding scenario {1}".format(group, bound))
                # Overlap Scenario 2- PCT
                # Summarize output with just the PCT adjustments
                # gets table name based on pct group and bounding scenario
                if count_on_off == 1:  # if table includes supplemental data
                    # update variables to the supplemental data tables and paths
                    table = BE_sup_table
                    in_path_table = BE_sum_PCT_path_sup
                else:  # standard species data including census masking
                    in_path_table = BE_sum_PCT_path
                    table = be_pct_table

                # generate table name for pct/bounding scenario
                table_pct = table.split("_")[0] + "_" + bound + "_" + table.split("_")[2] + "_" + \
                            table.split("_")[3] + "_" + table.split("_")[4] + "_" + group + "_" + \
                            table.split("_")[6]
                BE_sum_PCT_interval = in_path_table + os.sep + table_pct  # path to table
                chemical_pct, out_path_pct = pct_table(BE_sum_PCT_interval, conus_cols_load, nl48_cols_load, out_path,
                                                       group, bound, col_prefix_conus, aa_layers_CONUS, bins,
                                                       col_prefix_NL48, aa_layers_NL48, base_sp_df, conus_cols_f,
                                                       nl48_cols_f, count_on_off)

                # updated parameter tracker and counter
                parameters_used = save_parameter(parameters_used, chemical_name, file_type, use_lookup, master_list,
                                                 BE_interval,
                                                 BE_sum_PCT_interval, '', out_path_pct, count, acres_col)
                count += 1

                # Overlap Scenario 3- PCT and Redundancy tables
                # redundancy adjustments - adjusts the direct overlap based on the ag, nonag and composite scaling
                # factors that are calculated
                out_path_redundancy, chemical_pct_redundancy, skip_non_ag_adjustment, use_direct_only_conus_ag, \
                use_direct_only_nl48_ag, use_direct_only_conus_ag_aa, use_direct_only_nl48_ag_aa, \
                use_direct_only_conus_nonag, use_direct_only_nl48_nonag, use_direct_only_conus_nonag_aa, \
                use_direct_only_nl48_nonag_aa, overlap_cols_conus, overlap_cols_nl48, aa_col_conus,aa_col_nl48 = redundancy_tables(out_path, group,
                                                                                                         bound,
                                                                                                         chemical_pct,
                                                                                                         conus_cols_f,
                                                                                                         nl48_cols_f,
                                                                                                         other_layer_cols,
                                                                                                         ag_cols,
                                                                                                         nonag_cols,
                                                                                                         chemical_noadjustment,
                                                                                                         skip_non_ag_adjustment)

                # update parameter tracker counter
                parameters_used = save_parameter(parameters_used, chemical_name, file_type, use_lookup, master_list,
                                                 BE_interval,
                                                 BE_sum_PCT_interval, '', out_path_redundancy, count, acres_col)
                count += 1
                #
                # Overlap Scenario 4 and 5- PCT, Redundancy, and On/Off site adjusted tables; includes supplemented
                # information export if parameters set by users
                # redundancy adjustments - adjusts the direct overlap based on the ag, nonag and composite scaling
                # factors that are calculated; On/Off site removed direct overlap for species not found on those use
                # sites; supplemental information will update the overlap based on the additional data such as habitat,
                # elevation etc

                # on off site adjustments- direct overlap is set to zero if for species identified as not be present on
                # the on/off site types  - on/off site tables use an update species acres values in the percent
                # overlap calculation, the update value removes the area where the species won't go, ie off after apply
                # the redundancy factors the the overlap, before the redundancy is applied more tha 100% off the species
                # range could be removed; the update area value are used for the direct overlap use site not impacted
                # by the on/off site calls

                out_path_on_off, parameters_used, count = on_off_tables(count_on_off, out_path, group, bound,
                                                                        BE_onoff_sup_table,
                                                                        BE_sum_PCT_path_sup, BE_sum_PCT_path,
                                                                        BE_sum_PCT_onoff_interval_table, parameters_used,
                                                                        count, chemical_noadjustment,
                                                                        chemical_pct_redundancy, col_prefix_conus,
                                                                        aa_layers_CONUS, bins, col_prefix_NL48,
                                                                        aa_layers_NL48,
                                                                        base_sp_df, conus_cols_f, nl48_cols_f,
                                                                        conus_cols_load, nl48_cols_load, use_lookup_df,
                                                                        on_off_df,
                                                                        acres_df, BE_sum_PCT_interval,
                                                                        skip_non_ag_adjustment, use_direct_only_conus_ag,
                                                                        use_direct_only_nl48_ag,
                                                                        use_direct_only_conus_ag_aa,
                                                                        use_direct_only_nl48_ag_aa,
                                                                        use_direct_only_conus_nonag,
                                                                        use_direct_only_nl48_nonag,
                                                                        use_direct_only_conus_nonag_aa,
                                                                        use_direct_only_nl48_nonag_aa, overlap_cols_conus,
                                                                        overlap_cols_nl48, aa_col_conus,aa_col_nl48)
        count_on_off += 1
    # Save parameter tracker
    parameters_used.to_csv(
        out_location + os.sep + chemical_name + os.sep + 'Summarized Tables' + os.sep + 'Parameters_used_'
        + file_type + "_Step 2 Interval_" + adjustment_type + "_" + date + '.csv')
    print("Parameter file can be found at {0}".format(
        out_location + os.sep + chemical_name + os.sep + 'Summarized Tables' + os.sep + 'Parameters_used_'
        + file_type + "Step 2 Intervals_" + date + '.csv'))

    end = datetime.datetime.now()
    print "End Time: " + end.ctime()
    elapsed = end - start_time
    print "Elapsed  Time: " + str(elapsed)


main()
