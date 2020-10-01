import datetime
import os
import sys

import numpy as np
import pandas as pd

# Author J.Connolly USEPA
# Description: Generates summary overlap tables that includes results for all UDLs used for Step 2. Output	includes the
#  direct overlap and drift overlap for ground, aerial or both as identified by the use	look-up table.
# This script has been approved for release by the U.S. Environmental Protection Agency (USEPA). Although
# the script has been subjected to rigorous review, the USEPA reserves the right to update the script as needed
# pursuant to further analysis and review. No warranty, expressed or implied, is made by the USEPA or the U.S.
# Government as to the functionality of the script and related material nor shall the fact of release constitute
# any such warranty. Furthermore, the script is released on condition that neither the USEPA nor the U.S. Government
# shall be held liable for any damages resulting from its authorized or unauthorized use.

# User input variables
chemical_name = 'Glyphosate'   # chemical name used for tracking
file_type = 'Range'  # Range or CriticalHabitat

# chemical use look up
use_lookup = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs" \
             + os.sep + chemical_name + os.sep + "GLY_Uses_lookup_June2020_v2.csv"
# table with on/off use site calls- format specific for table and extension
on_off_excel = "C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\On_Off_calls_June2020.xlsx"
# acres_col = must load path to acres tables - path set on line 104/106
max_drift = '792'
nl48 = True  # boolean True = include the NL48 False = Excludes the NL48

# On/Off Adjustments; True- adjusted the overlap for on/off use site group, False do not adjust the on/off use site
# group- based on registered uses for the chemical
cult_crop = True
orchards_crops = True
pastures = True
residential = True
forest = True
row = True

# root path directory
# out tabulated root path - ie Tabulated_[suffix] folder
root_path = r'D:\Tabulated_Habitat'

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

# folder_path_sup = r''  # Set parameter to '' to skip
# sup_key = ''  # term used to identify what the supplemental information is

# TABLE NAMES these are example names for the file name structure- script will loop through all pct groups and all
# treated acres bounding; if NL48 is not included table name can be " ", these can be range or critical habitat tables

# Unadjusted Tables - if you concatenate the file path into the chemical folder these will be the table names

# l48_BE_sum_table = "CH_UnAdjusted_Full CH_AllUses_BE_L48_SprayInterval_noadjust_20200812.csv"
# nl48_BE_sum_table = r"CH_UnAdjusted_Full CH_AllUses_BE_NL48_SprayInterval_noadjust_20200812.csv"

l48_BE_sum_table = "R_UnAdjusted_Full Range_AllUses_BE_L48_SprayInterval_noadjust_20200813.csv"
nl48_BE_sum_table = r"R_UnAdjusted_Full Range_AllUses_BE_NL48_SprayInterval_noadjust_20200813.csv"

# PCT adjusted Tables for census habitat elevation or both- example tables loops over all tables

# l48_BE_sum_PCT_table = "CH_Lower_census_AllUses_BE_L48_SprayInterval_Full CH_avg_20200812.csv"
# nl48_BE_sum_PCT_table = "CH_Lower_census_AllUses_BE_NL48_SprayInterval_Full CH_avg_20200812.csv"

l48_BE_sum_PCT_table = "R_Lower_census_AllUses_BE_L48_SprayInterval_Full Range_avg_20200813.csv"
nl48_BE_sum_PCT_table = "R_Lower_census_AllUses_BE_NL48_SprayInterval_Full Range_avg_20200813.csv"

# PCT, On/Off site adjusted Tables for census - example tables loops over all tables - use tables with on off acres
# If user doesn't want to include on/off use the same tables as the PCT adjusted

# l48_BE_sum_onoff_table = "R_Lower_census_AllUses_BE_L48_SprayInterval_On OffField_avg_20200720.csv"
# l48_BE_sum_onoff_table = "CH_Lower_census_AllUses_BE_L48_SprayInterval_Full CH_avg_20200812.csv"
# nl48_BE_sum_onoff_table = "CH_Lower_census_AllUses_BE_NL48_SprayInterval_Full CH_avg_20200812.csv"

# l48_BE_sum_onoff_table = "R_Lower_census_AllUses_BE_L48_SprayInterval_On OffField_avg_20200720.csv"
l48_BE_sum_onoff_table = "R_Lower_census_AllUses_BE_L48_SprayInterval_On OffField_avg_20200902.csv"
nl48_BE_sum_onoff_table = "R_Lower_census_AllUses_BE_NL48_SprayInterval_On OffField_avg_20200902.csv"


# PCT, On/Off site adjusted Tables for census habitat elevation or both- example tables loops over all tables -
# use tables with on off acres
# If user doesn't want to include on/off use the same tables as the PCT adjusted

# l48_BE_sum_sup_table = "R_Lower_adjHab_AllUses_BE_L48_SprayInterval_Full Range_max_20200720.csv"
l48_BE_sum_sup_table = "R_Lower_adjHab_AllUses_BE_L48_SprayInterval_Full Range_avg_20200813.csv"
nl48_BE_sum_sup_table = "R_Lower_adjHab_AllUses_BE_NL48_SprayInterval_Full Range_avg_20200813.csv"
#
# l48_BE_sum_sup_table = ""
# nl48_BE_sum_sup_table = ""
#
# l48_BE_sum_onoff_sup_table = "R_Lower_adjHab_AllUses_BE_L48_SprayInterval_On OffField_max_20200720.csv"
l48_BE_sum_onoff_sup_table = "R_Lower_adjHab_AllUses_BE_L48_SprayInterval_On OffField_avg_20200909.csv"
nl48_BE_sum_onoff_sup_table = "R_Lower_adjHab_AllUses_BE_NL48_SprayInterval_On OffField_avg_20200909.csv"

# l48_BE_sum_onoff_sup_table = ""
# nl48_BE_sum_onoff_sup_table = ""

# master species list
master_list = "C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\MasterListESA_Dec2018_June2020.csv"
# columns from master species list to include in the output tables
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS', 'WoE Summary Group', 'Critical_Habitat_YesNo', 'Migratory',
                      'Migratory_YesNo', 'CH_Filename', 'Range_Filename', 'L48/NL48']

# file type - Critical habitat or R- because R and critical habitat files are split is separate folders even if
# script is started without updating the critical hab and range path the script will **not** run because the files
# nape will not start with the correct file_type identifier
file_type_marker = l48_BE_sum_PCT_table.split("_")[0] + "_"

# acres tables
if file_type_marker == 'CH_':  # unadjusted acres tables
    acres_col = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\CH_Acres_Pixels_20200628.csv"
else:
    acres_col = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\R_Acres_Pixels_20200628.csv"
print acres_col
out_location = root_path

#  Static variables - set up path - this is done so the path to the chemical does point to a previous run - it is linked
# to the chemical name at the top
# path to  Unadjusted Tables
l48_BE_sum = root_path + os.sep + chemical_name + os.sep + file_type + os.sep + folder_path + os.sep + l48_BE_sum_table
if nl48:
    nl48_BE_sum = root_path + os.sep + chemical_name + os.sep + file_type + os.sep + folder_path + os.sep + \
                  nl48_BE_sum_table

# path to Census, PCT, on.off  adjusted Tables
BE_sum_PCT_path = root_path + os.sep + chemical_name + os.sep + file_type + os.sep + folder_path_census

# Census, PCT, On/Off site, supplemental info adjusted Tables
BE_sum_hab_path = root_path + os.sep + chemical_name + os.sep + file_type + os.sep + folder_path_sup

# tables to loop on, may include the unadjusted/census/PCT table or unadjusted/census/PCT/supplemental info tables
if l48_BE_sum_onoff_sup_table != '':
    table_loop = [l48_BE_sum_onoff_table, l48_BE_sum_onoff_sup_table]
else:
    table_loop = [l48_BE_sum_onoff_table]


# Functions
def create_directory(dbf_dir):
    # Create output directories
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


def save_parameter(df, chemicalname, filetype, uselookup, masterlist, no_l48, no_nl48, l48_be_sum_pct,
                   nl48_be_sum_pct, l48_be_sum_pct_onoff, nl48_be_sum_pct_onoff, outbase, index_num, acres):
    # Function to save/track input parameters used
    df.loc[index_num, 'Chemical Name'] = chemicalname
    df.loc[index_num, 'File Type'] = filetype
    df.loc[index_num, 'Use Lookup'] = uselookup
    df.loc[index_num, 'Species List'] = masterlist
    df.loc[index_num, 'Acres'] = acres
    df.loc[index_num, 'In Location L48'] = no_l48
    df.loc[index_num, 'In Location NL48'] = no_nl48
    df.loc[index_num, 'In Location PCT L48'] = l48_be_sum_pct
    df.loc[index_num, 'In Location PCT NL48'] = nl48_be_sum_pct
    df.loc[index_num, 'In Location PCT/OnOff L48'] = l48_be_sum_pct_onoff
    df.loc[index_num, 'In Location PCT/OnOff NL48'] = nl48_be_sum_pct_onoff
    df.loc[index_num, 'Out file'] = 'Step 2 Summarized'
    df.loc[index_num, 'Out Base Location'] = outbase
    return df


def on_off_field(cols, df, on_off_species, df_not_on_off, acres):
    # Adjust the summarized drift values for the on/off use site cols
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
    col_direct_conus = [p for p in l48_cols if p.endswith('_0')]

    for col in col_direct_conus:
        species_unadjusted["CONUS_Adjusted"] = species_unadjusted["Acres_CONUS"].sub(
            species_unadjusted[col].where(species_unadjusted["Acres_CONUS"] != 0, np.nan), fill_value=0)
        drift_cols = [p for p in l48_cols if p.startswith(col.split("_")[0] + "_" + col.split("_")[1])]
        drift_cols = [p for p in drift_cols if not p.endswith('_0')]
        for u in drift_cols:  # removed the direct overlap that was rolled into the drift overlap
            species_unadjusted[u] = species_unadjusted[u].sub(
                species_unadjusted[col].where(species_unadjusted["Acres_CONUS"] != 0, np.nan), fill_value=0)

        species_unadjusted.ix[:, drift_cols] = species_unadjusted.ix[:, drift_cols].div(
            species_unadjusted["CONUS_Adjusted"].where(species_unadjusted["Acres_CONUS"] != 0, np.nan), axis=0)
        species_unadjusted.ix[:, drift_cols] *= 100

    # nl48
    if nl48:
        species_unadjusted.drop("Acres_CONUS", axis=1, inplace=True)
        species_unadjusted = pd.merge(species_unadjusted, nl48_acres, on='EntityID', how='left')
        species_unadjusted.ix[:, nl48_cols] = species_unadjusted.ix[:, nl48_cols].multiply(
            species_unadjusted["TotalAcresNL48"].where(species_unadjusted["TotalAcresNL48"] != 0, np.nan), axis=0)
        col_direct_nl48 = [p for p in nl48_cols if p.endswith('_0')]
        for col in col_direct_nl48:
            species_unadjusted["NL48_Adjusted"] = species_unadjusted["TotalAcresNL48"].sub(
                species_unadjusted[col].where(species_unadjusted["TotalAcresNL48"] != 0, np.nan), fill_value=0)
            drift_cols = [p for p in nl48_cols if p.startswith(col.split("_")[0] + "_" + col.split("_")[1])]
            drift_cols = [p for p in drift_cols if not p.endswith('_0')]
            for u in drift_cols:  # removed the direct overlap that was rolled into the drift overlap
                species_unadjusted[u] = species_unadjusted[u].sub(
                    species_unadjusted[col].where(species_unadjusted["TotalAcresNL48"] != 0, np.nan), fill_value=0)

            species_unadjusted.ix[:, drift_cols] = species_unadjusted.ix[:, drift_cols].div(
                species_unadjusted["NL48_Adjusted"].where(species_unadjusted["TotalAcresNL48"] != 0, np.nan), axis=0)
            species_unadjusted.ix[:, drift_cols] *= 100

    df_all_drift = [p for p in cols if not p.endswith("_0")]
    drift_df = species_unadjusted[["EntityID"] + df_all_drift]
    drift_df.ix[:, 'EntityID'] = drift_df.ix[:, 'EntityID'].map(lambda t: t.split('.')[0]).astype(str)
    left_update = df.set_index('EntityID')

    right_update = drift_df.set_index('EntityID')
    res = left_update.reindex(columns=left_update.columns.union(right_update.columns))
    res.update(right_update)
    df = res.reset_index()

    return df


def extract_chemical_overlap(col_prefix, aa_layers, region, df, section):
    # Generated a list of columns that will be found in the overlap tables based on the grounds and aerial values from
    # the chemical use look up table - we can specify ground or aerial based on chemical labels; direct overlap is
    # always included
    col_selection = ['EntityID']
    for c_col in col_prefix:
        col_selection.append(c_col + "_0")
        if len(aa_layers.loc[(aa_layers['FinalColHeader'] == c_col) & (aa_layers['ground'] == 'x')]) > 0:
            col_selection.append(c_col + "_305")
        if int(max_drift) > 305:
            if len(aa_layers.loc[(aa_layers['FinalColHeader'] == c_col) & (aa_layers['aerial'] == 'x')]) > 0:
                col_selection.append(c_col + "_792")
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
    if region != 'CONUS':
        binned_use = []
        for x in col_selection:
            if x in col_include_output:
                pass
            else:
                if x.split("_")[1] not in binned_use:
                    binned_use.append(x.split("_")[1])

        # SUM INDIVIDUAL NL48 REGION TO ONE GROUPED VALUE FOR ALL NL48
        for bin_col in binned_use:
            list_col = [v for v in df.columns.values.tolist() if bin_col in v.split("_")]
            interval_list = list(set([t.split("_")[2] for t in list_col]))
            for interval in interval_list:
                list_col_interval = [z for z in list_col if z.endswith(interval)]
                df = df.apply(pd.to_numeric, errors='coerce')
                df['NL48_' + list_col_interval[0].split("_")[1] + "_" + list_col_interval[0].split("_")[2]] = df[
                    list_col_interval].sum(axis=1)
        out_col = [v for v in df.columns.values.tolist() if v.startswith('NL48_')]
        out_col.insert(0, 'EntityID')
        out_df = df.ix[:, out_col]
        out_df.ix[:, 'EntityID'] = out_df.ix[:, 'EntityID'].map(lambda r: str(r).split('.')[0]).astype(str)
    else:
        out_df = df.copy()
    return out_df, col_selection


def apply_factor(factor_df, pct_df):
    # Loads redundancy scaling factors (calculated from the un-adjusted overlap) into the PCT overlap table and applies
    # them
    if not skip_non_ag_adjustment:
        if nl48:
            factors_df = factor_df.ix[:, ['EntityID', 'CONUS_Ag_Ag_Factor', 'CONUS_NonAg_NonAg_Factor', 'NL48_Ag_Ag_Factor',
                                          'NL48_NonAg_NonAg_Factor', 'CONUS_Composite_Factor',
                                          'NL48_Composite_Factor']].fillna(0)
        else:
            factors_df = factor_df.ix[:, ['EntityID', 'CONUS_Ag_Ag_Factor', 'CONUS_NonAg_NonAg_Factor',
                                          'CONUS_Composite_Factor']].fillna(0)
    else:
        if nl48:
            factors_df = factor_df.ix[:, ['EntityID', 'CONUS_Ag_Ag_Factor', 'NL48_Ag_Ag_Factor', 'CONUS_Composite_Factor',
                                      'NL48_Composite_Factor']].fillna(0)
        else:
            factors_df = factor_df.ix[:, ['EntityID', 'CONUS_Ag_Ag_Factor', 'CONUS_Composite_Factor']].fillna(0)

    pct_df.ix[:, 'EntityID'] = pct_df.ix[:, 'EntityID'].map(lambda t: t.split('.')[0]).astype(str)
    factors_df['EntityID'] = factors_df['EntityID'].map(lambda t: t.split('.')[0]).astype(str)
    pct_df = pd.merge(pct_df, factors_df, on='EntityID', how='left')

    # # Applies the factor adjustments to the pct adjusted overlap
    # To Ag uses layer
    pct_df.ix[:, use_direct_only_conus_ag] = pct_df.ix[:, use_direct_only_conus_ag].div(
        pct_df['CONUS_Composite_Factor'].where(pct_df['CONUS_Composite_Factor'] != 0, np.nan), axis=0)
    if nl48:
        pct_df.ix[:, use_direct_only_nl48_ag] = pct_df.ix[:, use_direct_only_nl48_ag].div(
        pct_df['NL48_Composite_Factor'].where(pct_df['NL48_Composite_Factor'] != 0, np.nan), axis=0)
    # To composites
    pct_df.ix[:, use_direct_only_conus_ag_aa] = pct_df.ix[:, use_direct_only_conus_ag_aa].div(
        pct_df['CONUS_Composite_Factor'].where(pct_df['CONUS_Composite_Factor'] != 0, np.nan), axis=0)
    if nl48:
        pct_df.ix[:, use_direct_only_nl48_ag_aa] = pct_df.ix[:, use_direct_only_nl48_ag_aa].div(
        pct_df['NL48_Composite_Factor'].where(pct_df['NL48_Composite_Factor'] != 0, np.nan), axis=0)
    # print use_direct_only_conus_ag, use_direct_only_nl48_ag_aa #- columns adjusted
    # To Non Ag use layers if we have non-ag uses
    if not skip_non_ag_adjustment:
        pct_df.ix[:, use_direct_only_conus_nonag] = pct_df.ix[:, use_direct_only_conus_nonag].div(
            pct_df['CONUS_Composite_Factor'].where(pct_df['CONUS_Composite_Factor'] != 0, np.nan), axis=0)
        if nl48:
            pct_df.ix[:, use_direct_only_nl48_nonag] = pct_df.ix[:, use_direct_only_nl48_nonag].div(
            pct_df['NL48_Composite_Factor'].where(pct_df['NL48_Composite_Factor'] != 0, np.nan), axis=0)
        # To composites
        pct_df.ix[:, use_direct_only_conus_nonag_aa] = pct_df.ix[:, use_direct_only_conus_nonag_aa].div(
            pct_df['CONUS_Composite_Factor'].where(pct_df['CONUS_Composite_Factor'] != 0, np.nan), axis=0)
        if nl48:
            pct_df.ix[:, use_direct_only_nl48_nonag_aa] = pct_df.ix[:, use_direct_only_nl48_nonag_aa].div(
            pct_df['NL48_Composite_Factor'].where(pct_df['NL48_Composite_Factor'] != 0, np.nan), axis=0)

    # # APPLIES AG Factor to Ag use layers
    pct_df.ix[:, use_direct_only_conus_ag] = pct_df.ix[:, use_direct_only_conus_ag].div(
        (pct_df['CONUS_Ag_Ag_Factor']).where(pct_df['CONUS_Ag_Ag_Factor'] != 0, np.nan), axis=0)
    if nl48:
        pct_df.ix[:, use_direct_only_nl48_ag] = pct_df.ix[:, use_direct_only_nl48_ag].div(
        pct_df['NL48_Ag_Ag_Factor'].where(pct_df['NL48_Ag_Ag_Factor'] != 0, np.nan), axis=0)
    # # Applies Non-Ag factor to non-ag use layer if there are non-ag use layers
    # print use_direct_only_conus_ag, use_direct_only_nl48_ag  #- columns adjusted
    if not skip_non_ag_adjustment:
        pct_df.ix[:, use_direct_only_conus_nonag] = pct_df.ix[:, use_direct_only_conus_nonag].div(
            pct_df['CONUS_NonAg_NonAg_Factor'].where(pct_df['CONUS_NonAg_NonAg_Factor'] != 0, np.nan), axis=0)
        if nl48:
            pct_df.ix[:, use_direct_only_nl48_nonag] = pct_df.ix[:, use_direct_only_nl48_nonag].div(
            pct_df['NL48_NonAg_NonAg_Factor'].where(pct_df['NL48_NonAg_NonAg_Factor'] != 0, np.nan), axis=0)
    # print use_direct_only_conus_nonag,use_direct_only_nl48_nonag #- columns adjusted

    # stores factor and pct adjusted values ag and non ag uses for conus and L48 - uses to calculate the difference
    # between the pct adjusted and the factor and redundancy adjusted values -impact of the redundancy adjustment and
    # adjust the drift values

    # Confirms values are in numeric format
    pct_df.ix[:, overlap_cols_conus] = pct_df.ix[:, overlap_cols_conus].apply(pd.to_numeric, errors='coerce')
    adjusted_conus_ag = pct_df[['EntityID'] + use_direct_only_conus_ag].copy()
    adjusted_conus_nonag = pct_df[['EntityID'] + use_direct_only_conus_nonag].copy()
    if nl48:
        pct_df.ix[:, overlap_cols_nl48] = pct_df.ix[:, overlap_cols_nl48].apply(pd.to_numeric, errors='coerce')
        adjusted_nl48_ag = pct_df[['EntityID'] + use_direct_only_nl48_ag].copy()
        adjusted_nl48_nonag = pct_df[['EntityID'] + use_direct_only_nl48_nonag].copy()

    # Calculated the difference seen in the factor adjusted overlap, this difference is removed from the summarize
    # ground and aerial values
    difference_conus_ag = pct_adjusted_conus_ag.set_index('EntityID').subtract(adjusted_conus_ag.set_index('EntityID'))
    difference_conus_nonag = pct_adjusted_conus_nonag.set_index('EntityID').subtract(
        adjusted_conus_nonag.set_index('EntityID'))
    if nl48:
        difference_nl48_ag = pct_adjusted_nl48_ag.set_index('EntityID').subtract(adjusted_nl48_ag.set_index('EntityID'))
        difference_nl48_nonag = pct_adjusted_nl48_nonag.set_index('EntityID').subtract(
        adjusted_nl48_nonag.set_index('EntityID'))

    # get col header for drift column by replacing the 0 with the standard values of 305 for ground and 792 for aerial
    columns_conus_ground_ag = [g.replace("_0", "_305") for g in difference_conus_ag.columns.values.tolist()]
    columns_conus_ground_nonag = [g.replace("_0", "_305") for g in difference_conus_nonag.columns.values.tolist()]
    columns_conus_aerial_ag = [g.replace("_0", "_792") for g in difference_conus_ag.columns.values.tolist()]
    columns_conus_aerial_nonag = [g.replace("_0", "_792") for g in difference_conus_nonag.columns.values.tolist()]

    if nl48:
        columns_nl48_ground_ag = [g.replace("_0", "_305") for g in difference_nl48_ag.columns.values.tolist()]
        columns_nl48_ground_nonag = [g.replace("_0", "_305") for g in difference_nl48_nonag.columns.values.tolist()]
        columns_nl48_aerial_ag = [g.replace("_0", "_792") for g in difference_nl48_ag.columns.values.tolist()]
        columns_nl48_aerial_nonag = [g.replace("_0", "_792") for g in difference_nl48_nonag.columns.values.tolist()]

    # Adjusted ground by subtracting difference
    difference_conus_ag.columns = columns_conus_ground_ag
    difference_conus_nonag.columns = columns_conus_ground_nonag
    adjusted_conus_ag_ground = pct_df.ix[:, ['EntityID'] + use_conus_ag_ground].set_index(
        'EntityID').subtract(difference_conus_ag)
    adjusted_conus_nonag_ground = pct_df.ix[:, ['EntityID'] + use_conus_nonag_ground].set_index(
        'EntityID').subtract(difference_conus_nonag)
    if nl48:
        difference_nl48_ag.columns = columns_nl48_ground_ag
        difference_nl48_nonag.columns = columns_nl48_ground_nonag
        adjusted_nl48_ag_ground = pct_df.ix[:, ['EntityID'] + use_nl48_ag_ground].set_index(
        'EntityID').subtract(difference_nl48_ag)
        adjusted_nl48_nonag_ground = pct_df.ix[:, ['EntityID'] + use_nl48_nonag_ground].set_index(
        'EntityID').subtract(difference_nl48_nonag)

    # Adjust aerial by subtracting difference
    difference_conus_ag.columns = columns_conus_aerial_ag
    difference_conus_nonag.columns = columns_conus_aerial_nonag
    adjusted_conus_ag_aerial = pct_df.ix[:, ['EntityID'] + use_conus_ag_aerial].set_index(
        'EntityID').subtract(difference_conus_ag)
    adjusted_conus_nonag_aerial = pct_df.ix[:, ['EntityID'] + use_conus_nonag_aerial].set_index(
        'EntityID').subtract(difference_conus_nonag)
    if nl48:
        difference_nl48_ag.columns = columns_nl48_aerial_ag
        difference_nl48_nonag.columns = columns_nl48_aerial_nonag

        adjusted_nl48_ag_aerial = pct_df.ix[:, ['EntityID'] + use_nl48_ag_aerial].set_index(
            'EntityID').subtract(difference_nl48_ag)
        adjusted_nl48_nonag_aerial = pct_df.ix[:, ['EntityID'] + use_nl48_nonag_aerial].set_index(
            'EntityID').subtract(difference_nl48_nonag)

    # reset index  for the drift adjusted dfs
    adjusted_conus_ag_ground = adjusted_conus_ag_ground.reset_index()
    adjusted_conus_nonag_ground = adjusted_conus_nonag_ground.reset_index()
    adjusted_conus_ag_aerial = adjusted_conus_ag_aerial.reset_index()
    adjusted_conus_nonag_aerial = adjusted_conus_nonag_aerial.reset_index()
    if nl48:
        adjusted_nl48_ag_ground = adjusted_nl48_ag_ground.reset_index()
        adjusted_nl48_nonag_ground = adjusted_nl48_nonag_ground.reset_index()
        adjusted_nl48_ag_aerial = adjusted_nl48_ag_aerial.reset_index()
        adjusted_nl48_nonag_aerial = adjusted_nl48_nonag_aerial.reset_index()

    # # Load the adjusted drift columns back to the overlap df for conus and NL48
    pct_df.ix[:, adjusted_conus_ag_ground.columns.tolist()] = adjusted_conus_ag_ground.ix[:,
                                                              adjusted_conus_ag_ground.columns.tolist()]
    pct_df.ix[:, adjusted_conus_nonag_ground.columns.tolist()] = adjusted_conus_nonag_ground.ix[:,
                                                                 adjusted_conus_nonag_ground.columns.tolist()]
    pct_df.ix[:, adjusted_conus_ag_aerial.columns.tolist()] = adjusted_conus_ag_aerial.ix[:,
                                                              adjusted_conus_ag_aerial.columns.tolist()]
    pct_df.ix[:, adjusted_conus_nonag_aerial.columns.tolist()] = adjusted_conus_nonag_aerial.ix[:,
                                                                 adjusted_conus_nonag_aerial.columns.tolist()]
    if nl48:
        pct_df.ix[:, adjusted_nl48_ag_ground.columns.tolist()] = adjusted_nl48_ag_ground.ix[:,
                                                                 adjusted_nl48_ag_ground.columns.tolist()]
        pct_df.ix[:, adjusted_nl48_nonag_ground.columns.tolist()] = adjusted_nl48_nonag_ground.ix[:,
                                                                    adjusted_nl48_nonag_ground.columns.tolist()]
        pct_df.ix[:, adjusted_nl48_ag_aerial.columns.tolist()] = adjusted_nl48_ag_aerial.ix[:,
                                                                 adjusted_nl48_ag_aerial.columns.tolist()]
        pct_df.ix[:, adjusted_nl48_nonag_aerial.columns.tolist()] = adjusted_nl48_nonag_aerial.ix[:,
                                                                    adjusted_nl48_nonag_aerial.columns.tolist()]
    return pct_df


def apply_on_off(use_look_df, col_header_on_off, working_df, spe_on_off, adjust_df, acres_df, pre_df, use_cols):
    # applies the on/off site adjustment to the direct overlap based on use input
    cult_use_cols = []
    on_off_cult = use_look_df.loc[(use_look_df[col_header_on_off] == 'x')]
    on_off_cult_cols = list(set(on_off_cult['Chem Table FinalColHeader'].values.tolist()))

    on_off_cult_df = spe_on_off.loc[(on_off_df[col_header_on_off] == 'OFF')]
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
    # adjust the summarized drift cols
    working_df = on_off_field(cult_use_cols, working_df, on_off_cult_species, adjust_df, acres_df)
    # any species not par of the on/off site confirmed they were not altered
    working_df.update(pre_df.loc[~(pre_df['EntityID'].isin(on_off_cult_species)), cult_use_cols])
    use_cols = use_cols + cult_use_cols
    return working_df, use_cols


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

adjustment_type = os.path.basename(folder_path_census)
# df to track parameters used to generate these tables
parameters_used = pd.DataFrame(index=(list(range(0, 1000))),
                               columns=['Chemical Name', 'File Type', 'Use Lookup', 'Species List', 'Acres',
                                        'In Location L48', 'In Location NL48', 'In Location PCT L48',
                                        'In Location PCT NL48', 'In Location PCT/OnOff L48',
                                        'In Location PCT/OnOff NL48', 'Out file', 'Out Base Location'])
# date
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

# Create out location
create_directory(out_location + os.sep + chemical_name + os.sep + "Summarized Tables")
out_path = out_location + os.sep + chemical_name + os.sep + "Summarized Tables"
out_path_original = out_location + os.sep + chemical_name + os.sep + "Summarized Tables"

# Loads input values from lookup tables
use_lookup_df = pd.read_csv(use_lookup)
# Extract columns that will not be adjusted for redundancy -other layer in use look-up
other_layer = use_lookup_df.loc[(use_lookup_df['other layer'] == 'x')]
other_layer_cols = other_layer['Chem Table FinalColHeader'].values.tolist()
# get ag cols
ag_headers = use_lookup_df.loc[(use_lookup_df['Included AA Ag'] == 'x')]
ag_cols = ag_headers['Chem Table FinalColHeader'].values.tolist()
# load acres tables- use to adjust drift cols when applying on/off
acres_df = pd.read_csv(acres_col)
# set entityid as string for merges
acres_df['EntityID'] = acres_df['EntityID'].map(lambda t: t.split('.')[0]).astype(str)

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

# Extracts columns from lookup tables that should be included for chemical in the AA and the col header to use in the
# final tables for CONUS
aa_layers_CONUS = use_lookup_df.loc[((use_lookup_df['Included AA'] == 'x') | (use_lookup_df['other layer'] == 'x')) & (
        use_lookup_df['Region'] == 'CONUS')]
col_prefix_CONUS = aa_layers_CONUS['FinalColHeader'].values.tolist()

if nl48:
    # Extracts columns from lookup tables that should be included for chemical in the AA and the col header to use in
    # the final tables for NL48
    aa_layers_NL48 = use_lookup_df.loc[
        ((use_lookup_df['Action Area'] == 'x') | (use_lookup_df['other layer'] == 'x') | (
                use_lookup_df['Included AA'] == 'x')) & (use_lookup_df['Region'] != 'CONUS')]
    col_prefix_NL48 = aa_layers_NL48['FinalColHeader'].values.tolist()

# Load species that should be adjusted for on/off calls from master list
on_off_df = pd.read_excel(on_off_excel)
# set entityid as string for merges
on_off_df['EntityID'] = on_off_df['EntityID'].map(lambda t: str(t).split('.')[0]).astype(str)

# Species info from master list
species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]
# set entityid as string for merges
base_sp_df['EntityID'] = base_sp_df['EntityID'].map(lambda t: t.split(".")[0]).astype(str)

# Overlap Scenario 1- no adjustment

print("Working on No Adjustment Tables")
# out location for summarized tables
out_path_no_adjustment = out_path + os.sep + '1_No Adjustment'
create_directory(out_path_no_adjustment)
# loads no adjustment tables
l48_df = pd.read_csv(l48_BE_sum)
# confirm entity id loaded as str for merges and drops old index cols
l48_df['EntityID'] = l48_df['EntityID'].map(lambda t: t.split(".")[0]).astype(str)
drop_col = [m for m in l48_df.columns.values.tolist() if m.startswith('Unnamed')]
l48_df.drop(drop_col, axis=1, inplace=True)
if nl48: # repeats for nl48
    nl48_df = pd.read_csv(nl48_BE_sum)
    nl48_df['EntityID'] = nl48_df['EntityID'].map(lambda t: t.split(".")[0]).astype(str)
    drop_col = [m for m in nl48_df.columns.values.tolist() if m.startswith('Unnamed')]
    nl48_df.drop(drop_col, axis=1, inplace=True)

# Loads chemical information needed from no adjustment tables and merges to species list then merges L48 and NL48 into 1
# dataframe
chemical_noadjustment, col_selection_conus = extract_chemical_overlap(col_prefix_CONUS, aa_layers_CONUS, 'CONUS',
                                                                      l48_df, 'no adjustment - CONUS')
if nl48:
    out_nl48_df, col_selection_nl48 = extract_chemical_overlap(col_prefix_NL48, aa_layers_NL48, 'NL48', nl48_df,
                                                               'no adjustment - NL48')

chemical_noadjustment = pd.merge(base_sp_df, chemical_noadjustment, on='EntityID', how='left')
if nl48:
    # merges CONUS result with species base info with the nl48
    chemical_noadjustment = pd.merge(chemical_noadjustment, out_nl48_df, on='EntityID', how='left')
chemical_noadjustment.fillna(0, inplace=True)  # fills blanks with 0s

# exports tables with CONUS and NL48 in one table, then individual files
chemical_noadjustment.to_csv(
    out_path_no_adjustment + os.sep + file_type_marker + 'No Adjustment_GIS_Step2_' + chemical_name + '.csv')
conus_cols_f = [v for v in chemical_noadjustment.columns.values.tolist() if
                v.startswith('CONUS') or v in col_include_output]
conus_df_step1 = chemical_noadjustment[conus_cols_f]  # L48
conus_df_step1.to_csv(
    out_path_no_adjustment + os.sep + file_type_marker + 'No Adjustment_CONUS_Step2_' + chemical_name + '.csv')

nl48_cols_f = [v for v in chemical_noadjustment.columns.values.tolist() if
               v.startswith('NL48') or v in col_include_output]
if nl48:
    nl48_df_step1 = chemical_noadjustment[nl48_cols_f]  # NL48
    nl48_df_step1.to_csv(
        out_path_no_adjustment + os.sep + file_type_marker + 'No Adjustment_NL48_Step2_' + chemical_name + '.csv')
print ("Exported No Adjustment tables, found at: {0}".format(out_path_no_adjustment))
# updates parameter file
if nl48:
    parameters_used = save_parameter(parameters_used, chemical_name, file_type, use_lookup, master_list, l48_BE_sum,
                                     nl48_BE_sum, '', '', '', '', out_path_no_adjustment, '0', acres_col)
else:
    parameters_used = save_parameter(parameters_used, chemical_name, file_type, use_lookup, master_list, l48_BE_sum,
                                     '', '', '', '', '', out_path_no_adjustment, '0', acres_col)

# Overlap Scenario 2- PCT; Summarize output with just the PCT adjustments
# Loop over tables to generate PCT, Redundancy, and On/Off site tables; if running supplement data that will be included
# determines tables to loop on, list len will be 2 if running no adjustment and PCT but 3 when including supplemental
# data

count = 1  # counter for updating the parameter list
# loops over on/off and on/off with supplemental data
count_on_off = 0  # counter to direct root path and table to use for on/off - can include supplemental info
# loops over all PCT types and bounding scenarios
for in_table in table_loop:
    for group in ['min', 'max', 'avg']:  # Aggregated PCTs to loop on
        for bound in ['Uniform', 'Lower', 'Upper']:  # Treated acres distributions to loop on
            if count_on_off == 1:  # if table includes supplemental data
                # update variables to the supplemental data tables and paths
                l48_table = l48_BE_sum_sup_table
                if nl48:
                    nl48_table = nl48_BE_sum_sup_table
                in_path_table = BE_sum_hab_path
            else:  # standard species data including census masking
                in_path_table = BE_sum_PCT_path
                l48_table = l48_BE_sum_PCT_table
                if nl48:
                    nl48_table = nl48_BE_sum_PCT_table

            print("\nWorking on PCT group {0} and bounding scenario {1}".format(group, bound))
            out_path_pct = out_path_original + os.sep + '2_PCT'  # set out path
            create_directory(out_path_pct)
            out_path_pct = out_path_pct + os.sep + group  # set out path
            create_directory(out_path_pct)
            out_path_pct = out_path_pct + os.sep + bound  # set out path
            create_directory(out_path_pct)
            # print out_path_pct
            # generate table name for pct/treated acre distribution scenario
            pct_table = l48_table.split("_")[0] + "_" + bound + "_" + l48_table.split("_")[2] + "_" + \
                        l48_table.split("_")[3] + "_" + l48_table.split("_")[4] + "_" + \
                        l48_table.split("_")[5] + "_" + l48_table.split("_")[6] + "_" + \
                        l48_table.split("_")[7] + "_" + group + "_" + l48_table.split("_")[9]
            l48_BE_sum_PCT = in_path_table + os.sep + pct_table
            if nl48:
                pct_table_nl = nl48_table.split("_")[0] + "_" + bound + "_" + nl48_table.split("_")[2] + "_" + \
                               nl48_table.split("_")[3] + "_" + nl48_table.split("_")[4] + "_" + \
                               nl48_table.split("_")[5] + "_" + nl48_table.split("_")[6] + "_" + \
                               nl48_table.split("_")[7] + "_" + group + "_" + nl48_table.split("_")[9]
                nl48_BE_sum_PCT = in_path_table + os.sep + pct_table_nl

            # loads PCT and Census Adjusted tables and confirms entity id is a str for joins/merges
            l48_df_pct = pd.read_csv(l48_BE_sum_PCT)
            l48_df_pct['EntityID'] = l48_df_pct['EntityID'].map(lambda t: t.split(".")[0]).astype(str)
            if nl48:
                nl48_df_pct = pd.read_csv(nl48_BE_sum_PCT)
                nl48_df_pct['EntityID'] = nl48_df_pct['EntityID'].map(lambda t: t.split(".")[0]).astype(str)

            # Loads chemical information needed from PCT tables and merges to species list then merges CONUS and NL48
            # into 1 dataframe
            chemical_pct, col_selection_conus = extract_chemical_overlap(col_prefix_CONUS, aa_layers_CONUS, 'CONUS',
                                                                         l48_df_pct, 'pct -conus')
            # merges CONUS result with the base species information
            chemical_pct = pd.merge(base_sp_df, chemical_pct, on='EntityID', how='left')
            if nl48:
                out_nl48_df_pct, col_selection_nl48 = extract_chemical_overlap(col_prefix_NL48, aa_layers_NL48, 'NL48',
                                                                               nl48_df_pct, 'pct-nl48')
                # merges the CONUS and NL48 tables
                chemical_pct = pd.merge(chemical_pct, out_nl48_df_pct, on='EntityID', how='left')
                chemical_pct.fillna(0, inplace=True)

            # Export tables that are only PCT and census adjusted
            # with CONUS and NL48 in one table, then individual files
            conus_df_step1_pct = chemical_pct[conus_cols_f]  # L48 df
            if nl48:
                nl48_df_step1_pct = chemical_pct[nl48_cols_f]  # NL48 df
            if count_on_off == 0:
                chemical_pct.to_csv(
                    out_path_pct + os.sep + file_type_marker + 'PCT_GIS_Step2_' + chemical_name + '.csv')
                conus_df_step1_pct.to_csv(
                    out_path_pct + os.sep + file_type_marker + 'PCT_CONUS_Step2_' + chemical_name + '.csv')
                if nl48:
                    nl48_df_step1_pct.to_csv(
                        out_path_pct + os.sep + file_type_marker + 'PCT_NL48_Step2_' + chemical_name + '.csv')
                print (
                    "  Exported PCT adjusted table for PCT {0} and bounding scenario {1}: found at: {2}".format(group,
                                                                                                                bound,
                                                                                                                out_path_pct))
            else:
                pass

            # update parameter table and count
            if nl48:
                parameters_used = save_parameter(parameters_used, chemical_name, file_type, use_lookup, master_list,
                                                 l48_BE_sum, nl48_BE_sum, nl48_BE_sum_PCT, nl48_BE_sum_PCT, '', '',
                                                 out_path_pct, count, acres_col)
            else:
                parameters_used = save_parameter(parameters_used, chemical_name, file_type, use_lookup, master_list,
                                                 l48_BE_sum, '', '', '', '', '',
                                                 out_path_pct, count, acres_col)
            count += 1

            # Overlap Scenario 3- PCT and Redundancy tables
            # redundancy adjustments - adjusts the direct overlap based on the ag, nonag and composite factors that are
            # calculated

            out_path_redundancy = out_path_original + os.sep + '3_Redundancy'  # set out path
            create_directory(out_path_redundancy)  # create folder if needed
            out_path_redundancy = out_path_redundancy + os.sep + group  # set out path
            create_directory(out_path_redundancy)  # create folder if needed
            out_path_redundancy = out_path_redundancy + os.sep + bound  # set out path
            create_directory(out_path_redundancy)  # create folder if needed

            # creates a copy of the PCT table to be adjusted further for redundancy
            chemical_pct_redundancy = chemical_pct.copy()

            # COL FILTERS USED TO APPLY REDUNDANCY SCALING ADJUSTMENTS
            # list of columns that are just use sites that apply to conus vs nl48, aa, composites, use, ag and non-ag
            # conus/nl48
            overlap_cols_conus = [x for x in conus_cols_f if x not in col_include_output]
            if nl48:
                overlap_cols_nl48 = [x for x in nl48_cols_f if x not in col_include_output]
            # layer flag as other that should not be adjusted ie Federal Lands
            other_layer_header = [x for x in other_layer_cols]

            # AA and composite columns - HARD CODE composite and actions areas must have AA in the use name
            aa_col_conus = [x for x in overlap_cols_conus if 'AA' in x.split('_')[1].split(' ')]
            if nl48:
                aa_col_nl48 = [x for x in overlap_cols_nl48 if 'AA' in x.split('_')[1].split(' ')]

            # Use columns removes the Federal Lands AA, Ag and NonAg Composites from list
            use_col_conus = [x for x in overlap_cols_conus if 'AA' not in x.split('_')[1].split(' ') and (
                    x.split('_')[0] + "_" + x.split('_')[1]) not in other_layer_cols]
            if nl48:
                use_col_nl48 = [x for x in overlap_cols_nl48 if 'AA' not in x.split('_')[1].split(' ') and (
                        x.split('_')[0] + "_" + x.split('_')[1]) not in other_layer_cols]

            # col filters list for direct overlap for ag/non ag uses represented by 0 at the end of the column header -
            # these will be adjusted by redundancy scaling factor
            use_direct_only_conus_ag = [x for x in use_col_conus if
                                        x.endswith('_0') and (x.split('_')[0] + "_" + x.split('_')[1]) in ag_cols]
            use_direct_only_conus_nonag = [x for x in use_col_conus if
                                           x.endswith('_0') and (x.split('_')[0] + "_" + x.split('_')[1]) in nonag_cols]
            if nl48:
                use_direct_only_nl48_ag = [x for x in use_col_nl48 if
                                           x.endswith('_0') and (x.split('_')[0] + "_" + x.split('_')[1]) in ag_cols]
                use_direct_only_nl48_nonag = [x for x in use_col_nl48 if
                                              x.endswith('_0') and (
                                                      x.split('_')[0] + "_" + x.split('_')[1]) in nonag_cols]

            # col filter direct overlap for aa, aa ag and aa non ag composites- these ares used to calculate
            # redundancy scaling factor
            use_direct_only_conus_ag_aa = [x for x in aa_col_conus if
                                           x.endswith('_0') and 'Ag' in x.split("_")[1].split(" ")]
            use_direct_only_conus_nonag_aa = [x for x in aa_col_conus if
                                              x.endswith('_0') and 'NonAg' in x.split("_")[1].split(" ")]
            use_direct_only_conus_aa = [x for x in aa_col_conus if
                                        x.endswith('_0') and 'Ag' not in x.split("_")[1].split(" ") and 'NonAg' not in
                                        x.split("_")[1].split(" ")]
            print ("  Column representing direct overlap for AA is: {0}".format(use_direct_only_conus_aa))
            if nl48:
                # direct overlap for aa ag and aa non ag cols
                use_direct_only_nl48_ag_aa = [x for x in aa_col_nl48 if
                                              x.endswith('_0') and 'Ag' in x.split("_")[1].split(" ")]
                use_direct_only_nl48_nonag_aa = [x for x in aa_col_nl48 if
                                                 x.endswith('_0') and 'NonAg' in x.split("_")[1].split(" ")]
                use_direct_only_nl48_aa = [x for x in aa_col_nl48 if
                                           x.endswith('_0') and 'Ag' not in x.split("_")[1].split(
                                               " ") and 'NonAg' not in
                                           x.split("_")[1].split(" ")]

            # col filters list ground and aerial overlap represented by an _305 and _792 at the end of the column
            # header ag and non ag uses
            # ground
            use_conus_ag_ground = [x for x in use_col_conus if
                                   x.endswith('_305') and (x.split('_')[0] + "_" + x.split('_')[1]) in ag_cols]
            use_conus_nonag_ground = [x for x in use_col_conus if
                                      x.endswith('_305') and (x.split('_')[0] + "_" + x.split('_')[1]) in nonag_cols]
            if nl48:
                use_nl48_ag_ground = [x for x in use_col_nl48 if
                                      x.endswith('_305') and (x.split('_')[0] + "_" + x.split('_')[1]) in ag_cols]
                use_nl48_nonag_ground = [x for x in use_col_nl48 if
                                         x.endswith('_305') and (x.split('_')[0] + "_" + x.split('_')[1]) in nonag_cols]
            # aerial
            use_conus_ag_aerial = [x for x in use_col_conus if
                                   x.endswith('_792') and (x.split('_')[0] + "_" + x.split('_')[1]) in ag_cols]
            use_conus_nonag_aerial = [x for x in use_col_conus if
                                      x.endswith('_792') and (x.split('_')[0] + "_" + x.split('_')[1]) in nonag_cols]
            if nl48:
                use_nl48_ag_aerial = [x for x in use_col_nl48 if
                                      x.endswith('_792') and (x.split('_')[0] + "_" + x.split('_')[1]) in ag_cols]
                use_nl48_nonag_aerial = [x for x in use_col_nl48 if
                                         x.endswith('_792') and (x.split('_')[0] + "_" + x.split('_')[1]) in nonag_cols]

            # Stores unadjusted values ag and non ag uses for conus and NL48 - used for scaling factor adjustment
            # Confirms all number are set to a numeric data type to start calculations for scaling factor adjustments
            chemical_noadjustment.ix[:, overlap_cols_conus] = chemical_noadjustment.ix[:, overlap_cols_conus].apply(
                pd.to_numeric, errors='coerce')
            unadjusted_conus_ag = chemical_noadjustment[['EntityID'] + use_direct_only_conus_ag].copy()
            unadjusted_conus_nonag = chemical_noadjustment[['EntityID'] + use_direct_only_conus_nonag].copy()
            if nl48:
                chemical_noadjustment.ix[:, overlap_cols_nl48] = chemical_noadjustment.ix[:, overlap_cols_nl48].apply(
                    pd.to_numeric, errors='coerce')
                unadjusted_nl48_ag = chemical_noadjustment[['EntityID'] + use_direct_only_nl48_ag].copy()
                unadjusted_nl48_nonag = chemical_noadjustment[['EntityID'] + use_direct_only_nl48_nonag].copy()

            # Confirms all number are set to a numeric data type to start calculations for scaling factor adjustments
            chemical_pct.ix[:, overlap_cols_conus] = chemical_pct.ix[:, overlap_cols_conus].apply(pd.to_numeric,
                                                                                                  errors='coerce')
            if nl48:
                chemical_pct.ix[:, overlap_cols_nl48] = chemical_pct.ix[:, overlap_cols_nl48].apply(pd.to_numeric,
                                                                                                    errors='coerce')
            # makes a copy of the cols that will be adjusted
            pct_adjusted_conus_ag = chemical_pct[['EntityID'] + use_direct_only_conus_ag].copy()
            pct_adjusted_conus_nonag = chemical_pct[['EntityID'] + use_direct_only_conus_nonag].copy()
            if nl48:
                pct_adjusted_nl48_ag = chemical_pct[['EntityID'] + use_direct_only_nl48_ag].copy()
                pct_adjusted_nl48_nonag = chemical_pct[['EntityID'] + use_direct_only_nl48_nonag].copy()

            # Sum uses for scaling factor adjustment groups ag uses, non ag uses and composites for both CONUS and NL48
            #  - used for factor adjustment
            chemical_noadjustment['CONUS_Sum_Ag'] = chemical_noadjustment[use_direct_only_conus_ag].sum(axis=1)
            chemical_noadjustment['CONUS_Sum_NonAg'] = chemical_noadjustment[use_direct_only_conus_nonag].sum(axis=1)
            if nl48:
                chemical_noadjustment['NL48_Sum_Ag'] = chemical_noadjustment[use_direct_only_nl48_ag].sum(axis=1)
                chemical_noadjustment['NL48_Sum_NonAg'] = chemical_noadjustment[use_direct_only_nl48_nonag].sum(axis=1)
            # sums the composites used for factor adjustment based on user input on which composites to use
            if not skip_non_ag_adjustment:
                chemical_noadjustment['CONUS_Sum_Composites'] = chemical_noadjustment[
                    use_direct_only_conus_ag_aa + use_direct_only_conus_nonag_aa].sum(axis=1)
                if nl48:
                    chemical_noadjustment['NL48_Sum_Composites'] = chemical_noadjustment[
                        use_direct_only_nl48_ag_aa + use_direct_only_nl48_nonag_aa].sum(axis=1)
            else:
                chemical_noadjustment['CONUS_Sum_Composites'] = chemical_noadjustment[use_direct_only_conus_ag_aa].sum(
                    axis=1)
                if nl48:
                    chemical_noadjustment['NL48_Sum_Composites'] = chemical_noadjustment[
                        use_direct_only_nl48_ag_aa].sum(axis=1)

            # Calculates factors that will be used for adjustment based on the above values
            # Ag scaling factor and composite factor calculated from the AA and AG Composites then applied to all
            # individual Ag layers, Non-Ag factor and composite scaling factor calculated from the AA And Non-Ag
            # Composites is applied to all of the Non-Ag uses - Updated Winter 2018 based on QC before QC - the
            # composite scaling factor was not applied to the AG use layers.  Both QCers indicated the composite scaling
            # factor needs to be applied to the individual a layer too

            # Ag scaling factor = sum of ag uses/ ag composite - CONUS_Sum_Ag/ direct overlap of the ag composite
            # Non-Ag scaling factor sum of non ag use/ non ag composite; - CONUS_Sum_NonAg /direct overlap of the non
            # ag composite Composite factor sum of Ag and Non Ag composite / AA - CONUS_Sum_Composites/ direct overlap
            # of the AA

            # ### Calculated scaling factors
            # calculates the Ag scaling Factor for CONUS and NL48 = where clause removed the rows where the  Ag
            # composite is equal to 0
            chemical_noadjustment['CONUS_Ag_Ag_Factor'] = chemical_noadjustment['CONUS_Sum_Ag'].div(
                (chemical_noadjustment[use_direct_only_conus_ag_aa[0]]).where(
                    chemical_noadjustment[use_direct_only_conus_ag_aa[0]] != 0, np.nan), axis=0)
            if nl48:
                chemical_noadjustment['NL48_Ag_Ag_Factor'] = chemical_noadjustment['NL48_Sum_Ag'].div(
                    (chemical_noadjustment[use_direct_only_nl48_ag_aa[0]]).where(
                        chemical_noadjustment[use_direct_only_nl48_ag_aa[0]] != 0, np.nan), axis=0)
            # calculated the Non Ag scaling factors for CONUS and NL48 = where clause removed the rows where the  Ag
            # composite is equal to 0
            if not skip_non_ag_adjustment:
                chemical_noadjustment['CONUS_NonAg_NonAg_Factor'] = chemical_noadjustment['CONUS_Sum_NonAg'].div(
                    (chemical_noadjustment[use_direct_only_conus_nonag_aa[0]]).where(
                        chemical_noadjustment[use_direct_only_conus_nonag_aa[0]] != 0, np.nan), axis=0)
                if nl48:
                    chemical_noadjustment['NL48_NonAg_NonAg_Factor'] = chemical_noadjustment['NL48_Sum_NonAg'].div(
                        (chemical_noadjustment[use_direct_only_nl48_nonag_aa[0]]).where(
                            chemical_noadjustment[use_direct_only_nl48_nonag_aa[0]] != 0, np.nan), axis=0)
            # calculated the Composite scaling factors for CONUS and NL48 = where clause removed the rows where the Ag
            # composite is equal to 0
            chemical_noadjustment['CONUS_Composite_Factor'] = chemical_noadjustment['CONUS_Sum_Composites'].div(
                (chemical_noadjustment[use_direct_only_conus_aa[0]]).where(
                    chemical_noadjustment[use_direct_only_conus_aa[0]] != 0, np.nan), axis=0)
            if nl48:
                chemical_noadjustment['NL48_Composite_Factor'] = chemical_noadjustment['NL48_Sum_Composites'].div(
                    (chemical_noadjustment[use_direct_only_nl48_aa[0]]).where(
                        chemical_noadjustment[use_direct_only_nl48_aa[0]] != 0, np.nan), axis=0)

            # applies scaling factor
            chemical_pct_redundancy = apply_factor(chemical_noadjustment, chemical_pct_redundancy)
            chemical_pct_redundancy.fillna(0, inplace=True)

            # Export tables that are only PCT and census adjusted
            # with CONUS and NL48 in one table, then individual files
            conus_df_step1 = chemical_pct_redundancy[conus_cols_f]  # L48
            if nl48:
                nl48_df_step1 = chemical_pct_redundancy[nl48_cols_f]  # NL48
            if count_on_off == 0:
                chemical_pct_redundancy.to_csv(
                    out_path_redundancy + os.sep + file_type_marker + 'Redundancy_GIS_Step2_' + chemical_name + '.csv')
                conus_df_step1.to_csv(
                    out_path_redundancy + os.sep + file_type_marker + 'Redundancy_CONUS_Step2_' + chemical_name + '.csv')
                if nl48:
                    nl48_df_step1.to_csv(
                        out_path_redundancy + os.sep + file_type_marker + 'Redundancy_NL48_Step2_' + chemical_name + '.csv')
                print (
                    "  Exported PCT/ Redundancy adjusted table for PCT {0} and bounding scenario {1}: found at: {2}".format(
                        group, bound, out_path_redundancy))
            else:
                pass
            # updates the df tracking parameters used and counter
            if nl48:
                parameters_used = save_parameter(parameters_used, chemical_name, file_type, use_lookup, master_list,
                                                 l48_BE_sum,
                                                 nl48_BE_sum, l48_BE_sum_PCT, nl48_BE_sum_PCT, '', '',
                                                 out_path_redundancy,
                                                 count, acres_col)
            else:
                parameters_used = save_parameter(parameters_used, chemical_name, file_type, use_lookup, master_list,
                                                 l48_BE_sum,
                                                 '', l48_BE_sum_PCT, '', '', '', out_path_redundancy,
                                                 count, acres_col)
            count += 1

            # Overlap Scenario 4 and 5- PCT, Redundancy, and On/Off site adjusted tables; includes supplemented
            # information export if parameters set by users
            # redundancy scaling adjustments - adjusts the direct overlap based on the ag, nonag and composite
            # scaling factors that are calculated; On/Off site removed direct overlap for species not found on those
            # use sites; supplemental information will update the overlap based on the additional data such as habitat

            # on off site adjustments- direct overlap is set to zero if for species identified as not be present on
            # the on/off site group  - on/off site tables use an update species acres values in the percent
            # overlap calculation, the update value removes the area where the species won't go, ie off site applied
            # after the redundancy scaling factors to the overlap, if it is applied before the redundancy scaling is
            # applied more tha 100% of the species location could be removed; the update area value are used for the
            # direct overlap for use site not impacted by the on/off site calls

            if count_on_off == 1:  # if table includes supplemental data
                out_path_on_off = out_path_original + os.sep + '5_On_Off_Field_' + sup_key  # set out path
                # update variables to the supplemental data tables and paths
                l48_onoff_table = l48_BE_sum_onoff_sup_table
                if nl48:
                    nl48_onoff_table = nl48_BE_sum_onoff_sup_table
                in_path_table = BE_sum_hab_path
            else:  # standard species data including census masking
                in_path_table = BE_sum_PCT_path
                l48_onoff_table = l48_BE_sum_onoff_table
                if nl48:
                    nl48_onoff_table = nl48_BE_sum_onoff_table
                out_path_on_off = out_path_original + os.sep + '4_On_Off_Field'  # set out path
            create_directory(out_path_on_off)  # create out folder
            out_path_on_off = out_path_on_off + os.sep + group  # set out path
            create_directory(out_path_on_off)  # create out folder
            out_path_on_off = out_path_on_off + os.sep + bound  # set out path
            create_directory(out_path_on_off)

            # # load on/off site tables adjusted acres to remove areas found exclusively  in the off locations may also
            # included supplemental data
            # generate table name for pct/treated acres bounding scenario
            pct_table_onoff = l48_onoff_table.split("_")[0] + "_" + bound + "_" + l48_onoff_table.split("_")[2] + "_" + \
                              l48_onoff_table.split("_")[3] + "_" + l48_onoff_table.split("_")[4] + "_" + \
                              l48_onoff_table.split("_")[5] + "_" + l48_onoff_table.split("_")[6] + "_" + \
                              l48_onoff_table.split("_")[7] + "_" + group + "_" + l48_onoff_table.split("_")[9]
            if nl48:
                pct_table_nl_onoff = nl48_onoff_table.split("_")[0] + "_" + bound + "_" + nl48_onoff_table.split("_")[
                    2] + "_" + nl48_onoff_table.split("_")[3] + "_" + nl48_onoff_table.split("_")[4] + "_" + \
                                     nl48_onoff_table.split("_")[5] + "_" + nl48_onoff_table.split("_")[6] + "_" + \
                                     nl48_onoff_table.split("_")[7] + "_" + group + "_" + nl48_onoff_table.split("_")[9]

            # path to tables
            l48_BE_sum_PCT_onoff = in_path_table + os.sep + pct_table_onoff
            if nl48:
                nl48_BE_sum_PCT_onoff = in_path_table + os.sep + pct_table_nl_onoff

            # loads PCT, Census, on off  Adjusted tables with supplemental information (optional) and confirms entity id
            #  is a str for joins/merges
            l48_df_pct_onoff = pd.read_csv(l48_BE_sum_PCT_onoff)
            l48_df_pct_onoff['EntityID'] = l48_df_pct_onoff['EntityID'].map(lambda t: t.split(".")[0]).astype(str)
            if nl48:
                nl48_df_pct_onoff = pd.read_csv(nl48_BE_sum_PCT_onoff)
                nl48_df_pct_onoff['EntityID'] = nl48_df_pct_onoff['EntityID'].map(lambda t: t.split(".")[0]).astype(str)

            # Loads chemical information needed from PCT tables and merges to species list then merges CONUS and NL48
            # into 1
            chemical_pct_onoff, col_selection_conus = extract_chemical_overlap(col_prefix_CONUS, aa_layers_CONUS,
                                                                               'CONUS',
                                                                               l48_df_pct_onoff, 'pct -conus')
            if nl48:
                out_nl48_df_pct_onoff, col_selection_nl48 = extract_chemical_overlap(col_prefix_NL48, aa_layers_NL48,
                                                                                     'NL48',
                                                                                     nl48_df_pct_onoff, 'pct-nl48')
            # merges the CONUS to base species info then to the NL48 tables
            chemical_onoff = pd.merge(base_sp_df, chemical_pct_onoff, on='EntityID', how='left')
            if nl48:
                chemical_onoff = pd.merge(chemical_pct_onoff, out_nl48_df_pct_onoff, on='EntityID', how='left')
            # Apply factor
            chemical_on_off = apply_factor(chemical_noadjustment, chemical_onoff)

            # Copy of redundancy table from previous step
            chemical_pct_redundancy_on_off = chemical_pct_redundancy.copy()
            adjust_df = chemical_pct_redundancy.copy()

            # ## Extract use and columns from lookup table that will be adjusted for the on/off site cultivated crops,
            # pastures, orchards,residential, forest, row
            # Also load the species to be adjust for each group

            use_cols = []  # empty list to track columns adjusted for on/off
            # applies the on/off adjustment for direct overlap (ie updated to 0) and updates the summarized drift cols
            if cult_crop:
                chemical_pct_redundancy_on_off, use_cols = apply_on_off(use_lookup_df, 'On/Off_AG',
                                                                        chemical_pct_redundancy_on_off,
                                                                        on_off_df, adjust_df, acres_df,
                                                                        chemical_pct_redundancy, use_cols)

            if pastures:
                chemical_pct_redundancy_on_off, use_cols = apply_on_off(use_lookup_df, 'On/Off_Pasture',
                                                                        chemical_pct_redundancy_on_off, on_off_df,
                                                                        adjust_df, acres_df,
                                                                        chemical_pct_redundancy, use_cols)

            if orchards_crops:
                chemical_pct_redundancy_on_off, use_cols = apply_on_off(use_lookup_df, 'On/Off_Orchard_Plantation',
                                                                        chemical_pct_redundancy_on_off, on_off_df,
                                                                        adjust_df, acres_df,
                                                                        chemical_pct_redundancy, use_cols)

            if residential:
                chemical_pct_redundancy_on_off, use_cols = apply_on_off(use_lookup_df, 'On/Off_Residential',
                                                                        chemical_pct_redundancy_on_off, on_off_df,
                                                                        adjust_df,
                                                                        acres_df, chemical_pct_redundancy, use_cols)

            if forest:
                chemical_pct_redundancy_on_off, use_cols = apply_on_off(use_lookup_df, 'On/Off_Forest',
                                                                        chemical_pct_redundancy_on_off,
                                                                        on_off_df, adjust_df, acres_df,
                                                                        chemical_pct_redundancy, use_cols)

            if row:
                chemical_pct_redundancy_on_off, use_cols = apply_on_off(use_lookup_df, 'On/Off_ROW',
                                                                        chemical_pct_redundancy_on_off,
                                                                        on_off_df, adjust_df, acres_df,
                                                                        chemical_pct_redundancy, use_cols)

            # cols that the are not part of the on/off adjusted and should not be adjusted
            # this include the AA, Composites, Federal Lands, drfit layer and layers that are identical to the
            # AA or composites(ie Aquatic Herbicide, Noncultivated)
            cols_not_on_off = []
            for col in chemical_pct_redundancy_on_off:
                if col not in use_cols and col.endswith("_0") and 'AA' not in col.split(" ") \
                        and "AA_0" not in col.split(" ") and 'Federal Lands' not in col.split("_") and 'Aquatic Herbicide' \
                        not in col.split("_") and 'Noncultivated'not in col.split("_") and 'GlyphosateDrift'not in col.split("_") \
                        and 'Open Space Developed'not in col.split("_") and 'Pasture'not in col.split("_")and 'Bermuda Grass'not in col.split("_"):
                    cols_not_on_off.append(col)
            print cols_not_on_off

            # cols that weren't part of the off off shouldn't have been impacted to confirm they are updated from the df
            # in the previous step; based on full range
            chemical_pct_redundancy_on_off.ix[:, cols_not_on_off] = chemical_pct_redundancy.ix[:, cols_not_on_off]
            # confirms AA cols matches the values before the on/off
            if nl48:
                chemical_pct_redundancy_on_off.ix[:, aa_col_conus + aa_col_nl48] = chemical_pct_redundancy.ix[:,
                                                                                   aa_col_conus + aa_col_nl48]
            else:
                chemical_pct_redundancy_on_off.ix[:, aa_col_conus] = chemical_pct_redundancy.ix[:, aa_col_conus]

            # direct cols with no on/off cols should have a species area denominator that is adjusted to remove areas
            # only found in the 'off' locations; loaded from the direct overlap in the on/off table which was use the
            # adjusted species area for the denominator
            cols_not_on_off_direct = [v for v in cols_not_on_off if v.endswith("_0")]
            for col in cols_not_on_off_direct:
                chemical_pct_redundancy_on_off.ix[:, col] = chemical_on_off.ix[:, col]

            # sets the col order that is altered during the pd.df.updates
            chemical_pct_redundancy_on_off = chemical_pct_redundancy_on_off.reindex(
                columns=chemical_noadjustment.columns.values.tolist())
            chemical_pct_redundancy_on_off.fillna(0, inplace=True)  # fills empty cells with 0

            conus_df_step1 = chemical_pct_redundancy_on_off[conus_cols_f]
            if nl48:
                nl48_df_step1 = chemical_pct_redundancy_on_off[nl48_cols_f]

            # Export tables for the CONUS and NL48; selects the cols for CONUS and NL48 for individual tables; export
            # location is based on the which table is being loaded; on/off or on/off w supplemental data
            chemical_pct_redundancy_on_off.to_csv(
                out_path_on_off + os.sep + file_type_marker + 'On_Off_Field_GIS_Step2_' + chemical_name + '.csv')
            conus_df_step1.to_csv(
                out_path_on_off + os.sep + file_type_marker + 'On_Off_Field_CONUS_Step2_' + chemical_name + '.csv')
            if nl48:
                nl48_df_step1.to_csv(
                    out_path_on_off + os.sep + file_type_marker + 'On_Off_Field_NL48_Step2_' + chemical_name + '.csv')

            print (
                "  Exported PCT/ Redundancy/ On Off adjusted table for PCT {0} and bounding scenario {1}: found at:"
                " {2}".format(group, bound, out_path_on_off))

            # updates df tracking parameters used and the counter
            if nl48:
                parameters_used = save_parameter(parameters_used, chemical_name, file_type, use_lookup, master_list,
                                                 l48_BE_sum, nl48_BE_sum, nl48_BE_sum_PCT, nl48_BE_sum_PCT,
                                                 l48_BE_sum_PCT_onoff, nl48_BE_sum_PCT_onoff, out_path_on_off, count,
                                                 acres_col)
            else:
                parameters_used = save_parameter(parameters_used, chemical_name, file_type, use_lookup, master_list,
                                                 l48_BE_sum, '', l48_BE_sum_PCT, '', l48_BE_sum_PCT_onoff,
                                                 '', out_path_on_off, count, acres_col)

            count += 1
    count_on_off += 1
# Save parameter tracker
parameters_used.to_csv(out_location + os.sep + chemical_name + os.sep + 'Summarized Tables' + os.sep +
                       'Parameters_used_' + file_type + "_Step 2 Summarized_" + adjustment_type + "_" + date + '.csv')

print("Parameter file can be found at {0}".format(out_location + os.sep + chemical_name + os.sep + 'Summarized Tables'
                                                  + os.sep + 'Parameters_used_' + file_type + "Step 2 Summarized_" +
                                                  date + '.csv'))

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
