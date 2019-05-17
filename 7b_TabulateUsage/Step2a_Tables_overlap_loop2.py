import pandas as pd
import os
import datetime
import numpy as np
import sys

# chemical_name = 'Methomyl'  # PATH TO TABLES MUST BE UPDATED FOR CHEMICAL
chemical_name = 'Carbaryl'

use_lookup = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
             r'\SupportingTables' + os.sep + chemical_name + "Uses_lookup_20190409.csv"

on_off_excel = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
               r'\_ExternalDrive\_CurrentSupportingTables\Species Collection_cmr_jc.xlsx'
acres_col = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables\CH_Acres_Pixels_20180430.csv"
max_drift = '792'

# On/Off Adjustments
cult_crop = True
orchards_crops = False
pastures = True
residential = False

# root path directory
root_path = r'L:\Workspace\StreamLine\ESA\Tabulated_TabArea_HUCAB_Usage'
# Tables directory  one level done from chemical
# No adjustment
folder_path = r'CriticalHabitat\SprayInterval_IntStep_30_MaxDistance_1501\noadjust'
# census
folder_path_census = r'CriticalHabitat\SprayInterval_IntStep_30_MaxDistance_1501\census'

# TABLE NAMES these are example names for the file name structure- script will loop through all pct groups and all
# usage bounding
# Unadjusted Tables
l48_BE_sum_table = "CH_UnAdjusted_Full CH_AllUses_BE_L48_SprayInterval_noadjust_20190501.csv"
nl48_BE_sum_table = r"CH_UnAdjusted_Full CH_AllUses_BE_NL48_SprayInterval_noadjust_20190501.csv"

# Census and PCT adjusted Tables

l48_BE_sum_PCT_table = "CH_Uniform_census_AllUses_BE_L48_SprayInterval_Full CH_max_20190501.csv"
nl48_BE_sum_PCT_table = "CH_Uniform_census_AllUses_BE_NL48_SprayInterval_Full CH_max_20190501.csv"

# Census, PCT, On/Off Field adjusted Tables

l48_BE_sum_onoff_table = "CH_Uniform_census_AllUses_BE_L48_SprayInterval_Full CH_max_20190501.csv"
nl48_BE_sum_onoff_table = "CH_Uniform_census_AllUses_BE_NL48_SprayInterval_Full CH_max_20190501.csv"
# Census,PCT, Habitat adjusted Tables

# Census,PCT, Elevation adjusted Tables

# Census,PCT, Habitat, Elevation adjusted Tables

master_list = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'

col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'CHange_Filename', 'L48/NL48']

out_location = root_path

#  Static variables - set up path - this is done so the path to the chemical does point to a previous run - it is link
# to the chemical name at the top
# Unadjusted Tables
l48_BE_sum = root_path + os.sep + chemical_name + os.sep + folder_path + os.sep + l48_BE_sum_table
nl48_BE_sum = root_path + os.sep + chemical_name + os.sep + folder_path + os.sep + nl48_BE_sum_table

# Census and PCT adjusted Tables
BE_sum_PCT_path = root_path + os.sep + chemical_name + os.sep + folder_path_census

# Census, PCT, On/Off Field adjusted Tables
BE_sum_onoff_path = root_path + os.sep + chemical_name + os.sep + folder_path_census

#file type - Critical habitat or R- because R and critical habitat files are split is separate folders even if
# script is started without updating the critical habirt and range path the script will **not** run because the files
# nape will not start with the correct file_type identifier
file_type = l48_BE_sum_PCT_table.split("_")[0] + "_"

def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


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


def step_1_ED(row, col_aa):
    # Federal lands statement needs to be updated to an and statement that will still catch overlap from either L48 or
    # NL48.  These or statements take advantage of species in both the L48 and NL48 have low overlap with federal lands
    # but these may not be true in all cases
    col_l48 = "CONUS_" + col_aa
    col_nl48 = col_l48.replace('CONUS', 'NL48')
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
    else:
        value = 'no catch'

    if file_type == 'CH_':
        if row['Source of Call final BE-Critical Habitat'] != 'Terr WoE' and \
                row['Source of Call final BE-Critical Habitat'] != 'Aqua WoE' and \
                row['Source of Call final BE-Critical Habitat'] != 'Terr and Aqua WoE':
            value = 'No CritHab'
        if str(row['Source of Call final BE-Critical Habitat']).startswith('Qual'):
            value = 'Qualitative'

    if file_type == 'R_':

        if str(row['Source of Call final BE-Range']).startswith('Qu'):
            value = 'Qualitative'

    return value


def NLAA_overlap(row, cols_l48, cols_nl48):
    max_col = []
    # TODO do this dynamically
    # for col in cols:
    #     if col =='EntityID':
    #         pass
    #     elif col.split("_")[1].endswith('AA') or col.split("_")[2].endswith('_0') :
    #         pass
    #     elif col in max_col:
    #         pass
    #     else:
    #         use = col.split("_")[1]
    #         if col.split("_")[0] +"_" +use+"_765" in cols:
    #             if col.split("_")[0] +"_" +use+"_765" not in max_col:
    #                 max_col.append(col.split("_")[0] +"_" +use+"_792")
    #             elif col.split("_")[0] +"_" +use+"_305" in cols:
    #                 if col.split("_")[0] +"_" +use+"_792" not in cols:
    #                     if col.split("_")[0] +"_" +use+"_305" not in max_col:
    #                         max_col.append(col.split("_")[0] +"_" +use+"_305")
    #             else:
    #                 pass
    for col in cols_l48:

        if len(col.split("_")) == 3:
            if col.split("_")[2] == '792':
                max_col.append(col)
            else:
                pass
    for col in cols_nl48:
        if len(col.split("_")) == 3:

            if col.split("_")[2] == '792':
                max_col.append(col)
            else:
                pass
    final_col = []
    for col in max_col:
        if col.split("_")[0] != 'CONUS':
            val = 'NL48_' + col.split("_")[1] + "_" + col.split("_")[2]
            final_col.append(val)
        else:
            final_col.append(col)
    list_values = row[final_col]

    if len(list_values) == 0:
        value = row['Step 2 ED Comment']
    elif max(list_values) < 0.45:
        value = 'NLAA Overlap - 1percent'
    else:
        value = row['Step 2 ED Comment']
    if file_type == 'CH_':
        if row['Source of Call final BE-Critical Habitat'] != 'Terr WoE' and \
                row['Source of Call final BE-Critical Habitat'] != 'Aqua WoE' and \
                row['Source of Call final BE-Critical Habitat'] != 'Terr and Aqua WoE':
            value = 'No CritHab'
        if str(row['Source of Call final BE-Critical Habitat']).startswith('Qual'):
            value = 'Qualitative'

    if file_type == 'R_':
        if str(row['Source of Call final BE-Range']).startswith('Qu'):
            value = 'Qualitative'
    return value


def on_off_field(cols, df, on_off_species,df_not_on_off,acres):

    l48_cols = [v for v in cols if v.startswith('CONUS')]
    nl48_cols = [v for v in cols if not v.startswith('CONUS')]

    species_acres = acres.loc[acres['EntityID'].isin(on_off_species)]
    conus_acres = species_acres[["EntityID","Acres_CONUS"]]
    nl48_acres = species_acres[["EntityID","TotalAcresNL48"]]
    species_unadjusted =  df_not_on_off.loc[df_not_on_off['EntityID'].isin(on_off_species)]
    # back out acres for use
    species_unadjusted.ix[:,cols] = species_unadjusted.ix[:,cols].div(100)
    # conus

    species_unadjusted =pd.merge(species_unadjusted, conus_acres,on = 'EntityID', how = 'left')
    species_unadjusted.ix[:,l48_cols]= species_unadjusted.ix[:,l48_cols].multiply(species_unadjusted["Acres_CONUS"].where(species_unadjusted["Acres_CONUS"]!= 0, np.nan),axis=0)
    col_direct_conus = [ v for v in l48_cols if v.endswith('_0')]

    for col in col_direct_conus:
        species_unadjusted["CONUS_Adjusted"] = species_unadjusted["Acres_CONUS"].sub(species_unadjusted[col].where(species_unadjusted["Acres_CONUS"]!= 0, np.nan), fill_value=0)
        drift_cols = [v for v in l48_cols if v.startswith(col.split("_")[0] +"_"+col.split("_")[1])]
        drift_cols = [ v for v in drift_cols if not v.endswith('_0')]
        for u in drift_cols:  # removed the direct overlap that was rolled into the drift overlap
            species_unadjusted[u] = species_unadjusted[u].sub(species_unadjusted[col].where(species_unadjusted["Acres_CONUS"]!= 0, np.nan), fill_value=0)

        species_unadjusted.ix[:,drift_cols] = species_unadjusted.ix[:,drift_cols].div(species_unadjusted["CONUS_Adjusted"].where(species_unadjusted["Acres_CONUS"]!= 0, np.nan), axis=0)
        species_unadjusted.ix[:,drift_cols] *= 100


    #nl48
    species_unadjusted.drop("Acres_CONUS", axis=1, inplace=True)
    species_unadjusted =pd.merge(species_unadjusted, nl48_acres,on = 'EntityID', how = 'left')
    species_unadjusted.ix[:,nl48_cols]= species_unadjusted.ix[:,nl48_cols].multiply(species_unadjusted["TotalAcresNL48"].where(species_unadjusted["TotalAcresNL48"]!= 0, np.nan),axis=0)
    col_direct_nl48 = [ v for v in nl48_cols if v.endswith('_0')]
    for col in col_direct_nl48:
        species_unadjusted["NL48_Adjusted"]= species_unadjusted["TotalAcresNL48"].sub(species_unadjusted[col].where(species_unadjusted["TotalAcresNL48"]!= 0, np.nan), fill_value=0)
        drift_cols = [v for v in nl48_cols if v.startswith(col.split("_")[0] +"_"+col.split("_")[1])]
        drift_cols = [ v for v in drift_cols if not v.endswith('_0')]
        for u in drift_cols:  # removed the direct overlap that was rolled into the drift overlap
            species_unadjusted[u] = species_unadjusted[u].sub(species_unadjusted[col].where(species_unadjusted["TotalAcresNL48"]!= 0, np.nan), fill_value=0)

        species_unadjusted.ix[:,drift_cols] = species_unadjusted.ix[:,drift_cols].div(species_unadjusted["NL48_Adjusted"].where(species_unadjusted["TotalAcresNL48"]!= 0, np.nan), axis=0)
        species_unadjusted.ix[:,drift_cols] *= 100

    df_all_drift = [v for v in cols if not v.endswith("_0")]
    drift_df = species_unadjusted[["EntityID"]+df_all_drift]
    drift_df.ix[:, 'EntityID'] = drift_df.ix[:, 'EntityID'].map(lambda x: x).astype(str)
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
    for col in col_prefix:
        col_selection.append(col + "_0")
        if len(aa_layers.loc[(aa_layers['FinalColHeader'] == col) & (aa_layers['ground'] == 'x')]) > 0:
            col_selection.append(col + "_305")
        if len(aa_layers.loc[(aa_layers['FinalColHeader'] == col) & (aa_layers['aerial'] == 'x')]) > 0:
            col_selection.append(col + "_792")
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


def appply_factor(factor_df, pct_df):
    # Load factors (calculated from the un-adjusted overlap) into the PCT overlap table
    if not skip_non_ag_adjustment:
        factors_df = factor_df.ix[:, ['EntityID', 'CONUS_Ag_Ag_Factor', 'CONUS_NonAg_NonAg_Factor', 'NL48_Ag_Ag_Factor',
                                      'NL48_NonAg_NonAg_Factor', 'CONUS_Composite_Factor',
                                      'NL48_Composite_Factor']].fillna(0)

    else:
        factors_df = factor_df.ix[:, ['EntityID', 'CONUS_Ag_Ag_Factor', 'NL48_Ag_Ag_Factor', 'CONUS_Composite_Factor',
                                      'NL48_Composite_Factor']].fillna(0)
    pct_df.ix[:, 'EntityID'] = pct_df.ix[:, 'EntityID'].map(lambda x: x).astype(str)
    factors_df['EntityID'] = factors_df['EntityID'].map(lambda x: x).astype(str)
    pct_df = pd.merge(pct_df, factors_df, on='EntityID', how='left')

    # # Applies the factor adjustments to the pct adjusted overlap

    # # APPLIES COMPOSITE FACTOR

    # To Ag uses layer
    pct_df.ix[:, use_direct_only_conus_ag] = pct_df.ix[:, use_direct_only_conus_ag].div(
        pct_df['CONUS_Composite_Factor'].where(pct_df['CONUS_Composite_Factor'] != 0, np.nan), axis=0)
    pct_df.ix[:, use_direct_only_nl48_ag] = pct_df.ix[:, use_direct_only_nl48_ag].div(
        pct_df['NL48_Composite_Factor'].where(pct_df['NL48_Composite_Factor'] != 0, np.nan), axis=0)

    # To Non Ag use layers if we have non-ag uses
    if not skip_non_ag_adjustment:
        pct_df.ix[:, use_direct_only_conus_nonag] = pct_df.ix[:, use_direct_only_conus_nonag].div(
            pct_df['CONUS_Composite_Factor'].where(pct_df['CONUS_Composite_Factor'] != 0, np.nan), axis=0)
        pct_df.ix[:, use_direct_only_nl48_nonag] = pct_df.ix[:, use_direct_only_nl48_nonag].div(
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

    # stores factor and pct adjusted values ag and non ag uses for conus and L48 - uses to calculate the differnence between
    # the pct adjusted and the factor and redundancy adjusted values -impact of the redundancy adjustment and adjust the
    # drift values
    # Confirms values are in numeric format
    pct_df.ix[:, overlap_cols_conus] = pct_df.ix[:, overlap_cols_conus].apply(pd.to_numeric, errors='coerce')
    pct_df.ix[:, overlap_cols_nl48] = pct_df.ix[:, overlap_cols_nl48].apply(pd.to_numeric, errors='coerce')
    adjusted_conus_ag = pct_df[['EntityID'] + use_direct_only_conus_ag].copy()
    adjusted_conus_nonag = pct_df[['EntityID'] + use_direct_only_conus_nonag].copy()
    adjusted_nl48_ag = pct_df[['EntityID'] + use_direct_only_nl48_ag].copy()
    adjusted_nl48_nonag = pct_df[['EntityID'] + use_direct_only_nl48_nonag].copy()

    # Calculated the difference seen in the factor adjusted overlap, this difference is removed from the summarize
    # ground and aerial values
    difference_conus_ag = pct_adjusted_conus_ag.set_index('EntityID').subtract(adjusted_conus_ag.set_index('EntityID'))
    difference_conus_nonag = pct_adjusted_conus_nonag.set_index('EntityID').subtract(
        adjusted_conus_nonag.set_index('EntityID'))
    difference_nl48_ag = pct_adjusted_nl48_ag.set_index('EntityID').subtract(adjusted_nl48_ag.set_index('EntityID'))
    difference_nl48_nonag = pct_adjusted_nl48_nonag.set_index('EntityID').subtract(
        adjusted_nl48_nonag.set_index('EntityID'))

    # get col header for drift columnm by replacing the 0 with the standard values of 305 for ground and 792 for aerial
    columns_conus_ground_ag = [x.replace("_0", "_305") for x in difference_conus_ag.columns.values.tolist()]
    columns_conus_ground_nonag = [x.replace("_0", "_305") for x in difference_conus_nonag.columns.values.tolist()]
    columns_conus_aerial_ag = [x.replace("_0", "_792") for x in difference_conus_ag.columns.values.tolist()]
    columns_conus_aerial_nonag = [x.replace("_0", "_792") for x in difference_conus_nonag.columns.values.tolist()]

    columns_nl48_ground_ag = [x.replace("_0", "_305") for x in difference_nl48_ag.columns.values.tolist()]
    columns_nl48_ground_nonag = [x.replace("_0", "_305") for x in difference_nl48_nonag.columns.values.tolist()]
    columns_nl48_aerial_ag = [x.replace("_0", "_792") for x in difference_nl48_ag.columns.values.tolist()]
    columns_nl48_aerial_nonag = [x.replace("_0", "_792") for x in difference_nl48_nonag.columns.values.tolist()]

    # Adjusted ground by subtracting difference
    difference_conus_ag.columns = columns_conus_ground_ag
    difference_conus_nonag.columns = columns_conus_ground_nonag
    difference_nl48_ag.columns = columns_nl48_ground_ag
    difference_nl48_nonag.columns = columns_nl48_ground_nonag
    adjusted_conus_ag_ground = pct_df.ix[:, ['EntityID'] + use_direct_only_conus_ag_ground].set_index(
        'EntityID').subtract(difference_conus_ag)
    adjusted_conus_nonag_ground = pct_df.ix[:, ['EntityID'] + use_direct_only_conus_nonag_ground].set_index(
        'EntityID').subtract(difference_conus_nonag)
    adjusted_nl48_ag_ground = pct_df.ix[:, ['EntityID'] + use_direct_only_nl48_ag_ground].set_index(
        'EntityID').subtract(difference_nl48_ag)
    adjusted_nl48_nonag_ground = pct_df.ix[:, ['EntityID'] + use_direct_only_nl48_nonag_ground].set_index(
        'EntityID').subtract(difference_nl48_nonag)

    # Adjust aerial by subtracting difference
    difference_conus_ag.columns = columns_conus_aerial_ag
    difference_conus_nonag.columns = columns_conus_aerial_nonag
    difference_nl48_ag.columns = columns_nl48_aerial_ag
    difference_nl48_nonag.columns = columns_nl48_aerial_nonag
    adjusted_conus_ag_aerial = pct_df.ix[:, ['EntityID'] + use_direct_only_conus_ag_aerial].set_index(
        'EntityID').subtract(difference_conus_ag)
    adjusted_conus_nonag_aerial = pct_df.ix[:, ['EntityID'] + use_direct_only_conus_nonag_aerial].set_index(
        'EntityID').subtract(difference_conus_nonag)
    adjusted_nl48_ag_aerial = pct_df.ix[:, ['EntityID'] + use_direct_only_nl48_ag_aerial].set_index(
        'EntityID').subtract(difference_nl48_ag)
    adjusted_nl48_nonag_aerial = pct_df.ix[:, ['EntityID'] + use_direct_only_nl48_nonag_aerial].set_index(
        'EntityID').subtract(difference_nl48_nonag)

    # reset index  for the drift adjusted dfs
    adjusted_conus_ag_ground = adjusted_conus_ag_ground.reset_index()
    adjusted_conus_nonag_ground = adjusted_conus_nonag_ground.reset_index()
    adjusted_conus_ag_aerial = adjusted_conus_ag_aerial.reset_index()
    adjusted_conus_nonag_aerial = adjusted_conus_nonag_aerial.reset_index()
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

    pct_df.ix[:, adjusted_nl48_ag_ground.columns.tolist()] = adjusted_nl48_ag_ground.ix[:,
                                                             adjusted_nl48_ag_ground.columns.tolist()]
    pct_df.ix[:, adjusted_nl48_nonag_ground.columns.tolist()] = adjusted_nl48_nonag_ground.ix[:,
                                                                adjusted_nl48_nonag_ground.columns.tolist()]
    pct_df.ix[:, adjusted_nl48_ag_aerial.columns.tolist()] = adjusted_nl48_ag_aerial.ix[:,
                                                             adjusted_nl48_ag_aerial.columns.tolist()]
    pct_df.ix[:, adjusted_nl48_nonag_aerial.columns.tolist()] = adjusted_nl48_nonag_aerial.ix[:,
                                                                adjusted_nl48_nonag_aerial.columns.tolist()]
    return pct_df


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

# Create out location
create_directory(out_location + os.sep + chemical_name + os.sep + "Summarized Tables")
out_path = out_location + os.sep + chemical_name + os.sep + "Summarized Tables"
out_path_original = out_location + os.sep + chemical_name + os.sep + "Summarized Tables"

# Loads input values from lookup tables
use_lookup_df = pd.read_csv(use_lookup)
# col headers for final tables
list_final_uses = list(set(use_lookup_df['FinalUseHeader'].values.tolist()))
# Extract columns that will be adjusted for redundancy
other_layer = use_lookup_df.loc[(use_lookup_df['other layer'] == 'x')]
other_layer_cols = other_layer['Chem Table FinalColHeader'].values.tolist()

ag_headers = use_lookup_df.loc[(use_lookup_df['Included AA Ag'] == 'x')]
ag_cols = ag_headers['Chem Table FinalColHeader'].values.tolist()
acres_df = pd.read_csv(acres_col)


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

# ##Extracts columns from lookup tables that should be included for chemical in the AA and the col header to use in the
# final tables for CONUS
aa_layers_CONUS = use_lookup_df.loc[((use_lookup_df['Included AA'] == 'x') | (use_lookup_df['other layer'] == 'x')) & (
        use_lookup_df['Region'] == 'CONUS')]
col_prefix_CONUS = aa_layers_CONUS['FinalColHeader'].values.tolist()

##Extracts columns from lookup tables that should be included for chemical in the AA and the col header to use in the
# final tables for NL48 CONUS
aa_layers_NL48 = use_lookup_df.loc[((use_lookup_df['Action Area'] == 'x') | (use_lookup_df['other layer'] == 'x') | (
        use_lookup_df['Included AA'] == 'x')) & (use_lookup_df['Region'] != 'CONUS')]
col_prefix_NL48 = aa_layers_NL48['FinalColHeader'].values.tolist()

# ##Load species that should be adjusted for on/off calls from master list
on_off_df = pd.read_excel(on_off_excel)
on_off_df['EntityID'] = on_off_df['EntityID'].map(lambda x: x).astype(str)

# ## Extract use and columns from lookup table that will be adjusted for the on/off field, cultivated crops, pastures,
# orchards - can add others
# Also load the species to be adjust for each group
if cult_crop:
    on_off_cult = use_lookup_df.loc[(use_lookup_df['On/Off_AG'] == 'x')]
    on_off_cult_cols = on_off_cult['FinalColHeader'].values.tolist()
    on_off_cult_df = on_off_df.loc[(on_off_df['On/Off_AG'] == 'OFF')]
    on_off_cult_species = on_off_cult_df['EntityID'].values.tolist()
if pastures:
    on_off_pasture = use_lookup_df.loc[(use_lookup_df['On/Off_Pasture'] == 'x')]
    on_off_pasture_cols = on_off_pasture['FinalColHeader'].values.tolist()
    on_off_pasture_df = on_off_df.loc[(on_off_df['On/Off_Pasture'] == 'OFF')]
    on_off_pasture_species = on_off_pasture_df['EntityID'].values.tolist()
if orchards_crops:
    on_off_orchard = use_lookup_df.loc[(use_lookup_df['On/Off_Orchard_Plantation'] == 'x')]
    on_off_orchard_cols = on_off_orchard['FinalColHeader'].values.tolist()
    on_off_orchards_df = on_off_df.loc[(on_off_df['On/Off_Orchard_Plantation'] == 'OFF')]
    on_off_orchards_species = on_off_orchards_df['EntityID'].values.tolist()
if residential:
    on_off_residential = use_lookup_df.loc[(use_lookup_df['On/Off_residential'] == 'x')]
    on_off_residential_cols = on_off_residential['FinalColHeader'].values.tolist()
    on_off_res_df = on_off_df.loc[(on_off_df['On/Off_Res'] == 'OFF')]
    on_off_res_species = on_off_res_df['EntityID'].values.tolist()

# ## Species info from master list
species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]
base_sp_df['EntityID'] = base_sp_df['EntityID'].map(lambda x: x).astype(str)

# TABLE 1- no adjustment
out_path_no_adjustment = out_path + os.sep + '1_No Adjustment'
create_directory(out_path_no_adjustment)
# no adjustment tables
l48_df = pd.read_csv(l48_BE_sum)
drop_col = [m for m in l48_df.columns.values.tolist() if m.startswith('Unnamed')]
l48_df.drop(drop_col, axis=1, inplace=True)
nl48_df = pd.read_csv(nl48_BE_sum)
drop_col = [m for m in nl48_df.columns.values.tolist() if m.startswith('Unnamed')]
nl48_df.drop(drop_col, axis=1, inplace=True)

chemical_noadjustment, col_selection_conus = extract_chemical_overlap(col_prefix_CONUS, aa_layers_CONUS, 'CONUS',
                                                                      l48_df, 'no adjustment - CONUS')
chemical_noadjustment['EntityID'] = chemical_noadjustment['EntityID'].map(lambda x: x).astype(str)
chemical_noadjustment = pd.merge(base_sp_df, chemical_noadjustment, on='EntityID', how='left')
out_nl48_df, col_selection_nl48 = extract_chemical_overlap(col_prefix_NL48, aa_layers_NL48, 'NL48', nl48_df,
                                                           'no adjustment - NL48')
out_nl48_df['EntityID'] = out_nl48_df['EntityID'].map(lambda x: x).astype(str)
# merges the CONUS and NL48 tables
chemical_noadjustment = pd.merge(chemical_noadjustment, out_nl48_df, on='EntityID', how='left')

chemical_noadjustment['Step 2 ED Comment'] = chemical_noadjustment.apply(
    lambda row: step_1_ED(row, chemical_name + " AA""_" + max_drift), axis=1)
chemical_noadjustment['Step 2 ED Comment'] = chemical_noadjustment.apply(
    lambda row: NLAA_overlap(row, col_selection_conus, col_selection_nl48), axis=1)
chemical_noadjustment.fillna(0, inplace=True)

# exports tables that are not adjusted
chemical_noadjustment.to_csv(
    out_path_no_adjustment + os.sep + file_type + 'No Adjustment_GIS_Step2_' + chemical_name + '.csv')
conus_cols_f = [v for v in chemical_noadjustment.columns.values.tolist() if
                v.startswith('CONUS') or v in col_include_output]
nl48_cols_f = [v for v in chemical_noadjustment.columns.values.tolist() if
               v.startswith('NL48') or v in col_include_output]
conus_df_step1 = chemical_noadjustment[conus_cols_f]
nl48_df_step1 = chemical_noadjustment[nl48_cols_f]

conus_df_step1.to_csv(
    out_path_no_adjustment + os.sep + file_type + 'No Adjustment_CONUS_Step2_' + chemical_name + '.csv')
nl48_df_step1.to_csv(out_path_no_adjustment + os.sep + file_type + 'No Adjustment_NL48_Step2_' + chemical_name + '.csv')

# TABLE 2- PCT
# Summarize output with just the PCT adjustments
for group in ['min', 'max', 'avg']:  #
    for bound in [ 'Uniform','Lower','Upper']:  #
        out_path_pct = out_path_original + os.sep + '2_PCT'  # set out path
        create_directory(out_path_pct)
        out_path_pct = out_path_pct + os.sep + group  # set out path
        create_directory(out_path_pct)
        out_path_pct = out_path_pct + os.sep + bound  # set out path
        create_directory(out_path_pct)
        print out_path_pct
        pct_table = l48_BE_sum_PCT_table.split("_")[0] + "_" + bound + "_" + l48_BE_sum_PCT_table.split("_")[2] + "_" + \
                    l48_BE_sum_PCT_table.split("_")[3] + "_" + l48_BE_sum_PCT_table.split("_")[4] + "_" + \
                    l48_BE_sum_PCT_table.split("_")[5] + "_" + l48_BE_sum_PCT_table.split("_")[6] + "_" + \
                    l48_BE_sum_PCT_table.split("_")[7] + "_" + group + "_" + l48_BE_sum_PCT_table.split("_")[9]

        l48_BE_sum_PCT = BE_sum_PCT_path + os.sep + pct_table
        pct_table_nl = nl48_BE_sum_PCT_table.split("_")[0] + "_" + bound + "_" + nl48_BE_sum_PCT_table.split("_")[
            2] + "_" + nl48_BE_sum_PCT_table.split("_")[3] + "_" + nl48_BE_sum_PCT_table.split("_")[4] + "_" + \
                       nl48_BE_sum_PCT_table.split("_")[5] + "_" + nl48_BE_sum_PCT_table.split("_")[6] + "_" + \
                       nl48_BE_sum_PCT_table.split("_")[7] + "_" + group + "_" + nl48_BE_sum_PCT_table.split("_")[9]
        nl48_BE_sum_PCT = BE_sum_PCT_path + os.sep + pct_table_nl

        print l48_BE_sum_PCT
        print nl48_BE_sum_PCT

        # PCT and Census Adjusted tables
        l48_df_pct = pd.read_csv(l48_BE_sum_PCT)
        nl48_df_pct = pd.read_csv(nl48_BE_sum_PCT)

        chemical_pct, col_selection_conus = extract_chemical_overlap(col_prefix_CONUS, aa_layers_CONUS, 'CONUS',
                                                                     l48_df_pct, 'pct -conus')
        chemical_pct = pd.merge(base_sp_df, chemical_pct, on='EntityID', how='left')
        out_nl48_df_pct, col_selection_nl48 = extract_chemical_overlap(col_prefix_NL48, aa_layers_NL48, 'NL48',
                                                                       nl48_df_pct, 'pct-nl48')
        out_nl48_df_pct['EntityID'] = out_nl48_df_pct['EntityID'].map(lambda x: x).astype(str)

        # merges the CONUS and NL48 tables
        chemical_pct = pd.merge(chemical_pct, out_nl48_df_pct, on='EntityID', how='left')

        chemical_pct['Step 2 ED Comment'] = chemical_pct.apply(
            lambda row: step_1_ED(row, chemical_name + " AA""_" + max_drift), axis=1)
        chemical_pct['Step 2 ED Comment'] = chemical_pct.apply(
            lambda row: NLAA_overlap(row, col_selection_conus, col_selection_nl48), axis=1)
        chemical_pct.fillna(0, inplace=True)

        # Export tables that are only PCT and census adjusted
        chemical_pct.to_csv(out_path_pct + os.sep + file_type + 'PCT_GIS_Step2_' + chemical_name + '.csv')
        # COMMENTS THESE SHOULD NOT CHANGE FROM THE FIRST LOOP
        # conus_cols_f = [v for v in chemical_pct.columns.values.tolist() if v.startswith('CONUS') or v in col_include_output]
        # nl48_cols_f = [v for v in chemical_pct.columns.values.tolist() if v.startswith('NL48') or v in col_include_output]
        conus_df_step1_pct = chemical_pct[conus_cols_f]
        nl48_df_step1_pct = chemical_pct[nl48_cols_f]
        conus_df_step1_pct.to_csv(out_path_pct + os.sep + file_type + 'PCT_CONUS_Step2_' + chemical_name + '.csv')
        nl48_df_step1_pct.to_csv(out_path_pct + os.sep + file_type + 'PCT_NL48_Step2_' + chemical_name + '.csv')

        # TABLE 3- PCT and Redundancy tables
        # redundancy adjustments - adjusts the direct overlap based on the ag, nonag and composite factors that are calculated

        out_path_redundancy = out_path_original + os.sep + '3_Redundancy'  # set out path

        create_directory(out_path_redundancy)
        out_path_redundancy = out_path_redundancy + os.sep + group  # set out path
        create_directory(out_path_redundancy)
        out_path_redundancy = out_path_redundancy + os.sep + bound  # set out path
        create_directory(out_path_redundancy)

        # creates a copy of the PCT table to be adjusted further for redundancy
        chemical_pct_redundancy = chemical_pct.copy()

        # COL FILTERS USED TO APPLY ADJUSTMENTS
        # list of columns that are just use sites that apply to conus vs nl48, aa, composites, use, ag and non-ag
        # conus/nonl48
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

        # col filters list for direct overlap for ag and non ag uses represented by an _0 at the end of the column header
        use_direct_only_conus_ag = [x for x in use_col_conus if
                                    x.endswith('_0') and (x.split('_')[0] + "_" + x.split('_')[1]) in ag_cols]
        use_direct_only_conus_nonag = [x for x in use_col_conus if
                                       x.endswith('_0') and (x.split('_')[0] + "_" + x.split('_')[1]) in nonag_cols]
        use_direct_only_nl48_ag = [x for x in use_col_nl48 if
                                   x.endswith('_0') and (x.split('_')[0] + "_" + x.split('_')[1]) in ag_cols]
        use_direct_only_nl48_nonag = [x for x in use_col_nl48 if
                                      x.endswith('_0') and (x.split('_')[0] + "_" + x.split('_')[1]) in nonag_cols]

        # col filter direct overlap for aa, ag and non ag composites
        use_direct_only_conus_ag_aa = [x for x in aa_col_conus if
                                       x.endswith('_0') and 'Ag' in x.split("_")[1].split(" ")]
        use_direct_only_conus_nonag_aa = [x for x in aa_col_conus if
                                          x.endswith('_0') and 'NonAg' in x.split("_")[1].split(" ")]
        use_direct_only_conus_aa = [x for x in aa_col_conus if
                                    x.endswith('_0') and 'Ag' not in x.split("_")[1].split(" ") and 'NonAg' not in
                                    x.split("_")[1].split(" ")]

        use_direct_only_nl48_ag_aa = [x for x in aa_col_nl48 if x.endswith('_0') and 'Ag' in x.split("_")[1].split(" ")]
        use_direct_only_nl48_nonag_aa = [x for x in aa_col_nl48 if
                                         x.endswith('_0') and 'NonAg' in x.split("_")[1].split(" ")]
        use_direct_only_nl48_aa = [x for x in aa_col_nl48 if
                                   x.endswith('_0') and 'Ag' not in x.split("_")[1].split(" ") and 'NonAg' not in
                                   x.split("_")[1].split(" ")]

        # col filters list ground and aerial overlap represented by an _305 and _792 at the end of the column header ag and
        # non ag uses
        use_direct_only_conus_ag_ground = [x for x in use_col_conus if
                                           x.endswith('_305') and (x.split('_')[0] + "_" + x.split('_')[1]) in ag_cols]
        use_direct_only_conus_nonag_ground = [x for x in use_col_conus if x.endswith('_305') and (
                x.split('_')[0] + "_" + x.split('_')[1]) in nonag_cols]
        use_direct_only_nl48_ag_ground = [x for x in use_col_nl48 if
                                          x.endswith('_305') and (x.split('_')[0] + "_" + x.split('_')[1]) in ag_cols]
        use_direct_only_nl48_nonag_ground = [x for x in use_col_nl48 if x.endswith('_305') and (
                x.split('_')[0] + "_" + x.split('_')[1]) in nonag_cols]

        use_direct_only_conus_ag_aerial = [x for x in use_col_conus if
                                           x.endswith('_792') and (x.split('_')[0] + "_" + x.split('_')[1]) in ag_cols]
        use_direct_only_conus_nonag_aerial = [x for x in use_col_conus if x.endswith('_792') and (
                x.split('_')[0] + "_" + x.split('_')[1]) in nonag_cols]
        use_direct_only_nl48_ag_aerial = [x for x in use_col_nl48 if
                                          x.endswith('_792') and (x.split('_')[0] + "_" + x.split('_')[1]) in ag_cols]
        use_direct_only_nl48_nonag_aerial = [x for x in use_col_nl48 if x.endswith('_792') and (
                x.split('_')[0] + "_" + x.split('_')[1]) in nonag_cols]

        # Stores unadjusted values ag and non ag uses for conus and L48 - used for factor adjustment
        chemical_noadjustment.ix[:, overlap_cols_conus] = chemical_noadjustment.ix[:, overlap_cols_conus].apply(
            pd.to_numeric, errors='coerce')
        chemical_noadjustment.ix[:, overlap_cols_nl48] = chemical_noadjustment.ix[:, overlap_cols_nl48].apply(
            pd.to_numeric, errors='coerce')
        unadjusted_conus_ag = chemical_noadjustment[['EntityID'] + use_direct_only_conus_ag].copy()
        unadjusted_conus_nonag = chemical_noadjustment[['EntityID'] + use_direct_only_conus_nonag].copy()
        unadjusted_nl48_ag = chemical_noadjustment[['EntityID'] + use_direct_only_nl48_ag].copy()
        unadjusted_nl48_nonag = chemical_noadjustment[['EntityID'] + use_direct_only_nl48_nonag].copy()

        # stores values that are adjusts by PCT only and but adjusted for redundancy - used to calc the impact of the
        # redundancy adjustment
        # Confirms all number are set to a numeric data type to start calculations for factor adjustments
        chemical_pct.ix[:, overlap_cols_conus] = chemical_pct.ix[:, overlap_cols_conus].apply(pd.to_numeric,
                                                                                              errors='coerce')
        chemical_pct.ix[:, overlap_cols_nl48] = chemical_pct.ix[:, overlap_cols_nl48].apply(pd.to_numeric,
                                                                                            errors='coerce')
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
            chemical_noadjustment['CONUS_Sum_Composites'] = chemical_noadjustment[use_direct_only_conus_ag_aa + use_direct_only_conus_nonag_aa].sum(
                axis=1)
            chemical_noadjustment['NL48_Sum_Composites'] = chemical_noadjustment[
                use_direct_only_nl48_ag_aa + use_direct_only_nl48_nonag_aa].sum(axis=1)
        else:
            chemical_noadjustment['CONUS_Sum_Composites'] = chemical_noadjustment[
                use_direct_only_conus_ag_aa].sum(axis=1)
            chemical_noadjustment['NL48_Sum_Composites'] = chemical_noadjustment[use_direct_only_nl48_ag_aa].sum(axis=1)

        # chemical_noadjustment.to_csv(out_path_redundancy  + os.sep + 'test4.csv')

        # Calculates factors that will be used for adjustment based on the above values

        # Ag factor and composite factor calculated from the AA and AG Composites then applied to all individual Ag
        # layers, Non-Ag factor and composite factor calculated from the AA And Non-Ag Composites is applied to all of
        # the Non-Ag uses - Updated Winter 2018 based on QC before QC - the composite factor was not applied
        # to the AG use layers.  Both QCers indicated the composite factor need to be applied the the individual ag
        # layers (ESA team Fall 2017- BE Streamlining meeting lead CMR) QC of this process was conducted my CMR and CP
        # early summer 2018 (may/june see files Copy of Carbaryl_QC_FactorAdjustments_CP.xlsx and
        # Carbaryl_QC_FactorAdjustments.xlsx)

        # Ag factor = sum of ag uses/ ag composite - CONUS_Sum_Ag/ direct overlap of the ag composite
        # Non-Ag factor sum of non ag use/ non ag composite; - CONUS_Sum_NonAg /direct overlap of the non ag composite
        # Composite factor sum of Ag and Non Ag composite / AA - CONUS_Sum_Composites/ direct overlap of the AA

        # ### Calculated factors
        # calculates the Ag Factor for CONUS and NL48 = where clause removed the rows where the  Ag composite is equal to 0
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
        # calculated the Composite factors for CONUS and NL48 = where clause removed the rows where the  Ag composite is equal to 0
        chemical_noadjustment['CONUS_Composite_Factor'] = chemical_noadjustment['CONUS_Sum_Composites'].div(
            (chemical_noadjustment[use_direct_only_conus_aa[0]]).where(
                chemical_noadjustment[use_direct_only_conus_aa[0]] != 0, np.nan), axis=0)
        chemical_noadjustment['NL48_Composite_Factor'] = chemical_noadjustment['NL48_Sum_Composites'].div(
            (chemical_noadjustment[use_direct_only_nl48_aa[0]]).where(
                chemical_noadjustment[use_direct_only_nl48_aa[0]] != 0, np.nan), axis=0)
        # print for QC
        # chemical_noadjustment.to_csv(r'L:\Workspace\StreamLine\ESA\Tabulated_TabArea_HUCAB_Usage\Methomyl\Summarized Tables\Full Range\3_Redundancy\test.csv')

        # Column headers will be the same for all tables therefore they don't need to updated; TODO  they should be loaded into
        # the function
        chemical_pct_redundancy = appply_factor(chemical_noadjustment, chemical_pct_redundancy)

        # # Applies the effects determinations based on pct and redundancy redundancy adjustments and saves tables
        chemical_pct_redundancy.fillna(0, inplace=True)
        chemical_pct_redundancy['Step 2 ED Comment'] = chemical_pct_redundancy.apply(
            lambda row: step_1_ED(row, chemical_name + " AA""_" + max_drift), axis=1)
        chemical_pct_redundancy['Step 2 ED Comment'] = chemical_pct_redundancy.apply(
            lambda row: NLAA_overlap(row, col_selection_conus, col_selection_nl48), axis=1)
        chemical_pct_redundancy.to_csv(
            out_path_redundancy + os.sep + file_type + 'Redundancy_GIS_Step2_' + chemical_name + '.csv')
        # Export tables for the CONUS and NL48
        conus_df_step1 = chemical_pct_redundancy[conus_cols_f]
        nl48_df_step1 = chemical_pct_redundancy[nl48_cols_f]
        conus_df_step1.to_csv(
            out_path_redundancy + os.sep + file_type + 'Redundancy_CONUS_Step2_' + chemical_name + '.csv')
        nl48_df_step1.to_csv(
            out_path_redundancy + os.sep + file_type + 'Redundancy_NL48_Step2_' + chemical_name + '.csv')

        # TABLE 4- PCT, Redundancy, and On/Off field adjusted  tables
        # redundancy adjustments - adjusts the direct overlap based on the ag, nonag and composite factors that are calculated
        # On/Off field removed direct overlap for species not found on those use sites

        # on off field adjustments- direct overlap is set to zero if for species identified as not be present on the on/off
        # habitat group types  - decided to set to zero rather than re-calucating the overlap dut to the impact of drift in
        # for the whole range; if pixels are remove a new acre value for the species would need to be calculated and drift
        # overlap would be greater than 100 unless the drift pixel are also removed - it is possible to do this but not in the
        # time frame provided so it was decided to set the direct overlap to zero Fall 2017

        out_path_on_off = out_path_original + os.sep + '4_On_Off_Field'  # set out path
        copy_red = chemical_noadjustment.copy()
        create_directory(out_path_on_off)
        out_path_on_off = out_path_on_off + os.sep + group  # set out path
        create_directory(out_path_on_off)
        out_path_on_off = out_path_on_off + os.sep + bound  # set out path
        create_directory(out_path_on_off)
        #
        # # load on/off field tables adjusted
        # pct_tabl_onoff = l48_BE_sum_onoff_table.split("_")[0] + "_" + bound + "_" + l48_BE_sum_onoff_table.split("_")[
        #     2] + "_" + l48_BE_sum_onoff_table.split("_")[3] + "_" + l48_BE_sum_onoff_table.split("_")[4] + "_" + \
        #                  l48_BE_sum_onoff_table.split("_")[5] + "_" + l48_BE_sum_onoff_table.split("_")[6] + "_" + \
        #                  l48_BE_sum_onoff_table.split("_")[7] + "_" + group + "_" + l48_BE_sum_onoff_table.split("_")[9]
        # l48_BE_sum_PCT_onoff = BE_sum_onoff_path + os.sep + pct_tabl_onoff
        #
        # pct_table_nl_onoff = nl48_BE_sum_onoff_table.split("_")[0] + "_" + bound + "_" + \
        #                      nl48_BE_sum_onoff_table.split("_")[2] + "_" + nl48_BE_sum_onoff_table.split("_")[3] + "_" + \
        #                      nl48_BE_sum_onoff_table.split("_")[4] + "_" + nl48_BE_sum_onoff_table.split("_")[5] + "_" + \
        #                      nl48_BE_sum_onoff_table.split("_")[6] + "_" + nl48_BE_sum_onoff_table.split("_")[
        #                          7] + "_" + group + "_" + nl48_BE_sum_onoff_table.split("_")[9]
        # nl48_BE_sum_PCT_onoff = BE_sum_onoff_path + os.sep + pct_table_nl_onoff
        #
        # print l48_BE_sum_PCT_onoff
        # print nl48_BE_sum_PCT_onoff
        #
        # l48_df_pct_onoff = pd.read_csv(l48_BE_sum_PCT_onoff)
        # nl48_df_pct_onoff = pd.read_csv(nl48_BE_sum_PCT_onoff)
        #
        # chemical_pct_onoff, col_selection_conus = extract_chemical_overlap(col_prefix_CONUS, aa_layers_CONUS, 'CONUS',
        #                                                                    l48_df_pct_onoff, 'pct -conus')
        # chemical_pct_onoff['EntityID'] = chemical_pct_onoff['EntityID'].map(lambda x: x).astype(str)
        # chemical_pct_onoff = pd.merge(base_sp_df, chemical_pct_onoff, on='EntityID', how='left')
        # out_nl48_df_pct_onoff, col_selection_nl48 = extract_chemical_overlap(col_prefix_NL48, aa_layers_NL48, 'NL48',
        #                                                                      nl48_df_pct_onoff, 'pct-nl48')
        # # merges the CONUS and NL48 tables
        # out_nl48_df_pct_onoff['EntityID'] = out_nl48_df_pct_onoff['EntityID'].map(lambda x: x).astype(str)
        # chemical_pct_onoff = pd.merge(chemical_pct_onoff, out_nl48_df_pct_onoff, on='EntityID', how='left')
        #
        # chemical_pct_redundancy_on_off = appply_factor(chemical_noadjustment, chemical_pct_onoff)
        # chemical_pct_redundancy_on_off.to_csv(r'L:\Workspace\StreamLine\ESA\Tabulated_TabArea_HUCAB_Usage\Carbary_pres.csv')
        chemical_pct_redundancy_on_off = chemical_pct_redundancy.copy()
        adjust_df = chemical_pct_redundancy.copy()


        # extract columns from overlap table that will be adjusted for on/off field and makes adjustments- species found
        #  off site we keep the pct adjust nut not on off so it is based on full range
        # ## Extract use and columns from lookup table that will be adjusted for the on/off field, cultivated crops, pastures,
        # orchards - can add others
        # Also load the species to be adjust for each group

        cult_use_cols = []
        pasture_use_cols = []
        orchard_use_cols = []
        residential_use_cols = []

        if cult_crop:
            on_off_cult = use_lookup_df.loc[(use_lookup_df['On/Off_AG'] == 'x')]
            on_off_cult_cols = on_off_cult['FinalColHeader'].values.tolist()
            on_off_cult_df = on_off_df.loc[(on_off_df['On/Off_AG'] == 'OFF')]
            on_off_cult_species = on_off_cult_df['EntityID'].values.tolist()
            for z in chemical_pct_redundancy_on_off.columns.values.tolist():
                for p in on_off_cult_cols:
                    if z.startswith(p):
                        cult_use_cols.append(z)
            # if speiceis is off field make direct overlap 0
            direct_cult = [v for v in cult_use_cols if v.endswith("_0")]
            species_cult_df = chemical_pct_redundancy_on_off.loc[(chemical_pct_redundancy_on_off['EntityID'].isin(on_off_cult_species))]
            for col in direct_cult:
                species_cult_df[col].values[:] = 0
            # moves 0 values to working df using update
            left_update = chemical_pct_redundancy_on_off.set_index('EntityID')
            right_update = species_cult_df.set_index('EntityID')
            res = left_update.reindex(columns=left_update.columns.union(right_update.columns))
            res.update(right_update)
            chemical_pct_redundancy_on_off = res.reset_index()
            # adjusted the drift intervals - removing the direct overlap from the species range area
            chemical_pct_redundancy_on_off = on_off_field(cult_use_cols, chemical_pct_redundancy_on_off,on_off_cult_species, adjust_df, acres_df)
            # any species not par of the on/off field confimed they were not altered
            chemical_pct_redundancy_on_off.update(chemical_pct_redundancy.loc[~(chemical_pct_redundancy['EntityID'].isin(on_off_cult_species)), cult_use_cols])

        if pastures:
            on_off_pasture = use_lookup_df.loc[(use_lookup_df['On/Off_Pasture'] == 'x')]
            on_off_pasture_cols = on_off_pasture['FinalColHeader'].values.tolist()
            on_off_pasture_df = on_off_df.loc[(on_off_df['On/Off_Pasture'] == 'OFF')]
            on_off_pasture_species = on_off_pasture_df['EntityID'].values.tolist()
            for z in chemical_pct_redundancy_on_off.columns.values.tolist():
                for p in on_off_pasture_cols:
                    if z.startswith(p):
                        pasture_use_cols.append(z)
            # if speice is is off field make direct overlap 0
            direct_pasture= [v for v in pasture_use_cols if v.endswith("_0")]
            species_pasture_df = chemical_pct_redundancy.loc[(chemical_pct_redundancy['EntityID'].isin(on_off_pasture_species))]
            for col in direct_pasture:
                species_pasture_df[col].values[:] = 0
            # moves 0 values to working df using update
            left_update = chemical_pct_redundancy_on_off.set_index('EntityID')
            right_update = species_pasture_df.set_index('EntityID')
            res = left_update.reindex(columns=left_update.columns.union(right_update.columns))
            res.update(right_update)
            chemical_pct_redundancy_on_off = res.reset_index()
            # adjusted the drift intervals - removing the direct overlap from the species range area
            chemical_pct_redundancy_on_off = on_off_field(pasture_use_cols, chemical_pct_redundancy_on_off,on_off_pasture_species, adjust_df, acres_df)
            # any species not par of the on/off field confimed they were not altered
            chemical_pct_redundancy_on_off.update(chemical_pct_redundancy.loc[~(chemical_pct_redundancy['EntityID'].isin(on_off_pasture_species)), pasture_use_cols])


        if orchards_crops:
            on_off_orchard = use_lookup_df.loc[(use_lookup_df['On/Off_Orchard_Plantation'] == 'x')]
            on_off_orchard_cols = on_off_orchard['FinalColHeader'].values.tolist()
            on_off_orchards_df = on_off_df.loc[(on_off_df['On/Off_Orchard_Plantation'] == 'OFF')]
            on_off_orchards_species  = on_off_orchards_df['EntityID'].values.tolist()
            for z in chemical_pct_redundancy_on_off.columns.values.tolist():
                for p in on_off_orchard_cols:
                    if z.startswith(p):
                        orchard_use_cols.append(z)
            # if speiceis is off field make direct overlap 0
            direct_orchards = [v for v in orchard_use_cols if v.endswith("_0")]
            species_orchards_df = chemical_pct_redundancy.loc[(chemical_pct_redundancy['EntityID'].isin(on_off_orchards_species))]
            for col in direct_orchards:
                species_orchards_df[col].values[:] = 0
            left_update = chemical_pct_redundancy_on_off.set_index('EntityID')
            right_update = species_pasture_df.set_index('EntityID')
            res = left_update.reindex(columns=left_update.columns.union(right_update.columns))
            res.update(right_update)
            chemical_pct_redundancy_on_off = res.reset_index()
            # adjusted the drift intervals - removing the direct overlap from the species range area
            chemical_pct_redundancy_on_off = on_off_field(orchard_use_cols, chemical_pct_redundancy_on_off,on_off_orchards_species, adjust_df, acres_df)
            # any species not par of the on/off field confimed they were not altered
            chemical_pct_redundancy_on_off.update(chemical_pct_redundancy.loc[~(chemical_pct_redundancy['EntityID'].isin(on_off_orchards_species)), orchard_use_cols])
        if residential:
            on_off_residential = use_lookup_df.loc[(use_lookup_df['On/Off_residential'] == 'x')]
            on_off_residential_cols = on_off_residential['FinalColHeader'].values.tolist()
            on_off_res_df = on_off_df.loc[(on_off_df['On/Off_Res'] == 'OFF')]
            on_off_res_species = on_off_res_df['EntityID'].values.tolist()
            for z in chemical_pct_redundancy_on_off.columns.values.tolist():
                for p in on_off_residential_cols:
                    if z.startswith(p):
                        residential_use_cols.append(z)
            direct_res = [v for v in residential_use_cols if v.endswith("_0")]
            species_res_df = chemical_pct_redundancy.loc[(chemical_pct_redundancy['EntityID'].isin(on_off_res_species))]
            # moves 0 values to working df using update
            left_update = chemical_pct_redundancy_on_off.set_index('EntityID')
            right_update = species_res_df.set_index('EntityID')
            res = left_update.reindex(columns=left_update.columns.union(right_update.columns))
            res.update(right_update)
            chemical_pct_redundancy_on_off = res.reset_index()
            # adjusted the drift intervals - removing the direct overlap from the species range area
            chemical_pct_redundancy_on_off = on_off_field(residential_use_cols, chemical_pct_redundancy_on_off,on_off_res_species, adjust_df, acres_df)
            # any species not par of the on/off field confimed they were not altere
            chemical_pct_redundancy_on_off.update(chemical_pct_redundancy.loc[~(chemical_pct_redundancy['EntityID'].isin(on_off_res_species)), residential_use_cols])

        cols_not_on_off = []
        for col in chemical_pct_redundancy_on_off:
            if col not in residential_use_cols and col not in pasture_use_cols and col not in orchard_use_cols and col not in cult_use_cols:
                cols_not_on_off.append(col)

        # cols that the are not part of the on/off adjusted and should use the full range; these are loaded in from the
        # PCT/redundancy adjusted table- also adjust species just found some of the on/off classes
        chemical_pct_redundancy_on_off.ix[:, cols_not_on_off ] = chemical_pct_redundancy.ix[:,cols_not_on_off]
        # sets the col order that is altered during the pd.df.updates
        chemical_pct_redundancy_on_off= chemical_pct_redundancy_on_off.reindex(columns=chemical_noadjustment.columns.values.tolist())
        chemical_pct_redundancy_on_off.fillna(0, inplace=True)

        chemical_pct_redundancy_on_off['Step 2 ED Comment'] = chemical_pct_redundancy_on_off.apply(
            lambda row: step_1_ED(row, chemical_name + " AA""_" + max_drift), axis=1)
        chemical_pct_redundancy_on_off['Step 2 ED Comment'] = chemical_pct_redundancy_on_off.apply(
            lambda row: NLAA_overlap(row, col_selection_conus, col_selection_nl48), axis=1)
        chemical_pct_redundancy_on_off.fillna(0, inplace=True)
        chemical_pct_redundancy_on_off.to_csv(
            out_path_on_off + os.sep + file_type + 'On_Off_Field_GIS_Step2_' + chemical_name + '.csv')
        print out_path_on_off + os.sep + file_type + 'On_Off_Field_GIS_Step2_' + chemical_name + '.csv'

        conus_df_step1 = chemical_pct_redundancy_on_off[conus_cols_f]
        nl48_df_step1 = chemical_pct_redundancy_on_off[nl48_cols_f]

        conus_df_step1.to_csv(
            out_path_on_off + os.sep + file_type + 'On_Off_Field_CONUS_Step2_' + chemical_name + '.csv')
        nl48_df_step1.to_csv(out_path_on_off + os.sep + file_type + 'On_Off_Field_NL48_Step2_' + chemical_name + '.csv')

        # Census,PCT, Habitat adjusted Tables

        # Census,PCT, Elevation adjusted Tables

        # Census,PCT, Habitat, Elevation adjusted Tables

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)