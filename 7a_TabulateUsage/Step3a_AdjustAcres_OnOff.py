import pandas as pd
import os
import numpy as np
import datetime

# update species area to remove areas of the range deemed 'OFF' for on/off adjustments by applying redundancy factors
# then calculating the area in those use site and subtacting

full_impact = True  # if drift values should include use + drift True if direct use and drift should be separate false
chemical_name = 'Glyphosate' # Methomyl; Carbaryl
file_type = ' Range'  # CriticalHabitat, Range
run_nl48 = True  # NOTE to run NL48 table must include the NL48 columns and not the region specific ones

acres_table = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\R_Acres_Pixels_20200628.csv"
# out location
outpath =r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs'
on_off_excel = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\On_Off_calls_June2020.xlsx"
#  tabulated root path
root_path  = r'D:\Tabulated_HUCAB\Glyphosate\Summarized Tables'
# DO this for no adjust and adjusted tables  #TODO update so it run both un-adjusted and adjusted at the same time

# folder and table names from previous steps
folder_path_no = r'1_No Adjustment'
# table found in the noadjust folder for chem
# ie -concatenate the path root_path +os.sep+chemical_name+os.sep+file_type+os.sep+folder_path_no
table_no =r"R_No Adjustment_GIS_Step2_Glyphosate.csv"

use_lookup = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs" \
             + os.sep + chemical_name + os.sep + "GLY_Uses_lookup_June2020_v2.csv"

col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'country', 'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_YesNo', 'Migratory', 'Migratory_YesNo',
                      'CH_Filename', 'CHange_Filename', 'L48/NL48']

# On/Off Adjustments
cult_crop = True
orchards_crops = True
pastures = True
residential = True
forest = True
row = True
on_off_header = []  # empty list used as a place holder
use_lookup_df = pd.read_csv(use_lookup)


def get_onff_cols (on_off_col_header, use_look_df):
    # get cols associated with each on/off class
    if cult_crop:
        on_off_col_header.append('On/Off_AG')
    if orchards_crops:
        on_off_col_header.append('On/Off_Orchard_Plantation')
    if residential:
        on_off_col_header.append('On/Off_Residential')
    if pastures:
        on_off_col_header.append('On/Off_Pasture')
    if forest:
        on_off_col_header.append('On/Off_Forest')
    if row:
        on_off_col_header.append('On/Off_ROW')

    # Extract uses impacted by on/off

    for col in on_off_col_header:
        if on_off_col_header.index(col) == 0:
            on_off= use_look_df.loc[(use_look_df[col] == 'x')]
            on_off_cols = on_off['Chem Table FinalColHeader'].values.tolist()
        else:
            on_off_group= use_look_df.loc[(use_look_df[col] == 'x')]
            on_off_group_cols = on_off_group['Chem Table FinalColHeader'].values.tolist()
            on_off_cols = on_off_cols + on_off_group_cols
    unq_cols = list(set(on_off_cols))
    return unq_cols, on_off_col_header


def get_ag_nonag_cols(use_look_df, onoff_cols):
    # get col header used for direct for ag and non ag uses
    ag_headers = use_look_df.loc[(use_lookup_df['Included AA Ag'] == 'x')]  # extract ag cols
    ag_cols = ag_headers['Chem Table FinalColHeader'].values.tolist()
    ag_cols =list(set(ag_cols))

    # Try except loop to set a boolean variable to skip non ag adjustment if there are no non ag uses
    try:
        non_ag_headers = use_look_df.loc[(use_lookup_df['Included AA NonAg'] == 'x')]  # extract nonag cols
        nonag_cols = non_ag_headers['Chem Table FinalColHeader'].values.tolist()
        if len(non_ag_headers) == 0:
            skip_non_ag_adjustment = True
        else:
            skip_non_ag_adjustment = False
    except TypeError:
        non_ag_headers = []
        nonag_cols = []
        skip_non_ag_adjustment = True

    ag_onff = []
    nonag_onff =[]

    for col in  ag_cols:
        if col in onoff_cols:
            ag_onff.append(col)
    for col in nonag_cols:
        if col in onoff_cols:
            nonag_onff.append(col)

    # print ag_onff
    # print nonag_onff
    conus_direct_ag = [x + "_0" for x in ag_onff if x.startswith('CONUS')]
    conus_direct_nonag = [x + "_0" for x in nonag_onff if x.startswith('CONUS')]
    nl48_direct_ag = [x + "_0" for x in ag_onff if x.startswith('NL48')]
    nl48_direct_nonag = [x + "_0" for x in nonag_onff if x.startswith('NL48')]

    conus_direct_ag = list(set(conus_direct_ag))
    conus_direct_nonag = list(set(conus_direct_nonag))
    nl48_direct_ag = list(set(nl48_direct_ag))
    nl48_direct_nonag = list(set(nl48_direct_nonag))

    # print conus_direct_ag
    # print conus_direct_nonag
    # print nl48_direct_ag
    # print nl48_direct_nonag

    return conus_direct_ag, conus_direct_nonag, nl48_direct_ag, nl48_direct_nonag, skip_non_ag_adjustment


def get_aa_cols(noadjust_df):
    # get aa cols for direct overlap
    conus_cols_f = [v for v in noadjust_df.columns.values.tolist() if
                    v.startswith('CONUS')]
    aa_col_conus = [x for x in conus_cols_f if 'AA' in x.split('_')[1].split(' ')]
    use_direct_only_conus_ag_aa = [x for x in aa_col_conus if
                                   x.endswith('_0') and 'Ag' in x.split("_")[1].split(" ")]
    use_direct_only_conus_nonag_aa = [x for x in aa_col_conus if
                                  x.endswith('_0') and 'NonAg' in x.split("_")[1].split(" ")]
    use_direct_only_conus_aa = [x for x in aa_col_conus if
                            x.endswith('_0') and 'Ag' not in x.split("_")[1].split(" ") and 'NonAg' not in
                            x.split("_")[1].split(" ")]

    if run_nl48:
        nl48_cols_f = [v for v in noadjust_df.columns.values.tolist() if
                   v.startswith('NL48')]

        aa_col_nl48 = [x for x in nl48_cols_f  if 'AA' in x.split('_')[1].split(' ')]

        use_direct_only_nl48_ag_aa = [x for x in aa_col_nl48 if x.endswith('_0') and 'Ag' in x.split("_")[1].split(" ")]
        use_direct_only_nl48_nonag_aa = [x for x in aa_col_nl48 if x.endswith('_0') and 'NonAg' in x.split("_")[1].split(" ")]
        use_direct_only_nl48_aa = [x for x in aa_col_nl48 if x.endswith('_0') and 'Ag' not in x.split("_")[1].split(" ") and 'NonAg' not in x.split("_")[1].split(" ")]
    else:
        use_direct_only_nl48_aa = ''
        use_direct_only_nl48_ag_aa= ''
        use_direct_only_nl48_nonag_aa =''

    return use_direct_only_conus_aa, use_direct_only_conus_ag_aa, use_direct_only_conus_nonag_aa, \
           use_direct_only_nl48_aa, use_direct_only_nl48_ag_aa, use_direct_only_nl48_nonag_aa


def calc_factors (noadjust_df, conus_ag, conus_nonag,nl48_ag, nl48_nonag, skip_non_ag_adjustment, conus_aa, conus_ag_aa, conus_nonag_aa, nl48_aa, nl48_ag_aa, nl48_nonag_aa):
    # calculate redundancy factors
    noadjust_df['CONUS_Sum_Ag'] = noadjust_df[conus_ag].sum(axis=1)
    print conus_nonag
    print noadjust_df.columns.tolist()
    noadjust_df['CONUS_Sum_NonAg'] = noadjust_df[conus_nonag].sum(axis=1)
    if run_nl48:

        noadjust_df['NL48_Sum_Ag'] = noadjust_df[nl48_ag].sum(axis=1)
        noadjust_df['NL48_Sum_NonAg'] = noadjust_df[nl48_nonag].sum(axis=1)
    if not skip_non_ag_adjustment:
        noadjust_df['CONUS_Sum_Composites'] = noadjust_df[conus_ag_aa + conus_nonag_aa].sum(axis=1)
        if run_nl48:
            noadjust_df['NL48_Sum_Composites'] = noadjust_df[nl48_ag_aa + nl48_nonag_aa].sum(axis=1)
    else:
        noadjust_df['CONUS_Sum_Composites'] = noadjust_df[conus_ag_aa].sum(axis=1)
        if run_nl48:
            noadjust_df['NL48_Sum_Composites'] = noadjust_df[nl48_ag_aa].sum(axis=1)
    noadjust_df['CONUS_Composite_Factor'] = noadjust_df['CONUS_Sum_Composites'].div((noadjust_df[conus_aa[0]]).where(noadjust_df[conus_aa[0]] != 0, np.nan), axis=0)
    noadjust_df['CONUS_Ag_Ag_Factor'] = noadjust_df['CONUS_Sum_Ag'].div(
        (noadjust_df[conus_ag_aa[0]]).where(noadjust_df[conus_ag_aa[0]] != 0, np.nan), axis=0)
    if run_nl48:
        noadjust_df['NL48_Composite_Factor'] = noadjust_df['NL48_Sum_Composites'].div((noadjust_df[nl48_aa[0]]).where(noadjust_df[nl48_aa[0]] != 0, np.nan), axis=0)
        noadjust_df['NL48_Ag_Ag_Factor'] = noadjust_df['NL48_Sum_Ag'].div((noadjust_df[nl48_ag_aa[0]]).where(noadjust_df[nl48_ag_aa[0]] != 0, np.nan), axis=0)
    # calculated the Non Ag factors for CONUS and NL48 = where clause removed the rows where the  Ag composite is equal to 0
    if not skip_non_ag_adjustment:
        noadjust_df['CONUS_NonAg_NonAg_Factor'] = noadjust_df['CONUS_Sum_NonAg'].div(
            (noadjust_df[conus_nonag_aa[0]]).where(noadjust_df[conus_nonag_aa[0]] != 0, np.nan), axis=0)
        if run_nl48:
            noadjust_df['NL48_NonAg_NonAg_Factor'] = noadjust_df['NL48_Sum_NonAg'].div((noadjust_df[nl48_nonag_aa[0]]).where(
                noadjust_df[nl48_nonag_aa[0]] != 0, np.nan), axis=0)
    return noadjust_df


def apply_factor(factor_df, conus_ag, conus_nonag, nl48_ag, nl48_nonag, skip_nonag, conus_ag_aa, conus_nonag_aa, nl48_ag_aa, nl48_nonag_aa):
    # applies factors to overlap
    factor_df.ix[:, 'EntityID'] = factor_df.ix[:, 'EntityID'].map(lambda x: str(x).split('.')[0]).astype(str)
    factor_df['EntityID'] = factor_df['EntityID'].map(lambda x: str(x).split('.')[0]).astype(str)
    # # Applies the factor adjustments to the pct adjusted overlap

    # To Ag uses layer
    print conus_ag
    print
    factor_df.ix[:, conus_ag] = factor_df.ix[:, conus_ag].div(factor_df['CONUS_Composite_Factor'].where(factor_df['CONUS_Composite_Factor'] != 0, np.nan), axis=0)
    # factor_df.to_csv(r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ED_results\_CurrentSupportingTables\OnOff\check2.csv")
    if run_nl48:
        factor_df.ix[:, nl48_ag] = factor_df.ix[:, nl48_ag].div(factor_df['NL48_Composite_Factor'].where(factor_df['NL48_Composite_Factor'] != 0, np.nan), axis=0)
    # To composites
    factor_df.ix[:, conus_ag_aa] = factor_df.ix[:, conus_ag_aa].div(factor_df['CONUS_Composite_Factor'].where(factor_df['CONUS_Composite_Factor'] != 0, np.nan), axis=0)
    if run_nl48:
        factor_df.ix[:, nl48_ag_aa] = factor_df.ix[:, nl48_ag_aa].div(factor_df['NL48_Composite_Factor'].where(factor_df['NL48_Composite_Factor'] != 0, np.nan), axis=0)
    # factor_df.to_csv(r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ED_results\_CurrentSupportingTables\OnOff\check.csv")
    # print use_direct_only_conus_ag, use_direct_only_nl48_ag_aa #- columns adjusted
    # To Non Ag use layers if we have non-ag uses
    if not skip_nonag:
        factor_df.ix[:, conus_nonag] = factor_df.ix[:, conus_nonag].div(factor_df['CONUS_Composite_Factor'].where(factor_df['CONUS_Composite_Factor'] != 0, np.nan), axis=0)
        if run_nl48:
            factor_df.ix[:, nl48_nonag] = factor_df.ix[:, nl48_nonag].div(factor_df['NL48_Composite_Factor'].where(factor_df['NL48_Composite_Factor'] != 0, np.nan), axis=0)
        # To composites
        factor_df.ix[:, conus_nonag_aa] = factor_df.ix[:, conus_nonag_aa].div(factor_df['CONUS_Composite_Factor'].where(factor_df['CONUS_Composite_Factor'] != 0, np.nan), axis=0)
        if run_nl48:
            factor_df.ix[:, nl48_nonag_aa] = factor_df.ix[:, nl48_nonag_aa].div(factor_df['NL48_Composite_Factor'].where(factor_df['NL48_Composite_Factor'] != 0, np.nan), axis=0)

    # # APPLIES AG Factor to Ag use layers

    factor_df.ix[:, conus_ag] = factor_df.ix[:, conus_ag].div((factor_df['CONUS_Ag_Ag_Factor']).where(factor_df['CONUS_Ag_Ag_Factor'] != 0, np.nan), axis=0)
    if run_nl48:
        factor_df.ix[:, nl48_ag] = factor_df.ix[:, nl48_ag].div(factor_df['NL48_Ag_Ag_Factor'].where(factor_df['NL48_Ag_Ag_Factor'] != 0, np.nan), axis=0)

    # # Applies Non-Ag factor to non-ag use layer if there are non-ag use layers
    # print use_direct_only_conus_ag, use_direct_only_nl48_ag  #- columns adjusted
    if not skip_nonag:
        factor_df.ix[:, conus_nonag] = factor_df.ix[:, conus_nonag].div(factor_df['CONUS_NonAg_NonAg_Factor'].where(factor_df['CONUS_NonAg_NonAg_Factor'] != 0, np.nan), axis=0)
        if run_nl48:
            factor_df.ix[:, nl48_nonag] = factor_df.ix[:, nl48_nonag].div(factor_df['NL48_NonAg_NonAg_Factor'].where(factor_df['NL48_NonAg_NonAg_Factor'] != 0, np.nan), axis=0)
    out_col = [x for x in factor_df.columns.values.tolist() if x in col_include_output or x.endswith('_0')
               or x.endswith('_Factor') or x.endswith('_Sum_NonAg') or x.endswith('_Sum_Ag') or  x.endswith('_Sum_Composites')]
    out_df = factor_df[out_col]
    return out_df


def convert_percent_to_decimal_calcares(df, acres):
    # converts overlap to decimals
    percent_cols = [x for x in df.columns.values.tolist() if x.endswith('_0')]
    df.ix[:, percent_cols] = df.ix[:, percent_cols].div((100))
    l48_col = [x for x in df.columns.values.tolist() if x.startswith('CONUS')]
    nl48_col = [x for x in df.columns.values.tolist() if x.startswith('NL48')]

    conus_acres = acres[["EntityID","Acres_CONUS"]]
    nl48_acres = acres[["EntityID","TotalAcresNL48"]]
    df =pd.merge(df, conus_acres,on = 'EntityID', how = 'left')
    df =pd.merge(df, nl48_acres,on = 'EntityID', how = 'left')
    df.ix[:,l48_col]= df.ix[:,l48_col].multiply(df["Acres_CONUS"].where(df["Acres_CONUS"]!= 0, np.nan),axis=0)
    df.ix[:,nl48_col]= df.ix[:,nl48_col].multiply(df["TotalAcresNL48"].where(df["TotalAcresNL48"]!= 0, np.nan),axis=0)

    return df


def combinations_of_onoff (onoff_header, on_off_df, overlap_df, use_df, acres):
    # SUM the area found in a use site that is OFF for a speices then removes that sum from the area for the species
    # locations
    # for species only off one habitat classes
    for col in onoff_header:
        # Get columns found in chem table from use lookup
        on_off_use = use_df.loc[(use_df[col] == 'x')]
        on_off_use_cols = list(set(on_off_use['Chem Table FinalColHeader'].values.tolist()))

        # Sum cols across uses
        overlap_use_cols = [v + "_0" for v in on_off_use_cols]
        conus_overlap_cols = [v for v in overlap_use_cols if v.startswith('CONUS')]
        nl48_overlap_cols = [v for v in overlap_use_cols if v.startswith('NL48')]
        overlap_df[conus_overlap_cols].fillna(0, inplace=True)
        overlap_df[nl48_overlap_cols].fillna(0, inplace=True)
        overlap_df.ix[:,overlap_use_cols] = overlap_df.ix[:,overlap_use_cols].apply(pd.to_numeric, errors='coerce')
        overlap_df['EntityID'] = overlap_df['EntityID'].map(lambda x:str(x).split('.')[0]).astype(str)
        overlap_df.ix[:,'CONUS_Sum_'+col.replace("/","_")] = overlap_df.ix[:,conus_overlap_cols].sum(axis=1, skipna = True, numeric_only = True)
        overlap_df.ix[:,'NL48_Sum_'+col.replace("/","_")] = overlap_df.ix[:,nl48_overlap_cols].sum(axis=1,skipna = True, numeric_only = True)
        # get the list of species that should be 'OFF site' for group
        on_off_working = on_off_df.copy()
        onspecies_cols = [x for x in onoff_header if x != col]
        for i in  onspecies_cols:
            on_off_working  = on_off_working.loc[(on_off_working [i] != 'OFF')]
        on_off_working=on_off_working.loc[(on_off_working [col] == 'OFF')]

        #adjust acres for species that are off sites
        species_off= on_off_working['EntityID'].values.tolist()
        species_to_adjust = overlap_df [['EntityID', 'CONUS_Sum_'+col.replace("/","_"),'NL48_Sum_'+col.replace("/","_")]]
        species_to_adjust = species_to_adjust.loc[species_to_adjust['EntityID'].isin(species_off)]
        species_to_adjust.ix[:,'EntityID'] = species_to_adjust.ix[:,'EntityID'].map(lambda x: x.split('.')[0]).astype(str)

        species_to_adjust=pd.merge(acres,species_to_adjust,on = 'EntityID', how = 'left')
        # print species_to_adjust.head()

        species_to_adjust["CONUS_Adjusted"] = species_to_adjust["Acres_CONUS"].sub(species_to_adjust['CONUS_Sum_'+col.replace("/","_")].where(species_to_adjust['CONUS_Sum_'+col.replace("/","_")]!= 0, np.nan), fill_value=0)
        species_to_adjust["NL48_Adjusted"] = species_to_adjust["TotalAcresNL48"].sub(species_to_adjust['NL48_Sum_'+col.replace("/","_")].where(species_to_adjust['NL48_Sum_'+col.replace("/","_")]!= 0, np.nan), fill_value=0)

        species_to_adjust.ix[:,"Acres_CONUS"] = species_to_adjust.ix[:,"CONUS_Adjusted"]
        species_to_adjust.ix[:,"TotalAcresNL48"] = species_to_adjust.ix[:,"NL48_Adjusted"]
        acres = species_to_adjust.copy()

    # species off multiple habitat class
    all_completed_combo = []

    for col in onoff_header:
        off_colums= [col]
        onoff_header_current = onoff_header[:]
        onoff_header_current.remove(col)
        index_pos = onoff_header.index(col)
        if index_pos > 0:
            onoff_header_current.remove(onoff_header[index_pos-1])
        # print onoff_header_current
        for i in onoff_header_current:
            off_colums.append(i)
            sorted_off_cols = sorted(off_colums)
            if sorted_off_cols not in all_completed_combo:
                all_completed_combo.append(sorted_off_cols)
                # print sorted_off_cols
                on_off_working = on_off_df.copy()
                out_flag =''
                for x in onoff_header:
                    if x not in sorted_off_cols:
                        on_off_working  = on_off_working.loc[(on_off_working [x] != 'OFF')]
                    elif x in sorted_off_cols:
                        if len(out_flag) == 0:
                            out_flag= x.replace("/","_")
                        else:
                            out_flag = out_flag + "_"+  x.replace("/","_")
                        on_off_working  = on_off_working.loc[(on_off_working [x] == 'OFF')]

                alloff_on_off_use_cols = []
                for y in sorted_off_cols:
                    on_off_use = use_df.loc[(use_df[y] == 'x')]
                    on_off_use_cols = list(set(on_off_use['Chem Table FinalColHeader'].values.tolist()))
                    alloff_on_off_use_cols = alloff_on_off_use_cols + on_off_use_cols
                # Sum cols across uses
                all_off_overlap_use_cols = [v + "_0" for v in alloff_on_off_use_cols]
                all_off_conus_overlap_cols = [v for v in all_off_overlap_use_cols if v.startswith('CONUS')]
                all_off_nl48_overlap_cols = [v for v in all_off_overlap_use_cols if v.startswith('NL48')]
                overlap_df[all_off_conus_overlap_cols].fillna(0, inplace=True)
                overlap_df[all_off_nl48_overlap_cols].fillna(0, inplace=True)
                overlap_df.ix[:,all_off_overlap_use_cols] = overlap_df.ix[:,all_off_overlap_use_cols].apply(pd.to_numeric, errors='coerce')

                overlap_df.ix[:,'CONUS_Sum_'+out_flag] = overlap_df.ix[:,all_off_conus_overlap_cols].sum(axis=1, skipna = True, numeric_only = True)
                overlap_df.ix[:,'NL48_Sum_'+out_flag] = overlap_df.ix[:,all_off_nl48_overlap_cols].sum(axis=1,skipna = True, numeric_only = True)

                #adjust acres for species that are off sites
                species_off= on_off_working['EntityID'].values.tolist()
                print species_off, out_flag, all_off_conus_overlap_cols
                species_to_adjust = overlap_df [['EntityID', 'CONUS_Sum_'+out_flag,'NL48_Sum_'+out_flag]]
                # print species_to_adjust.head(n=20)
                species_to_adjust = species_to_adjust.loc[species_to_adjust['EntityID'].isin(species_off)]
                species_to_adjust.ix[:,'EntityID'] = species_to_adjust.ix[:,'EntityID'].map(lambda x: x.split('.')[0]).astype(str)

                species_to_adjust=pd.merge(acres,species_to_adjust,on = 'EntityID', how = 'left')
                # print species_to_adjust.head()

                species_to_adjust["CONUS_Adjusted"] = species_to_adjust["Acres_CONUS"].sub(species_to_adjust['CONUS_Sum_'+out_flag].where(species_to_adjust['CONUS_Sum_'+out_flag]!= 0, np.nan), fill_value=0)
                species_to_adjust["NL48_Adjusted"] = species_to_adjust["TotalAcresNL48"].sub(species_to_adjust['NL48_Sum_'+out_flag].where(species_to_adjust['NL48_Sum_'+out_flag]!= 0, np.nan), fill_value=0)

                species_to_adjust.ix[:,"Acres_CONUS"] = species_to_adjust.ix[:,"CONUS_Adjusted"]
                species_to_adjust.ix[:,"TotalAcresNL48"] = species_to_adjust.ix[:,"NL48_Adjusted"]
                acres = species_to_adjust.copy()

    print all_completed_combo
    # acres.to_csv(outpath + os.sep+ chemical_name +"_acres_onoff_adjustments.csv", encoding = 'utf-8')
    return acres




def main(on_off_col_header, use_look_df):

    today = datetime.datetime.today()
    date = today.strftime('%Y%m%d')
    start_time = datetime.datetime.now()
    print "Start Time: " + start_time.ctime()

    # get the columns impacted by the on/off cols
    onoff_col, onoff_header = get_onff_cols(on_off_col_header, use_look_df)
    # print onoff_col

    # get list of ag and nonag cols
    conus_ag, conus_nonag, nl48_ag, nl48_nonag, skip_nonag = get_ag_nonag_cols(use_look_df, onoff_col)
    chemical_noadjustment = pd.read_csv(root_path  +os.sep + folder_path_no + os.sep +table_no)
    # get list of aa columns
    conus_aa, conus_ag_aa, conus_nonag_aa, nl48_aa, nl48_ag_aa, nl48_nonag_aa = get_aa_cols(chemical_noadjustment)

    # Calc factors and apply, adjust overlap to percent and calc acres
    working_df = calc_factors (chemical_noadjustment,conus_ag, conus_nonag, nl48_ag, nl48_nonag, skip_nonag,conus_aa, conus_ag_aa, conus_nonag_aa, nl48_aa, nl48_ag_aa, nl48_nonag_aa)
    working_df = apply_factor(working_df, conus_ag, conus_nonag, nl48_ag, nl48_nonag, skip_nonag, conus_ag_aa, conus_nonag_aa, nl48_ag_aa, nl48_nonag_aa)
    # read in acres tables
    acres_df = pd.read_csv(acres_table)
    acres_df['EntityID'] = acres_df['EntityID'].map(lambda x: str(x).split('.')[0]).astype(str)
    # convert percents back to decimals
    working_df = convert_percent_to_decimal_calcares(working_df, acres_df)
    # Determined on/off combination that are possible
    on_off_df = pd.read_excel(on_off_excel)  # reads in on/off table
    on_off_df['EntityID'] = on_off_df['EntityID'].map(lambda x: str(x).split('.')[0]).astype(str)
    # get all the types of combinations of uses where a  species is off site an and removes area from species location
    out_acres = combinations_of_onoff (onoff_header, on_off_df,working_df,use_look_df, acres_df)
    # saves updated acres table
    print outpath + os.sep + chemical_name + "_"+ file_type+'_OnOffAdjust_'+ date + '.csv'
    out_acres.to_csv(outpath + os.sep + chemical_name + "_"+ file_type+'_OnOffAdjust_'+ date + '.csv')

    end = datetime.datetime.now()
    print "End Time: " + end.ctime()
    elapsed = end - start_time
    print "Elapsed  Time: " + str(elapsed)



main (on_off_header, use_lookup_df)

