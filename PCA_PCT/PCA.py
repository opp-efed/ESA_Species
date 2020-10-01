import datetime
import os

import numpy as np
import pandas as pd

# Authors J.Connolly
# Internal deliberative, do not cite or distribute


chemical_name = 'Chlorpyrifos'  # Methomyl Carbaryl
# column names and order for the requested output
final_col=['Source', 'BasinID', 'acres_watershed','HUC2', 'Use Name', 'PWC_Run', 'PCA', 'max_upper_frac',
           'max_uniform_frac', 'max_lower_frac',	'avg_upper_frac', 'avg_uniform_frac',
           'avg_lower_frac', 'min_upper_frac',	'min_uniform_frac', 'min_lower_frac']
st_cnty = 'County'  # if running on cnty change to County, State if using state

suffixes = ['noadjust']  # 'noadjust', 'adjEle','adjEleHab','adjHab' result suffixes

state_fp_lookup = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Sinnathamby, Sumathy - Drinking water projects\Scripts\Inputs\STATEFP_lookup.csv"
# define output location
out_location = r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Drinking water projects\Scripts\Outputs"

# NOTE PCTS FOR AA AND COMPOSITES MOST BE 1 FOR THE FACTOR ADJUSTMENTS
# PCT, pre/ab, and use look up table must use same use names - this is the Usage lookup values on the use lookup table

# Folder where the PCT tables are found: PCT tables must be in the format [chemical name]_[pct group]_[date].csv
pct_table_directory = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Sinnathamby, Sumathy - Drinking water projects\Scripts\Inputs\PCT' + os.sep + chemical_name
# Folder where the PCA tables are found: PCA tables must be in the format [chemical name]_[pct group]_[date].csv
pca_table_directory = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Sinnathamby, Sumathy - Drinking water projects\Scripts\Inputs\PCA"

# load pca table
pca_table = pd.read_csv('C:\Users\JConno02\Environmental Protection Agency (EPA)\Sinnathamby, Sumathy - Drinking water projects\Scripts\Inputs\PCA\County\PCA_20190627_Final.csv')
# pca_table= pd.read_csv(pca_table_loc + '/PCA_20190627_Final.csv')

# Load in watershed broken down by county
cws_cnty_table = pd.read_csv(pca_table_directory + '/CWS_Cnties.csv')
cws_cnty_table['GEOID'] = cws_cnty_table['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
count_series = cws_cnty_table.groupby(['BasinID']).size()
# get the number of counties in each watershed
num_cnties_in_csw = count_series.to_frame(name = 'cnty count').reset_index()
# join the count of cnty to to the PCA tables
merge_cws_crops = pd.merge(pca_table,num_cnties_in_csw, on ='BasinID', how = 'right')
# get the acres/area columns to adjusted by the number of counties
acres_cols = [v for v in merge_cws_crops.columns.values.tolist() if v.endswith('acres') or v.endswith('area')]
# divide each crop in column by the number of counties in the watershed
# TODO update this simple divide and adjust the crop area weighted by the NLCD
merge_cws_crops.ix[:, acres_cols] = merge_cws_crops.ix[:, acres_cols].div(merge_cws_crops['cnty count'].where(merge_cws_crops['cnty count'] != 0, np.nan), axis=0)
# merge_cws_crops is pca+no of counties watershed spread table
# merge_cws_crops.to_csv(r'E:\PCA\Drinking water projects\Scripts\Outputs\merge_cws_crops.csv')

# load census - used as the political boundary overlap
census_ag = pd.read_csv(pca_table_directory + '/censusag_2007.csv')
census_ag = census_ag.rename(columns={'STCOFIPS': 'GEOID'})
census_ag['GEOID'] = census_ag['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)

# merge_cws_crops.to_csv(r'E:\PCA\Drinking water projects\Scripts\Outputs\adjust_crop_area.csv')
#TODO use this load you had of the NLCD to weight the crop adjustment rather than dividing, I think you'll want to
# join to this table rather than the census of ag but maybe not
# # load nlcd table
# nlcd_ag = pd.read_csv(pca_table_directory + '/nlcd_2006_cnty.csv')
# nlcd_ag['GEOID'] = nlcd_ag['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
# merged_cen_nlcd = pd.merge(census_ag, nlcd_ag, on=['GEOID'])
# merged_cws_cnty_nlcd = pd.merge(nlcd_ag, cws_cnty_table, on='GEOID', how = 'right')

state_fp = pd.read_csv(state_fp_lookup)
state_fp['STATEFP'] = state_fp['STATEFP'].map(lambda x: str(x) if len(str(x)) == 2 else '0' + str(x)).astype(str)
state_fp['GEOID'] = state_fp['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)

use_lookup = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Sinnathamby, Sumathy - Drinking water projects\Scripts\Inputs" + os.sep + chemical_name + "_Uses_lookup_PCAPCT.csv"

# chemical masks

presence_absence_cnty = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Sinnathamby, Sumathy - Drinking water projects\Scripts\Inputs\chlorpyrifos_cnty_mask_pre_abs_20191106.csv"


# watershed overlap
cws = merge_cws_crops
# overlap for the political boundary alone - not species info
in_locations_states = census_ag


def state_pct(row, msq_state_col, fpct_col):
    adjust_value = row[msq_state_col] * row[fpct_col]

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


def uniform_spe(row, fpct, sp_total_col):
    return row[sp_total_col] * row[fpct]


def mod_area(row, total, wa_area, ):
    total_adjust = row[total]
    adj_cws = row[wa_area]
    if total_adjust > 0:
        return total_adjust*adj_cws
    else:
        return adj_cws*0


def update_cols_tofinal_terms(df):
    col_dictionary ={'BasinID' : 'BasinID',
                     'acres_cws': 'acres_watershed',
                     'huc2':'HUC2'}
    cols = df.columns.values.tolist()
    for c in cols:
        if c not in col_dictionary.keys():
            if c.endswith('_pca'):
                col_dictionary[c]='PCA'
            else:
                split_col = c.split("_")
                use = split_col[0]
                pct= split_col[1]
                distribution = split_col[2]
                col_dictionary['Use']= use
                col_dictionary[c] = pct +"_"+distribution+"_frac"
    return col_dictionary


def pca_pct_min(row, total, cwsarea):
    min_total = row[total]
    cws_area = row[cwsarea]

    if min_total <1:
        return 0
    else:
        return min_total/cws_area


def uniform_adj_pca(row, crop_pca, pct_pca_factor ):
    crop_pca = row[crop_pca]
    pca_factor = row[pct_pca_factor]
    if pca_factor < crop_pca:
        return pca_factor
    else:
        return crop_pca


def upper_adj_pca(row, crop_pca, pct_pca_factor ):
    crop_pca = row[crop_pca]
    pca_factor = row[pct_pca_factor]
    if pca_factor < crop_pca:
        return pca_factor
    else:
        return crop_pca


def lower_adj_pca(row, crop_pca, pct_pca_factor):
    crop_pca = row[crop_pca]
    pca_factor = row[pct_pca_factor ]
    if pca_factor < crop_pca:
        return pca_factor
    else:
        return crop_pca


# ## Start Elapse Clock
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Get date
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
# LOADS INPUT TABLES
# load presences absences table for at least 1 registered crop occurs by county
presence_absence_df= pd.read_csv(presence_absence_cnty, low_memory=False)
# set data type an for the GEOID (county ID or FIPS) to string and make sure all values are 5 digits; leading 0's can
# be dropped from text tables
presence_absence_df['GEOID'] = presence_absence_df['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
# load the state look up table to extract state name from the STATEFP (state ID)
# set the STATEFP to a string and make sure all values are 2 digits; leading 0's can be dropped
# set data type an for the GEOID (county ID or FIPS) to string and make sure all values are 5 digits; leading 0's can be
# dropped
state_fp = pd.read_csv(state_fp_lookup)
state_fp['STATEFP'] = state_fp['STATEFP'].map(lambda x: str(x) if len(str(x)) == 2 else '0' + str(x)).astype(str)
state_fp['GEOID'] = state_fp['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
# converts state name to all upper cas to match the PCT table
state_fp['STATE_Upper'] = state_fp['STATE'].map(lambda x: str(x).upper()).astype(str)
# Loads the use look-up tables
use_lookup_df = pd.read_csv(use_lookup) # reads use lookup tables
# get a list of PCT tables found in the PCT directory
list_pct_table = [v for v in os.listdir(pct_table_directory) if v.endswith('.csv')]
# pcts to include; HARD CODE NOTE: the terms in this list must be found in the PCT table file name; PCT table file  name
# must  be in the format [chemical name]_[pct group]_[date].csv
print list_pct_table  # print list of PCT table to be selected from


# Empty dataframe to store results for all uses as they complete by watershed; final output does not include state
# state values; the state information is used to calculated the treated acres
final_watershed_crop_df = pd.DataFrame()
# LOOPS OVER USES BASED ON THE LOOK-UP TABLE
for use in use_lookup_df['Usage lookup'].values.tolist():
    use_df = pd.DataFrame()  # Empty dataframe to store results for a single use as they complete
    # LOADS USE AND USAGE LOOKUP INFORMATION FOR MERGING
    usage_lookup_df = use_lookup_df.ix[:, ['FullName', 'Usage lookup', 'Included AA']] # filters tables
    # generated the raster file name using the folder name - raster file name = folder name +"_euc
    usage_lookup_df['Filename'] = usage_lookup_df['FullName'].map(lambda x: str(x) + "_euc").astype(str) # set filename
    # look-up used to identify the uses for the chemical to just those uses in the current chemical
    usage_lookup_df = usage_lookup_df.loc[usage_lookup_df['Included AA'] == 'x']
    # print use_lookup_df['Usage lookup'].values.tolist()  # print the uses to be included - visual check
    usage_col_header = usage_lookup_df.loc[usage_lookup_df['Usage lookup'] == use, 'Usage lookup'].iloc[0]
    use_lookup_df_value = usage_lookup_df.loc[usage_lookup_df['Usage lookup'] == use, 'Usage lookup'].iloc[0]

    # LOOPS OVER PCT TYPES
    for group in ['max','avg','min']:
        # assumes identifier is in this position
        print "Working on use {0} and pct {1}".format(use, group)
        pct_table = [v for v in list_pct_table if v.split("_")[1] == group][0]  # selects table
        # LOADS PCT TABLES AND APPLIES FORMATTING ADJUSTMENTS
        pct_df = pd.read_csv(pct_table_directory + os.sep + pct_table) # read pct table
        # print pct_df
        # look up used as key to combine information from different tables term found in Usage lookup be the same terms
        # used to describe the use in the pct table
        # FullName- must be the name of use site; Included AA indicates if the use is part of the
        # chemical
        # loop over uses in the lookup table
        t_pct = pct_df.T  # transforms PCT table so uses are the headers
        t_pct = t_pct.reset_index()  # reset index
        update_cols = t_pct.iloc[0].values.tolist()  # col headers are in row 0- extract values to be used for header
        update_cols[0] = 'STATE_Upper'  # sets col name for index position 0; State name needs to  be in all upper case
        t_pct.columns = update_cols  # updates col headers
        t_pct = t_pct.reindex(t_pct.index.drop(0))  # drops row 0; previous the col headers
        # merges the pct table with the state look upt table used the state name; state name must be in all upper case
        t_pct = pd.merge(t_pct, state_fp,  on= 'STATE_Upper', how='left')  # merges to pol bound lookup

        # LOADS COUNTY/STATE OVERLAP BASED ON THE CENSUS OF AG ACRES BY COUNTY
        #  To simplify this we know we want cnties
        pol_id = 'GEOID'
        pct_header = 'GEOID'
        # loading county file so we can apply the pre/ab mask for non registered crops; then  sum to state
        cnty_df = census_ag
        # if not os.path.exists(out_path+os.sep+csv_out):
        cnty_df['GEOID'] = cnty_df['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
        cnty_df['STATEFP'] = cnty_df['GEOID'].str[:2]  # extracts StatteFP from GeoID
        #cnty_df.to_csv(r'E:\PCA\Drinking water projects\Scripts\Outputs\cnty_df.csv')
        # filter crop pca
        crop_area = [col for col in cnty_df.columns if use_lookup_df_value in col]  # use_lookup_df_value
        # print crop_area
        to_select = ['STATEFP', 'GEOID']
        to_select.extend(crop_area)
        filtered_cnty = cnty_df.ix[:, to_select]
        # APPLIES THE PRESENCE ABSENCE INFORMATION TO THE COUNTY/STATE OVERLAP
        filtered_cnty = pd.merge(presence_absence_df, filtered_cnty, on=['GEOID'], suffixes=['', '_area'])
        # print filtered_cnty.columns.values.tolist()
        filtered_cnty.loc[:, [use_lookup_df_value + '_area']] = filtered_cnty.loc[:,[use_lookup_df_value + '_area']].multiply(filtered_cnty[usage_col_header], axis=0)
        filtered_cnty['STATEFP'] = filtered_cnty['GEOID'].str[:2]  # extracts StateFP from GeoID
        # sums crop to the state based on the cnty acres from the census
        filtered_state = filtered_cnty.groupby(['STATEFP'])[[use_lookup_df_value + '_area']].sum().reset_index()
        #filtered_state.to_csv(r'E:\PCA\Drinking water projects\Scripts\Outputs\groupby_state.csv')

        # LOADS THE WATERSHED OVERLAP FOR THE STATE
        watershed_df = cws
        crop_col = [v for v in watershed_df.columns.values.tolist() if v.startswith(use) and v.endswith('acres')]
        watershed_crop_df = watershed_df[['BasinID', crop_col[0]]]
        watershed_crop_df_cnties = pd.merge(watershed_crop_df, cws_cnty_table, on='BasinID', how='right')
        watershed_crop_df_cnties['GEOID'] = watershed_crop_df_cnties['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
        watershed_crop_df_cnties['STATEFP'] = watershed_crop_df_cnties['GEOID'].str[:2]
        #watershed_crop_df_cnties.to_csv(r'E:\PCA\Drinking water projects\Scripts\Outputs\watershed_crop_df_cnties.csv')
        # joins watershed overlap with pre/ab table
        # APPLIES THE PRESENCE ABSENCE INFORMATION TO THE COUNTY/STATE OVERLAP
        cnty_w_p_a = pd.merge(watershed_crop_df_cnties, presence_absence_df, on=['GEOID'], how='left')
        # cnty_w_p_a.to_csv(r'E:\PCA\Drinking water projects\Scripts\Outputs\cnty_w_p_a.csv')
        cnty_w_p_a.drop_duplicates(inplace=True)
        cnty_w_p_a_use = cnty_w_p_a[watershed_crop_df_cnties.columns.values.tolist() + [usage_col_header]].copy()
        # if a cnty is missing from pre.ab table fills the blanks ith 1 so assumes it is present
        cnty_w_p_a_use[usage_col_header].fillna(1, inplace=True)
        # multiples the presences/ab by the species overlap making counties with no registered crops 0
        # TODO we will need to confirm that all of the columns follows the use _acres format
        cnty_w_p_a_use.loc[:, use_lookup_df_value + '_acres'] = cnty_w_p_a_use.loc[:,use_lookup_df_value + '_acres'].multiply(cnty_w_p_a_use[usage_col_header], axis=0)

        col_to_sum_state = ['BasinID','STATEFP'] + [use_lookup_df_value + '_acres']
        sum_df_state = cnty_w_p_a_use[col_to_sum_state].copy()
        watershed_state_df = sum_df_state.groupby(['BasinID', 'STATEFP'])[use_lookup_df_value + '_acres'].sum().reset_index()

        # GENERATES TABLES USED FOR USAGE CALCULATIONS
        # extract the columns needed from each table for the current crop/pct combo these columns are used for the
        # PCT/Acre treated calculations
        filter_col = ['BasinID', use_lookup_df_value, 'acres_cws','huc2']
        filtered_pca = cws.ix[:, filter_col].copy()
        filtered_pca.columns = ['BasinID', use_lookup_df_value + '_pca', 'acres_cws','huc2']
        filtered_pct = t_pct.ix[:, ['STATEFP', use_lookup_df_value]].copy()
        filtered_pct.columns = ['STATEFP', 'PCT_' + use_lookup_df_value]
        filtered_pct.drop_duplicates(inplace=True)
        watershed_state_df_pca = pd.merge(watershed_state_df, filtered_pca, on="BasinID", how='left')
        watershed_state_df_pca_pct = pd.merge(watershed_state_df_pca, filtered_pct, on="STATEFP", how='left')
        watershed_state_df_pca_pct.drop_duplicates(inplace=True)
        watershed_state_df_pca_pct_statecrop = pd.merge(watershed_state_df_pca_pct, filtered_state, on="STATEFP",
                                                        how='left')
        watershed_state_df_pca_pct_statecrop.drop_duplicates(inplace=True)
        total_cw_crop_area = watershed_state_df_pca_pct_statecrop.groupby(['BasinID'])[
            use_lookup_df_value + '_acres'].sum().reset_index()
        #total_cw_crop_area.to_csv(r'E:\PCA\Drinking water projects\Scripts\Outputs\total_cw_crop_area.csv')
        #print total_cw_crop_area.dtypes
        watershed_state_df_pca_pct_statecrop_in = pd.merge(watershed_state_df_pca_pct_statecrop, total_cw_crop_area, on="BasinID",suffixes=['', '_total'])
        # print watershed_state_df_pca_pct_statecrop_in
        watershed_state_df_pca_pct_statecrop_in.columns.values.tolist()
        #print watershed_state_df_pca_pct_statecrop_in[use_lookup_df_value + '_area_total']
        #watershed_state_df_pca_pct_statecrop_in.to_csv(r'E:\PCA\Drinking water projects\Scripts\Outputs\watershed_state_df_pca_pct_statecrop_in.csv')
        watershed_state_df_pca_pct_statecrop_in['% of Total crop area'] = watershed_state_df_pca_pct_statecrop_in[use_lookup_df_value + '_acres'] / watershed_state_df_pca_pct_statecrop_in[use_lookup_df_value + '_acres_total']
        #watershed_state_df_pca_pct_statecrop_in['mod_acres_cws'] = watershed_state_df_pca_pct_statecrop_in['% of Total crop area'] * watershed_state_df_pca_pct_statecrop_in['acres_cws']
        # PCT Adjustments by  bounding scenario
        #watershed_state_df_pca_pct_statecrop_in.to_csv(r'E:\PCA\Drinking water projects\Scripts\Outputs\watershed_state_df_pca_pct_statecrop_in.csv')
        # TODO add in the PCA/PCT ratio
        # Applies the 3 different treated acres distributions
        watershed_state_df_pca_pct_statecrop_in['mod_acres_cws'] = watershed_state_df_pca_pct_statecrop_in.apply(lambda row: mod_area(row,'% of Total crop area' ,'acres_cws'), axis=1)
        watershed_state_df_pca_pct_statecrop_in[use_lookup_df_value +"_"+group +'_State Area adjusted by PCT'] = watershed_state_df_pca_pct_statecrop_in.apply(lambda row: state_pct(row, use_lookup_df_value+'_area', 'PCT_' + use_lookup_df_value), axis=1)
        watershed_state_df_pca_pct_statecrop_in[use_lookup_df_value +"_"+group +'_Total outside watershed'] = watershed_state_df_pca_pct_statecrop_in.apply(lambda row: outside_range(row, use_lookup_df_value+'_area', use_lookup_df_value+'_acres'), axis=1)
        watershed_state_df_pca_pct_statecrop_in[use_lookup_df_value +"_"+group +'_lower_in watershed'] = watershed_state_df_pca_pct_statecrop_in.apply(lambda row: min_range(row, use_lookup_df_value+"_" +group +'_Total outside watershed', use_lookup_df_value +"_" +group +'_State Area adjusted by PCT'), axis=1)
        watershed_state_df_pca_pct_statecrop_in[use_lookup_df_value +"_"+group +'_upper_in watershed'] = watershed_state_df_pca_pct_statecrop_in.apply(lambda row: max_range(row, use_lookup_df_value+'_acres', use_lookup_df_value +"_"+group +'_State Area adjusted by PCT'), axis=1)
        watershed_state_df_pca_pct_statecrop_in[use_lookup_df_value +"_"+group +'_uniform_in watershed'] = watershed_state_df_pca_pct_statecrop_in.apply(lambda row: uniform_spe(row, 'PCT_' + use_lookup_df_value, use_lookup_df_value+'_acres'), axis=1)

        # TODO SUM TO WATERSHED FOR THE UPPER, LOWER AND UNIFORM DISTRIBUTIONS CALC PCA/PCT FACTOR (TREATED AREA/AREA OF WATERSHED) WITH YOUR FUCTION AND COMPART TO THE PCA FOR WATERSHED EVERUTHING SHOULD BE IN THIS TABLES
        # Extracted the columns from the working df need to select the fraction to be used the PCA ot the PCA/PCT value and merge with the acre total for the watershed
        watershed_state_df_pca_pct_statecrop_grp = watershed_state_df_pca_pct_statecrop_in.groupby(['BasinID'])[use_lookup_df_value +"_"+group +'_uniform_in watershed',use_lookup_df_value +"_"+group +'_upper_in watershed',use_lookup_df_value +"_"+group +'_lower_in watershed'].sum().reset_index()
        # EXTRACT WATERSHED INFORMATION
        watershed_acres = watershed_state_df_pca_pct_statecrop_in.ix[:,['BasinID', 'acres_cws',use+'_pca', 'huc2']]
        # Sums the modified acres by state to the whole watershed - this is used for weighting
        mod_watershed_acres = watershed_state_df_pca_pct_statecrop_in.groupby(['BasinID'])['mod_acres_cws'].sum().reset_index()
        mod_watershed_acres.columns = ['BasinID','mod_acres_cws']
        # Merge modified acres used for weighting into the tables
        watershed_acres = pd.merge(watershed_acres,mod_watershed_acres,on='BasinID', how='left')
        watershed_acres.drop_duplicates(inplace=True)
        watershed_state_df_pca_pct_statecrop_final = pd.merge(watershed_acres , watershed_state_df_pca_pct_statecrop_grp,on="BasinID", how='outer')

        # CALCULATES THE PCA/PCT FACTORS AFTER SUMMING TO WATERSHED
        watershed_state_df_pca_pct_statecrop_final[use_lookup_df_value+"_"+ group +'_PCA/PCT Factor Upper in watershed']  = watershed_state_df_pca_pct_statecrop_final[use_lookup_df_value +"_"+group +'_upper_in watershed'].div((watershed_state_df_pca_pct_statecrop_final['mod_acres_cws']).where(watershed_state_df_pca_pct_statecrop_final['mod_acres_cws'] != 0, np.nan), axis=0)
        watershed_state_df_pca_pct_statecrop_final[use_lookup_df_value+"_"+ group +'_PCA/PCT Factor Uniform in watershed'] = watershed_state_df_pca_pct_statecrop_final[use_lookup_df_value +"_"+group +'_uniform_in watershed'].div((watershed_state_df_pca_pct_statecrop_final['mod_acres_cws']).where(watershed_state_df_pca_pct_statecrop_final['mod_acres_cws'] != 0, np.nan), axis=0)
        watershed_state_df_pca_pct_statecrop_final[use_lookup_df_value+"_"+ group +'_PCA/PCT Factor Lower in watershed'] = watershed_state_df_pca_pct_statecrop_final[use_lookup_df_value +"_"+group +'_lower_in watershed'].div((watershed_state_df_pca_pct_statecrop_final['mod_acres_cws']).where(watershed_state_df_pca_pct_statecrop_final['mod_acres_cws'] != 0, np.nan), axis=0)
        watershed_state_df_pca_pct_statecrop_final.fillna(0,inplace=True)

        # SELECTS THE FRACTION TO USE FOR ADJUSTING THE EDWC; THIS CAN BE THE PCA/PCT FACTOR OR THE PCA

        watershed_state_df_pca_pct_statecrop_final[use_lookup_df_value + "_" + group +'_uniform_PCA/PCT frac'] = watershed_state_df_pca_pct_statecrop_final.apply(lambda row: uniform_adj_pca(row, use_lookup_df_value + '_pca', use_lookup_df_value+"_" +group +'_PCA/PCT Factor Uniform in watershed'), axis = 1)
        watershed_state_df_pca_pct_statecrop_final[use_lookup_df_value + "_"+ group +'_upper_PCA/PCT frac'] = watershed_state_df_pca_pct_statecrop_final.apply(lambda row: uniform_adj_pca(row, use_lookup_df_value + '_pca', use_lookup_df_value +"_"+group +'_PCA/PCT Factor Upper in watershed'), axis=1)
        watershed_state_df_pca_pct_statecrop_final[use_lookup_df_value + "_" + group + '_lower_PCA/PCT frac'] = watershed_state_df_pca_pct_statecrop_final.apply(lambda row: lower_adj_pca(row, use_lookup_df_value + '_pca',use_lookup_df_value +"_"+ group + '_PCA/PCT Factor Lower in watershed'), axis=1)
        # Export the working file for the use/pct combo
        watershed_state_df_pca_pct_statecrop_final.to_csv('C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Drinking water projects\Scripts\Outputs' + os.sep+ use+"_"+ group +'_SemiFinal_.csv')
        #watershed_state_df_pca_pct_statecrop_final.drop_duplicates(inplace=True)

        # EXTRACTS THE SELECTED FRACTIONS TO BE EXPORTED INTO THE FINAL TABLES
        final = watershed_state_df_pca_pct_statecrop_final[['BasinID', 'acres_cws', 'huc2', use_lookup_df_value + '_pca',use_lookup_df_value+"_" + group + '_upper_PCA/PCT frac',use_lookup_df_value +"_"+ group + '_uniform_PCA/PCT frac',use_lookup_df_value +"_"+ group + '_lower_PCA/PCT frac']].copy()
        # UPDATE COL HEADER TO THE ONES REQUESTED BY THE TEAM
        col_dict = update_cols_tofinal_terms(final)
        out_cols =[]
        for cols in final.columns.values.tolist():
            out_cols.append(col_dict[cols])
        # print out_cols
        final.columns = out_cols
        final['Use Name'] = use
        # print final.head()

        # APPENDS THE FRACTIONS FOR THIS USE/PCT TO THE WORKING USE DF
        if len(use_df) ==0:
            use_df=final
        else:
            common_col = []
            for x in use_df.columns.values.tolist():
                if x in final.columns.values.tolist():
                    common_col.append(x)
                else:
                    pass
            use_df = pd.merge(use_df,final, on=common_col, how='outer')
            use_df.drop_duplicates(inplace=True)

    # APPENDS THE COMPLETED USE DF WITH ALL PCTs TO THE WORKING FINAL DF
    if len(final_watershed_crop_df) == 0:
        final_watershed_crop_df= use_df
    else:
        final_watershed_crop_df = pd.concat([final_watershed_crop_df,use_df], axis=0)
        final_watershed_crop_df.drop_duplicates(inplace=True)

final_watershed_crop_df.drop_duplicates(inplace=True)
final_watershed_crop_df = final_watershed_crop_df.reindex(columns=[final_col])  # UPDATES  TO THE REQUESTED COL ORDER
# EXPORTS FINAL TABLE BY WATERSHED AND CROP
final_watershed_crop_df.to_csv('C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Drinking water projects\Scripts\Outputs\Final_Watershed_Crop_Summary.csv')
# GENERATED THE AGGREGATED WATERSHED TABLE
# watershed aggregate table drops the use data to come up with a value for the watershed based on the selected fractions
# for each use
watershed_aggregated = final_watershed_crop_df.copy()
watershed_aggregated.drop("Use Name", axis=1,inplace=True)
watershed_cols = [v for v in watershed_aggregated.columns.values.tolist() if not v.endswith("_frac")]
frac_cols =  [v for v in watershed_aggregated.columns.values.tolist() if  v.endswith("_frac")]
watershed_aggregated[frac_cols] = watershed_aggregated[frac_cols].multiply(watershed_aggregated['acres_watershed'], axis=0)
watershed_aggregated = watershed_aggregated.groupby( ['BasinID', 'acres_watershed','HUC2'])[frac_cols].sum().reset_index()
watershed_aggregated[frac_cols] = watershed_aggregated[frac_cols].div(watershed_aggregated['acres_watershed'], axis=0)
watershed_aggregated.drop_duplicates(inplace=True)
watershed_aggregated= watershed_aggregated.reindex(columns=[final_col])  # UPDATES  TO THE REQUESTED COL ORDER
watershed_aggregated.drop(["Use Name","PCA"], axis=1,inplace=True)
watershed_aggregated.to_csv('C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Drinking water projects\Scripts\Outputs\Final_Watershed_Aggregate_Summary.csv')


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)