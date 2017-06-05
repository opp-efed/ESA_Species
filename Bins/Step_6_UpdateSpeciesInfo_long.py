# NOTES: Before running this make sure WoE have not been changes since they were updated during Step 1 or 2

# ############## ASSUMPTIONS
# Col order has not changed since step 1
# Long bin table is used a master for entity id and huc2 columns names
# Master huc12/ab split is used for huc12 columns names
# Col header in long bin table should be used out output header for final data frame
# Woe groups have not changes since they were updated in Step 1 or Step 2 NOTE can add loop in to also make update to
# WoE groups

import pandas as pd
import os
import datetime
import sys

# #############  User input variables
# location where out table will be saved - INPUT Source user
table_folder = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Script_Check_20170531'

in_table_nm = 'LongBins_unfiltered_AB_20170605.csv'
entity_id_col_c = 'EntityID'

# Note extinct species will be included in the output if a bin assignment applies
current_master_nm = r'MasterListESA_Feb2017_20170410.csv'
entity_id_col_m = 'EntityID'
lead_agency_col = 'Lead Agency'


# ############# Static input variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
archived_location = table_folder + os.sep + 'Archived'  # scratch workspace
os.mkdir(archived_location) if not os.path.exists(archived_location) else None
long_bins = archived_location + os.sep + in_table_nm
# used to make a wide version of the A/B splits; should be the same as long_bins unless species info is updated
df_unfiltered_outfile = table_folder + os.sep + 'LongBins_unfiltered_AB_' + str(date) + '.csv'
# Used as input for post-processor
df_filtered_outfile = table_folder + os.sep + 'LongBins_PP_filter_AB_' + str(date) + '.csv'
current_master = archived_location + os.sep + current_master_nm


def update_bin_info(bins_df, current_species):
    current_species_bins = bins_df.loc[bins_df[entity_id_col_m].isin(current_species)]
    drop_value_no = current_species_bins.loc[~current_species_bins['Value'].isin(['No'])]
    drop_value_no = drop_value_no.loc[~drop_value_no['Bins'].isin(['Bin 1'])]

    return drop_value_no, current_species_bins


def set_common_cols(master_entid, bin_entid, bin_df, sp_df):
    if master_entid != bin_entid:
        print ('EntityID column headers in the species master list, {0}, and the current bin table, {1}'.format
               (entity_id_col_m, entity_id_col_c))
        print('Verify all species info columns you wish to be updates have the same column header in both tables.')
    else:
        sp_df_cols = sp_df.columns.values.tolist()
        bin_df_cols = bin_df.columns.values.tolist()
        common_cols = [j for j in sp_df_cols if j in bin_df_cols]

        poss_answer = ['Yes', 'No']
        ask_q = True
        while ask_q:
            for p in common_cols:
                if p == '':
                    common_cols.remove(p)
            user_input = raw_input(
                'Are these are the species columns that should be updated{0}: Yes or No: '.format(common_cols))
            if user_input not in poss_answer:
                print 'This is not a valid answer: type Yes or No'
            elif user_input == 'Yes':
                break
            else:
                additional_cols = raw_input('Which additional columns should be included - {0}: '.format(sp_df_cols))
                if type(additional_cols) is str:
                    additional_cols = additional_cols.split(",")
                    additional_cols = [j.replace("' ", "") for j in additional_cols]
                    additional_cols = [j.replace("'", "") for j in additional_cols]
                    additional_cols = [j.lstrip() for j in additional_cols]
                    common_cols.extend(additional_cols)
        return common_cols

def update_columns(current_col, updated_col, list_of_col):
    # VARS: current:current_col: value in current list, updated_col: value it should be updated to, list_of_col: list of
    # values
    # DESCRIPTION: Standardize column headers using the current_bin_table header values as the master
    # RETURN: Update list of value used to update column headers

    if current_col != updated_col:
        loc = list_of_col.index(str(current_col))
        list_of_col.remove(str(current_col))
        list_of_col.insert(loc, str(updated_col))
        return list_of_col
    else:
        return list_of_col

def load_data(current, master_sp):
    # VARS: current: current long bin table in use; removed: master_sp current species master list
    # DESCRIPTION: removes columns without headers from all data frames; sets entity id col as str in all tables;
    #  generated list of col headers from in table. Try/Excepts makes sure we have a complete archive of data used for
    # update, and intermediate tables.
    # RETURN: data frames of inputs tables; col header from in bin table

    try:
        c_df = pd.read_csv(current, dtype=object)
    except IOError:
        print('\nYou must move the current bin table to Archived folder for this update')
        sys.exit()
    [c_df.drop(m, axis=1, inplace=True) for m in c_df.columns.values.tolist() if m.startswith('Unnamed')]
    c_df[str(entity_id_col_c)] = c_df[str(entity_id_col_c)].astype(str)
    c_cols = c_df.columns.values.tolist()
    c_cols = update_columns(str(entity_id_col_c), str(entity_id_col_m), c_cols)
    c_df.columns = c_cols

    wide_cols = c_df.columns.values.tolist()

    try:
        sp_df = pd.read_csv(master_sp, dtype=object)
    except IOError:
        print('\nYou must move the in table bin table to table folder')
        sys.exit()
    return c_df, sp_df, wide_cols


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

long_bin_df, df_sp, out_cols = load_data(long_bins, current_master)

cols_to_update = set_common_cols(entity_id_col_m, entity_id_col_c, long_bin_df, df_sp)
df_spe_info = df_sp[cols_to_update]
out_col_bin = [v for v in out_cols if v == str(entity_id_col_m) or v not in cols_to_update]
df_bins_no_sp_info = long_bin_df.reindex(columns=out_col_bin)
df_final = pd.merge(df_bins_no_sp_info, df_spe_info, on=str(entity_id_col_m), how='left')

final_cols = long_bin_df.columns.values.tolist()

df_final = df_final.reindex(columns=final_cols)

df_final.loc[df_final[lead_agency_col] >= '2', lead_agency_col] = 'NMFS'
df_final.loc[df_final[lead_agency_col] == '1', lead_agency_col] = 'USFWS'

df_filtered, df_unfiltered = update_bin_info(df_final, (df_spe_info[entity_id_col_m].values.tolist()))

df_filtered.to_csv(df_filtered_outfile)
df_unfiltered.to_csv(df_unfiltered_outfile)

end_script = datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)
