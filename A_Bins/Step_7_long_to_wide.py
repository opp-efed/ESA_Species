# Author: J. Connolly: updated 5/31/2017
# ##############General notes
# All hard coded columns are purposefully, these are new columns used for tracking
# ############## ASSUMPTIONS
# Long bin table is used a master for entity id and huc2 columns names
# Col header in long bin table should be used out output header for final data frame
# If duplicates are found, tables from Step 6 should be updated.
import pandas as pd
import os
import datetime
import sys

# #############  User input variables
# location where out table will be saved - INPUT Source user
table_folder = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Script_Check_20170627'
# INPUT SOURCES LongBins_unfiltered_AB_[date].csv from Step_5_A_B_split; file should already be located at the
# path table_folder
in_table_nm = 'LongBins_unfiltered_AB_20170627.csv'  # this input file is updated saved to root folder table_folder
entity_id_col_c = 'EntityID'
huc2_col_c = 'HUC_2'

# out table column headers
# species info col header
spe_col = ['Lead Agency', 'EntityID', 'Group', 'Common Name', 'Scientific Name', 'Status', 'WoE_group_1', 'WoE_group_2',
           'WoE_group_3', 'Multi HUC', 'HUC_2']
# bin col header to be used in transformation
bin_col = ['Terrestrial Bin', 'Bin 1', 'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6', 'Bin 7', 'Bin 8', 'Bin 9', 'Bin 10']
# database col headers
db_col = ['AttachID', 'Updated', 'Reassigned', 'sur_huc', 'DD_Species', 'Spe_HUC']

# ############# Static input variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
archived_location = table_folder + os.sep + 'Archived'
os.mkdir(archived_location) if not os.path.exists(archived_location) else None

in_table = table_folder + os.sep + in_table_nm
out_table = table_folder + os.sep + 'WideBins_unfiltered_AB_' + date + '.csv'
duplicate_assignments = archived_location + os.sep + 'DuplicateBinAssignment_' + str(date) + '.csv'

def label_huc2(row):
    # VARS:row
    # DESCRIPTION: apply function that checks the the HUC2 was not dropped during the transformation and  matches the
    # HUC2 the Spe_HUC col,
    # RETURN: huc 2 based on spe_huc col populated when table was loaded
    huc = row['Spe_HUC'].split("_")[1]
    return huc


def load_data(table_in):
    # VARS: table_in bin table with A/B splits and surrogate hucs in long format
    # DESCRIPTION: removes columns without headers from all data frames; sets entity id col as str;
    # generated list of col headers;['Spe_HUC'] added to all tables as aunique identifier common across tables,
    #  Try/Excepts makes sure we have a complete archive of data used for update, and
    # intermediate tables
    # RETURN: data frames of inputs tables; KEY col headers standardize
    try:
        in_df = pd.read_csv(table_in, dtype= object)
    except IOError:
        print('\nYou must move the long bin table to the table_folder location')
        sys.exit()
    [in_df.drop(z, axis=1, inplace=True) for z in in_df.columns.values.tolist() if z.startswith('Unnamed')]
    in_df[str(entity_id_col_c)] = in_df[str(entity_id_col_c)].astype(str)
    in_df[str(huc2_col_c)] = in_df[str(huc2_col_c)].astype(str)
    in_df['Spe_HUC'] = in_df[str(entity_id_col_c)] + "_" + in_df[str(huc2_col_c)]
    final_cols = list(spe_col)
    final_cols.extend(bin_col)
    final_cols.extend(db_col)

    poss_answer = ['Yes', 'No']
    ask_q = True
    while ask_q:
        user_input = raw_input('Is this the order you would like the columns to be {0}: Yes or No: '.format(final_cols))
        if user_input not in poss_answer:
            print 'This is not a valid answer'
        elif user_input == 'Yes':
            break
        else:
            final_cols = raw_input('Please enter the order of columns comma sep str ')
    if type(final_cols) is str:
        final_cols = final_cols.split(",")
        final_cols = [j.replace("' ", "") for j in final_cols]
        final_cols = [j.replace("'", "") for j in final_cols]
        final_cols = [j.lstrip() for j in final_cols]

    return in_df, final_cols


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Step 1: Load data from current bin tables; breaks into three parts, species info, bin and bin database info; Sets the
# columns from the species tables that should be included in the output bin tables.
df_in, col_final = load_data(in_table)
species_info = df_in.loc[:, spe_col]  # species info
species_info['Spe_HUC'] = species_info[entity_id_col_c].astype(str) + "_" + species_info[huc2_col_c].astype(str)
trailing_col = df_in.loc[:, db_col]  # bin database info

# Step 2: Drops all columns in bin data frame expect for ['Spe_HUC', 'A_Bins', 'Value'] and removes duplicates
df_in['Spe_HUC'] =  df_in[entity_id_col_c].astype(str) + "_" +  df_in[huc2_col_c].astype(str)  # unique ID

check_dup_bin = df_in.loc[:,['Spe_HUC', 'A_Bins', 'Value', 'Updated']]
check_dup_bin.drop_duplicates(inplace=True)

# Step 3: Generates column with unique identifier that represent the species/huc/bin and check for duplicates.
# Duplicates will cause tranformation to fail.  Duplicates are printed for user address.
check_dup_bin['Spe_HUC_bin'] = check_dup_bin['Spe_HUC'].astype(str) + "_" + check_dup_bin['A_Bins'].astype(str)
dup = check_dup_bin.set_index('Spe_HUC_bin').index.get_duplicates()

if len(dup) != 0:
    print dup
    print check_dup_bin.loc[check_dup_bin['Spe_HUC_bin'].isin(dup)]
    print '\n Duplicate values for a species in a huc and bin need to be addressed before transformation will succeed' \
              'See file: {0} \n Update tables from Step 6 once duplicates are addressed'.format(duplicate_assignments)
    check_dup_bin.loc[check_dup_bin['Spe_HUC_bin'].isin(dup), ['Updated']] = 'Duplicate Assignment'
    check_dup_bin.to_csv(duplicate_assignments)
    sys.exit()

# Step 4: Reindex working df back to ['Spe_HUC', 'A_Bins', 'Value'] and does one more check for exact duplicates by
# dropping them
reindex = df_in.reindex(columns=['Spe_HUC', 'A_Bins', 'Value'])
df_in.drop_duplicates(inplace=True)

# Step 5: Runs pivot to transform data from long to wide; drop any duplicates generate in transformation and updates
# [HUC_2] col; pivot function - Reshape data (produce a 'pivot' table) based on column values. Uses unique values from
# index / columns to form axes of the resulting DataFrame.
wide_df = df_in.pivot('Spe_HUC', 'A_Bins', 'Value')
wide_df.reset_index(inplace=True)
wide_df.drop_duplicates(inplace=True)
wide_df['HUC_2'] = wide_df.apply(lambda row: label_huc2(row), axis=1)

# Step 6: Merges species info, bin database info back to wide bin data frame; run reindex to set col based on input
# table; Left outer join produces a complete set of records from Table A, with the matching records (where available)
# in Table B. If there is no match, the right side will contain null; drops duplicates
wide_df = pd.merge(wide_df, species_info, on='Spe_HUC', how='left')
wide_df = pd.merge(wide_df, trailing_col, on='Spe_HUC', how='left')
wide_df.drop_duplicates(inplace=True)
wide_df = wide_df.reindex(columns=col_final)
wide_df.drop_duplicates(inplace=True)

# Step 7: Exports data frame to csv
# WideBins_unfiltered_AB_[date].csv  # wide bin table with shorthand for bins values and a/b split; used for BE
wide_df.to_csv(out_table)

# Elapsed time
print "Script completed in: {0}".format(datetime.datetime.now() - start_time)
