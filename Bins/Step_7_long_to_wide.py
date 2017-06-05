# Author: J. Connolly: updated 5/31/2017
# ############## ASSUMPTIONS
# Long bin table is used a master for entity id and huc2 columns names
# Master huc12/ab split is used for huc12 columns names
# Col header in long bin table should be used out output header for final data frame
import pandas as pd
import os
import datetime
import sys

# #############  User input variables
# location where out table will be saved - INPUT Source user
table_folder = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Script_Check_20170531'
# INPUT SOURCES LongBins_unfiltered_AB_[date].csv from Step_5_A_B_split; file should already be located at the
# path table_folder
in_table_nm = 'LongBins_unfiltered_AB_20170605.csv'
entity_id_col_c = 'EntityID'
huc2_col_c = 'HUC_2'

# out table column headers

# species info col header
spe_col = ['Lead Agency', 'EntityID', 'Group', 'Common Name', 'Scientific Name', 'Status','WoE_group_1','WoE_group_2',
           'WoE_group_3','Multi HUC', 'HUC_2']
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


def label_huc2(row):
    huc = row['Spe_HUC'].split("_")[1]
    return huc


def load_data(table_in):
    try:
        in_df = pd.read_csv(table_in, dtype=object)
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

df_in, col_final = load_data(in_table)

species_info = df_in.loc[:,spe_col]
species_info['Spe_HUC'] = species_info[entity_id_col_c] + "_" + species_info[huc2_col_c].astype(str)
trailing_col = df_in.loc[:,db_col]

[df_in.drop(v, axis=1, inplace=True) for v in df_in.columns.values.tolist() if v not in ['Spe_HUC', 'Bins', 'Value']]
df_in.drop_duplicates(inplace=True)
df_in['Spe_HUC_bin'] = df_in['Spe_HUC'] + "_" + df_in['Bins'].astype(str)

dup = df_in.set_index('Spe_HUC_bin').index.get_duplicates()
if len(dup) != 0:
    print dup
    print df_in.loc[df_in['Spe_HUC'].isin(dup)]

reindex = df_in.reindex(columns=['Spe_HUC', 'Bins', 'Value'])
df_in.drop_duplicates(inplace=True)

wide_df = df_in.pivot('Spe_HUC', 'Bins', 'Value')
wide_df.reset_index(inplace=True)
wide_df.drop_duplicates(inplace=True)
wide_df['HUC_2'] = wide_df.apply(lambda row: label_huc2(row), axis=1)

wide_df = pd.merge(wide_df, species_info, on='Spe_HUC', how='left')
wide_df = pd.merge(wide_df, trailing_col, on='Spe_HUC', how='left')
wide_df.drop_duplicates(inplace=True)

wide_df = wide_df.reindex(columns=col_final)
wide_df.drop_duplicates(inplace=True)

wide_df.to_csv(out_table)

end_script = datetime.datetime.now()
print "Elapse time {0}".format(end_script - start_time)
