import pandas as pd
import os
import datetime
import sys

# #############  User input variables
# location where out table will be saved - INPUT Source user
table_folder = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Script_Check_20170531'

# INPUT SOURCES Recode_BinTable_as_of_[date].csv from Step_3_Recode Bin Table; file should already be located at the
# path table_folder
short_hand_wide_table_nm = 'Recode_BinTable_as_of_20170531.csv'
# column headers for bins short_hand_wide_table - INPUT SOURCE- User
bin_col = ['Terrestrial Bin', 'Bin 1', 'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6', 'Bin 7', 'Bin 8', 'Bin 9', 'Bin 10']

# ############# Static input variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
archived_location = table_folder + os.sep + 'Archived'
os.mkdir(archived_location) if not os.path.exists(archived_location) else None

in_table = table_folder + os.sep + short_hand_wide_table_nm
out_table = archived_location + os.sep + 'LongBins_' + date + '.csv'

# ############## ASSUMPTIONS
# Columns in the input table are in the desire order
# All columns in table should be retained

# Time tracker
start_script = datetime.datetime.now()
print "Start Time: " + start_script.ctime()

# Step 1: Load data from shorthand bin tables, removed unnamed columns. Try/Excepts makes sure we have a complete
# archive of data used for update,and intermediate tables.
try:
    df_in = pd.read_csv(in_table)
    [df_in.drop(v, axis=1, inplace=True) for v in df_in.columns.values.tolist() if v.startswith('Unnamed')]
except IOError:
    print('\nYou must move the Recoded shorthand bin table to the table_folder location')
    sys.exit()

# Step 2: Generates list used as for the id_vars and value_vars variables in transformation and runs wide to long
# transformation
df_cols = df_in.columns.values.tolist()
id_vars_cols = [v for v in df_cols if v not in bin_col]
long_df = pd.melt(df_in, id_vars=id_vars_cols, value_vars=bin_col, var_name='Bins', value_name='Value')

# Step 3: Reindex transformed df to original column order of input table, with added columns from the transformation
out_col = id_vars_cols
out_col.extend(['Bins', 'Value'])
long_df = long_df.reindex(columns=out_col)
long_df['Value'].fillna(0)

# Step 4:Exports transformed data frame to csv
long_df.to_csv(out_table)

# Elapsed time
print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
