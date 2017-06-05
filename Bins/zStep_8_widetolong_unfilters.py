# Author: J. Connolly: updated 6/1/2017
# ############## ASSUMPTIONS

import pandas as pd
import datetime
import os
import sys

# #############  User input variables

# location where out table will be saved - INPUT Source user
table_folder = r'C:\Users\JConno02\Documents\Projects\ESA\Bins\updates\Script_Check_20170531'
# INPUT SOURCES 'WideBins_unfiltered_AB_[date].csv' from Step_6_long_to_wide; file should already be located at the
# path table_folder
in_table_nm = 'WideBins_unfiltered_AB_20170605.csv'
# column headers for bins from in_table - INPUT SOURCE- User
bin_col = ['Terrestrial Bin', 'Bin 1', 'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5', 'Bin 6', 'Bin 7', 'Bin 8', 'Bin 9', 'Bin 10']

# ############# Static input variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

in_table = table_folder + os.sep + in_table_nm
out_table = table_folder + os.sep + 'LongBins_AB_' + date + '.csv'


# Time tracker
start_script = datetime.datetime.now()
print "Start Time: " + start_script.ctime()
# Step 1: Load data from shorthand bin tables, removed unnamed columns. Try/Excepts makes sure we have a complete
# archive of data used for update,and intermediate tables.
try:
    df_in = pd.read_csv(in_table)
    [df_in.drop(v, axis=1, inplace=True) for v in df_in.columns.values.tolist() if v.startswith('Unnamed')]
except IOError:
    print('\nYou must move the in table  to table_folder location')
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
long_df['Value'].fillna('NAN',inplace=True)

# Step 4:Exports transformed data frame to csv
long_df.to_csv(out_table)

# Elapsed time
print "Script completed in: {0}".format(datetime.datetime.now() - start_script)
