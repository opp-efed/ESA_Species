import datetime
import os

import pandas as pd

# Title - Transforms out results by zone and summarize totals by species - final output is a master sum table of results
# by use and interval for each species

# TODO set up separate script that will spit out chem specific table with different interval include aerial and group
# inlocation
in_folder = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\tabulated_results\Master'

temp_folder = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\tabulated_results'

out_csv = temp_folder + os.sep + 'CH_Use_byinterval_20161003.csv'

# TODO set up a dict to read in the use index base on layer name
group_index = 1  # place to extract species group from tablename
SkipUses = []

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Reads in all information for species that should considered)


main_list_tables = os.listdir(in_folder)
main_list_table = [v for v in main_list_tables if v.endswith('.csv')]
df_final = pd.DataFrame()
for table in main_list_table:
    print table

    sp_table = in_folder + os.sep + table
    sp_table_df = pd.read_csv(sp_table, dtype=object)

    sp_table_df.drop('Unnamed: 0', axis=1, inplace=True)
    final_col = sp_table_df.columns.values.tolist()
    sp_table_df['EntityID'] = sp_table_df['EntityID'].astype(str)
    #pd.sp_table_df.convert_objects (convert_dates=False, convert_numeric=True, convert_timedeltas=False, copy=True)

    df_final = pd.concat([df_final, sp_table_df])
    df_final= df_final.reindex(columns=final_col)



df_final= df_final.reindex(columns=final_col)
sp_info_df = df_final.iloc[:, :7]

use_results_df = df_final.iloc[:, 7:]
use_results_df = use_results_df.apply(pd.to_numeric, errors='coerce')
num = use_results_df._get_numeric_data()
num[num < -33333] = 'Not in Region'


df_final = pd.concat([sp_info_df,num], axis=1)
df_final = df_final.fillna('Run not Complete')
df_final.to_csv(out_csv)
print 'Saved output file {0}'.format(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
