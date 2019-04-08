import pandas as pd
import os
import datetime


in_tabulated = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects' \
               r'\Risk Assessments\GMOs\dicamba\Tabulated\Range'
out_location = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects' \
               r'\Risk Assessments\GMOs\dicamba\Tabulated_byCounties\Range'
# grouping_col = ['EntityID', 'STUSPS']
grouping_col = ['EntityID', 'GEOID', 'STUSPS']


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

if not os.path.exists(out_location):
    os.mkdir(out_location)

list_results_directory = os.listdir(in_tabulated)


for folder in list_results_directory:
    print '\nWorking on {0}'.format(folder)
    csv_list = os.listdir(in_tabulated + os.sep+ folder)
    merged_df = pd.DataFrame()
    for csv in csv_list:
        print csv

        results_df = pd.read_csv(in_tabulated + os.sep+ folder + os.sep +csv, low_memory=False)
        val_col = [v for v in results_df.columns.values.tolist() if v.startswith('VALUE')]

        if 'VALUE' in val_col:  # zone key for overlap runs no longer needed
            val_col.remove('VALUE')
        df_state = results_df[grouping_col + val_col].copy()
        col_order = grouping_col + val_col

        df_state = df_state.groupby(grouping_col)[val_col].sum().reset_index()
        df_state = df_state.reindex(columns = col_order)
        merged_df = pd.concat([merged_df,df_state])
        merged_df  = merged_df .reindex(columns = col_order)

    merged_df.to_csv(out_location + os.sep + folder +'.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)