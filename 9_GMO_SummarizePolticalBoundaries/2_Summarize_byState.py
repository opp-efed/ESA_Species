import pandas as pd
import os
import datetime


in_tabulated = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\Update Summer 2020\Tabulated_Overlap\CriticalHabitat'
out_location = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\Update Summer 2020\Tabulated County\CriticalHabitat'
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
        if len(merged_df)== 0:
            merged_df = df_state.copy()
        else:
            values_cols = [v for v in df_state.columns.values.tolist() if v.startswith(('VALUE'))]
            merge_cols = grouping_col + values_cols
            merged_df = pd.merge(merged_df,df_state,left_on= merge_cols ,right_on= merge_cols , how='outer')

    if len(grouping_col) == 3:
        merged_df.to_csv(out_location + os.sep + folder +'Cnty.csv')
    else:
        merged_df.to_csv(out_location + os.sep + folder +'State.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)