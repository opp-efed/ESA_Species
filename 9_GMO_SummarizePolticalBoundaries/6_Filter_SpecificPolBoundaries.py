import pandas as pd
import datetime
import os

# if using one table
# in_table = r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\Enlist Duo\Request_20190906\AllUses\AllUses_R_Enlist_20190924.csv"
# outlocation = os.path.dirname(in_table)
# state_or_cnty = 'STUSPS'  # GEOID if cnty

# if running multiple tables with the MSQ values and not the percent overlap
in_table = r"E:\Dicamba Update Summer 2020\Tabulated_hab"
outlocation = r"E:\Dicamba Update Summer 2020\Tabulated_hab_filtered"
state_or_cnty = 'STUSPS'  # GEOID if cnty

list_pol_ids = ['AL', 'AR', 'AZ', 'CO', 'DE', 'FL', 'GA', 'IA', 'IL', 'IN', 'KS', 'KY', 'LA', 'MD', 'MI', 'MN', 'MO',
                'MS', 'NC','ND', 'NE', 'NJ', 'NM', 'NY', 'OH', 'OK', 'PA', 'SC', 'SD', 'TN', 'TX', 'VA', 'WI', 'WV']


def filter_bystate(df, in_table_path, outlocation, one_table):
    if 'GEOID' in df.columns.values.tolist():
        df['GEOID'] = df['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)

    df = df.loc[df[state_or_cnty].isin(list_pol_ids)].copy()
    [df.drop(m, axis=1, inplace=True)for m in df.columns.values.tolist() if m.startswith('Unnamed')]
    df.to_csv(outlocation +os.sep+ 'Filtered_' + os.path.basename(in_table_path) )

    if one_table:  # Additional filter when overlap has already been compeleted compared to just the MSQ numbers
        cols = df.columns.values.tolist()
        group_col = [v for v in cols if not v.startswith('CONUS')]
        v_col = [v for v in cols if v.startswith('CONUS')]
        df.ix[:,v_col] = df.ix[:, v_col].apply(pd.to_numeric, errors='coerce')

        df_cnty = df.groupby(group_col)[v_col].sum().reset_index()
        group_col.remove('GEOID')
        df_state = df.groupby(group_col)[v_col].sum().reset_index()
        group_col.remove('STUSPS')
        df_species = df.groupby(group_col)[v_col].sum().reset_index()

        df_cnty.to_csv(outlocation + os.sep + 'Filtered_Cnty_' + os.path.basename(in_table_path))
        df_state.to_csv(outlocation + os.sep + 'Filtered_State_' + os.path.basename(in_table_path))
        df_species.to_csv(outlocation + os.sep + 'Filtered_Species_' + os.path.basename(in_table_path))


# #############Static Variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

if in_table.endswith('.csv'):
    single_table = True
    in_df = pd.read_csv(in_table, dtype=object)
    filter_bystate(in_df, in_table,outlocation, single_table)

else:
    single_table = False
    list_csv = os.listdir(in_table)
    for csv in list_csv:
        print("Working on {0}".format(csv))
        path_csv = in_table+os.sep+csv
        in_df = pd.read_csv(path_csv, dtype=object)
        filter_bystate(in_df, path_csv, outlocation, single_table)


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
