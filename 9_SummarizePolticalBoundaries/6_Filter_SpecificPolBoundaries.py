import pandas as pd
import datetime
import os

in_table = r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects" \
           r"\Risk Assessments\GMOs\dicamba\Tabulated_AllUses\AllUses_R_Dicamba_20181114.csv"
state_or_cnty = 'STUSPS'  # GEOID if cnty

list_pol_ids = ['AL', 'AZ', 'AR', 'CO', 'DE', 'FL', 'GA', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'MD', 'MI', 'MN', 'MS',
                'MO', 'NE', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'PA', 'SC', 'SD', 'TN', 'TX', 'VA', 'WV', 'WI',
                ]


# #############Static Variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

outlocation = os.path.dirname(in_table)
df = pd.read_csv(in_table, dtype=object)

df = df.loc[df[state_or_cnty].isin(list_pol_ids)]
[df.drop(m, axis=1, inplace=True) for m in df.columns.values.tolist() if m.startswith('Unnamed')]
df.to_csv(outlocation +os.sep+ 'Filtered_' + os.path.basename(in_table) )
cols = df.columns.values.tolist()
group_col = [v for v in cols if not v.startswith('CONUS')]
v_col = [v for v in cols if  v.startswith('CONUS')]
df.ix[:, v_col] = df.ix[:, v_col].apply(pd.to_numeric, errors='coerce')

df_cnty = df.groupby(group_col)[v_col].sum().reset_index()
group_col.remove('GEOID')
df_state = df.groupby(group_col)[v_col].sum().reset_index()
group_col.remove('STUSPS')
df_species = df.groupby(group_col)[v_col].sum().reset_index()

df_cnty.to_csv(outlocation +os.sep+ 'Filtered_Cnty' + os.path.basename(in_table) )
df_state.to_csv(outlocation +os.sep+ 'Filtered_State' + os.path.basename(in_table) )
df_species .to_csv(outlocation +os.sep+ 'Filtered_Species' + os.path.basename(in_table) )

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)


