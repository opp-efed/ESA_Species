import pandas as pd
import datetime
import os

chemical = 'Diazinon_example'
in_long_table = r'E:\usage_diazinon_long_fl_co.csv'
outlocation = r'C:\Users\JConno02\Environmental Protection Agency (EPA)' \
              r'\Endangered Species Pilot Assessments - OverlapTables\SupportingTables\PCT'

state_list = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware',
              'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas',
              'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi',
              'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York',
              'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island',
              'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
              'West Virginia', 'Wisconsin', 'Wyoming', 'American Samoa', 'Guam',
              'Commonwealth of the Northern Mariana Islands', 'Puerto Rico', 'United States Virgin Islands', ]


def weight_pct(df, working_df):
    # PCT for GenClass = SUM[PCT BEAD Crop*(Avg. Annual Crop Acres Grown/Total of Avg Annual Acres in State)]
    total_crop_state = df['Acreage'].sum()
    df.ix[:, 'Proportion Crop'] = (df.ix[:, 'Acreage'] / total_crop_state)

    df.ix[:, 'Max PCT'] = df.ix[:, 'Max PCT'] / 100
    df.ix[:, 'Weighted Max'] = (df.ix[:, 'Proportion Crop'] * df.ix[:, 'Max PCT'])
    weight = df['Weighted Max'].sum()
    df.loc[:, 'Weighted Max Group'] = weight

    df.ix[:, 'Avg PCT'] = df.ix[:, 'Avg PCT'] / 100
    df.ix[:, 'Weighted Avg'] = (df.ix[:, 'Proportion Crop'] * df.ix[:, 'Avg PCT'])
    weight = df['Weighted Avg'].sum()
    df.loc[:, 'Weighted Avg Group'] = weight

    df.ix[:, 'Min PCT'] = df.ix[:, 'Min PCT'] / 100
    df.ix[:, 'Weighted Min'] = (df.ix[:, 'Proportion Crop'] * df.ix[:, 'Min PCT'])
    weight = df['Weighted Min'].sum()
    df.loc[:, 'Weighted Min Group'] = weight
    working_df = pd.concat([working_df, df], axis=0)
    return working_df


def no_use(df, working_df):
    df.ix[:, 'Weighted Max Group'] = 0
    df.ix[:, 'Weighted Avg Group'] = 0
    df.ix[:, 'Weighted Min Group'] = 0
    working_df = pd.concat([working_df, df], axis=0)
    return working_df

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()


in_df = pd.read_csv(in_long_table)
[in_df.drop(m, axis=1, inplace=True) for m in in_df.columns.values.tolist() if m.startswith('Unnamed')]

out_path = outlocation + os.sep + chemical
if not os.path.exists(out_path):
    os.mkdir(out_path)

out_df = pd.DataFrame(columns=[
    ['Crop', 'State', 'Acreage', 'Min PCT', 'Max PCT', 'Avg PCT', 'GenClass', 'Proportion Crop', 'Weighted Max',
     'Weighted Max Group', 'Weighted Avg', 'Weighted Avg Group', 'Weighted Min', 'Weighted Min Group']])


states = list(set(in_df['State'].values.tolist()))
out_df = no_use((in_df.loc[in_df['Acreage'] == -9]).copy(), out_df)

for state in states:
    print state
    state_df = in_df.loc[in_df['State'] == state].copy()
    genclasses = state_df['GenClass'].values.tolist()
    for genclasses in genclasses:
        genclass_df = state_df.loc[(in_df['GenClass'] == genclasses) & (state_df['Acreage'] != -9)].copy()
        if len(genclass_df) > 0:
            out_df = weight_pct(genclass_df, out_df)
out_df.drop_duplicates(inplace = True)
out_df.fillna(-987654321, inplace=True)
out_df.to_csv(out_path + os.sep + chemical + "_test" + "_" + date + '.csv')
max = out_df.pivot_table(values='Weighted Max Group', index=['GenClass'], columns='State')
max = max.reindex(columns=state_list)

min = out_df.pivot_table(values='Weighted Min Group', index=['GenClass'], columns='State')
min = min.reindex(columns=state_list)

avg = out_df.pivot_table(values='Weighted Avg Group', index=['GenClass'], columns='State')
avg = avg.reindex(columns=state_list)

max.to_csv(out_path + os.sep + chemical + "_max" + "_" + date + '.csv')
min.to_csv(out_path + os.sep + chemical + "_min" + "_" + date + '.csv')
avg.to_csv(out_path + os.sep + chemical + "_avg" + "_" + date + '.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
