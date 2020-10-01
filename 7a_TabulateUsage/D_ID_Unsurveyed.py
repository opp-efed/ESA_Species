import pandas as pd
import datetime
import os
import numpy as np
# Author J.Connolly
# Internal deliberative, do not cite or distribute

# NOTE right now this is surrogate values are only pulled from CONUS and applied to CONUS as a regional loop if we need
# to do this for theNL48
# NOTE Values for States with Reported Usage columns  Not Registered Not Grown in State
# NOTE VALUES for AgClass = AgCompositeNonAgComposite

# chemical team QC table with all PCT from SUUM
path = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\Glyphosate\GLY_findsurrogate_v2.csv"
out_table_name ="GLY_withsurrogate_v2"
# placeholder value for no PCT data in the table above; this should be a low number 0 or negative and is the value
# found in the input table when a surrogate needs to be found
surrogate_value = -1

pct_values = ['Max', 'Min', 'Avg']
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
df = pd.read_csv(path)
# HARD CODE TO THE States with Reported Usage column TODO add in user input variable to remove hard code
filtered = df[(df['States with Reported Usage'] != 'Not Registered') & (df['States with Reported Usage'] != 'Not Grown')]
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

final_cols = df.columns.values.tolist()
for pct in pct_values:
    final_cols.append(pct+pct+' State PCT')
    final_cols.append(pct+' State Crop')
    final_cols.append(pct+'UDL State PCT')
    final_cols.append(pct+'UDL State Crop')
    final_cols.append(pct+pct+' Crop National PCT')
    final_cols.append(pct+'State Crop National')
    final_cols.append(pct+'UDL National PCT')
    final_cols.append(pct+'UDLState National')
    final_cols.append(pct+pct+'National')
df = df.reindex(columns= final_cols)
final_cols = df.columns.values.tolist()

for region in ['CONUS', 'NL48']:
    # HARD CODE TO THE States with Reported Usage column
    registered = filtered [(filtered['States with Reported Usage'] != 'Not Registered') & (filtered['States with Reported Usage'] != 'Not Grown')& (filtered['Region'] == region)]
    ag = registered  [(registered  ['AgClass'] == 'AgComposite')]
    crops =  list(set(ag["CONCAT USE SITE"].values.tolist()))
    udls = list(set(ag["UDL"].values.tolist()))

    if 'NonAgComposite' in registered ['AgClass'].values.tolist():
        nonag = registered   [(registered   ['AgClass'] == 'NonAgComposite')]
        nonag_udl =  list(set(nonag["UDL"].values.tolist()))
    else:
        nonag_udl=[]
    for pct in pct_values:
        states = list(set(registered['State'].values.tolist()))
        for state in states:
            print (state)
            state_filter = registered [(registered ['State'] == state)]
            state_crops = list(set(state_filter["CONCAT USE SITE"].values.tolist()))
            state_udls = list(set(state_filter["UDL"].values.tolist()))

            print("Checking for crop specific PCT in state")
            for crop in state_crops:
                filter_df = state_filter[(state_filter ['CONCAT USE SITE'] == crop)]
                # print list(set(filter_df['Max PCT'].values.tolist())), crop
                max_pct = filter_df.loc[filter_df[pct+' PCT'].idxmax()]
                df.loc[(df["CONCAT USE SITE"] == crop) & (df[pct+' PCT'] == surrogate_value) & (df["State"] == state), [pct + pct + ' State PCT', pct + ' State Crop']] = max_pct[pct + ' PCT'], max_pct['CONCAT USE SITE']

            print("Checking for udl specific PCT in state")
            for udl in state_udls:
                filter_df = state_filter [(state_filter['UDL'] == udl)]
                # print list(set(filter_df['Max PCT'].values.tolist())), udl
                max_pct = filter_df.loc[filter_df[pct+' PCT'].idxmax()]
                df.loc[(df["UDL"] == udl) & (df[pct+' PCT'] == surrogate_value) & (df["State"] == state), [pct + 'UDL State PCT', pct + 'UDL State Crop']] = max_pct[pct + ' PCT'], max_pct['CONCAT USE SITE']
            if len(nonag_udl)>0:
                for udl in nonag_udl:
                    filter_df = state_filter [(state_filter['UDL'] == udl)]
                    # print list(set(filter_df['Max PCT'].values.tolist())), udl
                    max_pct = filter_df.loc[filter_df[pct+' PCT'].idxmax()]
                    df.loc[(df["UDL"] == udl) & (df[pct+' PCT'] == surrogate_value) & (df["State"] == state) & (df['AgClass'] == 'NonAgComposite'), [pct + 'UDL State PCT', pct + 'UDL State Crop']] = max_pct[pct + ' PCT'], max_pct['CONCAT USE SITE']

        print("Checking for crop specific PCT nationally")
        for crop in crops:
            if crop in state_crops:
                filter_df = registered [(registered ['CONCAT USE SITE'] == crop)]
            else:
                filter_df = registered [(registered ['CONCAT USE SITE'] == crop)]
            # print list(set(filter_df['Max PCT'].values.tolist())), crop
            max_pct = filter_df.loc[filter_df[pct+' PCT'].idxmax()]
            df.loc[(df["CONCAT USE SITE"] == crop) & (df[pct+' PCT'] == surrogate_value), [pct + pct + ' Crop National PCT', pct + 'State Crop National']] = max_pct[pct + ' PCT'], max_pct['State']

        print("Checking for udl specific PCT nationally")
        for udl in udls:
            filter_df = registered [(registered ['UDL'] == udl)]
            # print list(set(filter_df['Max PCT'].values.tolist())), udl
            max_pct = filter_df.loc[filter_df[pct+' PCT'].idxmax()]
            df.loc[(df["UDL"] == udl) & (df[pct+' PCT'] == surrogate_value), [pct + 'UDL National PCT', pct + 'UDLState National']] = max_pct[pct + ' PCT'], max_pct['State']

        if len(nonag_udl)>0:
            for udl in nonag_udl:
                filter_df = registered [(registered ['UDL'] == udl)]
                # print list(set(filter_df['Max PCT'].values.tolist())), udl
                max_pct = filter_df.loc[filter_df[pct+' PCT'].idxmax()]
                df.loc[(df["UDL"] == udl) & (df[pct+' PCT'] == surrogate_value), [pct + 'UDL National PCT', pct + 'UDLState National']] = max_pct[pct + ' PCT'], max_pct['State']
        max_pct = df.loc[df[pct+' PCT'].idxmax()]
        df.loc[(df[pct+' PCT'] == surrogate_value), [pct + pct + 'National']] =max_pct[pct + ' PCT']

        df[pct+' PCT'] =  np.where(((df["Surrogate PCT"] == 'Yes') & (df[pct+pct+' State PCT'] > -1)& (df[pct+' PCT'] ==-1)), df[pct+pct+' State PCT'], df[pct+' PCT'] )
        df[pct+' PCT'] =  np.where(((df["Surrogate PCT"] == 'Yes') & (df[pct+'UDL State PCT'] > -1)& (df[pct+' PCT'] ==-1)), df[pct+'UDL State PCT'], df[pct+' PCT'] )
        df[pct+' PCT'] =  np.where(((df["Surrogate PCT"] == 'Yes') & (df[pct+pct+' Crop National PCT'] > -1)& (df[pct+' PCT'] ==-1)), df[pct+pct+' Crop National PCT'], df[pct+' PCT'] )
        df[pct+' PCT'] =  np.where(((df["Surrogate PCT"] == 'Yes') & (df[pct+'UDL State PCT'] > -1)& (df[pct+' PCT'] ==-1)), df[pct+'UDL State PCT'], df[pct+' PCT'] )
        df[pct+' PCT'] =  np.where(((df["Surrogate PCT"] == 'Yes') & (df[pct+'UDL National PCT'] > -1)& (df[pct+' PCT'] ==-1)), df[pct+'UDL National PCT'], df[pct+' PCT'] )
        # only looks at at Ag for the national max
        df[pct+' PCT'] =  np.where(((df["Surrogate PCT"] == 'Yes') & (df[pct+pct+'National'] > -1)& (df[pct+' PCT'] ==-1)& (df['AgClass'] == 'AgComposite')), df[pct+pct+'National'], df[pct+' PCT'] )

max_col  = [i for i in df.columns.values.tolist() if i.startswith('Max PCT')][0]
min_col  = [i for i in df.columns.values.tolist() if i.startswith('Min PCT')][0]
avg_col  = [i for i in df.columns.values.tolist() if i.startswith('Avg PCT')][0]
print ("Final PCT colums are {0}, {1}, {2}".format(max_col,min_col,avg_col))

df['Compare Max_Avg PCT'] = df[max_col].ge(df[avg_col], axis=0)  # returns TRUE - max is greater or equal to avg
df['Compare Avg_Min PCT'] = df[avg_col].ge(df[min_col], axis=0)  # returns TRUE avg is greater or equal to min
df['Compare Max_PCT Greater than 100'] = df[max_col].gt(100, axis=0)  # returns TRUE - if value is >100
df['Compare Avg_PCT Greater than 100'] = df[avg_col].gt(100, axis=0)  # returns TRUE - if value is >100
df['Compare Min_PCT Greater than 100'] = df[max_col].gt(100, axis=0)  # returns TRUE - if value is >100
df['Compare Max_PCT Less than 0'] = df[max_col].lt(0, axis=0)  # returns TRUE - if value is <0
df['Compare Avg_PCT Less than 0'] = df[avg_col].lt(0, axis=0)  # returns TRUE - if value is <0
df['Compare Min_PCT Less than 0'] = df[max_col].lt(0, axis=0)  # returns TRUE - if value is <0

df.to_csv(os.path.dirname(path) +os.sep +out_table_name+"_" +date+'.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)

