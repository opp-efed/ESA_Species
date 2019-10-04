import pandas as pd
import datetime
import os
import numpy as np
# Author J.Connolly
# Internal deliberative, do not cite or distribute

# NOTE right now this is surrogate values are only pulled from CONUS and applied to CONUS as a regional loop if we need
# to do this for theNL$*

# chemical team QC table with all PCT from SUUM
path = r"path\table name.csv"
# placeholder value for no PCT data in the table above; this should be a low number 0 or negative
surrogate_value = -1

pct_values = ['Max', 'Min', 'Avg']
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
df = pd.read_csv(path)
filtered = df[(df['States with Reported Usage'] != 'Not Registered') & (df['States with Reported Usage'] != 'Not Grown in State')]

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
    registered = filtered [(filtered['States with Reported Usage'] != 'Not Registered') & (filtered['States with Reported Usage'] != 'Not Grown in State')& (filtered['Region'] == region)]
    ag = registered  [(registered  ['AgClass'] == 'Ag Composite')]
    crops =  list(set(ag["CONCAT USE SITE"].values.tolist()))
    udls = list(set(ag["GenClass"].values.tolist()))

    if 'NonAg Composite' in registered ['AgClass'].values.tolist():
        nonag = registered   [(registered   ['AgClass'] == 'NonAg Composite')]
        nonag_udl =  list(set(nonag["GenClass"].values.tolist()))
    else:
        nonag_udl=[]
    for pct in pct_values:
        states = list(set(registered['State'].values.tolist()))
        for state in states:
            print (state)
            state_filter = registered [(registered ['State'] == state)]
            state_crops = list(set(state_filter["CONCAT USE SITE"].values.tolist()))
            state_udls = list(set(state_filter["GenClass"].values.tolist()))
            print("Checking for crop specific PCT in state")
            for crop in state_crops:
                filter_df = state_filter[(state_filter ['CONCAT USE SITE'] == crop)]
                # print list(set(filter_df['Max PCT'].values.tolist()))
                max_pct = filter_df.loc[filter_df[pct+' PCT'].idxmax()]
                df.loc[(df["CONCAT USE SITE"] == crop) & (df[pct+' PCT'] == surrogate_value) & (df["State"] == state), [pct + pct + ' State PCT', pct + ' State Crop']] = max_pct[pct + ' PCT'], max_pct['CONCAT USE SITE']

            print("Checking for udl specific PCT in state")
            for udl in state_udls:
                filter_df = state_filter [(state_filter['GenClass'] == udl)]
                # print list(set(filter_df['Max PCT'].values.tolist()))
                max_pct = filter_df.loc[filter_df[pct+' PCT'].idxmax()]
                df.loc[(df["GenClass"] == udl) & (df[pct+' PCT'] == surrogate_value) & (df["State"] == state), [pct + 'UDL State PCT', pct + 'UDL State Crop']] = max_pct[pct + ' PCT'], max_pct['CONCAT USE SITE']
            if len(nonag_udl)>0:
                for udl in nonag_udl:
                    filter_df = state_filter [(state_filter['GenClass'] == udl)]
                    # print list(set(filter_df['Max PCT'].values.tolist()))
                    max_pct = filter_df.loc[filter_df[pct+' PCT'].idxmax()]
                    df.loc[(df["GenClass"] == udl) & (df[pct+' PCT'] == surrogate_value) & (df["State"] == state) & (df['AgClass'] == 'NonAg Composite'), [pct + 'UDL State PCT', pct + 'UDL State Crop']] = max_pct[pct + ' PCT'], max_pct['CONCAT USE SITE']
        print("Checking for crop specific PCT nationally")

        for crop in crops:
            if crop in state_crops:
                filter_df = registered [(registered ['CONCAT USE SITE'] == crop)]
            else:
                filter_df = registered [(registered ['CONCAT USE SITE'] == crop)]
            # print list(set(filter_df['Max PCT'].values.tolist()))
            max_pct = filter_df.loc[filter_df[pct+' PCT'].idxmax()]
            df.loc[(df["CONCAT USE SITE"] == crop) & (df[pct+' PCT'] == surrogate_value), [pct + pct + ' Crop National PCT', pct + 'State Crop National']] = max_pct[pct + ' PCT'], max_pct['State']

        print("Checking for udl specific PCT nationally")

        for udl in udls:
            filter_df = registered [(registered ['GenClass'] == udl)]
            # print list(set(filter_df['Max PCT'].values.tolist()))
            max_pct = filter_df.loc[filter_df[pct+' PCT'].idxmax()]
            df.loc[(df["GenClass"] == udl) & (df[pct+' PCT'] == surrogate_value), [pct + 'UDL National PCT', pct + 'UDLState National']] = max_pct[pct + ' PCT'], max_pct['State']

        if len(nonag_udl)>0:
            for udl in nonag_udl:
                filter_df = registered [(registered ['GenClass'] == udl)]
                # print list(set(filter_df['Max PCT'].values.tolist()))
                max_pct = filter_df.loc[filter_df[pct+' PCT'].idxmax()]
                df.loc[(df["GenClass"] == udl) & (df[pct+' PCT'] == surrogate_value), [pct + 'UDL National PCT', pct + 'UDLState National']] = max_pct[pct + ' PCT'], max_pct['State']
        max_pct = df.loc[df[pct+' PCT'].idxmax()]
        df.loc[(df[pct+' PCT'] == surrogate_value), [pct + pct + 'National']] =max_pct[pct + ' PCT']

        df[pct+' PCT'] =  np.where(((df["Surrogate PCT"] == 'Yes') & (df[pct+pct+' State PCT'] > -1)& (df[pct+' PCT'] ==-1)), df[pct+pct+' State PCT'], df[pct+' PCT'] )
        df[pct+' PCT'] =  np.where(((df["Surrogate PCT"] == 'Yes') & (df[pct+'UDL State PCT'] > -1)& (df[pct+' PCT'] ==-1)), df[pct+'UDL State PCT'], df[pct+' PCT'] )
        df[pct+' PCT'] =  np.where(((df["Surrogate PCT"] == 'Yes') & (df[pct+pct+' Crop National PCT'] > -1)& (df[pct+' PCT'] ==-1)), df[pct+pct+' Crop National PCT'], df[pct+' PCT'] )
        df[pct+' PCT'] =  np.where(((df["Surrogate PCT"] == 'Yes') & (df[pct+'UDL State PCT'] > -1)& (df[pct+' PCT'] ==-1)), df[pct+'UDL State PCT'], df[pct+' PCT'] )
        df[pct+' PCT'] =  np.where(((df["Surrogate PCT"] == 'Yes') & (df[pct+'UDL National PCT'] > -1)& (df[pct+' PCT'] ==-1)), df[pct+'UDL National PCT'], df[pct+' PCT'] )
        # only looks at at Ag for the national max
        df[pct+' PCT'] =  np.where(((df["Surrogate PCT"] == 'Yes') & (df[pct+pct+'National'] > -1)& (df[pct+' PCT'] ==-1)& (df['AgClass'] == 'Ag Composite')), df[pct+pct+'National'], df[pct+' PCT'] )

df.to_csv(os.path.dirname(path) +os.sep +'state_unsurveyed_crops.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)

