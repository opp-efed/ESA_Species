import pandas as pd
import os
import datetime

in_table = r'E:\Tabulated_NewComps\NL48\AG\CriticalHabitat\CH_NL48_SprayInterval_20170103_All.csv'
out_tables =r'E:'
regions = ['AK', 'GU', 'HI', 'AS', 'PR', 'VI', 'CONUS', 'CNMI']

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

outfiles ={0 :'RawUse.csv',
           305: 'Ground_SprayDrift.csv',
           765: 'Aerial_SprayDrift.csv'}
df_in = pd.read_csv(in_table)
spe_info = df_in.iloc[:,0:5]
bins = [0, 305, 765]
list_col = df_in.columns.values.tolist()
list_use =[]

def summary_tables_check (in_df, uses, distance, out_df):
    col_header = uses +"_"+str(distance)
    out_df[col_header] =  in_df.ix[:,col_header].map(lambda x: True if x > 0  else False)
    return out_df


for value in list_col:
    split_value = value.split("_")
    if split_value[0] not in regions:
        pass
    else:
        use = split_value[0]+ "_"+split_value[1]
        if use not in list_use:
            list_use.append(use)
    list_use = sorted(list_use)



for distances in bins:
    df_out = pd.DataFrame()
    for use in list_use:
        df_out =summary_tables_check(df_in,use,distances, df_out)
    out_csv = out_tables + os.sep + outfiles[distances]
    df_final = pd.concat([spe_info,df_out],axis=1)
    df_final.to_csv(out_csv)


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
