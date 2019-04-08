import pandas as pd
import arcpy
import os
import datetime


look_up_fc= r'L:\Workspace\StreamLine\ESA\UnionFiles_Winter2018\CriticalHabitat\CH_Clipped_Union_20180110.gdb'
out_location= r'L:\Workspace\StreamLine\ESA\UnionFiles_Winter2018\CriticalHabitat\LookUp_CH_Clipped_Union_20180110'

arcpy.env.workspace = look_up_fc
list_fc = arcpy.ListFeatureClasses()


def melt_df(df_melt):
    cols = df_melt.columns.values.tolist()
    id_vars_melt = []# other columns (non EntityID columns)
    val_vars = [] # EntityID columns
    for k in cols:
        val_vars.append(k) if type(k) is long else id_vars_melt.append(k)
    df_melt_row = pd.melt(df_melt, id_vars=id_vars_melt, value_vars=val_vars, var_name='melt_var',
                          value_name='EntityID')

    df_melt_row['EntityID'].fillna('None', inplace=True)
    df_melt_row = df_melt_row.loc[df_melt_row['EntityID'] != 'None']
    df_melt_row.drop('melt_var', axis=1, inplace=True)


    return df_melt_row

def parse_tables( in_row_sp):
    in_row_sp['ZoneID'] = in_row_sp['ZoneID'].map(lambda x: x.replace(',', '')).astype(str)
    in_row_sp['ZoneSpecies'] = in_row_sp['ZoneSpecies'].apply(
        lambda x: x.replace('[', '').replace(']', '').replace('u', '').replace(' ', '').replace("'", ""))

    spl = in_row_sp['ZoneSpecies'].str.split(',', expand=True)
    spl['ZoneID'] = in_row_sp['ZoneID'].map(lambda x: x.replace(',', '')).astype(str)

    merged_df = pd.merge(in_row_sp, spl, on='ZoneID', how='left')
    merged_df.drop('ZoneSpecies', axis=1, inplace=True)

    look_up_by_ent = melt_df(merged_df)

    return look_up_by_ent


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

if not os.path.exists(out_location):
    os.mkdir(out_location)

for fc in list_fc:
    sp_zone_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + fc, ['ZoneID', 'ZoneSpecies'])
    sp_zone_df = pd.DataFrame(data=sp_zone_array, dtype=object)

    sp_zone_df['ZoneID'] = sp_zone_df['ZoneID'].map(lambda x: str(x).split('.')[0]).astype(str)
    out_df = parse_tables(sp_zone_df)
    out_df.to_csv(out_location+os.sep+fc+'.csv')


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)