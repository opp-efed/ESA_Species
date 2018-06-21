import pandas as pd
import arcpy
import datetime
import os


today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()


in_directory_species_grids = r'L:\ESA\UnionFiles_Winter2018\Range\SpComp_UsageHUCAB_byProjection_2' \
                             r'\Grid_byProjections_Combined'
look_up_fc_ab = r'L:\ESA\UnionFiles_Winter2018\Range\R_Clipped_Union_CntyInter_HUC2ABInter_20180612.gdb'
look_up_fc = r'L:\ESA\UnionFiles_Winter2018\Range\R_Clipped_Union_20180110.gdb'
out_path = r'L:\ESA\UnionFiles_Winter2018\input tables\Elevation_Summary.csv'
out_path_2 = r'L:\ESA\UnionFiles_Winter2018\input tables\Elevation_Summary_b.csv'


arcpy.env.workspace = look_up_fc
list_fc = arcpy.ListFeatureClasses()
arcpy.env.workspace = look_up_fc_ab
list_fc_ab = arcpy.ListFeatureClasses()
list_dir = os.listdir(in_directory_species_grids)

def melt_df(df_melt):
    cols = df_melt.columns.values.tolist()
    id_vars_melt = []  # other columns (non EntityID columns)
    val_vars = []  # columns with EntityID
    for k in cols:
        val_vars.append(k) if type(k) is long else id_vars_melt.append(k)
    df_melt_row = pd.melt(df_melt, id_vars=id_vars_melt, value_vars=val_vars, var_name='melt_var',
                          value_name='EntityID')

    df_melt_row['EntityID'].fillna('None', inplace=True)
    df_melt_row = df_melt_row.loc[df_melt_row['EntityID'] != 'None']
    df_melt_row.drop('melt_var', axis=1, inplace=True)
    df_melt_row.ix[:, id_vars_melt] = df_melt_row.ix[:, id_vars_melt].apply(pd.to_numeric)
    df_melt_row = df_melt_row.loc[df_melt_row[id_vars_melt[0]] != -9990.0]
    min_by_ent = df_melt_row.groupby('EntityID').min()
    max_by_ent = df_melt_row.groupby('EntityID').max()
    min_by_ent = min_by_ent.reset_index()
    min_by_ent.columns = ['EntityID','Min Elevation GIS']


    max_by_ent = max_by_ent.reset_index()
    max_by_ent.columns = ['EntityID','Max Elevation GIS']
    out_elev =pd.merge(min_by_ent,max_by_ent,on='EntityID',how='left')
    return out_elev


def parse_tables(in_table, in_row_sp):
    in_table['ZoneID'] = in_table['ZoneID'].map(lambda x: x.replace(',', '')).astype(str)
    in_row_sp['ZoneSpecies'] = in_row_sp['ZoneSpecies'].apply(
        lambda x: x.replace('[', '').replace(']', '').replace('u', '').replace(' ', '').replace("'", ""))
    # Entity are split into their own columns, headers are number and can be id as type(col) is long
    spl = in_row_sp['ZoneSpecies'].str.split(',', expand=True)
    spl['ZoneID'] = in_row_sp['ZoneID'].map(lambda x: x.replace(',', '')).astype(str)

    merged_df = pd.merge(in_table, spl, on='ZoneID', how='left')


    for col in merged_df.columns.values.tolist():
        if type(col) is long or col.startswith('dem'):
            pass
        else:
            merged_df.drop(col, axis=1, inplace=True)

    out_elevation = melt_df(merged_df)


    return out_elevation

out_elevation = pd.DataFrame(columns=['EntityID','Min Elevation GIS','Max Elevation GIS'])
for folder in list_dir:
    region = folder.split('_')[0]
    list_csv = os.listdir(in_directory_species_grids+os.sep+folder)

    list_csv = [csv for csv in list_csv if csv.endswith('_att.csv')]
    for csv in list_csv:

        spe_att = pd.read_csv(in_directory_species_grids+os.sep+folder+os.sep+csv)
        col_header = pd.read_csv(in_directory_species_grids+os.sep+folder+os.sep+csv.replace('_att.csv','_lookup_rasters.csv'))
        spe_col = spe_att.columns.values.tolist()
        update_col = []

        for col in spe_col:
            if col in col_header['Default output header'].values.tolist():
                new_col = col_header.loc[col_header['Default output header'] == col, 'Desired output header'].iloc[0]
                update_col.append(new_col)
            else:
                update_col.append(col)
        spe_att.columns = update_col
        dem_col = [u'VALUE', u'COUNT']
        for col in spe_att:
            if col.startswith('ch_') or col.startswith('r_') or col.startswith('dem'):
                dem_col.append(col)

        dem_df = spe_att[dem_col].copy()
        parent_id_col = [v for v in dem_df.columns.values.tolist() if v.startswith('ch_') or v.startswith('r_')]

        dem_df[parent_id_col[0]] = dem_df[parent_id_col[0]].map(lambda x: str(x).split('.')[0]).astype(str)
        dem_df['HUCID'] = dem_df[parent_id_col[0]].map(lambda x: str(x).split('.')[0]).astype(str)
        c_parent_id = dem_df[parent_id_col[0]].values.tolist()

        par_zone_fc = [j for j in list_fc_ab if
                       j.startswith(os.path.basename(look_up_fc).split("_")[0] + "_" + csv.split("_")[1].title())]
        zone_fc = [j for j in list_fc if
                   j.startswith(os.path.basename(look_up_fc).split("_")[0] + "_" + csv.split("_")[1].title())]

        par_zone_array = arcpy.da.TableToNumPyArray(look_up_fc_ab + os.sep + par_zone_fc[0], ['ZoneID', 'HUCID'])
        par_zone_df = pd.DataFrame(data=par_zone_array, dtype=object)
        par_zone_df['ZoneID'] = par_zone_df['ZoneID'].map(lambda x: str(x).split('.')[0]).astype(str)
        par_zone_df['HUCID'] = par_zone_df['HUCID'].map(lambda x: str(x).split('.')[0]).astype(str)
        par_zone_df = par_zone_df[par_zone_df['HUCID'].isin(c_parent_id)]
        merg_dem_par = pd.merge(dem_df, par_zone_df, on='HUCID', how='left')
        c_zones = merg_dem_par['ZoneID'].values.tolist()

        zone_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + zone_fc[0], ['ZoneID', 'ZoneSpecies'])
        sp_zone_df = pd.DataFrame(data=zone_array, dtype=object)
        sp_zone_df['ZoneID'] = sp_zone_df['ZoneID'].map(lambda x: str(x).split('.')[0]).astype(str)
        # Filter so on the zone from the current use table is in the working df
        sp_zone_df = sp_zone_df[sp_zone_df['ZoneID'].isin(c_zones)]
        out_elevation_working = parse_tables(merg_dem_par , sp_zone_df)
        out_elevation = pd.concat([out_elevation,out_elevation_working])
        print out_elevation
out_elevation.to_csv(out_path_2)
min_by_ent =out_elevation[['EntityID','Min Elevation GIS']]
max_by_ent = out_elevation[['EntityID','Max Elevation GIS']]

min_by_ent = min_by_ent.groupby('EntityID').min()
max_by_ent = max_by_ent.groupby('EntityID').max()
min_by_ent = min_by_ent.reset_index()
min_by_ent.columns = ['EntityID', 'Min Elevation GIS']

max_by_ent = max_by_ent.reset_index()
max_by_ent.columns = ['EntityID', 'Max Elevation GIS']
out_elev = pd.merge(min_by_ent, max_by_ent, on='EntityID', how='left')

out_elev.to_csv(out_path)
