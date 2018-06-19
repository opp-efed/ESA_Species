import pandas as pd
import arcpy
import datetime
import os


today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()


in_directory_species_grids = r'D:\ESA\UnionFiles_Winter2018\CriticalHabitat\SpComp_UsageHUCAB_byProjection\Grid_byProjections_Combined'
look_up_fc_ab = r'D:\ESA\UnionFiles_Winter2018\CriticalHabitat\CH_Clipped_Union_CntyInter_HUC2ABInter_20180612.gdb'
look_up_fc = r'D:\ESA\UnionFiles_Winter2018\CriticalHabitat\CH_SpGroup_Union_final_20180110.gdb\CH_Mammals_Union_20180110'
raster_layer_libraries = r'D:\Workspace\UseSites\ByProjection'

arcpy.env.workspace = look_up_fc
list_fc = arcpy.ListFeatureClasses()
arcpy.env.workspace = look_up_fc_ab
list_fc_ab = arcpy.ListFeatureClasses()
list_dir = os.listdir(in_directory_species_grids)

def melt_df(df_melt, id_col):
    cols = df_melt.columns.values.tolist()
    id_vars_melt = []
    val_vars = []
    for k in cols:
        val_vars.append(k) if type(k) is long else id_vars_melt.append(k)
    df_melt_row = pd.melt(df_melt, id_vars=id_vars_melt, value_vars=val_vars, var_name='melt_var',
                          value_name=id_col)

    df_melt_row[id_col].fillna('None', inplace=True)
    df_melt_row = df_melt_row.loc[df_melt_row[id_col] != 'None']
    df_melt_row.drop('melt_var', axis=1, inplace=True)
    df_melt_row.ix[:, id_vars_melt] = df_melt_row.ix[:, id_vars_melt].apply(pd.to_numeric)
    sum_by_ent = df_melt_row.groupby('id_col').sum()

    return sum_by_ent

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
            if col.startswith ('ch_') or col.startswith ('r_') or col.startswith ('dem'):
                dem_col.append(col)

        dem_df = spe_att[dem_col].copy()
        parent_id_col =[v for v in dem_df.columns.values.tolist() if v.startswith ('ch_') or v.startswith ('r_') ]

        dem_df [parent_id_col[0]] = dem_df [parent_id_col[0]].map(lambda x: str(x).split('.')[0]).astype(str)
        dem_df ['HUCID'] = dem_df [parent_id_col[0]].map(lambda x: str(x).split('.')[0]).astype(str)
        c_parent_id = dem_df [parent_id_col[0]].values.tolist()


        par_zone_fc = [j for j in list_fc_ab if j.startswith(os.path.basename(look_up_fc).split("_")[0] +"_"+ csv.split("_")[1].title())]
        par_zone_array = arcpy.da.TableToNumPyArray(look_up_fc_ab + os.sep + par_zone_fc[0], ['ZoneID','HUCID'])
        par_zone_df = pd.DataFrame(data=par_zone_array, dtype=object)
        par_zone_df['ZoneID'] = par_zone_df['ZoneID'].map(lambda x: str(x).split('.')[0]).astype(str)
        par_zone_df['HUCID'] = par_zone_df['HUCID'].map(lambda x: str(x).split('.')[0]).astype(str)
        par_zone_df = par_zone_df[par_zone_df['HUCID'].isin(c_parent_id)]
        merg_dem_par = pd.merge(dem_df,par_zone_df,on='HUCID',how='left')
        merg_dem_par = melt_df(merg_dem_par, 'HUCID')
        print par_zone_df.head()

        # c_zones = par_zone_df['ZoneID'].values.tolist()
        # sp_zone_fc = [j for j in list_fc if j.startswith(os.path.basename(look_up_fc).split("_")[0] +"_"+ csv.split("_")[1].title())]
        # sp_zone_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + sp_zone_fc[0], ['ZoneID','ZoneSpecies'])
        # sp_zone_df = pd.DataFrame(data=sp_zone_array, dtype=object)
        # sp_zone_df['ZoneID'] = sp_zone_df['ZoneID'].map(lambda x: str(x).split('.')[0]).astype(str)
        # sp_zone_df = sp_zone_df[sp_zone_df['ZoneID'].isin(c_zones)]
        #
        #



