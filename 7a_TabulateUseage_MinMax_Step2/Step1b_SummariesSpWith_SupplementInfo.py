import pandas as pd
import arcpy
import datetime
import os

in_directory_csv = r'L:\ESA\Results_Usage\L48\Range\Agg_Layers'
out_path = r'L:\ESA\Tabulate_Usage_TabArea'
out_poltical = r'L:\ESA\Tabulate_Usage_TabArea\PolBoundaries'

in_directory_grids = r'L:\ESA\UnionFiles_Winter2018\Range\SpComp_UsageHUCAB_byProjection_2\Grid_byProjections_Combined'
look_up_fc_ab = r'L:\ESA\UnionFiles_Winter2018\Range\R_Clipped_Union_CntyInter_HUC2ABInter_20180612.gdb'
look_up_fc = r'L:\ESA\UnionFiles_Winter2018\Range\R_Clipped_Union_20180110.gdb'

grid_folder_lookup = {'AK': 'AK_WGS_1984_Albers',
                      'AS': 'AS_WGS_1984_UTM_Zone_2S',
                      'CNMI': 'CNMI_WGS_1984_UTM_Zone_55N',
                      'CONUS': 'CONUS_Albers_Conical_Equal_Area',
                      'GU': 'GU_WGS_1984_UTM_Zone_55N',
                      'HI': 'HI_NAD_1983_UTM_Zone_4N',
                      'PR': 'PR_Albers_Conical_Equal_Area',
                      'VI': 'VI_WGS_1984_UTM_Zone_20N'}

elevation_adjustments = r'L:\ESA\UnionFiles_Winter2018\input tables\Elevation_Summary_test.csv'
habitat_adjustment_path = r'L:\ESA\UnionFiles_Winter2018\input tables'
habitat_dict = {'AK': 'AK_Species_habitat_classes_20180624_test.csv',
                'AS': 'AS_Species_habitat_classes_20180624_test.csv',
                'CNMI': 'CNMI_Species_habitat_classes_20180624_test.csv',
                'CONUS': 'CONUS_Species_habitat_classes_20180624_test.csv',
                'GU': 'GU_Species_habitat_classes_20180624_test.csv',
                'HI': 'HI_Species_habitat_classes_20180624_test.csv',
                'PR': 'PR_Species_habitat_classes_20180624_test.csv',
                'VI': 'VI_Species_habitat_classes_20180624_test.csv'}

skip_species = ['r_birds', 'r_clams']

arcpy.env.workspace = look_up_fc
list_fc = arcpy.ListFeatureClasses()
arcpy.env.workspace = look_up_fc_ab
list_fc_ab = arcpy.ListFeatureClasses()
list_dir = os.listdir(in_directory_csv)


def melt_df(df_melt):
    cols = df_melt.columns.values.tolist()
    id_vars_melt = []  # other columns (non EntityID columns)
    val_vars = []  # columns with EntityID
    for k in cols:
        val_vars.append(k) if type(k) is long else id_vars_melt.append(k)
    df_melt_row = pd.melt(df_melt, id_vars=id_vars_melt, value_vars=val_vars, var_name='melt_var',
                          value_name='EntityID')
    df_melt_row['EntityID'].fillna('None', inplace=True)
    if 'None' in df_melt_row['EntityID'].values.tolist():
        df_melt_row = df_melt_row.loc[df_melt_row['EntityID'] != 'None']
    df_melt_row.drop('melt_var', axis=1, inplace=True)

    numeric_cols = []
    group_cols = []

    for r in df_melt_row.columns.values.tolist():
        if r.startswith('VALUE'):
            numeric_cols.append(r)
        else:
            group_cols.append(r)

    df_melt_row.ix[:, numeric_cols] = df_melt_row.ix[:, numeric_cols].apply(pd.to_numeric)

    sum_by_ent = df_melt_row.groupby(group_cols).sum()
    df_out = sum_by_ent.reset_index()

    return df_out


def parse_tables(in_table, in_row_sp, col_pre):
    # Sets data type and replaces extra characters found in the ZoneSpecies field do they are just separated by a comma
    in_table['ZoneID'] = in_table['ZoneID'].map(lambda y: y.replace(',', '')).astype(str)
    c_in_row_sp =  in_row_sp.copy()
    c_in_row_sp .loc[:,['ZoneSpecies']] = c_in_row_sp .loc[:,['ZoneSpecies']].apply(
        lambda d: d.replace('[', '').replace(']', '').replace('u', '').replace(' ', '').replace("'", ""))

    # EntityIDs in ZoneSpecies are split into their own columns, headers for these fields are number and can be id
    # as type(col) is long
    spl = c_in_row_sp ['ZoneSpecies'].str.split(',', expand=True)
    # Adds the ZoneID field to the spl dataframes
    spl['ZoneID'] = c_in_row_sp ['ZoneID'].map(lambda u: u.replace(',', '')).astype(str)
    # merges table to the split dataframe, now it is in the format needed for the melt
    merged_df = pd.merge(in_table, spl, on='ZoneID', how='left')
    # drops extra columns from the merged tables

    for q in merged_df.columns.values.tolist():
        if type(q) is long or q in col_pre or q.startswith('VALUE'):
            pass
        else:
            merged_df.drop(q, axis=1, inplace=True)

    out_df = melt_df(merged_df)
    return out_df


def merge_to_hucid(table_lookup, spe_table, spe_cols, id_cols, join_col):
    for z in id_cols:
        table_lookup[z] = table_lookup[z].map(lambda t: str(t).split('.')[0]).astype(str)

    table_lookup = table_lookup[table_lookup[join_col].isin(spe_table[spe_cols].values.tolist())]
    merg_table = pd.merge(spe_table, table_lookup, on=join_col, how='left')
    zones_in_table = merg_table['ZoneID'].values.tolist()
    return merg_table, zones_in_table


def no_adjust(out_df, final_df, cnty_all, sta_all):
    # Adjust for elevation based on elevation input files

    val_col = [i for i in out_df.columns.values.tolist() if i.startswith('VALUE')]
    val_col.remove('VALUE')

    c_df = out_df[['EntityID']+val_col].copy()
    c_df = c_df.groupby(['EntityID']).sum().reset_index()

    final_df = pd.concat([final_df, c_df])

    w_df = out_df[['EntityID', 'GEOID','STATEFP', 'STUSPS']+val_col].copy()
    w_df['GEOID'] = w_df['GEOID'].map(lambda (n): n).astype(str)
    w_df.ix[:, 'STATEFP'] = w_df.ix[:, 'GEOID'].map(lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)

    df_cnty = w_df.groupby(['EntityID', 'GEOID','STATEFP', 'STUSPS'], as_index=False).sum()
    # cnty_pivot = df_pivot.pivot(index='EntityID', columns='GEOID')['VALUE_0']
    cnty_all= pd.concat([cnty_all, df_cnty])
    col_order = [v for v in df_cnty if v != 'GEOID']

    df_state =  out_df[['EntityID', 'STATEFP', 'STUSPS']+val_col].copy()
    df_state = df_state.groupby(['EntityID', 'STATEFP', 'STUSPS'], as_index=False).sum()
    df_state =  df_state.reindex(columns=col_order)
    sta_all = pd.concat([sta_all, df_state])

    return w_df, final_df, cnty_all, sta_all



def adjust_elevation(out_df, adjust_path, final_df, cnty_all, sta_all):
    # Adjust for elevation based on elevation input files
    val_col = [i for i in out_df.columns.values.tolist() if i.startswith('VALUE')]
    val_col.remove('VALUE')

    dem_col = [v for v in out_df.columns.values.tolist() if v.startswith('dem')]
    out_df.ix[:, dem_col[0]] = out_df.ix[:, dem_col[0]].map(lambda w: w).astype(int)
    adjust_df = pd.read_csv(adjust_path)
    adjust_df.ix[:, 'EntityID'] = adjust_df.ix[:, 'EntityID'].map(lambda r: r).astype(str)
    sp_to_adjust = adjust_df['EntityID'].values.tolist()
    e_working = out_df.loc[~out_df['EntityID'].isin(sp_to_adjust)].copy()
    e_adjust = out_df.loc[out_df['EntityID'].isin(sp_to_adjust)].copy()

    for v in sp_to_adjust:
        if v in e_adjust['EntityID'].values.tolist():
            min_v = adjust_df.loc[(adjust_df['EntityID'] == v, 'Min Elevation GIS')].iloc[0]
            max_v = adjust_df.loc[(adjust_df['EntityID'] == v, 'Max Elevation GIS')].iloc[0]
            w_df = e_adjust.loc[(e_adjust['EntityID'] == v) & (e_adjust[dem_col[0]] <= int(max_v)) & (
                    e_adjust[dem_col[0]] >= int(min_v))].copy()
            e_working = pd.concat([e_working, w_df])

    e_h_working = e_working.copy()
    e_h_working.ix[:, 'GEOID'] = e_h_working.ix[:, 'GEOID'].map(lambda (n): n).astype(str)
    e_h_working.ix[:, 'STATEFP'] = e_h_working.ix[:, 'GEOID'].map(
        lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)

    out_col = ['EntityID']
    out_ele = e_working[out_col +val_col]

    group_col = [v for v in out_ele.columns.values.tolist() if not v.startswith("VALUE")]
    out_ele = (out_ele.groupby(group_col).sum()).reset_index()
    final_df = pd.concat([final_df, out_ele])

    w_df = e_h_working[['EntityID', 'GEOID','STATEFP', 'STUSPS']+val_col].copy()
    w_df['GEOID'] = w_df['GEOID'].map(lambda (n): n).astype(str)
    w_df.ix[:, 'STATEFP'] = w_df.ix[:, 'GEOID'].map(lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)

    df_cnty = w_df.groupby(['EntityID', 'GEOID','STATEFP', 'STUSPS'], as_index=False).sum()
    cnty_all= pd.concat([cnty_all, df_cnty])
    col_order = [v for v in df_cnty if v != 'GEOID']

    df_state =  out_df[['EntityID', 'STATEFP', 'STUSPS']+val_col].copy()
    df_state = df_state.groupby(['EntityID', 'STATEFP', 'STUSPS'], as_index=False).sum()
    df_state =  df_state.reindex(columns=col_order)
    sta_all = pd.concat([sta_all, df_state])

    return e_h_working, final_df , cnty_all, sta_all


def adjust_habitat(adjust_path, out_df, out_final, cnty_all, sta_all):
    # Adjusts for habitat based on habitat input file
    val_col = [i for i in out_df.columns.values.tolist() if i.startswith('VALUE')]
    val_col.remove('VALUE')

    habitat_adjustment = adjust_path
    habitat_df = pd.read_csv(habitat_adjustment, dtype=object)
    sp_to_adjust_h = habitat_df.columns.values.tolist()
    hab_col = [v for v in out_df.columns.values.tolist() if v.startswith('Habit') or v.startswith(
        'gap') or v.startswith('2011')]
    out_df.loc[:,[hab_col[0]]] = out_df.loc[:,[hab_col[0]]].apply(lambda t: t).astype(str)
    h_working = out_df.loc[~out_df['EntityID'].isin(sp_to_adjust_h)].copy()
    h_adjust = out_df.loc[out_df['EntityID'].isin(sp_to_adjust_h)].copy()

    for v in sp_to_adjust_h:
        if v in h_adjust['EntityID'].values.tolist():
            hab_cat = habitat_df[v].values.tolist()
            w_df = h_adjust.loc[(h_adjust['EntityID'] == v) & (h_adjust[hab_col[0]].isin(hab_cat))].copy()
            h_working = pd.concat([h_working, w_df])
    h_working['GEOID'] = h_working['GEOID'].map(lambda (n): n).astype(str)
    h_working['STATEFP'] = h_working['GEOID'].map(lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)

    out_col = ['EntityID']
    out_hab = h_working[out_col +val_col]

    group_col = [v for v in out_hab.columns.values.tolist() if not v.startswith("VALUE")]
    out_hab = (out_hab.groupby(group_col).sum()).reset_index()
    out_final = pd.concat([out_final, out_hab])

    df = h_working[['EntityID', 'GEOID','STATEFP', 'STUSPS']+val_col].copy()
    df['GEOID'] = df['GEOID'].map(lambda (n): n).astype(str)
    df.ix[:, 'STATEFP'] = df.ix[:, 'GEOID'].map(lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)

    df_cnty = df.groupby(['EntityID', 'GEOID','STATEFP', 'STUSPS'], as_index=False).sum()
    cnty_all= pd.concat([cnty_all, df_cnty])
    col_order = [v for v in df_cnty if v != 'GEOID']

    df_state =  out_df[['EntityID', 'STATEFP', 'STUSPS']+val_col].copy()
    df_state = df_state.groupby(['EntityID', 'STATEFP', 'STUSPS'], as_index=False).sum()
    df_state =  df_state.reindex(columns=col_order)
    sta_all = pd.concat([sta_all, df_state])

    return sp_to_adjust_h,  h_working, out_final , cnty_all, sta_all


def adjust_elv_habitat(e_h_working, out_df, hab_sp_adjust, hab_df, out_final, cnty_all, sta_all):
    # Adjusts for elevation and habitat based on input file
    val_col = [i for i in out_df.columns.values.tolist() if i.startswith('VALUE')]
    val_col.remove('VALUE')

    e_h_working = e_h_working.loc[~e_h_working['EntityID'].isin(hab_sp_adjust)].copy()
    e_h_adjust = e_h_working.loc[e_h_working['EntityID'].isin(hab_sp_adjust)].copy()

    hab_col = [v for v in out_df.columns.values.tolist() if v.startswith('Habit') or v.startswith(
        'gap') or v.startswith('2011')]
    out_df[hab_col[0]] = out_df[hab_col[0]].map(lambda t: t).astype(str)
    for v in hab_sp_adjust:
        if v in e_h_adjust['EntityID'].values.tolist():
            hab_cat = hab_df[v].values.tolist()
            w_eh_df = e_h_adjust.loc[(e_h_adjust['EntityID'] == v) & (e_h_adjust[hab_col[0]].isin(hab_cat))].copy()
            e_h_working = pd.concat([e_h_working, w_eh_df])

    out_col = ['EntityID']
    out_hab = e_h_working[out_col +val_col]

    group_col = [v for v in out_hab.columns.values.tolist() if not v.startswith("VALUE")]
    out_hab = (out_hab.groupby(group_col).sum()).reset_index()
    out_final = pd.concat([out_final, out_hab])

    df = e_h_working[['EntityID', 'GEOID','STATEFP', 'STUSPS']+val_col].copy()
    df['GEOID'] = df['GEOID'].map(lambda (n): n).astype(str)
    df.ix[:, 'STATEFP'] = df.ix[:, 'GEOID'].map(lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)

    df_cnty = df.groupby(['EntityID', 'GEOID','STATEFP', 'STUSPS'], as_index=False).sum()
    cnty_all= pd.concat([cnty_all, df_cnty])
    col_order = [v for v in df_cnty if v != 'GEOID']

    df_state =  out_df[['EntityID', 'STATEFP', 'STUSPS']+val_col].copy()
    df_state = df_state.groupby(['EntityID', 'STATEFP', 'STUSPS'], as_index=False).sum()
    df_state =  df_state.reindex(columns=col_order)
    sta_all = pd.concat([sta_all, df_state])
    out_col = ['EntityID', 'GEOID', 'STUSPS']
    [out_col.append(i) for i in out_df.columns.values.tolist() if i.startswith('VALUE')]

    return out_final, cnty_all, sta_all

def export_aquatics(df, df_final):
    # Export information needed for aquatic tables

    # if ['VALUE_0'] in out_df.columns.values.tolist():
    #     val_col = ['EntityID', 'HUC2_AB', 'GEOID', 'STUSPS', 'VALUE_0']
    # else:
    val_col = ['EntityID', 'HUC2_AB', 'GEOID', 'STUSPS']
    [val_col.append(i) for i in df.columns.values.tolist() if i.startswith('VALUE')]
    val_col.remove('VALUE')

    out_aqu = df[val_col].copy()

    out_aqu['GEOID'] = out_aqu['GEOID'].map(lambda (n): n).astype(str)
    out_aqu.ix[:, 'STATEFP'] = out_aqu.ix[:, 'GEOID'].map(
        lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)
    out_aqu.drop('GEOID', axis=1, inplace=True)
    out_aqu = (out_aqu.groupby(['EntityID', 'HUC2_AB', 'STUSPS', 'STATEFP']).sum()).reset_index()
    df_final = pd.concat([df_final, out_aqu])
    return df_final


def split_csv_chucks (in_path):
    chunksize = 100000
    pp = 0
    j = 1
    out_all = pd.DataFrame(columns=[])

    for df in pd.read_csv(in_path , chunksize=chunksize, iterator=True):
        df = df.rename(columns={c: c.replace(' ', '') for c in df.columns})
        df.index += j
        pp += 1
        c_csv = df.copy()
        c_csv['VALUE'] = c_csv['VALUE'].map(lambda k: str(k).split('.')[0]).astype(str)

        # reads in the desire col headers from the look up raster df (raster col header have a limited number
        # of characters) for the parent attribute table
        col_header = pd.read_csv(in_directory_species_grids + os.sep + csv.split("_")[0] + "_" +
                                 csv.split("_")[1] + '_lookup_rasters.csv')
        # makes a list of the current col headers, and if they need be updated based on the look up table
        # then makes the update
        spe_col = spe_att.columns.values.tolist()
        update_col = []

        for col in spe_col:
            if col in col_header['Default output header'].values.tolist():
                new_col = \
                    col_header.loc[
                        col_header['Default output header'] == col, 'Desired output header'].iloc[
                        0]
                update_col.append(new_col)
            else:
                update_col.append(col)
        spe_att.columns = update_col

        # merges the current output file to the parent raster attribute table : HUCID value that can
        # then be joined back to extract state/cnties and species information
        merge_combine = pd.merge(c_csv, spe_att, on='VALUE', how='left')

        # find HUCID parent column ds), col header will be species raster name from parent raster att,
        # set dtype to str
        parent_id_col = [v for v in merge_combine.columns.values.tolist() if
                         v.startswith('ch_') or v.startswith('r_')]
        merge_combine[parent_id_col[0]] = merge_combine[parent_id_col[0]].map(
            lambda g: str(g).split('.')[0]).astype(str)
        col_prefix = []

        for i in update_col:
            if i in ['OID', 'VALUE', 'COUNT', parent_id_col[0]]:
                pass
            else:
                col_prefix.append(i)

        # add col HUCID mirror from the species parent column in table needed for join
        merge_combine['HUCID'] = merge_combine[parent_id_col[0]].map(
            lambda z: str(z).split('.')[0]).astype(str)

        # converts att from species input intersect fc, with all ID field, into df, captures the
        # ZoneID, InterID and HUCID to be joined to working table
        par_zone_array = arcpy.da.TableToNumPyArray(look_up_fc_ab + os.sep + par_zone_fc[0],
                                                    ['ZoneID', 'InterID', 'HUCID', 'GEOID',
                                                     'STUSPS', 'Region',
                                                     'HUC2_AB'])
        par_zone_df = pd.DataFrame(data=par_zone_array, dtype=object)
        for x in ['GEOID', 'STUSPS', 'Region', 'HUC2_AB']:
            col_prefix.append(x)
        # merges working table with HUCID field
        merge_par, sp_zones = merge_to_hucid(par_zone_df, merge_combine, 'HUCID',
                                             ['ZoneID', 'HUCID'], 'HUCID')
        # try:
        # Filter so on the zone from the current use table is in the working df
        # filters parent species lookup table from FC to just the zones in current table
        c_sp_zone_df = sp_zone_df[sp_zone_df['ZoneID'].isin(sp_zones)]
        # merges working table with the EntityID from parent lookup table based on the ZoneID
        out_sp_table = parse_tables(merge_par, c_sp_zone_df, col_prefix)
        if len(out_sp_table) > 0:
            if 'VALUE_0' not in out_sp_table.columns.values.tolist():
                out_sp_table['VALUE_0'] = 0
            out_sp_table.ix[:, ["VALUE_0"]] = out_sp_table.ix[:, ["VALUE_0"]].apply(pd.to_numeric)
            print len (out_sp_table)

            out_sp_table['GEOID'] = out_sp_table['GEOID'].map(lambda (n): n).astype(str)
            out_sp_table['STATEFP'] = out_sp_table['GEOID'].map(
                lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)
            out_all = pd.concat([out_all, out_sp_table])
            j = df.index[-1] + 1
            print('Finish part {0} of table'.format(pp))
        else:
            print '    ****Check overlap tables for above run, no row confirm this is correct***'

    return out_all
        # except:
        #     print 'Failed on {0}'.format(csv)
        #     return pd.DataFrame(columns=[]),pd.DataFrame(columns=[]),pd.DataFrame(columns=[])


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Get date
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

if not os.path.exists(os.path.dirname(out_path)):
    os.mkdir(os.path.dirname(out_path))

if not os.path.exists(out_path):
    os.mkdir(out_path)
for folder in list_dir:
    print folder
    out_folder = out_path + os.sep + folder
    if not os.path.exists(out_folder):
        os.mkdir(out_folder)

    if not os.path.exists(out_poltical):
        os.mkdir(out_poltical)

    if not os.path.exists(out_poltical + os.sep + 'States'):
        os.mkdir(out_poltical + os.sep + 'Counties')
        os.mkdir(out_poltical + os.sep + 'States')

    out_pol_folder_cny = out_poltical + os.sep + 'Counties'
    out_pol_folder_st = out_poltical + os.sep + 'States'

    region = folder.split('_')[0]  # extracts regions from folder title
    in_directory_species_grids = in_directory_grids + os.sep + grid_folder_lookup[region]  # path to region combine fld
    list_csv = os.listdir(in_directory_csv + os.sep + folder)  # list of csv in folder
    list_csv = [csv for csv in list_csv if csv.endswith('.csv')]  # list of att csvs
    # loops on each csv added the HUCIDs and ZoneIDs from parent fc att table to working dfs, then transforms table
    # so it is entityID by elevation or habitat
    for csv in list_csv:
        if csv.split("_")[0] + "_" + csv.split("_")[1] in skip_species:
            continue
        else:
            if not os.path.exists(out_pol_folder_st + os.sep + csv):
                print ("   Working on {0}...table {1} of {2}".format(csv, (list_csv.index(csv) + 1), len(list_csv)))
                # parent fc att table with all input ID field (list_fc_ab) and ZoneID and associated EntityID (list_fc)
                # for species listed in the csv title
                par_zone_fc = [j for j in list_fc_ab if
                               j.startswith(
                                   os.path.basename(look_up_fc).split("_")[0] + "_" + csv.split("_")[1].title())]
                zone_fc = [j for j in list_fc if
                           j.startswith(os.path.basename(look_up_fc).split("_")[0] + "_" + csv.split("_")[1].title())]
                # converts att from species fc with ZoneID and association entityID  to dfs
                zone_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + zone_fc[0], ['ZoneID', 'ZoneSpecies'])
                sp_zone_df = pd.DataFrame(data=zone_array, dtype=object)
                sp_zone_df['ZoneID'] = sp_zone_df['ZoneID'].map(lambda f: str(f).split('.')[0]).astype(str)

                # reads in csv to df for species and the parent raster attribute table for species
                if os.path.exists(in_directory_species_grids + os.sep + csv.split("_")[0] + "_" +
                                  csv.split("_")[1] + '_att.csv'):
                    spe_att = pd.read_csv(
                        in_directory_species_grids + os.sep + csv.split("_")[0] + "_" +
                        csv.split("_")[1] + '_att.csv')
                    spe_att['VALUE'] = spe_att['VALUE'].map(lambda n: str(n).split('.')[0]).astype(str)


                    in_csv_path = in_directory_csv + os.sep + folder + os.sep + csv

                    # out_all, out_all_cnty, out_all_state = split_csv_chucks (in_csv_path)
                    out_all = split_csv_chucks(in_csv_path)

                    # Generates very large files run transforms and summarize to species in memory to keep file size low
                    # out_all.to_csv(out_folder + os.sep + csv)

                    out_habitat = pd.DataFrame(columns=[])
                    out_habitat_cnty = pd.DataFrame(columns=[])
                    out_habitat_state = pd.DataFrame(columns=[])

                    out_ele = pd.DataFrame(columns=[])
                    out_ele_cnty = pd.DataFrame(columns=[])
                    out_ele_state = pd.DataFrame(columns=[])

                    out_hab_ele = pd.DataFrame(columns=[])
                    out_hab_ele_cnty = pd.DataFrame(columns=[])
                    out_hab_ele_state = pd.DataFrame(columns=[])

                    aqu = pd.DataFrame(columns=[])

                    no_adjust_no = pd.DataFrame(columns=[])
                    no_adjust_cnty = pd.DataFrame(columns=[])
                    no_adjust_state = pd.DataFrame(columns=[])
                    value_cols = [v for v in out_all.columns.values.tolist() if v.startswith("VALUE")]
                    out_all.ix[:, value_cols] = out_all.ix[:, value_cols].apply(pd.to_numeric)
                    min_row = 0
                    chunksize = 100000
                    max_row = chunksize
                    # Tables summed the the whole species range can be used to QC other output but are not used as
                    # inputs in any other step
                    while int(len(out_all)) >= int(min_row) and int(len(out_all))>0:
                        loop_sp_table = out_all[(out_all.index >= min_row) & ((out_all.index >= min_row) < max_row)]
                        if not os.path.exists(out_path +os.sep+ 'Agg_Layers' + os.sep+folder):
                            os.mkdir(out_path +os.sep+ 'Agg_Layers' + os.sep+folder)

                        if not os.path.exists(out_path +os.sep+ 'Agg_Layers' + os.sep+folder+os.sep+csv.replace('.csv', '_HUC2AB.csv')):
                            aqu = export_aquatics(loop_sp_table, aqu)
                            aqu.to_csv(out_path +os.sep+ 'Agg_Layers' + os.sep+folder+os.sep+ csv.replace('.csv', '_HUC2AB.csv'))
                            print '  Exported {0}'.format(out_path +os.sep+ 'Agg_Layers' + os.sep+folder+os.sep+ csv.replace('.csv', '_HUC2AB.csv'))

                        if not os.path.exists(out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv')):
                            no_adjust_working, no_adjust_no, no_adjust_cnty, no_adjust_state = no_adjust(loop_sp_table, no_adjust_no, no_adjust_cnty, no_adjust_state)
                            no_adjust_no.to_csv(out_path +os.sep+ 'Agg_Layers' + os.sep+folder+ os.sep + csv.replace('.csv', '_noadjust.csv'))
                            no_adjust_cnty.to_csv(out_pol_folder_cny + os.sep + csv.replace('.csv', '_noadjust.csv'))
                            no_adjust_state.to_csv(out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv'))
                            print '  Exported {0}'.format(out_pol_folder_st + os.sep + csv.replace('.csv', '_noadjust.csv'))

                        if not os.path.exists(out_pol_folder_st + os.sep + csv.replace('.csv', '_adjEle.csv')):
                            elev_hab_working, out_ele, out_ele_cnty, out_ele_state = adjust_elevation(loop_sp_table, elevation_adjustments, out_ele, out_ele_cnty, out_ele_state)
                            out_ele.to_csv(out_path +os.sep+ 'Agg_Layers' + os.sep+folder+ os.sep + csv.replace('.csv', '_adjEle.csv'))
                            out_ele_cnty.to_csv(out_pol_folder_cny + os.sep + csv.replace('.csv', '_adjEle.csv'))
                            out_ele_state.to_csv(out_pol_folder_st + os.sep + csv.replace('.csv', '_adjEle.csv'))
                            print '  Exported {0}'.format(out_pol_folder_st + os.sep + csv.replace('.csv', '_adjEle.csv'))
                        else:
                            elev_hab_working, out_ele, out_ele_cnty, out_ele_state = adjust_elevation(loop_sp_table, elevation_adjustments, out_ele, out_ele_cnty, out_ele_state)

                        if region != 'AK':
                            if not os.path.exists(out_pol_folder_st + os.sep + csv.replace('.csv', '_adjHab.csv')):
                                hab_sp_adjust, hab_df, out_habitat, out_habitat_cnty, out_habitat_state = adjust_habitat(habitat_adjustment_path + os.sep + habitat_dict[region],loop_sp_table, out_habitat, out_habitat_cnty, out_habitat_state)
                                out_habitat.to_csv(out_path +os.sep+ 'Agg_Layers' + os.sep+folder+ os.sep + csv.replace('.csv', '_adjHab.csv'))
                                out_habitat_cnty.to_csv(out_pol_folder_cny + os.sep + csv.replace('.csv', '_adjHab.csv'))
                                out_habitat_state.to_csv(out_pol_folder_st + os.sep + csv.replace('.csv', '_adjHab.csv'))
                                print '  Exported {0}'.format(out_pol_folder_st + os.sep + csv.replace('.csv', '_adjHab.csv'))
                            else:
                                hab_sp_adjust, hab_df, out_habitat, out_habitat_cnty, out_habitat_state = adjust_habitat(habitat_adjustment_path + os.sep + habitat_dict[region],loop_sp_table, out_habitat, out_habitat_cnty, out_habitat_state)

                            if not os.path.exists(out_pol_folder_st + os.sep + csv.replace('.csv', '_adjEleHaB.csv')):
                                out_hab_ele, out_hab_ele_cnty, out_hab_ele_state = adjust_elv_habitat(elev_hab_working,loop_sp_table,hab_sp_adjust, hab_df,out_hab_ele,out_hab_ele_cnty,out_hab_ele_state)
                                out_hab_ele.to_csv(out_path +os.sep+ 'Agg_Layers' + os.sep+folder+ os.sep + csv.replace('.csv', '_adjEleHab.csv'))
                                out_hab_ele_cnty.to_csv(out_pol_folder_cny + os.sep + csv.replace('.csv', '_adjEleHab.csv'))
                                out_hab_ele_state.to_csv(out_pol_folder_cny + os.sep + csv.replace('.csv', '_adjEleHab.csv'))
                                print '  Exported {0}'.format(out_pol_folder_st + os.sep + csv.replace('.csv', '_adjEleHaB.csv'))
                        min_row = max_row
                        max_row = max_row + chunksize

            else:
                print ('Already created all tables, the last table is: {0}'.format((out_pol_folder_st + os.sep + csv)))

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)

# example of DELETED PIVOTS
# df_pivot_cnty = out_sp_table[['EntityID', 'GEOID', 'VALUE_0']].copy()
# df_pivot_cnty.ix[:, ["VALUE_0"]] = df_pivot_cnty.ix[:, ["VALUE_0"]].apply(pd.to_numeric)
# df_pivot_cnty = df_pivot_cnty.groupby(['EntityID', 'GEOID'], as_index=False).sum()
# cnty_pivot_w = df_pivot_cnty.pivot(index='EntityID', columns='GEOID')['VALUE_0']
# out_all_cnty = pd.concat([out_all_cnty, cnty_pivot_w])
#
#
# df_pivot_st = out_sp_table[['EntityID', 'STATEFP', 'VALUE_0']].copy()
# df_pivot_st.ix[:, ["VALUE_0"]] = df_pivot_st.ix[:, ["VALUE_0"]].apply(pd.to_numeric)
# df_pivot_st = df_pivot_st.groupby(['EntityID', 'STATEFP'], as_index=False).sum()
# sta_pivot = df_pivot_st.pivot(index='EntityID', columns='STATEFP')['VALUE_0']
# out_all_state = pd.concat([out_all_state, sta_pivot])