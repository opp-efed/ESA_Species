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
out_path = r'L:\ESA\UnionFiles_Winter2018\input tables'

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
    return df_melt_row, id_vars_melt


def min_max_elev(df_melt_row, id_vars_melt):
    # takes any blank rows and fills then with None so they can be filtered
    df_melt_row['EntityID'].fillna('None', inplace=True)
    df_melt_row = df_melt_row.loc[df_melt_row['EntityID'] != 'None']
    df_melt_row.drop('melt_var', axis=1, inplace=True)
    df_melt_row.ix[:, id_vars_melt] = df_melt_row.ix[:, id_vars_melt].apply(pd.to_numeric)  # forces numeric types
    df_melt_row = df_melt_row.loc[df_melt_row[id_vars_melt[0]] != -9990.0]  # filters out the -99990 found in GIS layer
    min_by_ent = df_melt_row.groupby('EntityID').min()  # finds min value for the groupby variable
    max_by_ent = df_melt_row.groupby('EntityID').max()  # finds max value for the groupby variable

    # resets the groupby index and assigns col headers
    min_by_ent = min_by_ent.reset_index()
    min_by_ent.columns = ['EntityID', 'Min Elevation GIS']

    max_by_ent = max_by_ent.reset_index()
    max_by_ent.columns = ['EntityID', 'Max Elevation GIS']

    # merges dfs with the min and max values
    out_elev = pd.merge(min_by_ent, max_by_ent, on='EntityID', how='left')
    return out_elev


def habitat_xwalk(df_melt_row):
    # takes any blank rows and fills then with None so they can be filtered
    df_melt_row['EntityID'].fillna('None', inplace=True)
    df_melt_row = df_melt_row.loc[df_melt_row['EntityID'] != 'None']
    # dropts the col added during melt with the variable anme
    df_melt_row.drop('melt_var', axis=1, inplace=True)
    # # confirms all values are numeric
    # df_melt_row.ix[:, id_vars_melt] = df_melt_row.ix[:, id_vars_melt].apply(pd.to_numeric)
    hab_by_ent = df_melt_row.groupby('EntityID').reset_index()  # creating sequential index
    hab_by_ent["i"] = hab_by_ent.index  # move this index into column
    hab_by_ent["rn"] = hab_by_ent.groupby('EntityID')["i"].rank()  # add row_number inside each value in groupby term
    # pivot results into individual columns, each row list the EntityID follow by all of the habitats
    hab_by_ent.pivot_table(rows='EntityID', cols="rn", values=0)
    return hab_by_ent


def parse_tables(in_table, in_row_sp, col_prefix):
    # Sets data type and replaces extra characters found in the ZoneSpecies field do they are just separted by a comma
    in_table['ZoneID'] = in_table['ZoneID'].map(lambda x: x.replace(',', '')).astype(str)
    in_row_sp['ZoneSpecies'] = in_row_sp['ZoneSpecies'].apply(
        lambda x: x.replace('[', '').replace(']', '').replace('u', '').replace(' ', '').replace("'", ""))

    # EntityIDs in ZoneSpecies are split into their own columns, headers for these fields are number and can be id
    # as type(col) is long
    spl = in_row_sp['ZoneSpecies'].str.split(',', expand=True)
    # Adds the ZoneID field to the spl dataframes
    spl['ZoneID'] = in_row_sp['ZoneID'].map(lambda x: x.replace(',', '')).astype(str)
    # merges table to the split dataframe, now it is in the format needed for the melt
    merged_df = pd.merge(in_table, spl, on='ZoneID', how='left')
    # drops extra columns from the merged tables
    include_col = []
    for t in col_prefix:  # col_prefix ids the associated with data being extracted
        for j in merged_df.columns.values.tolist():
            if j.startswith(t):
                include_col.append(t)

    for col in merged_df.columns.values.tolist():
        if type(col) is long or col in include_col:
            pass
        else:
            merged_df.drop(col, axis=1, inplace=True)
    out_df, id_var_list = melt_df(merged_df)
    return out_df, id_var_list


def merge_to_hucid(table_lookup, spe_table, spe_col, id_cols, join_col):
    for col in id_cols:
        table_lookup[col] = table_lookup[col].map(lambda x: str(x).split('.')[0]).astype(str)

    table_lookup = table_lookup[table_lookup[join_col].isin(spe_col)]
    merg_table = pd.merge(spe_table, table_lookup, on=join_col, how='left')
    zones_in_table = merg_table['ZoneID'].values.tolist()
    return merg_table, zones_in_table


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Get date
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

# empty df for output elevation table
out_elevation = pd.DataFrame(columns=['EntityID', 'Min Elevation GIS', 'Max Elevation GIS'])

for folder in list_dir:
    print folder
    # empty df for regional output habitat table
    out_habitat = pd.DataFrame(columns=['EntityID'])
    region = folder.split('_')[0]  # extracts regions from folder title
    list_csv = os.listdir(in_directory_species_grids + os.sep + folder)  # list of csv in folder
    list_csv = [csv for csv in list_csv if csv.endswith('_att.csv')]  # list of att csvs
    # loops on each csv added the HUCIDs and ZoneIDs from parent fc att table to working dfs, then transforms table
    # so it is entityID by elevation or habitat
    for csv in list_csv:
        # parent fc att table with all input ID field (list_fc_ab) and ZoneID and associated EntityID (list_fc)
        # for species listed in the csv title
        par_zone_fc = [j for j in list_fc_ab if
                       j.startswith(os.path.basename(look_up_fc).split("_")[0] + "_" + csv.split("_")[1].title())]
        zone_fc = [j for j in list_fc if
                   j.startswith(os.path.basename(look_up_fc).split("_")[0] + "_" + csv.split("_")[1].title())]
        # converts att from species fc with ZoneID and association entityID  to dfs
        zone_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + zone_fc[0], ['ZoneID', 'ZoneSpecies'])
        sp_zone_df = pd.DataFrame(data=zone_array, dtype=object)
        sp_zone_df['ZoneID'] = sp_zone_df['ZoneID'].map(lambda x: str(x).split('.')[0]).astype(str)
        # reads in csv to df
        spe_att = pd.read_csv(in_directory_species_grids + os.sep + folder + os.sep + csv)
        # reads in the desire col headers from the look up raster df (raster col header have a limited number of
        # characters)
        col_header = pd.read_csv(
            in_directory_species_grids + os.sep + folder + os.sep + csv.replace('_att.csv', '_lookup_rasters.csv'))
        # makes a list of the current col headers, and if they need be updated based on the look up table makes the
        # updated
        spe_col = spe_att.columns.values.tolist()
        update_col = []

        for col in spe_col:
            if col in col_header['Default output header'].values.tolist():
                new_col = col_header.loc[col_header['Default output header'] == col, 'Desired output header'].iloc[0]
                update_col.append(new_col)
            else:
                update_col.append(col)
        spe_att.columns = update_col

        # find species parent column (values are the hucids), col header will be species raster name, set dtype to str
        parent_id_dem_col = [v for v in spe_att.columns.values.tolist() if v.startswith('ch_') or v.startswith('r_')]
        spe_att[parent_id_dem_col[0]] = spe_att[parent_id_dem_col[0]].map(lambda x: str(x).split('.')[0]).astype(str)
        # add col HUCID mirror from the species parent column in table needed for join
        spe_att['HUCID'] = spe_att[parent_id_dem_col[0]].map(lambda x: str(x).split('.')[0]).astype(str)
        # list of all HUCID values in the species raster col
        c_parent_id = spe_att[parent_id_dem_col[0]].values.tolist()

        # extract columns associated with dem from csv, value, count, species HUCID Value (value in the species raster)
        # col and dem value
        dem_col = [u'VALUE', u'COUNT']
        for col in spe_att:
            if col.startswith('ch_') or col.startswith('r_') or col.startswith('dem'):
                dem_col.append(col)
        dem_df = spe_att[dem_col].copy()

        # converts att from species input fc, with all ID field, into df, captures the ZoneID and HUCID to be joined
        # to working table
        par_zone_array = arcpy.da.TableToNumPyArray(look_up_fc_ab + os.sep + par_zone_fc[0], ['ZoneID', 'HUCID'])
        par_zone_df = pd.DataFrame(data=par_zone_array, dtype=object)
        # merges working table with HUCID field
        merg_dem_par, dem_zones = merge_to_hucid(par_zone_df, dem_df, c_parent_id, ['ZoneID', 'HUCID'], 'HUCID')

        # Filter so on the zone from the current use table is in the working df
        # filters parent species lookup table from FC to just the zones in current table
        sp_zone_dem_df = sp_zone_df[sp_zone_df['ZoneID'].isin(dem_zones)]
        # merges working table with the EntityID from parent lookup table based on the ZoneID
        out_elevation_working, id_var_list = parse_tables(merg_dem_par, sp_zone_dem_df, ['dem'])
        # Converts ZoneID to EntityID and extracts min and max elevation for as species and merges it to working output
        # table from previous csv
        out_elevation_working = min_max_elev(out_elevation_working, id_var_list)
        out_elevation = pd.concat([out_elevation, out_elevation_working])

        # extract columns associated with habitat from csv, value, count, species HUCID Value (value in the species
        # raster) col and dem value
        habitat_col = [u'VALUE', u'COUNT']
        for col in spe_att:
            if col.startswith('ch_') or col.startswith('r_') or col.startswith('Habit') or col.startswith(
                    'gap') or col.startswith('2011'):
                habitat_col.append(col)
        habitat_df = spe_att[habitat_col].copy()

        # # find species parent column (values are the hucids), col header will be species raster name, set dtype to str
        # parent_id_hab_col = [v for v in dem_df.columns.values.tolist() if v.startswith('ch_') or v.startswith('r_')]
        # habitat_df[parent_id_hab_col[0]] = habitat_df[parent_id_hab_col[0]].map(lambda x: str(x).split('.')[0]).astype(
        #     str)
        # # add col HUCID mirror from the species parent column in table needed for join
        # habitat_df['HUCID'] = habitat_df[parent_id_hab_col[0]].map(lambda x: str(x).split('.')[0]).astype(str)
        # c_hab_parent_id = habitat_df[parent_id_hab_col[0]].values.tolist()

        # merges working table with the EntityID from parent lookup table based on the ZoneID
        merg_hab_par, hab_zones = merge_to_hucid(par_zone_df, habitat_df, c_parent_id, ['ZoneID', 'HUCID'], 'HUCID')
         # filters parent species lookup table from FC to just the zones in current table
        sp_zone_hab_df = sp_zone_df[sp_zone_df['ZoneID'].isin(hab_zones)]
        # tranforma ZoneID to EntityID and extracts habitat values for as species and merges it to working output table
        # from previous csv
        out_habitat_working, id_var_list = parse_tables(merg_hab_par, sp_zone_hab_df, ['Habit', 'gap', '2011'])
        out_habitat_working = habitat_xwalk(out_habitat_working)
        out_habitat = pd.concat([out_habitat, out_habitat_working])
    # tranform regional habitat tabe from having species by row to by column
    out_habitat = out_habitat.T
    out_habitat.to_csv(out_path + os.sep + region + "_" + 'species_habitat_classes_' + date + '.csv')

min_by_ent = out_elevation[['EntityID', 'Min Elevation GIS']]
max_by_ent = out_elevation[['EntityID', 'Max Elevation GIS']]

min_by_ent = min_by_ent.groupby('EntityID').min()
max_by_ent = max_by_ent.groupby('EntityID').max()
min_by_ent = min_by_ent.reset_index()
min_by_ent.columns = ['EntityID', 'Min Elevation GIS']

max_by_ent = max_by_ent.reset_index()
max_by_ent.columns = ['EntityID', 'Max Elevation GIS']
out_elev = pd.merge(min_by_ent, max_by_ent, on='EntityID', how='left')

out_elev.to_csv(out_path + os.sep + 'Elevation_Summary_' + date + '.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
