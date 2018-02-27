import pandas as pd
import os
import arcpy
import datetime

master_list = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'
overlapping_table = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\Overlaping_species\overlapping_species'

look_up_fc = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\R_Clipped_Union_MAG_20161102.gdb'
look_up_raster = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\SpCompRaster_byProjection\Grids_byProjection\Albers_Conical_Equal_Area'

output_folder = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\Overlaping_species\output'
write_intermediate_date = True
out_path_sp_group_raster = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\Overlaping_species\output-raster'
out_path_overlapping_species = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\Overlaping_species\output_intermediate'

arcpy.env.workspace = look_up_fc
list_fc = arcpy.ListFeatureClasses()

arcpy.env.workspace = look_up_raster
list_raster = arcpy.ListRasters()

species_df = pd.read_csv(master_list)
list_sp = species_df['EntityID'].values.tolist()
sp_array = pd.DataFrame(data=list_sp, columns=['EntityID'])
pixel_l48 = pd.DataFrame(data=list_sp, columns=['EntityID'])
pixel_l48 = pixel_l48.reindex(columns=['EntityID', 'Pixels'])

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')


def raster_tables(in_spe, raster_row, raster):
    raster_row['Value'] = raster_row['Value'].astype(str)
    raster_row['ZoneID'] = raster_row['Value'].map(lambda x: x.replace('.0', '')).astype(str)

    in_spe['ZoneID'] = in_spe['ZoneID'].astype(str)
    in_spe['ZoneID'] = in_spe['ZoneID'].map(lambda x: x.replace('.0', ''))

    col_species = raster_row['ZoneID'].values.tolist()
    zone_array = pd.DataFrame(data=col_species, columns=['ZoneID'])
    new_columns_raster = list(col_species)
    new_columns_raster.insert(0, 'ZoneID')
    zone_array = zone_array.reindex(columns=new_columns_raster)

    for value in col_species:
        count = raster_row.loc[raster_row['ZoneID'] == str(value), 'Count']
        zone_array.loc[zone_array['ZoneID'] == value, value] = count

    in_spe['ZoneSpecies'] = in_spe['ZoneSpecies'].apply(
        lambda x: x.replace('[', '').replace(']', '').replace('u', '').replace(' ', '').replace("'", ""))
    spl = in_spe['ZoneSpecies'].str.split(',', expand=True)
    spl['ZoneID'] = in_spe['ZoneID'].map(lambda x: x.replace(',', '')).astype(str)

    merged_df = pd.merge(zone_array, spl, on='ZoneID', how='left')
    merged_df.drop('ZoneID', axis=1, inplace=True)

    sum_by_ent = melt_df(merged_df)

    transform_df = sum_by_ent.T.reset_index()
    new_columns_transform = transform_df.columns.values
    new_columns_transform[0] = 'ZoneID'
    transform_df.columns = new_columns_transform
    transform_df['ZoneID'] = transform_df['ZoneID'].map(lambda x: str(x)).astype(str)

    merged_df_col = pd.merge(transform_df, spl, on='ZoneID', how='outer')
    merged_df_col.drop('ZoneID', axis=1, inplace=True)

    sum_by_ent_col = melt_df(merged_df_col)
    out_df = sum_by_ent_col.reset_index()
    new_columns_out = out_df.columns.values
    new_columns_out[0] = 'EntityID'
    out_df.columns = new_columns_out
    updated_sp_df = pd.merge(sp_array, out_df, on='EntityID', how='outer')
    if write_intermediate_date:
        sum_by_ent.to_csv(out_path_sp_group_raster + os.sep + raster + '_Sp_by_Zone_' + str(date) + '.csv')
        out_df.to_csv(out_path_sp_group_raster + os.sep + raster + '_Sp_by_Sp_' + str(date) + '.csv')
        updated_sp_df.to_csv(out_path_overlapping_species + os.sep + raster + '_working_array_' + str(date) + '.csv')
    return updated_sp_df


def melt_df(df_melt):
    cols = df_melt.columns.values.tolist()
    id_vars_melt = []
    val_vars = []
    for k in cols:
        val_vars.append(k) if type(k) is long else id_vars_melt.append(k)

    df_melt_row = pd.melt(df_melt, id_vars=id_vars_melt, value_vars=val_vars, var_name='melt_var',
                          value_name='EntityID')

    df_melt_row['EntityID'].fillna('None', inplace=True)
    df_melt_row = df_melt_row.loc[df_melt_row['EntityID'] != 'None']
    df_melt_row.drop('melt_var', axis=1, inplace=True)
    df_melt_row.ix[:, id_vars_melt] = df_melt_row.ix[:, id_vars_melt].apply(pd.to_numeric)
    sum_by_ent = df_melt_row.groupby('EntityID').sum()

    return sum_by_ent


def parse_tables(in_table, in_row_sp, in_col_spe, final_df, csv):
    columns = in_table.columns.values.tolist()
    column_values = [col for col in columns if col.startswith('VALUE')]

    in_table['ZoneID'] = in_table['ZoneID'].map(lambda x: x.replace(',', '')).astype(str)
    in_table['sum'] = in_table[column_values].sum(axis=1)
    in_table_w_values = in_table.loc[in_table['sum'] != 0].copy()
    [in_table_w_values.drop(t, axis=1, inplace=True) for t in in_table_w_values.columns.values.tolist() if
     t.startswith('Unnamed') or t in ['sum', 'OBJECTID']]

    in_row_sp['ZoneID'] = in_row_sp['ZoneID'].map(lambda x: str(x).split('.')[0]).astype(str)
    in_row_sp['ZoneSpecies'] = in_row_sp['ZoneSpecies'].apply(
        lambda x: x.replace('[', '').replace(']', '').replace('u', '').replace(' ', '').replace("'", ""))
    spl = in_row_sp['ZoneSpecies'].str.split(',', expand=True)
    spl['ZoneID'] = in_row_sp['ZoneID'].map(lambda x: x.replace(',', '')).astype(str)

    merged_df = pd.merge(in_table_w_values, spl, on='ZoneID', how='left')
    merged_df.drop('ZoneID', axis=1, inplace=True)
    sum_by_ent = melt_df(merged_df)
    transform_df = sum_by_ent.T.reset_index()

    new_columns_parse = transform_df.columns.values
    new_columns_parse[0] = 'ZoneID'
    transform_df.columns = new_columns_parse
    transform_df['ZoneID'] = transform_df['ZoneID'].map(lambda x: str(x).split('_')[1]).astype(str)

    in_col_spe['ZoneID'] = in_col_spe['ZoneID'].map(lambda x: str(x).split('.')[0]).astype(str)
    in_col_spe['ZoneSpecies'] = in_col_spe['ZoneSpecies'].apply(
        lambda x: x.replace('[', '').replace(']', '').replace('u', '').replace(' ', '').replace("'", ""))
    spl_col = in_col_spe['ZoneSpecies'].str.split(',', expand=True)
    spl_col['ZoneID'] = in_col_spe['ZoneID'].map(lambda x: x.replace(',', '')).astype(str)

    merged_df_col = pd.merge(transform_df, spl_col, on='ZoneID', how='outer')
    merged_df_col.drop('ZoneID', axis=1, inplace=True)

    sum_by_ent = melt_df(merged_df_col)
    out_df = sum_by_ent.reset_index()
    new_columns_col = out_df.columns.values
    new_columns_col[0] = 'EntityID'
    out_df.columns = new_columns_col  # Col wise matrix
    out_df = out_df.loc[out_df['EntityID'].isin(list_sp)]

    updated_sp_df = pd.merge(final_df, out_df, on='EntityID', how='outer')
    updated_sp_df = updated_sp_df.loc[updated_sp_df['EntityID'].isin(list_sp)]
    if write_intermediate_date:
        updated_sp_df.to_csv(
            out_path_overlapping_species + os.sep + csv.replace('.csv', '') + '_working_array_' + str(date) + '.csv')

    return updated_sp_df


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

list_folders = os.listdir(overlapping_table)

for folder in list_folders:
    sp_array.fillna(0, inplace=True)

    print 'Working on folder {0}: completed {2} of {1}'.format(folder, len(list_folders), list_folders.index(folder))

    in_raster = [v for v in list_raster if v.startswith('r_' + folder.split("_")[1])]
    raster_array = arcpy.da.TableToNumPyArray(look_up_raster + os.sep + in_raster[0], ['Value', 'Count'])
    raster_df = pd.DataFrame(data=raster_array)
    c_zones = raster_df['Value'].values.tolist()

    in_fc = [v for v in list_fc if v.startswith('R_' + folder.split("_")[1].title())]
    fc_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + in_fc[0], ['ZoneID', 'ZoneSpecies'])
    fc_df = pd.DataFrame(data=fc_array)
    fc_df = fc_df.loc[fc_df['ZoneID'].isin(c_zones)]

    sp_array = raster_tables(fc_df, raster_df, in_raster[0])  # species in same sp group overlapping with each other
    sp_array.fillna(0, inplace=True)

    list_csv_in = os.listdir(overlapping_table + os.sep + folder)
    list_csv = [csv for csv in list_csv_in if csv.endswith('.csv')]
    for csv in list_csv:
        sp_array.fillna(0, inplace=True)
        parse_nm = csv.split("_")
        col_sp = parse_nm[1]
        row_sp = parse_nm[3].replace('.csv', '')
        overlapping_df = pd.read_csv(overlapping_table + os.sep + folder + os.sep + csv, dtype=object)
        new_columns_overlapping = overlapping_df.columns.values
        new_columns_overlapping[2] = 'ZoneID'
        overlapping_df.columns = new_columns_overlapping

        row_raster = [v for v in list_raster if v.startswith('r_' + csv.split("_")[3].replace('.csv', ''))]
        row_raster_array = arcpy.da.TableToNumPyArray(look_up_raster + os.sep + row_raster[0], ['Value', 'Count'])
        row_raster_df = pd.DataFrame(data=row_raster_array)
        row_zones = row_raster_df['Value'].values.tolist()

        in_row = [v for v in list_fc if v.startswith('R_' + row_sp.title())]
        row_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + in_row[0], ['ZoneID', 'ZoneSpecies'])
        row_df = pd.DataFrame(data=row_array, dtype=object)
        row_df = row_df.loc[row_df['ZoneID'].isin(row_zones)]

        print "  {0}".format(csv)

        in_col = [v for v in list_fc if v.startswith('R_' + col_sp.title())]
        col_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + in_col[0], ['ZoneID', 'ZoneSpecies'])
        col_df = pd.DataFrame(data=col_array, dtype=object)
        col_df = col_df.loc[col_df['ZoneID'].isin(c_zones)]
        sp_array = parse_tables(overlapping_df, row_df, col_df, sp_array, csv)
        sp_array.fillna(0, inplace=True)

    sp_array.to_csv(output_folder + os.sep + 'All_sp_working_array_' + str(date) + '.csv')

raster_array = arcpy.da.TableToNumPyArray(look_up_raster + os.sep + 'r_snails', ['Value', 'Count'])
raster_df = pd.DataFrame(data=raster_array)

in_fc = [v for v in list_fc if v.startswith('R_Snails')]
fc_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + in_fc[0], ['ZoneID', 'ZoneSpecies'])
fc_df = pd.DataFrame(data=fc_array)

sp_array = raster_tables(fc_df, raster_df, 'r_snails')  # species in same sp group overlapping with each other
sp_array.fillna(0, inplace=True)
print 'Cleaning up final output'

transform = sp_array.T.reset_index()
transform.columns = transform.iloc[0]
transform = transform.reindex(transform.index.drop(0))
transform['EntityID'] = transform['EntityID'].map(lambda x: str(x).split('_')[0]).astype(str)
sum_df = transform.groupby('EntityID').sum().reset_index()
new_columns_final = sum_df.columns.values
new_columns_final[0] = 'EntityID'
sum_df.columns = new_columns_final
sum_df = sum_df.loc[sum_df['EntityID'].isin(list_sp)]

sum_df.to_csv(output_folder + os.sep + 'Sp_array_w_self_' + date + '.csv')
for j in list_sp:
    try:
        self = sum_df.loc[sum_df['EntityID'] == str(j), str(j)].iloc[0]
    except (KeyError, IndexError):  #
        self = 0
    pixel_l48.loc[pixel_l48['EntityID'] == str(j), 'Pixels'] = self
    sum_df.loc[sum_df['EntityID'] == str(j), str(j)] = ''

final_cols = sum_df.columns.values.tolist()
final_cols.remove('EntityID')

out_table = sum_df.loc[sum_df['EntityID'].isin(final_cols)]
row_entity = out_table['EntityID'].values.tolist()
row_entity.insert(0, 'EntityID')
out_table = out_table.sort_values(by='EntityID')

pixel_l48['MSQ'] = pixel_l48['Pixels'].multiply(900)
pixel_l48['Acres_L48'] = pixel_l48['MSQ'].multiply(0.000247)

out_table.to_csv(output_folder + os.sep + 'Sp_array_final_' + date + '.csv')
pixel_l48.to_csv(output_folder + os.sep + 'SpeciesPixelCount_' + date + '.csv')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
