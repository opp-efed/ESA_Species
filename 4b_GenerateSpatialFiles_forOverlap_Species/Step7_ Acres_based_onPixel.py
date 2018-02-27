import datetime
import os
import arcpy

import pandas as pd

# Title- Generate overlap tables from zone species rasters to use layers results;
#               1) Generates tables for aggregated layers, AA, Ag and NonAG
#                       1a) The final merged output are used to generate distance interval table for spray drift; and
#                           summarized BE table (0, 305m and 765)
# TODO Add look to read in vector table once vector overlap final

# Static variables are updated once per update; user input variables update each  run

# ASSUMPTIONS
# Species group is found in index position 1 of all input result tables when split by '_'
# All raster are 30 meter cells - note previously VI and CNMI has some use with a different cell size

in_raster_location = r'L:\ESA\UnionFiles_Winter2018\Range\SpCompRaster_byProjection\Grids_byProjection'
outpath= r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA\_ED_results\Acres_by_Pixels'
# ###############user input variables
master_list = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - OverlapTables' \
              r'\MasterListESA_Feb2017_20180110.csv'

col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename', 'Range_Filename', 'L48/NL48']

look_up_fc = r'L:\ESA\UnionFiles_Winter2018\Range\R_Clipped_Union_20180110.gdb'
list_folder = os.listdir(in_raster_location)


today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

arcpy.env.workspace = look_up_fc
list_fc = arcpy.ListFeatureClasses()

species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]


# ###Functions


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        # print "created directory {0}".format(dbf_dir)


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


def parse_tables(in_table, in_row_sp):
    in_row_sp['ZoneID'].map(lambda x: x).astype(str)

    in_row_sp['ZoneSpecies'] = in_row_sp['ZoneSpecies'].apply(
        lambda x: x.replace('[', '').replace(']', '').replace('u', '').replace(' ', '').replace("'", ""))
    spl = in_row_sp['ZoneSpecies'].str.split(',', expand=True)
    spl['ZoneID'] = in_row_sp['ZoneID'].map(lambda x: str(x).split('.')[0]).astype(str)
    print spl
    merged_df = pd.merge(in_table, spl, on='ZoneID', how='left')
    merged_df.drop('ZoneID', axis=1, inplace=True)

    sum_by_ent = melt_df(merged_df)
    df_out = sum_by_ent.reset_index()
    new_columns_col = df_out.columns.values.tolist()
    new_columns_col[0] = 'EntityID'
    df_out.columns = new_columns_col
    print df_out

    return df_out



start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

for folder in list_folder:
    arcpy.env.workspace = in_raster_location + os.sep + folder
    outlocation = outpath +os.sep + folder
    list_raster = arcpy.ListRasters()
    create_directory(outlocation)
    for raster in list_raster:
        raster_array = arcpy.da.TableToNumPyArray(in_raster_location + os.sep + raster, ['VALUE', 'COUNT'])
        raster_zone_df = pd.DataFrame(data=raster_array, dtype=object)
        raster_zone_df['ZoneID'] = raster_zone_df['VALUE'].map(lambda x: str(x).split('.')[0]).astype(str)
        sp_zone_fc = [j for j in list_fc if j.startswith(raster.split("_")[0].title()+ "_" + raster.split("_")[1].title())]

        sp_zone_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + sp_zone_fc[0], ['ZoneID', 'ZoneSpecies'])
        sp_zone_df = pd.DataFrame(data=sp_zone_array, dtype=object)

        use_array = parse_tables(raster_zone_df, sp_zone_df)
        use_array['MSQ'] = use_array['COUNT'].map(lambda x: int(x) * 900)
        use_array['Acres'] = use_array['MSQ'].map(lambda x: int(x) * 0.000247)


        use_array.to_csv(outlocation+os.sep+raster+'.csv')
        # interval by species


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
