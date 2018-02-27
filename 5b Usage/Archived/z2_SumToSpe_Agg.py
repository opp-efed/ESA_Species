import datetime
import os
import arcpy

import pandas as pd

overwrite_inter_data = False

master_list = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              r'\_ExternalDrive\_CurrentSupportingTables\MasterLists\MasterListESA_Feb2017_20170410_b.csv'
col_include_output = ['EntityID', 'Common Name', 'Scientific Name', 'Status', 'pop_abbrev', 'family', 'Lead Agency',
                      'Group', 'Des_CH', 'CH_GIS', 'Source of Call final BE-Range', 'WoE Summary Group',
                      'Source of Call final BE-Critical Habitat', 'Critical_Habitat_', 'Migratory', 'Migratory_',
                      'CH_Filename','Range_Filename']

in_sum_file = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              r'\_ExternalDrive\_CurrentSpeciesSpatialFiles\Boundaries.gdb\Counties_all_overlap_albers'

raw_results_csv = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
                  r'\_ExternalDrive\_CurrentResults\Tabulated_usage_clipped\L48\Range\Agg_Layers'
find_file_type = raw_results_csv.split(os.sep)

if 'Range' in find_file_type or 'range' in find_file_type:

    look_up_fc = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\_MovedOneDrive\Union\Range' \
                 r'\R_Clipped_Union_MAG_20161102.gdb'

    file_type = 'R_'
    species_file_type = 'Range'
else:
    look_up_fc = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\CriticalHabitat' \
                 r'\CH_Clipped_Union_MAG_20161102.gdb'

    species_file_type = 'CriticalHabitat'
    file_type = "CH_"

in_acres_list = [
    r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\_MovedOneDrive\Tables\CH_Acres_by_region_20170208.csv',
    r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\_MovedOneDrive\Tables\R_Acres_by_region_20171204.csv']

# ## Static variables
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

array = arcpy.da.TableToNumPyArray(in_sum_file, [u'STUSPS', u'Region', u'GEOID'], skip_nulls=True)
df = pd.DataFrame(array)
list_geoid = df['GEOID'].values.tolist()

out_root = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
           r'\_ExternalDrive\_CurrentResults\tabulated_usage_byspecies_clipped' + os.sep + 'L48' + os.sep \
           + species_file_type + os.sep + 'Agg_Layers'

in_acres_table = ''
for v in in_acres_list:
    path, tail = os.path.split(v)
    if species_file_type == 'Range' and (tail[0] == 'R' or tail[0] == 'r'):
        in_acres_table = v
    elif species_file_type == 'CriticalHabitat' and (tail[0] == 'C' or tail[0] == 'C'):
        in_acres_table = v
    else:
        pass

arcpy.env.workspace = look_up_fc
list_fc = arcpy.ListFeatureClasses()

species_df = pd.read_csv(master_list, dtype=object)
[species_df.drop(m, axis=1, inplace=True) for m in species_df.columns.values.tolist() if m.startswith('Unnamed')]
base_sp_df = species_df.loc[:, col_include_output]
master_df = pd.read_csv(master_list)
out_df_template = master_df['EntityID'].copy()

list_sp = species_df['EntityID'].values.tolist()
acres_df = pd.read_csv(in_acres_table)
acres_df['EntityID'] = acres_df['EntityID'].astype(str)


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
    in_table['ZoneID'] = in_table['ZoneID'].map(lambda x: x.replace(',', '')).astype(str)
    in_row_sp['ZoneSpecies'] = in_row_sp['ZoneSpecies'].apply(
        lambda x: x.replace('[', '').replace(']', '').replace('u', '').replace(' ', '').replace("'", ""))
    spl = in_row_sp['ZoneSpecies'].str.split(',', expand=True)
    spl['ZoneID'] = in_row_sp['ZoneID'].map(lambda x: x.replace(',', '')).astype(str)

    merged_df = pd.merge(in_table, spl, on='ZoneID', how='left')
    merged_df.drop('ZoneID', axis=1, inplace=True)

    sum_by_ent = melt_df(merged_df)
    df_out = sum_by_ent.reset_index()
    new_columns_col = df_out.columns.values.tolist()
    new_columns_col[0] = 'EntityID'
    df_out.columns = new_columns_col

    return df_out


def use_by_species(use_df, sp_group_abb):
    # removed all extraneous columns only import columns are the VALUE_[zoneID] and default Label col from tool
    # export
    [use_df.drop(j, axis=1, inplace=True) for j in use_df.columns.values.tolist() if j.startswith("Unnamed")]
    # transform table so it is zones by distance interval; rest index; update column header and remove 'VALUE' form
    # zone ID- value term is the default output of the tool

    sp_zone_fc = [j for j in list_fc if j.startswith(file_type + sp_group_abb.title())]
    sp_zone_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + sp_zone_fc[0], ['ZoneID', 'ZoneSpecies'])
    sp_zone_df = pd.DataFrame(data=sp_zone_array, dtype=object)
    sp_zone_df['ZoneID'] = sp_zone_df['ZoneID'].map(lambda x: str(x).split('.')[0]).astype(str)
    # Filter so on the zone from the current use table is in the working df
    c_zones = use_df['ZoneID'].values.tolist()
    sp_zone_df = sp_zone_df[sp_zone_df['ZoneID'].isin(c_zones)]

    return use_df, sp_zone_df, c_zones


def collapse_csv(list_of_csv, out_df, dis_type):
    for table in list_of_csv:
        # load outside function due to double _ added filename by default w. arcpy- corrected moving forward
        df_use = pd.read_csv(raw_results_csv + os.sep + folder + os.sep + 'Counties' + os.sep + table, dtype=object)
        table = table.replace('__', '_')  # error correction for file names with double _ in name- remove in future

        # Step 1: Sum by species- convert the zoneIDs to the species in the zones, then sum so there is one value per
        # species

        print '   Summing tables by species...species group:{0}, {1}'.format(table.split('_')[1], dis_type)
        use_df_transformed, sp_zone_df_fc, zones = use_by_species(df_use, table.split('_')[1])

        if len(zones) == 0:  # no zones in the raw output table no overlap; 0 for these species add at end
            pass
        else:
            use_array = parse_tables(use_df_transformed, sp_zone_df_fc)
            use_array = use_array.reindex(columns=list_geoid)
            out_df = pd.concat([out_df, use_array])
    out_df.fillna(0, inplace=True)

    return out_df


def collapse_state(df, dis_type):
    print '   Collapsing by state: {0}, {1}'.format(folder, dis_type)
    out_df_state = df.T.reset_index()
    out_df_state.columns = out_df_state.iloc[0]
    out_df_state = out_df_state.reindex(out_df_state.index.drop(0))

    out_df_state['STUSPS'] = out_df_state['EntityID'].map(lambda x: str(x)[:2]).astype(str)
    out_df_state.drop('EntityID', axis=1, inplace=True)
    sum_by_state = out_df_state.groupby('STUSPS').sum()

    df_out_state_t = sum_by_state.T
    df_out_state_t_reindex = df_out_state_t.reset_index()
    final_columns = df_out_state_t_reindex.columns.values.tolist()
    del final_columns[0]

    final_columns.insert(0, 'EntityID')
    df_out_state_t_reindex.columns = final_columns
    return df_out_state_t_reindex


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_directory(os.path.dirname(os.path.dirname(out_root)))
create_directory(os.path.dirname(out_root))
create_directory(out_root)
for folder in os.listdir(raw_results_csv):
    s_out_folder = out_root + os.sep + folder
    create_directory(s_out_folder)

    list_geoid.insert(0, 'EntityID')
    s_out_use_sum_folder = s_out_folder + os.sep + 'Counties'
    create_directory(s_out_use_sum_folder)
    s_results_folder = raw_results_csv
    out_folder_sum = s_out_use_sum_folder
    create_directory(out_folder_sum)

    print '\nWorking on {0}: {1} of {2}'.format(folder, folder, 'Counties')
    # set up list of result csv files
    list_csv = os.listdir(raw_results_csv + os.sep + folder + os.sep + 'Counties')

    list_csv = [csv for csv in list_csv if csv.endswith('.csv')]
    direct = [csv for csv in list_csv if csv.endswith('euc.csv')]
    ground = [csv for csv in list_csv if csv.endswith('ground.csv')]
    aerial = [csv for csv in list_csv if csv.endswith('aerial.csv')]

    out_use_pixel_by_species = out_folder_sum + os.sep + folder + '.csv'
    out_use_pixel_by_species_ground = out_folder_sum + os.sep + folder + '_ground.csv'
    out_use_pixel_by_species_aerial = out_folder_sum + os.sep + folder + '_aerial.csv'

    s_out_use_sum_folder = s_out_folder + os.sep + 'States'
    create_directory(s_out_use_sum_folder)
    out_state = s_out_use_sum_folder + os.sep + folder + '.csv'
    out_state_ground = s_out_use_sum_folder + os.sep + folder + '_ground.csv'
    out_state_aerial = s_out_use_sum_folder + os.sep + folder + '_aerial.csv'

    if not overwrite_inter_data and os.path.exists(out_use_pixel_by_species):
        use_sp_df = pd.read_csv(out_use_pixel_by_species)
        [use_sp_df.drop(m, axis=1, inplace=True) for m in use_sp_df.columns.values.tolist() if m.startswith('Unnamed')]
    else:
        use_sp_df = pd.DataFrame(columns=list_geoid)
        use_sp_df = collapse_csv(direct, use_sp_df, 'direct')
        print direct
        use_sp_df.to_csv(out_use_pixel_by_species)
    out_df_state_use = collapse_state(use_sp_df, 'direct')
    out_df_state_use.fillna(0, inplace=True)
    out_df_state_use.to_csv(out_state)

    if not overwrite_inter_data and os.path.exists(out_use_pixel_by_species_ground):
        use_sp_df_g = pd.read_csv(out_use_pixel_by_species_ground)
        [use_sp_df_g.drop(m, axis=1, inplace=True) for m in use_sp_df_g.columns.values.tolist() if
         m.startswith('Unnamed')]
    else:
        use_sp_df_g = pd.DataFrame(columns=list_geoid)
        use_sp_df_g = collapse_csv(ground, use_sp_df_g, 'ground')
        print ground
        use_sp_df_g.to_csv(out_use_pixel_by_species_ground)
    out_df_state_ground_use = collapse_state(use_sp_df_g, 'ground')
    out_df_state_ground_use.fillna(0, inplace=True)
    out_df_state_ground_use.to_csv(out_state_ground)

    if not overwrite_inter_data and os.path.exists(out_use_pixel_by_species_aerial):
        use_sp_df_a = pd.read_csv(out_use_pixel_by_species_aerial)
        [use_sp_df_a.drop(m, axis=1, inplace=True) for m in use_sp_df_a.columns.values.tolist() if
         m.startswith('Unnamed')]
    else:
        use_sp_df_a = pd.DataFrame(columns=list_geoid)
        use_sp_df_a = collapse_csv(aerial, use_sp_df_a, 'aerial')
        print aerial
        use_sp_df_a.to_csv(out_use_pixel_by_species_aerial)
    out_df_state_aerial_use = collapse_state(use_sp_df_a, 'aerial')
    out_df_state_aerial_use.fillna(0, inplace=True)
    out_df_state_aerial_use.to_csv(out_state_aerial)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
