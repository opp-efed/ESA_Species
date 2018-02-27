import pandas as pd
import os
import arcpy
import datetime

species_file_type = 'Range'  # can be 'Range' or 'CriticalHabitat'
in_location = 'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              '\_ExternalDrive\_CurrentResults\Results_usage_range_clipped\L48'

in_sum_file = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              r'\_ExternalDrive\_CurrentSpeciesSpatialFiles\Boundaries.gdb\Counties_all_overlap_albers'

out_path = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
           r'\_ExternalDrive\_CurrentResults\Tabulated_usage_clipped\L48'

if species_file_type == 'Range' or species_file_type == 'range':
    # look_up_fc = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\_MovedOneDrive\Union\Range' \
    #             r'\R_Clipped_Union_MAG_20161102.gdb'
    look_up_fc = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\_MovedOneDrive\Union\Range' \
                 r'\SpCompRaster_byProjection\Grids_byProjection\Albers_Conical_Equal_Area'
    look_up_use = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
                  r'\_ExternalDrive\_CurrentResults\Results_diaz\RangeUses_lookup.csv'
    in_location = in_location + os.sep + 'Range'
else:
    look_up_fc = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\CriticalHabitat' \
                 r'\CH_Clipped_Union_MAG_20161102.gdb'
    look_up_use = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Results_NewComps\CH_Uses_lookup.csv'
    in_location = in_location + os.sep + 'CriticalHabitat'


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)
        # print "created directory {0}".format(dbf_dir)


def collapse_state(df):
    out_df_state = df.T.reset_index()
    out_df_state.columns = out_df_state.iloc[0]
    out_df_state = out_df_state.reindex(out_df_state.index.drop(0))
    zone_id = out_df_state.columns.values.tolist()
    out_df_state['STUSPS'] = out_df_state['ZoneID'].map(lambda x: str(x)[:2]).astype(str)
    out_df_state.drop('ZoneID', axis=1, inplace=True)
    sum_by_state = out_df_state.groupby('STUSPS').sum()

    df_out_state_t = sum_by_state.T
    df_out_state_t_reindex = df_out_state_t.reset_index()
    final_columns = df_out_state_t_reindex.columns.values.tolist()
    del final_columns[0]

    final_columns.insert(0, 'ZoneID')
    df_out_state_t_reindex.columns = final_columns
    return df_out_state_t_reindex


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_directory(out_path)
arcpy.MakeFeatureLayer_management(in_sum_file, "fc")
array = arcpy.da.TableToNumPyArray(in_sum_file, [u'STUSPS', u'Region', u'GEOID'], skip_nulls=True)

arcpy.env.workspace = look_up_fc
list_fc = arcpy.ListRasters()

df = pd.DataFrame(array)
out_col = df['GEOID'].values.tolist()
out_col.insert(0, 'ZoneID')
out_df = pd.DataFrame(columns=['ZoneID'])

list_output_root = os.listdir(in_location)
find_file_type = in_location.split(os.sep)
if 'Range' in find_file_type:
    file_type = 'Range'
else:
    file_type = 'CriticalHabitat'
for folder in list_output_root:
    c_path = in_location + os.sep + folder
    c_outpath = out_path + os.sep + file_type + os.sep + folder
    create_directory(os.path.dirname(c_outpath))
    create_directory(c_outpath)

    for species_group in list_fc:
        out_df_working_sp = out_df.copy()
        sp_zone_array = arcpy.da.TableToNumPyArray(look_up_fc + os.sep + species_group, ['Value'])
        sp_zone_df = pd.DataFrame(data=sp_zone_array, dtype=object)
        sp_zone_df['ZoneID'] = sp_zone_df['Value'].map(lambda x: str(x).split('.')[0]).astype(str)
        sp_zones = sp_zone_df['ZoneID'].values.tolist()
        se = pd.Series(sp_zones)
        out_df_working_sp['ZoneID'] = se.values
        list_uses = os.listdir(c_path)
        for use in list_uses:
            start_loop = datetime.datetime.now()
            print '\nWorking {0} and use {1}'.format(species_group.split("_")[0] + "_" + species_group.split("_")[1],
                                                     use)
            out_df_use = out_df_working_sp.copy()
            out_df_ground = out_df_working_sp.copy()
            out_df_aerial = out_df_working_sp.copy()
            c_use = c_outpath + os.sep + use
            create_directory(c_use)
            c_use_cnty = c_use + os.sep + 'Counties'
            c_use_state = c_use + os.sep + "States"
            out_cnty = c_use_cnty + os.sep + (
                species_group.split("_")[0].lower() + "_" + species_group.split("_")[1].lower()) + "_" + use + '.csv'
            out_state = c_use_state + os.sep + (
                species_group.split("_")[0].lower() + "_" + species_group.split("_")[1].lower()) + "_" + use + '.csv'

            out_cnty_g = c_use_cnty + os.sep + (species_group.split("_")[0].lower() + "_" + species_group.split("_")[
                1].lower()) + "_" + use + '_ground' + '.csv'
            out_state_g = c_use_state + os.sep + (species_group.split("_")[0].lower() + "_" + species_group.split("_")[
                1].lower()) + "_" + use + '_ground' + '.csv'

            out_cnty_a = c_use_cnty + os.sep + (species_group.split("_")[0].lower() + "_" + species_group.split("_")[
                1].lower()) + "_" + use + '_aerial' + '.csv'
            out_state_a = c_use_state + os.sep + (species_group.split("_")[0].lower() + "_" + species_group.split("_")[
                1].lower()) + "_" + use + '_aerial' + '.csv'
            if not os.path.exists(out_cnty):
                create_directory(c_use_cnty)
                create_directory(c_use_state)
                use_path = in_location + os.sep + folder + os.sep + use
                state_folders = os.listdir(use_path)
                for state in state_folders:
                    state_path = in_location + os.sep + folder + os.sep + use + os.sep + state
                    list_fips = os.listdir(state_path)
                    for fips in list_fips:
                        try:
                            fips_path = in_location + os.sep + folder + os.sep + use + os.sep + state + os.sep + fips
                            list_csv = os.listdir(fips_path)
                            list_csv = [csv for csv in list_csv if csv.endswith('.csv')]
                            list_csv = [csv for csv in list_csv if csv.startswith(
                                species_group.split("_")[0].lower() + "_" + species_group.split("_")[1].lower()[:4])]
                            print len(list_csv)
                            for csv in list_csv:
                                c_csv = fips_path + os.sep + csv
                                sp_df = pd.read_csv(c_csv)
                                col = sp_df['LABEL'].values.tolist()
                                col.insert(0, 'ZoneID')
                                drop_cols = [z for z in sp_df.columns.values.tolist() if not z.startswith('V')]
                                [sp_df.drop(j, axis=1, inplace=True) for j in drop_cols if j in drop_cols]
                                sp_df_t = sp_df.T
                                sp_df_t = sp_df_t.reset_index()
                                direct_overlap = sp_df_t.iloc[:, 0:2]
                                direct_overlap.columns = ['ZoneID', str(fips)]
                                direct_overlap['ZoneID'] = direct_overlap['ZoneID'].map(
                                    lambda x: str(x).split("_")[1]).astype(str)
                                out_df_use = pd.merge(out_df_use, direct_overlap, on='ZoneID', how='outer')
                                if folder == 'Agg_Layers':
                                    ground_overlap = pd.DataFrame(columns=['ZoneID', str(fips)])
                                    ground_overlap['ZoneID'] = pd.Series(
                                        direct_overlap['ZoneID'].values.tolist()).values
                                    ground_overlap[fips] = pd.Series(sp_df_t.iloc[:, 1:47].sum(
                                        axis=1)).values  # to exclude direct overlap change the 1 to a 3

                                    aerial_overlap = pd.DataFrame(columns=['ZoneID', str(fips)])
                                    aerial_overlap['ZoneID'] = pd.Series(
                                        direct_overlap['ZoneID'].values.tolist()).values
                                    aerial_overlap[fips] = pd.Series(sp_df_t.iloc[:, 1:218].sum(
                                        axis=1)).values  # up to 780 so it is divisionable by 30

                                    out_df_ground = pd.merge(out_df_ground, ground_overlap, on='ZoneID', how='outer')
                                    out_df_aerial = pd.merge(out_df_aerial, aerial_overlap, on='ZoneID', how='outer')
                                    del ground_overlap, aerial_overlap

                                del sp_df, sp_df_t, direct_overlap,
                        except:
                            print 'Failed on {0}, {1}, {2}, {3}'.format(
                                species_group.split("_")[0] + "_" + species_group.split("_")[1], use, fips, state)

                out_df_use.to_csv(out_cnty)
                out_df_ground.to_csv(out_cnty_g)
                out_df_aerial.to_csv(out_cnty_a)
                print 'Completed counties {0} and use {1} in {2}'.format(
                    species_group.split("_")[0].lower() + "_" + species_group.split("_")[1].lower()[:4], use,
                    datetime.datetime.now() - start_loop)
            if not os.path.exists(out_state):
                if os.path.exists(out_cnty):
                    out_df_use = pd.read_csv(out_cnty)
                    out_df_ground = pd.read_csv(out_cnty_g)
                    out_df_aerial = pd.read_csv(out_cnty_a)
                    [out_df_use.drop(m, axis=1, inplace=True) for m in out_df_use.columns.values.tolist() if
                     m.startswith('Unnamed')]
                    [out_df_ground.drop(m, axis=1, inplace=True) for m in out_df_ground.columns.values.tolist() if
                     m.startswith('Unnamed')]
                    [out_df_aerial.drop(m, axis=1, inplace=True) for m in out_df_aerial.columns.values.tolist() if
                     m.startswith('Unnamed')]

                # ERROR WILL OCCUR IF CNTY EXSITS BUT NOT THE STATE
                out_df_state_use = collapse_state(out_df_use)
                out_df_state_use.to_csv(out_state)

                if folder == 'Agg_Layers':
                    out_df_state_ground = collapse_state(out_df_ground)
                    out_df_state_aerial = collapse_state(out_df_aerial)
                    out_df_state_ground.to_csv(out_state_g)
                    out_df_state_aerial.to_csv(out_state_a)

                print 'Completed states {0} and use {1} in {2}'.format(
                    species_group.split("_")[0].lower() + "_" + species_group.split("_")[1].lower()[:4], use,
                    datetime.datetime.now() - start_loop)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
