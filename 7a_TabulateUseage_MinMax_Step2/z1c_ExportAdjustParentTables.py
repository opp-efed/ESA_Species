import pandas as pd
import datetime
import os

in_path = r'L:\ESA\Tabulates_Usage\L48\Range\Agg_Layers'
out_poltical = r'L:\ESA\Tabulated_PolBoundaries\L48\Range\Agg_Layers'

in_directory_grids = r'L:\ESA\UnionFiles_Winter2018\Range\SpComp_UsageHUCAB_byProjection_2' \
                     r'\Grid_byProjections_Combined'
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

grid_folder_lookup = {'AK': 'AK_WGS_1984_Albers',
                      'AS': 'AS_WGS_1984_UTM_Zone_2S',
                      'CNMI': 'CNMI_WGS_1984_UTM_Zone_55N',
                      'CONUS': 'CONUS_Albers_Conical_Equal_Area',
                      'GU': 'GU_WGS_1984_UTM_Zone_55N',
                      'HI': 'HI_NAD_1983_UTM_Zone_4N',
                      'PR': 'PR_Albers_Conical_Equal_Area',
                      'VI': 'VI_WGS_1984_UTM_Zone_20N'}

def adjust_elevation(out_df, adjust_path, final_df, cnty_piv, sta_piv):
    # Adjust for elevation based on elevation input files

    dem_col = [v for v in out_df.columns.values.tolist() if v.startswith('dem')]
    out_df.ix[:,dem_col[0]] = out_df.ix[:,dem_col[0]].map(lambda w: w).astype(int)
    adjust_df = pd.read_csv(adjust_path)
    adjust_df.ix[:,'EntityID'] = adjust_df.ix[:,'EntityID'].map(lambda r: r).astype(str)
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
    e_h_working.ix[:,'GEOID'] = e_h_working.ix[:,'GEOID'].map(lambda (n): n).astype(str)
    e_h_working.ix[:,'STATEFP'] = e_h_working.ix[:,'GEOID'].map(
        lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)
    out_col = ['EntityID', 'GEOID', 'STUSPS']
    [out_col.append(i) for i in out_df.columns.values.tolist() if i.startswith('VALUE')]
    out_col.remove('VALUE')
    out_ele = e_working[out_col]

    group_col = [v for v in out_ele.columns.values.tolist() if not v.startswith("VALUE")]
    out_ele = (out_ele.groupby(group_col).sum()).reset_index()
    final_df = pd.concat([final_df, out_ele])

    if ['VALUE_0'] not in out_ele.columns.values.tolist():
        out_ele['VALUE_0'] = 0

    df_pivot = out_ele[['EntityID', 'GEOID', 'VALUE_0']].copy()
    # df_pivot.ix[:, ["VALUE_0"]] = df_pivot.ix[:, ["VALUE_0"]].apply(pd.to_numeric)
    df_pivot = df_pivot.groupby(['EntityID', 'GEOID'], as_index=False).sum()
    cnty_pivot = df_pivot.pivot(index='EntityID', columns='GEOID')['VALUE_0']
    cnty_piv = pd.concat([cnty_piv, cnty_pivot])

    df_pivot = out_ele[['EntityID', 'STATEFP', 'VALUE_0']].copy()
    # df_pivot.ix[:, ["VALUE_0"]] = df_pivot.ix[:, ["VALUE_0"]].apply(pd.to_numeric)
    df_pivot = df_pivot.groupby(['EntityID', 'STATEFP'], as_index=False).sum()
    sta_pivot = df_pivot.pivot(index='EntityID', columns='STATEFP')['VALUE_0']
    sta_piv = pd.concat([sta_piv, sta_pivot])

    return e_h_working, final_df, cnty_piv, sta_piv


def adjust_habitat(adjust_path, out_df, out_final, out_hab_cnty, out_hab_state):
    # Adjusts for habitat based on habitat input file

    habitat_adjustment = adjust_path
    habitat_df = pd.read_csv(habitat_adjustment, dtype=object)
    sp_to_adjust_h = habitat_df.columns.values.tolist()
    hab_col = [v for v in out_df.columns.values.tolist() if v.startswith('Habit') or v.startswith(
        'gap') or v.startswith('2011')]
    out_df[hab_col[0]] = out_df[hab_col[0]].map(lambda t: t).astype(str)
    h_working = out_df.loc[~out_df['EntityID'].isin(sp_to_adjust_h)].copy()
    h_adjust = out_df.loc[out_df['EntityID'].isin(sp_to_adjust_h)].copy()

    for v in sp_to_adjust_h:
        if v in h_adjust['EntityID'].values.tolist():
            hab_cat = habitat_df[v].values.tolist()
            w_df = h_adjust.loc[(h_adjust['EntityID'] == v) & (h_adjust[hab_col[0]].isin(hab_cat))].copy()
            h_working = pd.concat([h_working, w_df])
    out_col = ['EntityID', 'GEOID', 'STUSPS']
    [out_col.append(i) for i in out_df.columns.values.tolist() if i.startswith('VALUE')]
    out_col.remove('VALUE')
    out_hab = h_working[out_col]

    out_hab['GEOID'] = out_hab['GEOID'].map(lambda (n): n).astype(str)
    out_hab['STATEFP'] = out_hab['GEOID'].map(lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)
    group_col = [v for v in out_hab.columns.values.tolist() if not v.startswith("VALUE")]
    out_hab = (out_hab.groupby(group_col).sum()).reset_index()
    out_final = pd.concat([out_final, out_hab])

    # if ['VALUE_0'] not in out_hab.columns.values.tolist():
    #     out_hab['VALUE_0'] = 0
    df_pivot = out_hab[['EntityID', 'GEOID', 'VALUE_0']].copy()
    # df_pivot.ix[:, ["VALUE_0"]] = df_pivot.ix[:, ["VALUE_0"]].apply(pd.to_numeric)
    df_pivot = df_pivot.groupby(['EntityID', 'GEOID'], as_index=False).sum()
    cnty_pivot = df_pivot.pivot(index='EntityID', columns='GEOID')['VALUE_0']
    out_hab_cnty = pd.concat([out_hab_cnty, cnty_pivot])

    df_pivot = out_hab[['EntityID', 'STATEFP', 'VALUE_0']].copy()
    # df_pivot.ix[:, ["VALUE_0"]] = df_pivot.ix[:, ["VALUE_0"]].apply(pd.to_numeric)
    df_pivot = df_pivot.groupby(['EntityID', 'STATEFP'], as_index=False).sum()
    sta_pivot = df_pivot.pivot(index='EntityID', columns='STATEFP')['VALUE_0']
    out_hab_state = pd.concat([out_hab_state, sta_pivot])

    return sp_to_adjust_h, habitat_df, out_final, out_hab_cnty, out_hab_state


def adjust_elv_habitat(e_h_working, out_df, hab_sp_adjust, hab_df, out_final, out_hab_ele_cnty,
                       out_hab_ele_state):
    # Adjusts for elevation and habitat based on input file

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
    out_col = ['EntityID', 'GEOID', 'STUSPS']
    [out_col.append(i) for i in out_df.columns.values.tolist() if i.startswith('VALUE')]

    out_col.remove('VALUE')
    out_hab_ele = e_h_working[out_col]
    out_hab_ele['GEOID'] = out_hab_ele['GEOID'].map(lambda (n): n).astype(str)
    out_hab_ele['STATEFP'] = out_hab_ele['GEOID'].map(
        lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)
    group_col = [v for v in out_hab_ele.columns.values.tolist() if not v.startswith("VALUE")]
    out_hab_ele = (out_hab_ele.groupby(group_col).sum()).reset_index()
    out_final = pd.concat([out_final, out_hab_ele])

    print ('  Created {0}'.format(out_folder + os.sep + csv.replace('.csv', '_adj_EleHab.csv')))

    if ['VALUE_0'] not in out_hab_ele.columns.values.tolist():
        out_hab_ele['VALUE_0'] = 0
    df_pivot = out_hab_ele[['EntityID', 'GEOID', 'VALUE_0']].copy()
    # df_pivot.ix[:, ["VALUE_0"]] = df_pivot.ix[:, ["VALUE_0"]].apply(pd.to_numeric)
    df_pivot = df_pivot.groupby(['EntityID', 'GEOID'], as_index=False).sum()
    cnty_pivot = df_pivot.pivot(index='EntityID', columns='GEOID')['VALUE_0']
    out_hab_ele_cnty = pd.concat([out_hab_ele_cnty, cnty_pivot])

    df_pivot = out_hab_ele[['EntityID', 'STATEFP', 'VALUE_0']].copy()
    # df_pivot.ix[:, ["VALUE_0"]] = df_pivot.ix[:, ["VALUE_0"]].apply(pd.to_numeric)
    df_pivot = df_pivot.groupby(['EntityID', 'STATEFP'], as_index=False).sum()
    sta_pivot = df_pivot.pivot(index='EntityID', columns='STATEFP')['VALUE_0']
    out_hab_ele_state = pd.concat([out_hab_ele_state, sta_pivot])
    return out_final, out_hab_ele_cnty, out_hab_ele_state


def export_aquatics(out_csv, out_df):
    # Export information needed for aquatic tables
    if not os.path.exists(out_csv):
        # if ['VALUE_0'] in out_df.columns.values.tolist():
        #     val_col = ['EntityID', 'HUC2_AB', 'GEOID', 'STUSPS', 'VALUE_0']
        # else:
        val_col = ['EntityID', 'HUC2_AB', 'GEOID', 'STUSPS']
        # [val_col.append(i) for i in out_df.columns.values.tolist() if i.startswith('VALUE')]
        # val_col.remove('VALUE')
        out_aqu = out_df[val_col].copy()

        out_aqu['GEOID'] = out_aqu['GEOID'].map(lambda (n): n).astype(str)
        out_aqu.ix[:,'STATEFP'] = out_aqu.ix[:,'GEOID'].map(lambda (n): str(n)[:2] if len(n) == 5 else '0' + n[:1]).astype(str)
        out_aqu.drop('GEOID', axis=1, inplace=True)
        if ['VALUE_0'] not in out_df.columns.values.tolist():
            out_aqu['VALUE_0'] = 0
        out_aqu = (out_aqu.groupby(['EntityID', 'HUC2_AB', 'STUSPS', 'STATEFP']).sum()).reset_index()
        return out_aqu


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Get date
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

list_dir = os.listdir(in_path)
for folder in list_dir:
    print folder
    out_folder = in_path + os.sep + folder
    region = folder.split('_')[0]  # extracts regions from folder title

    out_pol_folder_cny = out_poltical + os.sep + 'Counties'
    out_pol_folder_st = out_poltical + os.sep + 'States'
    list_csv = os.listdir(in_path + os.sep + folder)
    list_csv =[ v for v in list_csv if len(v.split("_"))==8]
    print list_csv
    for csv in list_csv:
        print ('Working on {0}...{1} of {2}'.format(csv,list_csv.index(csv), len(list_csv)))
        if not os.path.exists(out_pol_folder_st + os.sep + csv.replace('.csv', '_adj_EleHaB.csv')):
            chunksize = 100000
            pp = 0
            j = 1
            print in_path + os.sep + folder + os.sep+csv

            for out_sp_table in pd.read_csv((in_path + os.sep + folder + os.sep + csv), dtype=object,
                                            chunksize=chunksize, iterator=True):

                print out_sp_table.columns.values.tolist()
                value_cols = [v for v in out_sp_table.columns.values.tolist() if v.startswith("VALUE")]
                out_sp_table.ix[:, value_cols] = out_sp_table.ix[:, value_cols].apply(pd.to_numeric)

                out_habitat = pd.DataFrame(columns=[])
                out_habitat_cnty = pd.DataFrame(columns=[])
                out_habitat_state = pd.DataFrame(columns=[])
                out_ele = pd.DataFrame(columns=[])

                out_hab_ele = pd.DataFrame(columns=[])
                out_hab_ele_cnty = pd.DataFrame(columns=[])
                out_hab_ele_state = pd.DataFrame(columns=[])
                aqu = pd.DataFrame(columns=[])

                aqu = export_aquatics(out_folder + os.sep + csv.replace('.csv', '_HUC2AB.csv'), out_sp_table)
                aqu.to_csv(out_folder + os.sep + csv.replace('.csv', '_HUC2AB.csv'))
                print '  Exported {0}'.format(out_folder + os.sep + csv.replace('.csv', '_HUC2AB.csv'))
                # aqu = pd.concat([aqu, aqu_w])

                elev_hab_working, out_ele, out_ele_cnty, out_ele_state = adjust_elevation(out_sp_table,
                                                                                          elevation_adjustments,
                                                                                          out_ele,
                                                                                          out_pol_folder_cny,
                                                                                          out_pol_folder_st)
                out_ele.to_csv(out_folder + os.sep + csv.replace('.csv', '_adj_Ele.csv'))
                print '  Exported {0}'.format(out_folder + os.sep + csv.replace('.csv', '_adj_Ele.csv'))
                out_ele_cnty.to_csv(out_pol_folder_cny + os.sep + csv.replace('.csv', '_adj_Ele.csv'))
                print '  Exported {0}'.format(out_pol_folder_cny + os.sep + csv.replace('.csv', '_adj_Ele.csv'))
                out_ele_state.to_csv(out_pol_folder_st + os.sep + csv.replace('.csv', '_adj_Ele.csv'))
                print '  Exported {0}'.format(out_pol_folder_st + os.sep + csv.replace('.csv', '_adj_Ele.csv'))

                if region != 'AK':
                    hab_sp_adjust, hab_df, out_habitat, out_habitat_cnty, out_habitat_state = adjust_habitat(
                        habitat_adjustment_path + os.sep + habitat_dict[region],
                        out_sp_table, out_habitat, out_habitat_cnty, out_habitat_state)

                    out_hab_ele, out_hab_ele_cnty, out_hab_ele_state = adjust_elv_habitat(elev_hab_working,
                                                                                          out_sp_table,
                                                                                          hab_sp_adjust, hab_df,

                                                                                          out_hab_ele,
                                                                                          out_hab_ele_cnty,
                                                                                          out_hab_ele_state)

                    out_habitat.to_csv(out_folder + os.sep + csv.replace('.csv', '_adj_Hab.csv'))
                    print '  Exported {0}'.format(out_folder + os.sep + csv.replace('.csv', '_adj_Hab.csv'))
                    out_habitat_cnty.to_csv(out_pol_folder_cny + os.sep + csv.replace('.csv', '_adj_Hab.csv'))
                    print '  Exported {0}'.format(out_pol_folder_cny + os.sep + csv.replace('.csv', '_adj_Hab.csv'))
                    out_habitat_state.to_csv(out_pol_folder_st + os.sep + csv.replace('.csv', '_adj_Hab.csv'))
                    print '  Exported {0}'.format(out_pol_folder_st + os.sep + csv.replace('.csv', '_adj_Hab.csv'))

                    out_hab_ele.to_csv(out_folder + os.sep + csv.replace('.csv', '_adj_EleHab.csv'))
                    print '  Exported {0}'.format(out_folder + os.sep + csv.replace('.csv', '_adj_EleHab.csv'))
                    out_hab_ele_cnty.to_csv(out_pol_folder_cny + os.sep + csv.replace('.csv', '_adj_EleHab.csv'))
                    print '  Exported {0}'.format(out_folder + os.sep + csv.replace('.csv', '_adj_Ele.csv'))
                    out_hab_ele_state.to_csv(out_pol_folder_cny + os.sep + csv.replace('.csv', '_adj_EleHab.csv'))
                    print '  Exported {0}'.format(out_pol_folder_st + os.sep + csv.replace('.csv', '_adj_EleHaB.csv'))


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)