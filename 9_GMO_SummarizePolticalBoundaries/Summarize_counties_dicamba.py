import os

import pandas as pd


def calculation(acres_col, use_cols, in_sum_df):
    in_sum_df.ix[:, use_cols] = in_sum_df.ix[:, use_cols].apply(pd.to_numeric, errors='coerce')
    in_sum_df = in_sum_df.loc[in_sum_df[acres_col] >= 0]
    overlap = in_sum_df.copy()
    # # Convert to acres
    # overlap.ix[:, use_cols] *= 0.000247
    # # generate percent overlap by taking acres of use divided by total acres of the species range
    overlap[use_cols] = overlap[use_cols].div(overlap[acres_col], axis=0)
    overlap.ix[:, use_cols] *= 100
    overlap.drop(acres_col, axis=1, inplace=True)
    overlap_cols = []
    for col in overlap.columns.values.tolist():

        if col.endswith('total'):
            col = col.replace('total', 'TotalOverlap')
            overlap_cols.append(col)
        else:
            overlap_cols.append(col)
    overlap.columns = overlap_cols

    return overlap


def on_off_apply(row, on_off_col, drift_col, direct_col):
    # removed on field overlap from drift buffer if species is designated as off site
    if row[on_off_col] == 'OFF':
        adjust_value = row[drift_col] - row[direct_col]
    else:
        adjust_value = row[drift_col]
    return adjust_value


# path_table = [r"E:\Dicamba Update Summer 2020\SpeciesTabulated\Range_Filtered_CONUS_CDL_1317_20x2_eucCnty_240m.csv",
#               "E:\Dicamba Update Summer 2020\SpeciesTabulated\Range_Filtered_CONUS_CDL_1317_40x2_eucCnty_240m.csv",]
# species_zone_lookup = r'D:\Species\UnionFile_Spring2020\Range\LookUp_Grid_byProjections_Combined_20200427\CONUS_Albers_Conical_Equal_Area'

path_table = ["E:\Dicamba Update Summer 2020\SpeciesTabulated\CH_Filtered_CONUS_CDL_1317_20x2_eucCnty_240m.csv",
              "E:\Dicamba Update Summer 2020\SpeciesTabulated\CH_Filtered_CONUS_CDL_1317_40x2_eucCnty_240m.csv"]
species_zone_lookup = r'D:\Species\UnionFile_Spring2020\CriticalHabitat\LookUp_Grid_byProjections_20200427'

crops = ['Cotton', 'Soybeans']
aqu_woe_groups = ['Amphibians', 'Fish', 'Aquatic Invertebrates', 'Fishes', 'Aquatic Invertebrate']
terr_woe_groups = ['Mammals', 'Birds', 'Reptiles', 'Terrestrial Invertebrates', 'Plants']

out_path = 'E:\Dicamba Update Summer 2020\Update Summer 2020\Overlap Summary by Crop_onoff'
out_path_combined = 'E:\Dicamba Update Summer 2020\Update Summer 2020\Overlap summary combined_onoff'

# out_path = 'E:\Dicamba Update Summer 2020\Update Summer 2020\Overlap Summary by Crop'
# out_path_combined = 'E:\Dicamba Update Summer 2020\Update Summer 2020\Overlap summary combined'

master_species_list = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\MasterListESA_Dec2018_June2020.csv"
on_off = r"E:\Dicamba Update Summer 2020\Update Summer 2020\On_Off_calls_June2020.xlsx"
pol_boundaries = r"E:\Dicamba Update Summer 2020\Update Summer 2020\PolBoundaries_Lookup.csv"
plant_lookup = r"E:\Dicamba Update Summer 2020\Update Summer 2020\Plant_lookup.csv"
collected_pces = r"E:\Dicamba Update Summer 2020\Update Summer 2020\PCE_lookup.csv"

obl_plant = r"E:\Dicamba Update Summer 2020\Update Summer 2020\Dicamba_obligate_plant.xlsx"
counties_area = r"E:\Dicamba Update Summer 2020\Update Summer 2020\counties_area.csv"
coa_dicamba = r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\Update Summer 2020\Dicamba_Census (2012) Crops Areas and Operations (interim draft for esa_11-15-18).csv"

species = pd.read_csv(master_species_list)
on_off = pd.read_excel(on_off)

obl_plant_df = pd.read_excel(obl_plant)
cnty_acres = pd.read_csv(counties_area)
coa = pd.read_csv(coa_dicamba)
pce = pd.read_csv(collected_pces)

pol_df = pd.read_csv(pol_boundaries)
plant = pd.read_csv(plant_lookup)
species['EntityID'] = species['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
species_groups = species[['EntityID', 'Common Name', 'Scientific Name', 'Group', 'WoE Summary Group']].copy()
on_off['EntityID'] = on_off['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
on_off_ag = on_off[['EntityID', 'On/Off_AG']]
plant['EntityID'] = plant['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
pce['EntityID'] = pce['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)

plant_df = plant[['EntityID', 'Plant taxa']]
obl_plant_df['EntityID'] = obl_plant_df['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
pol_df['GEOID'] = pol_df['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
pce_df = pce[['EntityID', 'PCE Collected/Modification needed']]

cnty_acres['GEOID'] = cnty_acres['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
cnty_acres_df = cnty_acres[['GEOID', 'STUSPS', 'State', 'cnty_Acres']]
coa['GEOID'] = coa['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
coa_df = coa[['GEOID', 'Acres_CoA', 'COMMODITY_DESC']].copy()
coa_df = coa_df[coa_df['Acres_CoA'] >= 0].copy()
coa_df = coa_df.groupby(['GEOID', 'COMMODITY_DESC']).sum().reset_index()
coa_df['Crop'] = coa_df['COMMODITY_DESC'].map(lambda x: str(x).capitalize())

list_csv = os.listdir(species_zone_lookup)
sp_out = pd.DataFrame()
for t in list_csv:
    sp_data = pd.read_csv(species_zone_lookup + os.sep + t)
    sp_df = sp_data[['EntityID', 'GEOID', 'COUNT']].copy()
    sp_df['GEOID'] = sp_df['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
    sp_df['EntityID'] = sp_df['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
    filter_sp = sp_df.groupby(['EntityID', 'GEOID']).sum().reset_index()
    # print set(filter_sp['EntityID'].values.tolist())
    sp_out = pd.concat([sp_out, filter_sp], axis=0)
    print t, len(filter_sp), len(sp_out)

sp_out['EntityID'] = sp_out['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
sp_out['GEOID'] = sp_out['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
sp_out['MSQ'] = sp_out['COUNT'] * 900
sp_out['SP_Cnt_Acres'] = sp_out['MSQ'] * 0.000247

# print set(sp_out['EntityID'].values.tolist())

for i in path_table:

    df = pd.read_csv(i)
    if os.path.basename(i).startswith('Range'):
        file_type = "_range_"
        species_area = r"E:\Dicamba Update Summer 2020\Update Summer 2020\R_Acres_Pixels_20200628_dicamba_on_off.csv"
        # species_area = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\R_Acres_Pixels_20200628.csv"
    else:
        file_type = "_critical habitat_"
        species_area = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\CH_Acres_Pixels_20200628.csv"

    acres = pd.read_csv(species_area)
    acres['EntityID'] = acres['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
    acres_df = acres[['EntityID', 'Acres_CONUS']]
    if file_type == "_critical habitat_":
        acres_df = pd.merge(acres_df, pce_df, how='left', on='EntityID')
        acres_df['PCE Collected/Modification needed'].fillna('NE', inplace=True)

    else:
        acres_df['PCE Collected/Modification needed'] = 'NA'

    sp_out_acres = pd.merge(sp_out, acres_df, how='left', on='EntityID')

    sp_out_acres['Percent of Species'] = (sp_out_acres['Acres'] / sp_out_acres['Acres_CONUS']) * 100
    sp_out_acres['EntityID_GEOID'] = sp_out_acres['EntityID'] + "_" + sp_out_acres['GEOID']

    crop = crops[path_table.index(i)]
    df['EntityID'] = df['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
    df['GEOID'] = df['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)

    df = pd.merge(df, on_off_ag, on='EntityID', how='left')
    df = pd.merge(df, plant, on='EntityID', how='left')
    common_col = [k for k in pol_df.columns.values.tolist() if k in df.columns.values.tolist()]
    df = pd.merge(df, pol_df, on=common_col, how='left')
    df = pd.merge(df, acres_df, on='EntityID', how='left')
    df = pd.merge(df, obl_plant_df, on='EntityID', how='left')
    common_col = [k for k in species_groups.columns.values.tolist() if k in df.columns.values.tolist()]
    df = pd.merge(df, species_groups, how='left', on=common_col)

    spe_cols = ['EntityID']
    # 'Common Name', 'Scientific Name', 'Group', 'WoE Summary Group', u'On/Off_AG', 'Plant taxa', 'Plant species range description',  'PCE Collected/Modification needed', u'obligate plant'
    species_acres_col = [v for v in df.columns.values.tolist() if v.endswith('_acres')]
    total_species_df = df[spe_cols + species_acres_col].copy()
    total_species_col = [v for v in total_species_df.columns.values.tolist() if not v.endswith("_acres")]
    total_species_sum = total_species_df.groupby(total_species_col).sum().reset_index()

    final_col = []
    for v in total_species_sum:
        if v.endswith('acres'):
            v = v.replace('acres', 'total')
        final_col.append(v)
    total_species_sum.columns = final_col

    df = pd.merge(df, total_species_sum, how='left', on='EntityID')

    list_crop_cols = [k for k in df.columns.values.tolist() if (k.startswith(crop) and k.endswith('total'))]
    list_crop_cnty = [k for k in df.columns.values.tolist() if (k.startswith(crop) and k.endswith('acres'))]
    # print list_crop_cols, list_crop_cnty
    list_intervals = [k.split("_")[1] for k in list_crop_cols]

    # calculates percent overlap
    df = calculation('Acres_CONUS', list_crop_cols, df)
    # list_crop_cols= [k for k in df.columns.values.tolist() if  (k.startswith(crop) and k.endswith('total'))]
    list_crop_cols = [k for k in df.columns.values.tolist() if (k.startswith(crop) and k.endswith('TotalOverlap'))]
    # list_crop_cnty = [k for k in df.columns.values.tolist() if (k.startswith(crop) and k.endswith('overlap'))]

    filtered_df_dict = {}
    for v in list_intervals:
        # print list_crop_cols,list_intervals.index(v), v , list_intervals
        filter_df = df[df[list_crop_cols[list_intervals.index(v)]] >= 0.45].copy()
        # filter_df = df.copy()
        filter_df['EntityID_GEOID'] = filter_df['EntityID'] + "_" + df['GEOID']
        if file_type == "_range_":
            filter_df['adjusted on_off_totaloverlap_' + crop] = filter_df.apply(
                lambda row: on_off_apply(row, 'On/Off_AG', list_crop_cols[list_intervals.index(v)], list_crop_cols[0]),
                axis=1)
            # filter_df['adjusted on_off_cntyoverlap_' + crop] = filter_df.apply(
            #     lambda row: on_off_apply(row, 'On/Off_AG', list_crop_cnty[list_intervals.index(v)], list_crop_cnty[0]),
            #     axis=1)
        else:
            filter_df['adjusted on_off_totaloverlap_' + crop] = filter_df[list_crop_cols[list_intervals.index(v)]]
            # filter_df['adjusted on_off_cntyoverlap_' + crop] = filter_df[list_crop_cnty[list_intervals.index(v)]]
        filtered_df_dict[v] = filter_df

    for v in list_intervals:
        df_interval = filtered_df_dict[v]
        df_interval['EntityID'] = df_interval['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
        common_col = [k for k in df_interval.columns.values.tolist() if k in sp_out_acres.columns.values.tolist()]
        output = pd.merge(df_interval, sp_out_acres, how='left', on=common_col)

        common_col = [k for k in output.columns.values.tolist() if k in cnty_acres_df.columns.values.tolist()]
        output = pd.merge(output, cnty_acres_df, on=common_col, how='left')
        # print set(output['EntityID'].values.tolist())
        output['Percent of County'] = (output['Acres'] / output['cnty_Acres']) * 100
        output['Plant taxa'].fillna('NE', inplace=True)
        output.to_csv(out_path + os.sep + crop + file_type + v + ".csv")
        # print out_path +os.sep+ crop+ file_type+v+".csv"

# #
# list_out_table = os.listdir(out_path)
# # print list_out_table
# intervals = list(set([k.split("_")[2].replace('.csv', '') for k in list_out_table]))
# # print  intervals
# coa_total = pd.DataFrame({'Crop': ['Cotton', 'Soybeans'], 'Totals': [7636358, 73872658]})
#
# for v in intervals:
#     # print v
#     crops_interval = [t for t in list_out_table if t.endswith("_" + v + '.csv')]
#     # print crops_interval
#     combine_crops = pd.DataFrame()
#     for c in crops_interval:
#         # print out_path +os.sep+c
#         if len(combine_crops) == 0:
#             df = pd.read_csv(out_path + os.sep + c)
#             [df.drop(f, axis=1, inplace=True) for f in df.columns.values.tolist() if f.startswith('Unnamed')]
#             common_col = [k for k in species_groups.columns.values.tolist() if k in df.columns.values.tolist()]
#             df = pd.merge(df, species_groups, how='left', on=common_col)
#             df.fillna('NA', inplace=True)
#             df['Crop'] = c.split("_")[0]
#             df['File type'] = c.split("_")[1].capitalize()
#             combine_crops = df.copy()
#         else:
#             df = pd.read_csv(out_path + os.sep + c)
#             [df.drop(f, axis=1, inplace=True) for f in df.columns.values.tolist() if f.startswith('Unnamed')]
#             common_col = [k for k in species_groups.columns.values.tolist() if k in df.columns.values.tolist()]
#             df = pd.merge(df, species_groups, how='left', on=common_col)
#             df.fillna('NA', inplace=True)
#             df['Crop'] = c.split("_")[0]
#             df['File type'] = c.split("_")[1].capitalize()
#             combine_crops = pd.concat([combine_crops, df], axis=0)
#     combine_crops.drop_duplicates(inplace=True)
#     # print combine_crops.columns.values.tolist()
#     combine_crops['GEOID'] = combine_crops['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(
#         str)
#
#     combine_crops = pd.merge(combine_crops, coa_df, how='left', on=['GEOID', 'Crop']).copy()
#     combine_crops.drop_duplicates(inplace=True)
#
#     drop_cols = ['VALUE_0', 'VALUE_108', 'VALUE_120', 'VALUE_123', 'VALUE_127', 'VALUE_134', 'VALUE_150', 'VALUE_152',
#                  'VALUE_161', 'VALUE_169', 'VALUE_174', 'VALUE_180', 'VALUE_182', 'VALUE_189', 'VALUE_192', 'VALUE_201',
#                  'VALUE_210', 'VALUE_212', 'VALUE_216', 'VALUE_218', 'VALUE_228', 'VALUE_234', 'VALUE_240', 'VALUE_30',
#                  'VALUE_42', 'VALUE_60', 'VALUE_67', 'VALUE_84', 'VALUE_90', 'VALUE_94', 'MSQ','Acres', 'COUNT',
#                  'Soybeans_0',	 'Soybeans_120',	 'Soybeans_150',	 'Soybeans_180',	 'Soybeans_210',
#                  'Soybeans_240',	 'Soybeans_30',	 'Soybeans_60',	 'Soybeans_90',	 'Cotton_0',
#                  'Cotton_120',	 'Cotton_150',	 'Cotton_180',	 'Cotton_210',	 'Cotton_240',
#                  'Cotton_30',	 'Cotton_60',	 'Cotton_90',
#                  'Soybeans_30_acres', 'Soybeans_60_acres', 'Soybeans_90_acres', 'Soybeans_120_acres',
#                  'Soybeans_150_acres', 'Soybeans_180_acres', 'Soybeans_210_acres', 'Soybeans_240_acres',
#                  'Cotton_30_acres', 'Cotton_60_acres', 'Cotton_90_acres', 'Cotton_120_acres',
#                  'Cotton_150_acres', 'Cotton_180_acres', 'Cotton_210_acres', 'Cotton_240_acres',]
#
#     # for t in out_all_crops.columns.values.tolist():
#     #     if t.endswith('_acres') or t.startswith('VALUE'):
#     #         out_all_crops.drop(t, inplace=True, axis=1)
#     #     elif t.startswith(crops[0]) and not t.endswith('TotalOverlap') or ('acres'):
#     #         out_all_crops.drop(t, inplace=True, axis=1)
#     #     elif t.startswith(crops[1]) and not t.endswith('TotalOverlap') or ('acres'):
#     #         out_all_crops.drop(t, inplace=True, axis=1)
#     # print out_all_crops.columns.values.tolist()
#     for t in drop_cols:
#         combine_crops.drop(t, inplace=True, axis=1)
#
#     combine_crops.drop_duplicates(inplace=True)
#     combine_crops= combine_crops.reindex(
#         columns=['EntityID_GEOID', 'EntityID', 'Common Name', 'Scientific Name', 'File type', 'Group',
#                  'WoE Summary Group', 'obligate plant', 'On/Off_AG', 'PCE Collected/Modification needed',
#                  'Percent of County', 'Percent of Species', 'Plant species range description', 'Plant taxa',
#                  'Acres_CoA', 'Acres_CONUS', 'STATE', 'State', 'STATEFP', 'STUSPS', 'USPS ABB', 'GEOID', 'cnty_Acres',
#                  'Counties', 'COMMODITY_DESC', 'Crop', 'adjusted on_off_cntyoverlap_Cotton', 'Cotton_0_acres',
#                  'adjusted on_off_totaloverlap_Cotton', 'Cotton_0_TotalOverlap', 'Cotton_30_TotalOverlap',
#                  'Cotton_60_TotalOverlap', 'Cotton_90_TotalOverlap', 'Cotton_120_TotalOverlap',
#                  'Cotton_150_TotalOverlap', 'Cotton_180_TotalOverlap', 'Cotton_210_TotalOverlap',
#                  'Cotton_240_TotalOverlap', 'adjusted on_off_cntyoverlap_Soybeans', 'Soybeans_0_acres',
#                  'adjusted on_off_totaloverlap_Soybeans', 'Soybeans_0_TotalOverlap', 'Soybeans_30_TotalOverlap',
#                  'Soybeans_60_TotalOverlap', 'Soybeans_90_TotalOverlap', 'Soybeans_120_TotalOverlap',
#                  'Soybeans_150_TotalOverlap', 'Soybeans_180_TotalOverlap', 'Soybeans_210_TotalOverlap',
#                  'Soybeans_240_TotalOverlap',
#                  ]    )
#     combine_crops = combine_crops.fillna(0)
#     out_all_crops = combine_crops.drop(['Crop', 'COMMODITY_DESC', 'Acres_CONUS', 'Acres_CoA'], axis=1)
#     out_all_crops.drop_duplicates(inplace=True)
#     # col_groupby =[]
#     #
#     # for j in out_all_crops.columns.values.tolist():
#     #     if not j.startswith(crops[1]):
#     #         if not j.startswith(crops[0]):
#     #             col_groupby.append(j)
#     # out_all_crops.drop_duplicates(inplace=True)
#
#     # out_all_crops = out_all_crops.groupby(col_groupby).sum().reset_index()
#     out_all_crops.to_csv(out_path_combined + os.sep + "All_crops_" + v + '.csv')
#     out_all_crops.drop([ 'Counties', 'EntityID_GEOID', 'GEOID', 'Percent of County',
#                         'Plant species range description', 'STATE', 'STATEFP', 'STUSPS', 'State', 'USPS ABB',
#                         'cnty_Acres', 'Percent of Species'], axis=1, inplace=True)
#
#     # col_groupby = ['EntityID', 'Common Name',  'Scientific Name', 'File type','On/Off_AG','WoE Summary Group',  'PCE Collected/Modification needed','Plant taxa','obligate plant']
#     # out_all_crops = out_all_crops.groupby(col_groupby).sum().reset_index()
#     out_all_crops.drop_duplicates(inplace=True)
#     out_all_crops.to_csv(out_path_combined + os.sep + "Species_All_crops_" + v + '.csv')
#     combine_crops.to_csv(out_path_combined + os.sep + "test.csv")
#     non_monocot =  combine_crops [(((combine_crops ['Plant taxa'] !='M')
#                                     &(combine_crops ['WoE Summary Group'] =='Plants') &
#                                     (combine_crops ['adjusted on_off_totaloverlap_Soybeans']>=0.45)&
#                                     (combine_crops ['Soybeans_0_acres']>=1) &
#                                     (combine_crops ['Percent of County']>=0.45)&
#                                     (combine_crops ['File type'] =='Range')))].copy()
#     print non_monocot.head()
# # |(((combine_crops ['Plant taxa'] !='M')&
# #    (combine_crops['WoE Summary Group'] == 'Plants') &
# #    (combine_crops['adjusted on_off_totaloverlap_Cotton'] >= 0.45) &
# #    (combine_crops['Cotton_0_acres'] >= 1) &
# #    (combine_crops['Percent of County'] >= 0.45) &
# #    (combine_crops['File type'] == 'Range')))
#
# #     |((((combine_crops ['WoE Summary Group'].isin(terr_woe_groups)
# #          & (combine_crops ['PCE Collected/Modification needed']=='NE'))
# #         |(combine_crops ['PCE Collected/Modification needed']=='yes'))&
# #        (combine_crops ['Soybeans_0_acres']>=1) &
# #        (combine_crops ['Percent of County']>=0.45)
# #        & (combine_crops ['Percent of County']>=0.0)
# #        &(combine_crops ['File type'] =='Critical habitat')))
# # |((((combine_crops ['WoE Summary Group'].isin(terr_woe_groups)
# #      & (combine_crops ['PCE Collected/Modification needed']=='NE'))
# #     |(combine_crops ['PCE Collected/Modification needed']=='yes'))&
# #    (combine_crops['adjusted on_off_totaloverlap_Cotton'] >= 0.45) &
# #    (combine_crops['Cotton_0_acres'] >= 1)
# #    & (combine_crops ['Percent of County']>=0.0)
# #    &(combine_crops ['File type'] =='Critical habitat')))
#
#     counties_non_monocot = (non_monocot[['GEOID','Counties','State', 'USPS ABB','Crop','cnty_Acres','Percent of County']]).copy()
#     group_counties_non_monocot = counties_non_monocot.groupby(
#         ['GEOID', 'Counties', 'State', 'USPS ABB', 'Crop', 'cnty_Acres']).agg(['max', 'count']).reset_index()
#     group_counties_non_monocot.columns = ['GEOID', 'Counties', 'State', 'USPS ABB', 'Crop', 'County Area',
#                                           'Percent of County', 'Number of Species']
#     group_counties_non_monocot = pd.merge(group_counties_non_monocot, coa_df, how='left', on=['GEOID', 'Crop'])
#     group_counties_non_monocot_totals = group_counties_non_monocot.groupby(['Crop']).sum().reset_index()
#     for col in ['County Area', 'Percent of County', 'Number of Species']:
#         if col in group_counties_non_monocot_totals.columns.values.tolist():
#             group_counties_non_monocot_totals = group_counties_non_monocot_totals.drop(col, axis=1)
#     group_counties_non_monocot_totals.columns = ['Crop', 'Acres_total']
#     group_counties_non_monocot = pd.merge(group_counties_non_monocot, group_counties_non_monocot_totals, how='left',
#                                           on=['Crop'])
#     group_counties_non_monocot = pd.merge(group_counties_non_monocot, coa_total, how='left', on=['Crop'])
#     group_counties_non_monocot['Percent of Impact'] = (group_counties_non_monocot['Acres_total'] /
#                                                        group_counties_non_monocot['Totals']) * 100
#     group_counties_non_monocot['County Count'] = group_counties_non_monocot['GEOID'].nunique()
#     if os.path.exists(out_path_combined + os.sep + 'Combined_NonMonocot_' + v + '.csv'):
#         previous = pd.read_csv(out_path_combined + os.sep + 'Combined_NonMonocot_' + v + '.csv')
#         group_counties_non_monocot.append(previous, ignore_index=True)
#     group_counties_non_monocot.to_csv(out_path_combined + os.sep + 'Combined_NonMonocot_' + v + '.csv')
#
#     species_non_monocot = (non_monocot[
#         ['EntityID', 'Common Name', 'Scientific Name', 'Group', 'WoE Summary Group', 'Plant taxa', 'File type',
#          'obligate plant', 'Crop','PCE Collected/Modification needed','Percent of Species']]).copy()
#     if 'NA' in species_non_monocot["Percent of Species"].values.tolist():
#         species_non_monocot["Percent of Species"].replace({"NA": 0}, inplace=True)
#         species_non_monocot['Percent of Species'] = species_non_monocot['Percent of Species'].map(lambda x: float(x)).astype(float)
#
#     species_non_monocot= species_non_monocot.fillna('NE')
#     group_species_non_monocot = species_non_monocot.groupby(['EntityID', 'Common Name', 'Scientific Name', 'Group', 'WoE Summary Group', 'Plant taxa', 'File type','obligate plant', 'Crop','PCE Collected/Modification needed'],sort=False)['Percent of Species'].agg(['sum', 'count']).reset_index()
#
#     group_species_non_monocot = group_species_non_monocot[group_species_non_monocot['count'] > 0]
#     group_species_non_monocot['Species Count'] = group_species_non_monocot.loc[group_species_non_monocot['File type'] == "Range", 'EntityID'].nunique()
#     group_species_non_monocot['Critical Habitat Count'] = group_species_non_monocot.loc[group_species_non_monocot['File type'] != "Range", 'EntityID'].nunique()
#     group_species_non_monocot.columns = ['EntityID', 'Common Name', 'Scientific Name', 'Group', 'WoE Summary Group',
#                                          'Plant taxa', 'File type','obligate plant', 'Crop', 'PCE Collected/Modification needed','Percent of Species',
#                                          'Number of Counties for Species', 'Species Count', 'Critical Habitat Count']
#
#     if os.path.exists(out_path_combined + os.sep + 'Species_Combined_NonMonocot_' + v + '.csv'):
#         previous = pd.read_csv(out_path_combined + os.sep + 'Species_Combined_NonMonocot_' + v + '.csv')
#         group_species_non_monocot.append(previous, ignore_index=True)
#     group_species_non_monocot.to_csv(out_path_combined + os.sep + 'Species_Combined_NonMonocot_' + v + '.csv')
#     #
#     # non_monocot_obl =  combine_crops [(((combine_crops ['Plant taxa'] !='M')
#     #                                     &(combine_crops ['WoE Summary Group'] =='Plants') &
#     #                                     (combine_crops ['adjusted on_off_totaloverlap']>=0.45) &
#     #                                     (combine_crops ['Percent of County']>=0.45)&
#     #                                     (combine_crops ['File type'] =='Range'))|
#     #                                    ((combine_crops ['obligate plant']=='Yes')&
#     #                                     (combine_crops ['adjusted on_off_totaloverlap']>=0.45)& (combine_crops ['File type'] =='Range')))    |
#     #                                   (((combine_crops ['WoE Summary Group'].isin(terr_woe_groups) &
#     #                                      (combine_crops ['PCE Collected/Modification needed']=='NE')&
#     #                                      (combine_crops ['adjusted on_off_totaloverlap']>=0.45) &
#     #                                      (combine_crops ['Percent of County']>=0.00)&
#     #                                      (combine_crops ['File type'] =='Critical habitat'))|
#     #                                     ((combine_crops ['PCE Collected/Modification needed']=='yes'))&
#     #                                     (combine_crops ['adjusted on_off_totaloverlap']>=0.45) & (combine_crops ['Percent of County']>=0.00)&(combine_crops ['File type'] =='Critical habitat')))].copy()
#     # #
#     #
#     #
#     # counties_non_monocot_obl = (non_monocot_obl[['GEOID', 'Counties', 'State', 'USPS ABB', 'Crop', 'cnty_Acres', 'Percent of County']]).copy()
#     #
#     # group_counties_non_monocot_obl = counties_non_monocot_obl.groupby(
#     #     ['GEOID', 'Counties', 'State', 'USPS ABB', 'Crop', 'cnty_Acres']).agg(['max', 'count']).reset_index()
#     # group_counties_non_monocot_obl.columns = ['GEOID', 'Counties', 'State', 'USPS ABB', 'Crop', 'County Area',
#     #                                           'Percent of County', 'Number of Species']
#     # group_counties_non_monocot_obl = pd.merge(group_counties_non_monocot_obl, coa_df, how='left', on=['GEOID', 'Crop'])
#     # group_counties_non_monocot_obl_totals = group_counties_non_monocot_obl.groupby(['Crop']).sum().reset_index()
#     # group_counties_non_monocot_obl_totals = group_counties_non_monocot_obl_totals.drop(
#     #     ['County Area', 'Percent of County', 'Number of Species'], axis=1)
#     # group_counties_non_monocot_obl_totals.columns = ['Crop', 'Acres_total']
#     # group_counties_non_monocot_obl = pd.merge(group_counties_non_monocot_obl, group_counties_non_monocot_obl_totals,
#     #                                           how='left', on=['Crop'])
#     # group_counties_non_monocot_obl = pd.merge(group_counties_non_monocot_obl, coa_total, how='left', on=['Crop'])
#     # group_counties_non_monocot_obl['Percent of Impact'] = (group_counties_non_monocot_obl['Acres_total'] /
#     #                                                        group_counties_non_monocot_obl['Totals']) * 100
#     # group_counties_non_monocot_obl['County Count'] = group_counties_non_monocot_obl['GEOID'].nunique()
#     # if os.path.exists(out_path_combined + os.sep + 'Combined_NonMonocotObl_' + v + '.csv'):
#     #     previous = pd.read_csv(out_path_combined + os.sep + 'Combined_NonMonocotObl_' + v + '.csv')
#     #     group_counties_non_monocot_obl.append(previous, ignore_index=True)
#     # group_counties_non_monocot_obl.to_csv(out_path_combined + os.sep + 'Combined_NonMonocotObl_' + v + '.csv')
#     #
#     # species_non_monocot_obl = (non_monocot_obl[
#     #     ['EntityID', 'Common Name', 'Scientific Name', 'Group', 'WoE Summary Group', 'Plant taxa', 'File type',
#     #      'obligate plant', 'Crop','PCE Collected/Modification needed', 'Percent of Species']]).copy()
#     #
#     # if 'NA' in species_non_monocot_obl["Percent of Species"].values.tolist():
#     #     species_non_monocot_obl["Percent of Species"].replace({"NA": 0}, inplace=True)
#     #     species_non_monocot_obl['Percent of Species'] = species_non_monocot_obl['Percent of Species'].map(lambda x: x).astype(float)
#     # # print species_non_monocot_obl.head()
#     # # species_non_monocot_obl.to_csv(r'E:\Dicamba Update Summer 2020\Update Summer 2020\New folder (2)\test.csv')
#     # # print species_non_monocot_obl.dtypes
#     # species_non_monocot_obl= species_non_monocot_obl.fillna('NE')
#     # group_species_non_monocot_obl = species_non_monocot_obl.groupby(['EntityID', 'Common Name', 'Scientific Name', 'Group', 'WoE Summary Group', 'Plant taxa', 'File type','obligate plant', 'Crop','PCE Collected/Modification needed'],sort=False)['Percent of Species'].agg(['sum', 'count']).reset_index()
#     # # print group_species_non_monocot_obl.head()
#     # # group_species_non_monocot_obl = group_species_non_monocot_obl[group_species_non_monocot_obl['count'] > 0]
#     # group_species_non_monocot_obl['Species Count'] = group_species_non_monocot_obl.loc[group_species_non_monocot_obl['File type'] == "Range", 'EntityID'].nunique()
#     # group_species_non_monocot_obl['Critical Habitat Count'] = group_species_non_monocot_obl.loc[group_species_non_monocot_obl['File type'] != "Range", 'EntityID'].nunique()
#     # group_species_non_monocot_obl.columns = ['EntityID', 'Common Name', 'Scientific Name', 'Group', 'WoE Summary Group',
#     #                                          'Plant taxa','File type', 'obligate plant',  'Crop', 'PCE Collected/Modification needed',
#     #                                          'Percent of Species','Number of Counties for Species', 'Species Count',
#     #                                          'Critical Habitat Count']
#     #
#     #
#     # if os.path.exists(out_path_combined + os.sep + 'Species_Combined_NonMonocotObl_' + v + '.csv'):
#     #     previous = pd.read_csv(out_path_combined + os.sep + 'Species_Combined_NonMonocotObl_' + v + '.csv')
#     #     group_species_non_monocot_obl.append(previous, ignore_index=True)
#     # group_species_non_monocot_obl.to_csv(out_path_combined + os.sep + 'Species_Combined_NonMonocotObl_' + v + '.csv')
#     #
#     #
#     # all_species=  combine_crops [((combine_crops ['adjusted on_off_totaloverlap']>=0.45)& (combine_crops ['Percent of County']>=0.45)&(combine_crops ['File type'] =='Range'))
#     #         |((((combine_crops ['WoE Summary Group'].isin(terr_woe_groups) & (combine_crops ['PCE Collected/Modification needed']=='NE'))|(combine_crops ['PCE Collected/Modification needed']=='yes'))&
#     #                                               (combine_crops ['adjusted on_off_totaloverlap']>=0.45) & (combine_crops ['Percent of County']>=0.00)&(combine_crops ['File type'] =='Critical habitat')) )].copy()
#     #
#     # counties_all_species = (
#     # all_species[['GEOID', 'Counties', 'State', 'USPS ABB', 'Crop', 'cnty_Acres', 'Percent of County']]).copy()
#     # group_counties_all_species = counties_all_species.groupby(
#     #     ['GEOID', 'Counties', 'State', 'USPS ABB', 'Crop', 'cnty_Acres']).agg(['max', 'count']).reset_index()
#     # group_counties_all_species.columns = ['GEOID', 'Counties', 'State', 'USPS ABB', 'Crop', 'County Area',
#     #                                       'Percent of County', 'Number of Species']
#     # group_counties_all_species = pd.merge(group_counties_all_species, coa_df, how='left', on=['GEOID', 'Crop'])
#     # group_counties_all_species_totals = group_counties_all_species.groupby(['Crop']).sum().reset_index()
#     # for col in ['County Area','Percent of County',  'Number of Species']:
#     #     if col in group_counties_all_species_totals.columns.values.tolist():
#     #         group_counties_all_species_totals.drop(col, axis=1, inplace=True)
#     # group_counties_all_species_totals.columns = ['Crop', 'Acres_total']
#     # group_counties_all_species = pd.merge(group_counties_all_species, group_counties_all_species_totals, how='left',
#     #                                       on=['Crop'])
#     # group_counties_all_species = pd.merge(group_counties_all_species, coa_total, how='left', on=['Crop'])
#     # group_counties_all_species['Percent of Impact'] = (group_counties_all_species['Acres_total'] /
#     #                                                    group_counties_all_species['Totals']) * 100
#     # group_counties_all_species['County Count'] = group_counties_all_species['GEOID'].nunique()
#     # if os.path.exists(out_path_combined + os.sep + 'Generalist_Combined_' + v + '.csv'):
#     #     previous = pd.read_csv(out_path_combined + os.sep + 'Generalist_Combined_' + v + '.csv')
#     #     group_counties_all_species.append(previous, ignore_index=True)
#     # group_counties_all_species.to_csv(out_path_combined + os.sep + 'Generalist_Combined_' + v + '.csv')
#     #
#     # count_all_species = (all_species[
#     #     ['EntityID', 'Common Name', 'Scientific Name', 'Group', 'WoE Summary Group', 'Plant taxa', 'File type',
#     #      'obligate plant', 'Crop','PCE Collected/Modification needed', 'Percent of Species']]).copy()
#     # if 'NA' in count_all_species ["Percent of Species"].values.tolist():
#     #     count_all_species ["Percent of Species"].replace({"NA": 0}, inplace=True)
#     #     count_all_species ['Percent of Species'] =  count_all_species ['Percent of Species'].map(lambda x: x).astype(float)
#     # count_all_species= count_all_species.fillna('NE')
#     # group_all_species =  count_all_species.groupby(['EntityID', 'Common Name', 'Scientific Name', 'Group', 'WoE Summary Group', 'Plant taxa', 'File type','obligate plant', 'Crop','PCE Collected/Modification needed'],sort=False)['Percent of Species'].agg(['sum', 'count']).reset_index()
#     #
#     # group_all_species = group_all_species[group_all_species ['count'] > 0]
#     # group_all_species['Species Count'] = group_all_species.loc[group_all_species['File type'] == "Range", 'EntityID'].nunique()
#     # group_all_species['Critical Habitat Count'] = group_all_species.loc[group_all_species['File type'] != "Range", 'EntityID'].nunique()
#     #
#     # group_all_species.columns = ['EntityID', 'Common Name', 'Scientific Name', 'Group', 'WoE Summary Group',
#     #                              'Plant taxa', 'obligate plant', 'File type', 'Crop', 'PCE Collected/Modification needed','Percent of Species',
#     #                              'Number of Counties for Species', 'Species Count',
#     #                              'Critical Habitat Count']
#     #
#     # if os.path.exists(out_path_combined + os.sep + 'Species_Generalist_Combined_' + v + '.csv'):
#     #     previous = pd.read_csv(out_path_combined + os.sep + 'Species_Generalist_Combined_' + v + '.csv')
#     #     group_all_species.append(previous, ignore_index=True)
#     # group_all_species.to_csv(out_path_combined + os.sep + 'Species_Generalist_Combined_' + v + '.csv')
#     #
#     #
#     # all_species=  combine_crops [(combine_crops ['adjusted on_off_totaloverlap']>=0.45) &(combine_crops ['Percent of County']>=0.00)&(combine_crops ['PCE Collected/Modification needed']!='no')].copy()
#     #
#     # counties_all_species = (all_species[['GEOID', 'Counties', 'State', 'USPS ABB', 'Crop', 'cnty_Acres', 'Percent of County']]).copy()
#     # group_counties_all_species = counties_all_species.groupby(['GEOID', 'Counties', 'State', 'USPS ABB', 'Crop', 'cnty_Acres']).agg(['max', 'count']).reset_index()
#     # group_counties_all_species.columns = ['GEOID', 'Counties', 'State', 'USPS ABB', 'Crop', 'County Area',
#     #                                       'Percent of County', 'Number of Species']
#     #
#     # group_counties_all_species = pd.merge(group_counties_all_species, coa_df, how='left', on=['GEOID', 'Crop'])
#     # group_counties_all_species_totals = group_counties_all_species.groupby(['Crop']).sum().reset_index()
#     #
#     # for col in ['County Area','Percent of County',  'Number of Species']:
#     #     if col in group_counties_all_species_totals.columns.values.tolist():
#     #         group_counties_all_species_totals.drop(col, axis=1, inplace=True)
#     # group_counties_all_species_totals.columns = ['Crop', 'Acres_total']
#     #
#     # group_counties_all_species = pd.merge(group_counties_all_species, group_counties_all_species_totals, how='left',
#     #                                       on=['Crop'])
#     # group_counties_all_species = pd.merge(group_counties_all_species, coa_total, how='left', on=['Crop'])
#     # group_counties_all_species['Percent of Impact'] = (group_counties_all_species['Acres_total'] /
#     #                                                    group_counties_all_species['Totals']) * 100
#     # group_counties_all_species['County Count'] = group_counties_all_species['GEOID'].nunique()
#     # if os.path.exists(out_path_combined + os.sep + 'CH_Generalist_Combined_' + v + '.csv'):
#     #     previous = pd.read_csv(out_path_combined + os.sep + 'CH_Generalist_Combined_' + v + '.csv')
#     #     group_counties_all_species.append(previous, ignore_index=True)
#     # group_counties_all_species.to_csv(out_path_combined + os.sep + 'CH_Generalist_Combined_' + v + '.csv')
#     #
#     # count_all_species = (all_species[
#     #     ['EntityID', 'Common Name', 'Scientific Name', 'Group', 'WoE Summary Group', 'Plant taxa', 'File type',
#     #      'obligate plant', 'Crop','PCE Collected/Modification needed','Percent of Species']]).copy()
#     # if 'NA' in count_all_species ["Percent of Species"].values.tolist():
#     #     count_all_species ["Percent of Species"].replace({"NA": 0}, inplace=True)
#     #     count_all_species ['Percent of Species'] =  count_all_species ['Percent of Species'].map(lambda x: x).astype(float)
#     # count_all_species= count_all_species.fillna('NE')
#     # group_all_species =  count_all_species.groupby(['EntityID', 'Common Name', 'Scientific Name', 'Group', 'WoE Summary Group', 'Plant taxa', 'File type','obligate plant', 'Crop','PCE Collected/Modification needed'],sort=False)['Percent of Species'].agg(['sum', 'count']).reset_index()
#     # group_all_species = group_all_species[group_all_species['count'] > 0]
#     # group_all_species['Species Count'] = group_all_species.loc[group_all_species['File type'] == "Range", 'EntityID'].nunique()
#     # group_all_species['Critical Habitat Count'] = group_all_species.loc[group_all_species['File type'] != "Range", 'EntityID'].nunique()
#     #
#     # group_all_species.columns = ['EntityID', 'Common Name', 'Scientific Name', 'Group', 'WoE Summary Group',
#     #                              'Plant taxa', 'obligate plant', 'File type', 'Crop', 'PCE Collected/Modification needed','Percent of Species',
#     #                              'Number of Counties for Species', 'Species Count',
#     #                              'Critical Habitat Count']
#     #
#     # if os.path.exists(out_path_combined + os.sep + 'Species_CH_Generalist_Combined_' + v + '.csv'):
#     #     previous = pd.read_csv(out_path_combined + os.sep + 'Species_CH_Generalist_Combined_' + v + '.csv')
#     #     group_all_species.append(previous, ignore_index=True)
#     # group_all_species.to_csv(out_path_combined + os.sep + 'CH_Species_Generalist_Combined_' + v + '.csv')
