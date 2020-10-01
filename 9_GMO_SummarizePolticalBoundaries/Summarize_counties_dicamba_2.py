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

        if col.endswith('acres'):
            col = col.replace('acres', 'overlap')
            overlap_cols.append(col)
        else:
            overlap_cols.append(col)
    overlap.columns = overlap_cols

    return overlap


def on_off_apply(row, on_off_col, drift_col, direct_col, ft):
    # removed on field overlap from drift buffer if species is designated as off site
    if row[ft] == 'Range':
        if row[on_off_col] == 'OFF':
            adjust_value = row[drift_col] - row[direct_col]
        else:
            adjust_value = row[drift_col]
    else:
        adjust_value = row[drift_col]
    return adjust_value


# path_table = [r"E:\Dicamba Update Summer 2020\SpeciesTabulated\Range_Filtered_CONUS_CDL_1317_20x2_eucCnty_240m.csv",
#               "E:\Dicamba Update Summer 2020\SpeciesTabulated\Range_Filtered_CONUS_CDL_1317_40x2_eucCnty_240m.csv",]
# species_zone_lookup = r'D:\Species\UnionFile_Spring2020\Range\LookUp_Grid_byProjections_Combined_20200427\CONUS_Albers_Conical_Equal_Area'

path_table = ["E:\Dicamba Update Summer 2020\SpeciesTabulated\CH_Filtered_CONUS_CDL_1317_20x2_eucCnty_240m.csv",
              "E:\Dicamba Update Summer 2020\SpeciesTabulated\CH_Filtered_CONUS_CDL_1317_40x2_eucCnty_240m.csv",
              r"E:\Dicamba Update Summer 2020\SpeciesTabulated\Range_Filtered_CONUS_CDL_1317_20x2_eucCnty_240m.csv",
              r"E:\Dicamba Update Summer 2020\SpeciesTabulated\Range_Filtered_CONUS_CDL_1317_40x2_eucCnty_240m.csv"]

crops = ['Cotton', 'Soybeans']
aqu_woe_groups = ['Amphibians', 'Fish', 'Aquatic Invertebrates', 'Fishes', 'Aquatic Invertebrate']
terr_woe_groups = ['Plants', 'Terrestrial Invertebrates', 'Mammals','Reptiles', 'Birds']

out_path = 'E:\Dicamba Update Summer 2020\Update Summer 2020\Test_Overlap Summary by Crop_onoff_wcnty'
out_path_combined = 'E:\Dicamba Update Summer 2020\Update Summer 2020\Test_Overlap summary combined_onoff_wcnty'

master_species_list = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\MasterListESA_Dec2018_June2020.csv"
on_off = r"E:\Dicamba Update Summer 2020\Update Summer 2020\On_Off_calls_June2020.xlsx"
plant_lookup = r"E:\Dicamba Update Summer 2020\Update Summer 2020\Plant_lookup.csv"
collected_pces = r"E:\Dicamba Update Summer 2020\Update Summer 2020\PCE_lookup.csv"
obl_plant = r"E:\Dicamba Update Summer 2020\Update Summer 2020\Dicamba_obligate_plant.xlsx"

species = pd.read_csv(master_species_list)
on_off = pd.read_excel(on_off)
obl_plant_df = pd.read_excel(obl_plant)

plant = pd.read_csv(plant_lookup)
species['EntityID'] = species['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
species_groups = species[['EntityID', 'Common Name', 'Scientific Name', 'Group', 'WoE Summary Group']].copy()
on_off['EntityID'] = on_off['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
on_off_ag = on_off[['EntityID', 'On/Off_AG']]
plant['EntityID'] = plant['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
plant_df = plant[['EntityID', 'Plant taxa']]
pce = pd.read_csv(collected_pces)
pce_df = pce[['EntityID', 'PCE Collected/Modification needed']]
pce_df['EntityID'] = pce_df['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
obl_plant_df['EntityID'] = obl_plant_df['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)

all_crop_df = pd.DataFrame()
for i in path_table:
    df = pd.read_csv(i)
    if os.path.basename(i).startswith('Range'):
        file_type = "_range_"
        file_marker = "Range"
        species_area = r"E:\Dicamba Update Summer 2020\Update Summer 2020\R_Acres_Pixels_20200628_dicamba_on_off.csv"
        # species_area = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\R_Acres_Pixels_20200628.csv"
    else:
        file_type = "_critical habitat_"
        file_marker = "Critial habitat"
        species_area = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\CH_Acres_Pixels_20200628.csv"

    acres = pd.read_csv(species_area)
    acres['EntityID'] = acres['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
    acres_df = acres[['EntityID', 'Acres_CONUS']]

    if '20x2' in os.path.basename(i).split('_'):
        crop = 'Cotton'
    else:
        crop = 'Soybeans'
    df['EntityID'] = df['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
    # removed the value columns in msq
    for v in df.columns.values.tolist():
        if v.startswith("VALUE"):
            df.drop(v,axis=1,inplace=True)
    # removed the interval summed msq columns
    for v in df.columns.values.tolist():
            if not v.endswith("acres") and v!= 'EntityID':
                df.drop(v,axis=1,inplace=True)
    df_sum = df.groupby(['EntityID']).sum().reset_index()
    df_sum =pd.merge(df_sum, acres_df, how='left', on='EntityID')
    use_cols = [v for v in df_sum if v.startswith(crop)]
    df_overlap = calculation('Acres_CONUS', use_cols, df_sum)
    df_overlap['File Type'] = file_marker
    if len(all_crop_df) == 0:
        all_crop_df = df_overlap.copy()
    else:
        col_overlap = df_overlap.columns.values.tolist()
        cols_final = all_crop_df.columns.values.tolist()
        common_col = [v for v in col_overlap if v in cols_final]
        all_crop_df= pd.merge(all_crop_df,df_overlap,how='outer',on=common_col)

all_crop_df.fillna(0, inplace=True)
all_crop_df= all_crop_df.groupby(['EntityID', 'File Type']).sum().reset_index()

all_crop_df = pd.merge(species_groups,all_crop_df,how='right', on='EntityID')
all_crop_df = pd.merge(all_crop_df,on_off_ag,how='left', on='EntityID')
all_crop_df = pd.merge(all_crop_df,plant_df,how='left', on='EntityID')
all_crop_df = pd.merge(all_crop_df,pce_df,how='left', on='EntityID')
all_crop_df= pd.merge(all_crop_df, obl_plant_df, on='EntityID', how='left')

list_crop_cols = [k for k in all_crop_df.columns.values.tolist() if  k.endswith('overlap')]

list_intervals = sorted(list(set([int(k.split("_")[1]) for k in list_crop_cols])))
crops = list(set([k.split("_")[0] for k in list_crop_cols]))
print list_intervals, crops

for c in crops:
    for v in list_intervals:
        all_crop_df['adjusted on_off_' + c + "_"+str(v)] =  all_crop_df.apply(lambda row: on_off_apply(row, 'On/Off_AG', c+"_"+str(v)+"_overlap", c+"_0_overlap",'File Type'),
            axis=1)

all_crop_df.to_csv(out_path + os.sep+'All_species_ch.csv')

for v in list_intervals:
    non_monocot =  all_crop_df[((all_crop_df ['Plant taxa'] !='M')&(all_crop_df ['WoE Summary Group'] =='Plants')&
                                (all_crop_df['adjusted on_off_Soybeans' + "_"+str(v)]>=0.45)&
                                (all_crop_df ['File Type'] =='Range'))
                               |((all_crop_df ['Plant taxa'] !='M')&(all_crop_df ['WoE Summary Group'] =='Plants')&
                                 (all_crop_df['adjusted on_off_Cotton' + "_"+str(v)]>=0.45)&
                                 (all_crop_df ['File Type'] =='Range'))|((all_crop_df ['WoE Summary Group'].isin(terr_woe_groups))
                                    &(all_crop_df['adjusted on_off_Soybeans' + "_"+str(v)]>=0.45)
                                    &(all_crop_df ['File Type'] !='Range'))|
                                   ((all_crop_df ['WoE Summary Group'].isin(terr_woe_groups))
                                    &(all_crop_df['adjusted on_off_Cotton' + "_"+str(v)]>=0.45)
                                    &(all_crop_df ['File Type'] !='Range'))].copy()

    non_monocot['Species Count'] = non_monocot.loc[non_monocot['File Type'] == "Range", 'EntityID'].nunique()
    non_monocot['Critical Habitat Count'] = non_monocot.loc[non_monocot['File Type'] != "Range", 'EntityID'].nunique()
    non_monocot.to_csv(out_path_combined + os.sep + 'Species_Combined_NonMonocot_' + str(v) + '.csv')

    # (all_crop_df['Soybeans_0_overlap' ]<1)
    for v in list_intervals:
        non_monocot_obl =  all_crop_df[((all_crop_df ['Plant taxa'] !='M')&(all_crop_df ['WoE Summary Group'] =='Plants')&
                                    (all_crop_df['adjusted on_off_Soybeans' + "_"+str(v)]>=0.45)&
                                    (all_crop_df ['File Type'] =='Range'))
                                   |((all_crop_df ['Plant taxa'] !='M')&(all_crop_df ['WoE Summary Group'] =='Plants')&
                                     (all_crop_df['adjusted on_off_Cotton' + "_"+str(v)]>=0.45)&
                                     (all_crop_df ['File Type'] =='Range'))|((all_crop_df ['WoE Summary Group'].isin(terr_woe_groups))
                                                                             &(all_crop_df['adjusted on_off_Soybeans' + "_"+str(v)]>=0.45)
                                                                             &(all_crop_df ['File Type'] !='Range'))
                                       |((all_crop_df ['WoE Summary Group'].isin(terr_woe_groups))
                                         &(all_crop_df['adjusted on_off_Cotton' + "_"+str(v)]>=0.45)
                                         &(all_crop_df ['File Type'] !='Range'))
                                    |((all_crop_df ['obligate plant']=='Yes')&
                                    (all_crop_df['adjusted on_off_Soybeans' + "_"+str(v)]>=0.45)&
                                    (all_crop_df ['File Type'] =='Range'))
                                   |((all_crop_df ['obligate plant']=='Yes')&
                                     (all_crop_df['adjusted on_off_Cotton' + "_"+str(v)]>=0.45)&
                                     (all_crop_df ['File Type'] =='Range'))].copy()

        non_monocot_obl['Species Count'] = non_monocot_obl.loc[non_monocot_obl['File Type'] == "Range", 'EntityID'].nunique()
        non_monocot_obl['Critical Habitat Count'] = non_monocot_obl.loc[non_monocot_obl['File Type'] != "Range", 'EntityID'].nunique()
        non_monocot_obl.to_csv(out_path_combined + os.sep + 'Species_Combined_NonMonocotObl_' + str(v) + '.csv')

    for v in list_intervals:
        generalist =  all_crop_df[((all_crop_df['adjusted on_off_Soybeans' + "_"+str(v)]>=0.45)&
                                        (all_crop_df ['File Type'] =='Range'))
                                       |((all_crop_df['adjusted on_off_Cotton' + "_"+str(v)]>=0.45)&
                                         (all_crop_df ['File Type'] =='Range'))|
                                  ((all_crop_df ['WoE Summary Group'].isin(terr_woe_groups))
                                   &(all_crop_df['adjusted on_off_Soybeans' + "_"+str(v)]>=0.45)
                                   &(all_crop_df ['File Type'] !='Range'))|((all_crop_df ['WoE Summary Group'].isin(terr_woe_groups))
                                                                           &(all_crop_df['adjusted on_off_Cotton' + "_"+str(v)]>=0.45)
                                                                           &(all_crop_df ['File Type'] !='Range'))].copy()

        generalist['Species Count'] = generalist.loc[generalist['File Type'] == "Range", 'EntityID'].nunique()
        generalist['Critical Habitat Count'] = generalist.loc[generalist['File Type'] != "Range", 'EntityID'].nunique()
        generalist.to_csv(out_path_combined + os.sep + 'Species_Generalist_Combined_' + str(v) + '.csv')

    for v in list_intervals:

        all_ch =  all_crop_df[((all_crop_df['adjusted on_off_Soybeans' + "_"+str(v)]>=0.45)&
                                   (all_crop_df ['File Type'] =='Range'))
                                  |((all_crop_df['adjusted on_off_Cotton' + "_"+str(v)]>=0.45)&
                                    (all_crop_df ['File Type'] =='Range'))|((all_crop_df['adjusted on_off_Soybeans' + "_"+str(v)]>=0.45)
                                                                            &(all_crop_df ['File Type'] !='Range'))|((all_crop_df['adjusted on_off_Cotton' + "_"+str(v)]>=0.45)
                                                                                                                     &(all_crop_df ['File Type'] !='Range'))].copy()

        all_ch ['Species Count'] = all_ch .loc[all_ch ['File Type'] == "Range", 'EntityID'].nunique()
        all_ch ['Critical Habitat Count'] = all_ch .loc[all_ch ['File Type'] != "Range", 'EntityID'].nunique()
        all_ch .to_csv(out_path_combined + os.sep + 'Species_Generalist_CH_Combined_' + str(v) + '.csv')