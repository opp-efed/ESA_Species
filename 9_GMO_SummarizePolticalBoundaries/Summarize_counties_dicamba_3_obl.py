import os
import numpy as np
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


path_table = ["E:\Dicamba Update Summer 2020\SpeciesTabulated\CH_Filtered_CONUS_CDL_1317_20x2_eucCnty_240m.csv",
              "E:\Dicamba Update Summer 2020\SpeciesTabulated\CH_Filtered_CONUS_CDL_1317_40x2_eucCnty_240m.csv",
              r"E:\Dicamba Update Summer 2020\SpeciesTabulated\Range_Filtered_CONUS_CDL_1317_20x2_eucCnty_240m.csv",
              r"E:\Dicamba Update Summer 2020\SpeciesTabulated\Range_Filtered_CONUS_CDL_1317_40x2_eucCnty_240m.csv"]

percent_spe_overlap_in_cnty = 0.45
percent_cnty_crop = 0.0001
crops = ['Cotton', 'Soybeans']
aqu_woe_groups = ['Amphibians', 'Fish', 'Aquatic Invertebrates', 'Fishes', 'Aquatic Invertebrate']
terr_woe_groups = ['Mammals', 'Birds', 'Reptiles', 'Terrestrial Invertebrates', 'Plants']

out_path = 'E:\Dicamba Update Summer 2020\Update Summer 2020\Overlap Summary by Crop_onoff_wcnty'
out_path_combined = 'E:\Dicamba Update Summer 2020\Update Summer 2020\Overlap summary combined_onoff_wcnty'

master_species_list = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\MasterListESA_Dec2018_June2020.csv"
on_off = r"E:\Dicamba Update Summer 2020\Update Summer 2020\On_Off_calls_June2020.xlsx"
plant_lookup = r"E:\Dicamba Update Summer 2020\Update Summer 2020\Plant_lookup.csv"
collected_pces = r"E:\Dicamba Update Summer 2020\Update Summer 2020\PCE_lookup.csv"
counties_area = r"E:\Dicamba Update Summer 2020\Update Summer 2020\counties_area.csv"
coa_dicamba = r"C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\Risk Assessments\GMOs\dicamba\Update Summer 2020\Dicamba_Census (2012) Crops Areas and Operations (interim draft for esa_11-15-18).csv"
obl_plant = r"E:\Dicamba Update Summer 2020\Update Summer 2020\Dicamba_obligate_plant.xlsx"

species = pd.read_csv(master_species_list)
on_off = pd.read_excel(on_off)
cnty_acres = pd.read_csv(counties_area)
coa = pd.read_csv(coa_dicamba)
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

cnty_acres['GEOID'] = cnty_acres['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
cnty_acres_df = cnty_acres[['GEOID', 'STUSPS', 'State', 'cnty_Acres']]
coa['GEOID'] = coa['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
coa_df = coa[['GEOID', 'COTTON','SOYBEANS']].copy()
coa_df['COTTON_Total']=8956226
coa_df['SOYBEAN_Total']=76322406
coa_df['COTTON_UDL_Total']=21936473
coa_df['SOYBEAN_UDL_Total']=184246777

all_crop_df = pd.DataFrame()
for i in path_table:
    df = pd.read_csv(i)
    if os.path.basename(i).startswith('Range'):
        file_type = "_range_"
        file_marker = "Range"
        # species_area = r"E:\Dicamba Update Summer 2020\Update Summer 2020\R_Acres_Pixels_20200628_dicamba_on_off.csv"
        sp_area_ctny = "E:\Dicamba Update Summer 2020\Update Summer 2020\species_cnty_area_3.csv"
        species_area = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\R_Acres_Pixels_20200628.csv"
    else:
        file_type = "_critical habitat_"
        file_marker = "Critial Habitat"
        species_area = r"C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - Herbicide BEs 2020\Overlap Inputs\CH_Acres_Pixels_20200628.csv"
        sp_area_ctny = "E:\Dicamba Update Summer 2020\Update Summer 2020\ch_cnty_area.csv"

    acres = pd.read_csv(species_area)
    acres['EntityID'] = acres['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
    acres_df = acres[['EntityID', 'Acres_CONUS']]
    sp_area_ctny_df = pd.read_csv(sp_area_ctny)
    sp_area_ctny_df['EntityID'] = sp_area_ctny_df['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
    sp_area_ctny_df['GEOID'] = sp_area_ctny_df['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
    sp_area_ctny_df = sp_area_ctny_df[['EntityID','SP_Cnt_Acres','GEOID']]

    if '20x2' in os.path.basename(i).split('_'):
        crop = 'Cotton'
        udl_cnty = r"E:\Dicamba Update Summer 2020\CONUS_CDL_1317_20x2_euc_County.csv"
        udl_col = 'Acres_UDL_Cotton_TOTAL'
    else:
        crop = 'Soybeans'
        udl_cnty = r"E:\Dicamba Update Summer 2020\CONUS_CDL_1317_40x2_euc_County.csv"
        udl_col = 'Acres_UDL_SOYBEANS_TOTAL'
    df['EntityID'] = df['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
    df['GEOID'] = df['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
    udl_cnty_df = pd.read_csv(udl_cnty)
    udl_cnty_df['GEOID'] = udl_cnty_df['GEOID'].map(lambda x: str(x) if len(str(x)) == 5 else '0' + str(x)).astype(str)
    udl_cnty_df = udl_cnty_df[['GEOID',udl_col ]]
    # removed the value columns in msq
    for v in df.columns.values.tolist():
        if v.startswith("VALUE"):
            df.drop(v,axis=1,inplace=True)
    # removed the interval summed msq columns
    for v in df.columns.values.tolist():
        if not v.endswith("acres") and v not in ['EntityID','GEOID','STUSPS']:
            df.drop(v,axis=1,inplace=True)
    df_sum = df.groupby(['EntityID','GEOID','STUSPS']).sum().reset_index()
    df_sum =pd.merge(df_sum, acres_df, how='left', on='EntityID')
    df_sum= pd.merge(df_sum,sp_area_ctny_df,how='left', on=['EntityID', 'GEOID'] )
    df_sum = pd.merge(df_sum, udl_cnty_df, how='left', on='GEOID')


    df_sum['File Type'] = file_marker
    if len(all_crop_df) == 0:
        all_crop_df = df_sum.copy()
    else:
        col_overlap = df_sum.columns.values.tolist()
        cols_final = all_crop_df.columns.values.tolist()
        common_col = [v for v in col_overlap if v in cols_final]
        all_crop_df= pd.merge(all_crop_df,df_sum,how='outer',on=common_col)

all_crop_df.fillna(0, inplace=True)
# print all_crop_df.columns.values.tolist()
all_crop_df= all_crop_df.groupby(['EntityID', 'File Type','GEOID','STUSPS','Acres_CONUS','SP_Cnt_Acres']).sum().reset_index()

all_crop_df = pd.merge(species_groups,all_crop_df,how='right', on='EntityID')
all_crop_df = pd.merge(all_crop_df,on_off_ag,how='left', on='EntityID')
all_crop_df = pd.merge(all_crop_df,plant_df,how='left', on='EntityID')
all_crop_df = pd.merge(all_crop_df,pce_df,how='left', on='EntityID')
all_crop_df= pd.merge(all_crop_df,cnty_acres_df,how='left', on='GEOID')
all_crop_df= pd.merge(all_crop_df,coa_df,how='left', on='GEOID')
all_crop_df= pd.merge(all_crop_df, obl_plant_df, on='EntityID', how='left')

all_crop_df['Percent of Species'] = (all_crop_df['SP_Cnt_Acres']/all_crop_df['Acres_CONUS'])*100
all_crop_df['Percent of County'] = (all_crop_df['SP_Cnt_Acres']/all_crop_df['cnty_Acres'])*100
all_crop_df['Percent of CoA Cotton'] = (all_crop_df['COTTON']/all_crop_df['cnty_Acres'])*100
all_crop_df['Percent of CoA Soybean'] = (all_crop_df['SOYBEANS']/all_crop_df['cnty_Acres'])*100
all_crop_df['Percent of Total CoA Cotton'] = (all_crop_df['COTTON']/all_crop_df['COTTON_Total'])*100
all_crop_df['Percent of Total CoA Soybean'] = (all_crop_df['SOYBEANS']/all_crop_df['SOYBEAN_Total'])*100
list_crop_cols = [k for k in all_crop_df.columns.values.tolist() if  k.endswith('acres')]

list_intervals = sorted(list(set([int(k.split("_")[1]) for k in list_crop_cols])))
crops = list(set([k.split("_")[0] for k in list_crop_cols]))
print list_intervals, crops

for c in crops:
    for v in list_intervals:
        all_crop_df['adjusted on_off_' + c + "_"+str(v)] =  all_crop_df.apply(lambda row: on_off_apply(row, 'On/Off_AG', c+"_"+str(v)+"_acres", c+"_0_acres",'File Type'),
                                                                              axis=1)
        all_crop_df['Percent_Cnty_' + c + "_"+str(v)] =  (all_crop_df['adjusted on_off_' + c + "_"+str(v)]/all_crop_df['cnty_Acres'])*100
        all_crop_df['Percent_Cnty_Full_' + c + "_"+str(v)] =  (all_crop_df['adjusted on_off_' + c + "_"+str(v)]/all_crop_df['Acres_CONUS'])*100

all_crop_df['Percent_Cnty_CoA_Soybeans_0'] =  (all_crop_df['SOYBEANS']/all_crop_df['cnty_Acres'])*100
all_crop_df['Percent_Cnty_CoA_Cotton_0'] =  (all_crop_df['COTTON']/all_crop_df['cnty_Acres'])*100
all_crop_df['Percent_Cnty_UDL_Soybeans_0'] =  (all_crop_df['Soybeans_0_acres']/all_crop_df['cnty_Acres'])*100
all_crop_df['Percent_Cnty_UDL_Cotton_0'] =  (all_crop_df['Cotton_0_acres']/all_crop_df['cnty_Acres'])*100
all_crop_df['County confidence'] = np.where(((all_crop_df['Percent of Species']>percent_cnty_crop)&(all_crop_df['Percent of County']>10)&
                                             ((all_crop_df['Percent_Cnty_CoA_Soybeans_0']> percent_spe_overlap_in_cnty) &
                                              (all_crop_df['Percent_Cnty_UDL_Soybeans_0']>percent_spe_overlap_in_cnty)|
                                              (all_crop_df['Percent_Cnty_CoA_Cotton_0']>percent_spe_overlap_in_cnty) &
                                              (all_crop_df['Percent_Cnty_UDL_Cotton_0']>percent_spe_overlap_in_cnty))) , 'Include', 'Not Confident')

all_crop_df.to_csv(out_path + os.sep+'All_species_ch_cnty.csv')

# print all_crop_df.columns.tolist()
for v in list_intervals:
    species_df = pd.read_csv(out_path_combined + os.sep + 'Species_Combined_NonMonocot_' + str(v) + '.csv')
    species_df ['EntityID'] = species_df ['EntityID'].map(lambda x: str(x).split(".")[0]).astype(str)
    sp_range =species_df[(species_df ['File Type'] =='Range') &((species_df['adjusted on_off_Soybeans_'+str(v)]>percent_spe_overlap_in_cnty)|species_df['adjusted on_off_Cotton_'+str(v)]>percent_spe_overlap_in_cnty)].copy()
    range_list = list(set(sp_range['EntityID'].values.tolist()))
    sp_ch =species_df[(species_df ['File Type'] !='Range')&((species_df['adjusted on_off_Soybeans_'+str(v)]>0)|species_df['adjusted on_off_Cotton_'+str(v)]>0)].copy()
    ch_list = list(set(sp_ch['EntityID'].values.tolist()))

    non_monocot_obl =  all_crop_df [((all_crop_df['Plant taxa'] !='M')&(all_crop_df ['WoE Summary Group'] =='Plants')&
                                     (all_crop_df['adjusted on_off_Soybeans_' + str(v)]>=1)&
                                     (all_crop_df['SOYBEANS']>=1)&
                                     (all_crop_df['Percent_Cnty_Soybeans_' + str(v)]>percent_spe_overlap_in_cnty)&
                                     (all_crop_df ['File Type'] =='Range')&
                                     (all_crop_df ['EntityID'].isin(range_list)))|
                                    ((all_crop_df['Plant taxa'] !='M')&(all_crop_df ['WoE Summary Group'] =='Plants')&
                                     (all_crop_df['COTTON']>=1)&
                                     (all_crop_df['adjusted on_off_Cotton_' + str(v)]>=1)&
                                     (all_crop_df['Percent_Cnty_Cotton_' + str(v)]>percent_spe_overlap_in_cnty)&
                                     (all_crop_df ['File Type'] =='Range')&
                                     (all_crop_df ['EntityID'].isin(range_list)))
                                    |
                                    ((all_crop_df ['WoE Summary Group'].isin(terr_woe_groups))
                                     &(all_crop_df['adjusted on_off_Soybeans' + "_"+str(v)]>=1) &
                                     (all_crop_df['SOYBEANS']>=1)&
                                     (all_crop_df['Percent_Cnty_Soybeans_' + str(v)]>percent_spe_overlap_in_cnty)
                                     &(all_crop_df ['File Type'] !='Range')&
                                     (all_crop_df ['EntityID'].isin(ch_list)))|
                                    ((all_crop_df ['WoE Summary Group'].isin(terr_woe_groups))
                                     &(all_crop_df['adjusted on_off_Cotton' + "_"+str(v)]>=1)&
                                     (all_crop_df['COTTON']>=1)&
                                     (all_crop_df['Percent_Cnty_Cotton_' + str(v)]>percent_spe_overlap_in_cnty)
                                     &(all_crop_df ['File Type'] !='Range') &
                                     (all_crop_df ['EntityID'].isin(ch_list)))
                                    |
                                    ((all_crop_df ['obligate plant']=='Yes')&
                                      (all_crop_df['adjusted on_off_Soybeans' + "_"+str(v)]>=1)&
                                    (all_crop_df ['File Type'] =='Range'))
                                    |
                                    ((all_crop_df ['obligate plant']=='Yes')&
                                      (all_crop_df['adjusted on_off_Cotton' + "_"+str(v)]>=1)&
                                      (all_crop_df ['File Type'] =='Range'))].copy()

    non_monocot_obl['County Count'] = non_monocot_obl['GEOID'].nunique()
    non_monocot_obl['County confidence'] = np.where((((non_monocot_obl['Percent_Cnty_Full_Soybeans_'+str(v)]<percent_cnty_crop)&
                                                      (non_monocot_obl['Percent_Cnty_Full_Cotton_'+str(v)]<percent_cnty_crop))),
                                                    'Not Confident', 'Include')
    percent_calc = non_monocot_obl[non_monocot_obl['County confidence']=='Include'].copy()
    percent_calc = percent_calc[['GEOID','COTTON','COTTON_Total','SOYBEANS','SOYBEAN_Total']].copy()
    percent_calc.drop_duplicates(inplace=True)
    percent_calc['Confident County Count'] = percent_calc['GEOID'].nunique()
    percent_calc['Percent Impact Cotton'] = (percent_calc ['COTTON'].sum()/percent_calc ['COTTON_Total'])*100
    percent_calc['Percent Impact Soybean'] = (percent_calc ['SOYBEANS'].sum()/percent_calc ['SOYBEAN_Total'])*100
    percent_calc= percent_calc[['GEOID','Percent Impact Soybean','Percent Impact Cotton','Confident County Count']]

    non_monocot_obl= pd.merge(non_monocot_obl,percent_calc,how='left',on='GEOID')
    non_monocot_obl.to_csv(out_path_combined + os.sep + 'Combined_NonMonocotObl_' + str(v) + '.csv')


