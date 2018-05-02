import datetime
import os

import pandas as pd

# Title - Transforms out results by zone and summarize totals by species - final output is a master sum table of results
# by use and interval for each species

# TODO set up separate script that will spit out chem specific table with different interval include aerial and group

# inlocation
in_table = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\FinalTables_Range\BETables\R_AllUses_BE_20170209.csv'
master_col = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'CH_GIS',
              'Source of Call final BE-Range', 'WoE Summary Group', 'Source of Call final BE-Range']
# master list
temp_folder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\FinalTables_Range\BETables'

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

out_csv = temp_folder + os.sep + 'R_Collapsed_WoE_'+ date + '.csv'
sp_index_cols = 16
col_reindex = ['EntityID', 'comname', 'sciname', 'family', 'status_text', 'pop_abbrev', 'Des_CH', 'Critical_Habitat_',
               'Migratory', 'Migratory_', 'Corn', 'Cotton', 'Orchards and Vineyards', 'Other Crops', 'Other Grains',
               'Other RowCrops', 'Pasture', 'Rice', 'Soybeans', 'Vegetables and Ground Fruit', 'Wheat', 'Developed',
               'Managed Forests', 'Nurseries', 'Open Space Developed', 'Pine Seed Orchards', 'Right of Way',
               'Christmas Trees', 'Golfcourses', 'Cattle Eartag', 'Alley Crops', 'Bermuda Grass', 'Culitivated',
               'Non Cultivated', 'Methomyl Wheat', 'Rangeland', 'Flybait', 'Mosquito Control', 'Wide Area Use',
               'CH_GIS',
               'Group', 'Alaska', 'American Samoa', 'Hawaii', 'Lower 48', 'Guam', 'Puerto Rico', 'Virgin Islands',
               'Commonwealth of the Northern Mariana Islands', 'Nurseries_DiazBuffer',
               'VegetablesGroundFruit_DiazBuffer',
               'OrchardsVineyards_DiazBuffer', 'Diazinon_ActionArea', 'Corn_CarbarylBuffer',
               'Orchards and Vineyards_CarbarylBuffer',
               'Other Crops_CarbarylBuffer', 'Other Grains_CarbarylBuffer', 'Other RowCrops_CarbarylBuffer',
               'Pasture_CarbarylBuffer', 'Rice_CarbarylBuffer', 'Soybeans_CarbarylBuffer',
               'Vegetables and Ground Fruit_CarbarylBuffer', 'Wheat_CarbarylBuffer', 'Developed_CarbarylBuffer',
               'Managed Forests_CarbarylBuffer', 'Nurseries_CarbarylBuffer', 'Open Space Developed_CarbarylBuffer',
               'Right of Way_CarbarylBuffer', 'Golfcourses_CarbarylBuffer','Rangeland_CarbarylBuffer', 'Culitivated_CarbarylBuffer','CattleEarTag_CarbarylBuffer',
               'Non Cultivated_CarbarylBuffer', 'Carbaryl_ActionArea', 'Corn_MethomylBuffer',
               'Orchards and Vineyards_MethomylBuffer', 'Other Crops_MethomylBuffer', 'Other Grains_MethomylBuffer',
               'Other RowCrops_MethomylBuffer', 'Pasture_MethomylBuffer', 'Soybeans_MethomylBuffer',
               'Vegetables and Ground Fruit_MethomylBuffer', 'Methomyl Wheat_Buffer', 'Bermuda Grass_MethomylBuffer',
               'Cotton_MethomylBuffer', 'Alley Cropping_MethomylBuffer', 'Methomyl_ActionArea',
               'Federally Managed Lands',
               'FWS Refuge', 'Indian Reservations', 'Wilderness land', 'Cull Piles'
               ]
## need ot add in AS for Range

collapses_dict = {
    'Corn': ['CONUS_Corn_0'],
    'Cotton': ['CONUS_Cotton_0'],
    'Orchards and Vineyards': ['CONUS_Orchards and Vineyards_0'],
    'Other Crops': ['CONUS_Other Crops_0'],
    'Other Grains': ['CONUS_Other Grains_0', ],
    'Other RowCrops': ['CONUS_Other RowCrops_0'],
    'Pasture': ['CONUS_Pasture_0', 'AK_Pasture_0', 'CNMI_Cattle Eartag_0',
                'GU_Cattle Eartag_0', 'AS_Cattle Eartag_0',
                'HI_Cattle Eartag_0', 'PR_Cattle Eartag_0', 'VI_Cattle Eartag_0'],
    'Rice': ['CONUS_Rice_0'],
    'Soybeans': ['CONUS_Soybeans_0'],
    'Vegetables and Ground Fruit': ['CONUS_Vegetables and Ground Fruit_0'],
    'Wheat': ['CONUS_Wheat_0'],
    'Developed': ['CONUS_Developed_0', 'AK_Developed_0', 'CNMI_Developed_0', 'GU_Developed_0',
                  'HI_Developed_0', 'AS_Developed_0','PR_Developed_0', 'VI_Developed_0'],
    'Managed Forests': ['CONUS_Managed Forests_0', 'AK_Managed Forests_0', 'CNMI_Managed Forests_0',
                        'GU_Managed Forests_0', 'HI_Managed Forests_0', 'PR_Managed Forests_0',
                        'VI_Managed Forests_0'],
    'Nurseries': ['CONUS_Nurseries_0', 'AK_Nurseries_0', 'HI_Nurseries_0', 'PR_Nurseries_0',
                  'VI_Nurseries_0'],
    'Open Space Developed': ['CONUS_Open Space Developed_0', 'AK_Open Space Developed_0',
                             'CNMI_Open Space Developed_0', 'GU_Open Space Developed_0', 'HI_Open Space Developed_0',
                             'PR_Open Space Developed_0', 'VI_Open Space Developed_0', 'AS_Open Space Developed_0'],
    'Pine Seed Orchards': ['CONUS_Pine seed orchards_0'],
    'Right of Way': ['CONUS_Right of Way_0', 'AK_Right of Way_0', 'CNMI_Right of Way_0', 'GU_Right of Way_0',
                     'HI_Right of Way_0', 'PR_Right of Way_0', 'VI_Right of Way_0', 'AS_Right of Way_0'],
    'Christmas Trees': ['CONUS_Christmas Trees_0'],
    'Golfcourses': ['CONUS_Golfcourses_0', 'AK_Golfcourses_0', 'GU_Golfcourses_0',
                    'HI_Golfcourses_0',
                    'PR_Golfcourses_0'],
    'Cattle Eartag': ['CONUS_Cattle Eartag_0', 'AK_Cattle Eartag_0', 'CNMI_Cattle Eartag_0',
                  'GU_Cattle Eartag_0', 'HI_Cattle Eartag_0', 'PR_Cattle Eartag_0', 'VI_Cattle Eartag_0',
                  'AS_Cattle Eartag_0'],
    'Cull Piles': ['CONUS_Cull Piles_0'],
    'Alley Crops': ['CONUS_Alley Cropping_0'],
    'Bermuda Grass': ['CONUS_Bermuda Grass_0'],
    'Culitivated': ['CONUS_Cultivated_0', 'HI_Ag_0', 'PR_Ag_0', 'AK_Ag_0', 'CNMI_Ag_0',
                    'GU_Ag_0', 'VI_Ag_0', 'AS_Ag_0'],
    'Rangeland': ['CONUS_Cattle Eartag_0', 'AK_Cattle Eartag_0', 'CNMI_Cattle Eartag_0',
                  'GU_Cattle Eartag_0', 'HI_Cattle Eartag_0', 'PR_Cattle Eartag_0', 'VI_Cattle Eartag_0',
                  'AS_Cattle Eartag_0'],
    'Non Cultivated': ['CONUS_Non Cultivated_0', 'AK_Non Cultivated_0',
                       'CNMI_Non Cultivated_0', 'GU_Non Cultivated_0', 'HI_Non Cultivated_0', 'PR_Non Cultivated_0',
                       'VI_Non Cultivated_0', 'AS_Non Cultivated_0'],
    'Methomyl Wheat': ['CONUS_zMethomylWheat_0'],

    'Flybait': ['CONUS_Developed_0', 'AK_Developed_0', 'CNMI_Developed_0', 'GU_Developed_0',
                'HI_Developed_0', 'PR_Developed_0', 'VI_Developed_0', 'AS_Developed_0'],
    'Mosquito Control': [''],
    'Wide Area Use': [''],
    'Alaska': ['AK_Ag_0'],
    'Hawaii': ['HI_Ag_0'],
    'Lower 48': ['CONUS_Cultivated_0'],
    'Guam': ['GU_Ag_0'],
    'Puerto Rico': ['PR_Ag_0'],
    'Virgin Islands': ['VI_Ag_0'],
    'American Samoa': ['AS_Ag_0'],
    'Commonwealth of the Northern Mariana Islands': ['CNMI_Ag_0'],
    'Nurseries_DiazBuffer': ['CONUS_Nurseries_305', 'AK_Nurseries_305', 'HI_Nurseries_305',
                             'PR_Nurseries_305', 'VI_Nurseries_305'],
    'VegetablesGroundFruit_DiazBuffer': ['CONUS_Vegetables and Ground Fruit_765', 'HI_Ag_765',
                                     'PR_Ag_765','AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765','VI_Ag_765', 'AS_Ag_765'],
    'OrchardsVineyards_DiazBuffer': ['CONUS_Orchards and Vineyards_305', 'HI_Ag_305',
                                     'PR_Ag_305','AK_Ag_305', 'CNMI_Ag_305', 'GU_Ag_305','VI_Ag_305', 'AS_Ag_305'],
    'Diazinon_ActionArea': ['CONUS_Diazinon_AA_765', 'AK_Diazinon_AA_765', 'CNMI_Diazinon_AA_765',
                            'GU_Diazinon_AA_765', 'HI_Diazinon_AA_765', 'PR_Diazinon_AA_765', 'VI_Diazinon_AA_765',
                            'AS_Diazinon_AA_765'],
    'Corn_CarbarylBuffer': ['CONUS_Corn_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765',
                            'VI_Ag_765', 'AS_Ag_765'],
    'Orchards and Vineyards_CarbarylBuffer': ['CONUS_Orchards and Vineyards_765', 'AK_Ag_765', 'CNMI_Ag_765',
                                              'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765', 'VI_Ag_765', 'AS_Ag_765'],
    'Other Crops_CarbarylBuffer': ['CONUS_Other Crops_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765',
                                   'PR_Ag_765', 'VI_Ag_765', 'AS_Ag_765'],
    'Other Grains_CarbarylBuffer': ['CONUS_Other Grains_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765',
                                    'PR_Ag_765', 'VI_Ag_765', 'AS_Ag_765'],
    'Other RowCrops_CarbarylBuffer': ['CONUS_Other RowCrops_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765',
                                      'PR_Ag_765', 'VI_Ag_765', 'AS_Ag_765'],
    'Pasture_CarbarylBuffer': ['CONUS_Pasture_765', 'AK_Pasture_765', 'CNMI_Cattle Eartag_765', 'GU_Cattle Eartag_765',
                               'HI_Cattle Eartag_765', 'PR_Cattle Eartag_765', 'VI_Cattle Eartag_765',
                               'AS_Cattle Eartag_765'],
    'Rice_CarbarylBuffer': ['CONUS_Rice_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765',
                            'VI_Ag_765', 'AS_Ag_765'],
    'Soybeans_CarbarylBuffer': ['CONUS_Soybeans_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765',
                                'VI_Ag_765', 'AS_Ag_765'],
    'Vegetables and Ground Fruit_CarbarylBuffer': ['CONUS_Vegetables and Ground Fruit_765', 'AK_Ag_765', 'CNMI_Ag_765',
                                                   'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765', 'VI_Ag_765', 'AS_Ag_765'],
    'Wheat_CarbarylBuffer': ['CONUS_Wheat_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765',
                             'VI_Ag_765', 'AS_Ag_765'],
    'Developed_CarbarylBuffer': ['CONUS_Developed_765', 'AK_Developed_765', 'CNMI_Developed_0', 'GU_Developed_765',
                                 'HI_Developed_765', 'PR_Developed_765', 'VI_Developed_765', 'AS_Developed_765'],
    'Managed Forests_CarbarylBuffer': ['CONUS_Managed Forests_765', 'AK_Managed Forests_765',
                                       'CNMI_Managed Forests_765', 'GU_Managed Forests_765', 'HI_Managed Forests_765',
                                       'PR_Managed Forests_765', 'VI_Managed Forests_765'],
    'Nurseries_CarbarylBuffer': ['CONUS_Nurseries_765', 'AK_Nurseries_765', 'HI_Nurseries_765', 'PR_Nurseries_765',
                                 'VI_Nurseries_765'],
    'Open Space Developed_CarbarylBuffer': ['CONUS_Open Space Developed_765', 'AK_Open Space Developed_765',
                                            'CNMI_Open Space Developed_765', 'AS_Open Space Developed_765',
                                            'GU_Open Space Developed_765', 'HI_Open Space Developed_765',
                                            'PR_Open Space Developed_765', 'VI_Open Space Developed_765'],
    'Right of Way_CarbarylBuffer': ['CONUS_Right of Way_765', 'AK_Right of Way_765', 'CNMI_Right of Way_765',
                                    'GU_Right of Way_765', 'HI_Right of Way_765', 'PR_Right of Way_765',
                                    'VI_Right of Way_765', 'AS_Right of Way_765'],
    'Golfcourses_CarbarylBuffer': ['CONUS_Golfcourses_765', 'AK_Golfcourses_765', 'GU_Golfcourses_765', 'HI_Golfcourses_765',
                    'PR_Golfcourses_765'],
    'Rangeland_CarbarylBuffer': ['CONUS_Cattle Eartag_765', 'AK_Cattle Eartag_765', 'CNMI_Cattle Eartag_765',
                                 'GU_Cattle Eartag_765', 'HI_Cattle Eartag_765', 'PR_Cattle Eartag_765',
                                 'VI_Cattle Eartag_765', 'AS_Cattle Eartag_765'],
    'Culitivated_CarbarylBuffer': ['CONUS_Cultivated_765', 'HI_Ag_765', 'PR_Ag_765', 'AK_Ag_765', 'CNMI_Ag_765',
                                   'GU_Ag_765', 'VI_Ag_765', 'AS_Ag_765'],
    'Non Cultivated_CarbarylBuffer': [ 'CONUS_Non Cultivated_765', 'AK_Non Cultivated_765',
                                      'CNMI_Non Cultivated_765', 'GU_Non Cultivated_765', 'HI_Non Cultivated_765',
                                      'PR_Non Cultivated_765',
                                      'VI_Non Cultivated_765', 'AS_Non Cultivated_765'],
    'CattleEarTag_CarbarylBuffer': ['CONUS_Cattle Eartag_765', 'AK_Cattle Eartag_765', 'CNMI_Cattle Eartag_765',
                                 'GU_Cattle Eartag_765', 'HI_Cattle Eartag_765', 'PR_Cattle Eartag_765',
                                 'VI_Cattle Eartag_765', 'AS_Cattle Eartag_765'],
    'Carbaryl_ActionArea': ['CONUS_Carbaryl_AA_765', 'AK_Carbaryl_AA_765', 'CNMI_Carbaryl_AA_765',
                            'GU_Carbaryl_AA_765', 'HI_Carbaryl_AA_765', 'PR_Carbaryl_AA_765', 'VI_Carbaryl_AA_765',
                            'AS_Carbaryl_AA_765'],
    'Corn_MethomylBuffer': ['AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765', 'VI_Ag_765',
                            'CONUS_Corn_765', 'AS_Ag_765'],
    'Orchards and Vineyards_MethomylBuffer': ['CONUS_Orchards and Vineyards_765', 'HI_Ag_765',
                                              'PR_Ag_765', 'AK_Ag_765', 'CNMI_Ag_765',
                                              'GU_Ag_765', 'VI_Ag_765', 'AS_Ag_765'],
    'Other Crops_MethomylBuffer': ['CONUS_Other Crops_765', 'HI_Ag_765', 'PR_Ag_765', 'AK_Ag_765',
                                   'CNMI_Ag_765', 'GU_Ag_765', 'VI_Ag_765', 'AS_Ag_765'],
    'Other Grains_MethomylBuffer': ['CONUS_Other Grains_765', 'HI_Ag_765', 'PR_Ag_765', 'AK_Ag_765',
                                    'CNMI_Ag_765', 'GU_Ag_765', 'VI_Ag_765', 'AS_Ag_765'],
    'Other RowCrops_MethomylBuffer': ['AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765',
                                      'VI_Ag_765', 'CONUS_Other RowCrops_765', 'AS_Ag_765'],
    'Pasture_MethomylBuffer': ['AK_Pasture_765', 'HI_Cattle Eartag_765', 'CONUS_Pasture_765',
                               'CNMI_Cattle Eartag_765', 'GU_Cattle Eartag_765', 'PR_Cattle Eartag_765',
                               'VI_Cattle Eartag_765', 'AS_Cattle Eartag_765', ],
    'Soybeans_MethomylBuffer': ['CONUS_Soybeans_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765',
                                'PR_Ag_765', 'VI_Ag_765', 'AS_Ag_765'],
    'Vegetables and Ground Fruit_MethomylBuffer': ['CONUS_Vegetables and Ground Fruit_765', 'HI_Ag_765',
                                                   'PR_Ag_765', 'AK_Ag_765', 'CNMI_Ag_765',
                                                   'GU_Ag_765', 'VI_Ag_765', 'AS_Ag_765'],
    'Methomyl Wheat_Buffer': ['CONUS_zMethomylWheat_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765',
                              'PR_Ag_765', 'VI_Ag_765', 'AS_Ag_765'],
    'Bermuda Grass_MethomylBuffer': ['CONUS_Bermuda Grass_765'],

    'Cotton_MethomylBuffer': ['CONUS_Cotton_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765', 'PR_Ag_765',
                              'VI_Ag_765', 'AS_Ag_765'],
    'Alley Cropping_MethomylBuffer': ['CONUS_Alley Cropping_765', 'AK_Ag_765', 'CNMI_Ag_765', 'GU_Ag_765', 'HI_Ag_765',
                                      'PR_Ag_765', 'VI_Ag_765', 'AS_Ag_765'],
    'Methomyl_ActionArea': ['CONUS_Methomyl_AA_765', 'AK_Methomyl_AA_765', 'CNMI_Methomyl_AA_765',
                            'GU_Methomyl_AA_765', 'HI_Methomyl_AA_765', 'PR_Methomyl_AA_765', 'VI_Methomyl_AA_765',
                            'AS_Methomyl_AA_765'],

    'Federally Managed Lands': ['Federally Managed Lands'],
    'FWS Refuge': ['FWS Refuge'],
    'Indian Reservations': ['Indian Reservations'],
    'Wilderness land': ['Wilderness land'],

}

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
list_uses = collapses_dict.keys()
print list_uses
# Sets up the intervals that are of interests for each of the uses

sp_table_df = pd.read_csv(in_table, dtype=object)
# print sp_table_df
sp_info_df = sp_table_df.iloc[:, :sp_index_cols]
use_df = sp_table_df.iloc[:, sp_index_cols:]
# print use_df

collapsed_df = pd.DataFrame(data=sp_info_df)

for use in list_uses:
    print use
    binned_col = list(collapses_dict[use])

    if not use == 'Mosquito Control':
        if not use == 'Wide Area Use':
            binned_df = use_df[binned_col]
            # print binned_df
            use_results_df = binned_df.apply(pd.to_numeric, errors='coerce')
            collapsed_df[(str(use))] = use_results_df.sum(axis=1)
        else:
            collapsed_df.ix[:, str(use)] = 100
    else:
        collapsed_df.ix[:, str(use)] = 100


        # collapsed_df = pd.concat([collapsed_df, new_df], axis=1)

# final_df = pd.concat([sp_info_df, collapsed_df], axis=1)



final_df = collapsed_df.reindex(columns=col_reindex)
final_df = final_df.fillna(0)
# print sorted(collapsed_df.columns.values.tolist())
# print (collapsed_df.columns.values.tolist())
final_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
