import datetime
import os

import pandas as pd

# Title - Transforms out results by zone and summarize totals by species - final output is a master sum table of results
# by use and interval for each species

# TODO set up separate script that will spit out chem specific table with different interval include aerial and group

# inlocation
in_table = r'E:\Tabulated_NewComps\FinalBETables\Range\BE_intervals\R_AllUses_BE_20170117.csv'
master_col = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'CH_GIS']
# master list
temp_folder = r'E:\Tabulated_NewComps\FinalBETables\DraftNewFormat'
out_csv = temp_folder + os.sep + 'R_Collapsed_WoE_20170117.csv'
sp_index_cols = 15
col_reindex = ['EntityID',  'comname', 'sciname','family', 'status_text','pop_abbrev', 'Des_CH','Critical_Habitat_',
               'Migratory','Migratory_', 'Corn', 'Cotton','Orchards and Vineyards', 'Other Crops', 'Other Grains',
               'Other RowCrops', 'Pasture', 'Rice', 'Soybeans','Vegetables and Ground Fruit', 'Wheat', 'Developed',
               'Managed Forests', 'Nurseries','Open Space Developed', 'Pine Seed Orchards', 'Right of Way',
               'Christmas Trees', 'Golfcourses','Rangeland', 'Mosquito Control', 'Wide Area Use','CH_GIS',
               'Group', 'Alaska', 'American Samoa', 'Hawaii', 'Lower 48','Guam', 'Puerto Rico', 'Virgin Islands',
               'Commonwealth of the Northern Mariana Islands','Nurseries_DiazBuffer',	'VegetablesGroundFruit_DiazBuffer',
               'OrchardsVineyards_DiazBuffer',	'Diazinon_ActionArea','Carbaryl_ActionArea','Methomyl_ActionArea','Federally Managed Lands',	'FWS Refuge',
               'Indian Reservations',	'Wilderness land',	'Cull Piles'

               ]
## need ot add in AS for Range

collapses_dict = {
    'Corn': ['CONUS_Corn_0'],
    'Cotton': ['CONUS_Cotton_0'],
    'Orchards and Vineyards': ['CONUS_Orchards and Vineyards_0', 'HI_Orchards and vineyards_0',
                               'PR_Orchards and vineyards_0'],
    'Other Crops': ['CONUS_Other Crops_0', 'HI_Other crops_0', 'PR_Other crops_0'],
    'Other Grains': ['CONUS_Other Grains_0', 'HI_Other grains_0', 'PR_Other grains_0'],
    'Other RowCrops': ['CONUS_Other RowCrops_0'],
    'Pasture': ['CONUS_Pasture_0', 'AK_Pasture/Hay/Forage_0', 'HI_Pasture/Hay/Forage_0'],
    'Rice': ['CONUS_Rice_0'],
    'Soybeans': ['CONUS_Soybeans_0'],
    'Vegetables and Ground Fruit': ['CONUS_Vegetables and Ground Fruit_0', 'HI_Veg Ground Fruit_0',
                                    'PR_Veg Ground Fruit_0'],
    'Wheat': ['CONUS_Wheat_0'],
    'Developed': ['CONUS_Developed_0', 'AK_Developed_0', 'CNMI_Developed_0', 'GU_Developed_0',
                  'HI_Developed_0',
                  'PR_Developed_0', 'VI_Developed_0','AS_Developed_0'],
    'Managed Forests': ['CONUS_Managed Forest_0', 'AK_Managed Forests_0', 'CNMI_Managed Forests_0',
                        'GU_Managed Forests_0', 'HI_Managed Forests_0', 'PR_Managed Forests_0',
                        'VI_Managed Forests_0'],
    'Nurseries': ['CONUS_Nurseries_0', 'AK_Nurseries_0', 'HI_Nurseries_0', 'PR_Nurseries_0',
                  'VI_Nurseries_0'],
    'Open Space Developed': ['CONUS_Open Space Developed_0', 'AK_Open Space Developed_0',
                             'CNMI_Open Space Developed_0',
                             'GU_Open Space Developed_0', 'HI_Open Space Developed_0',
                             'PR_Open Space Developed_0',
                             'VI_Open Space Developed_0','AS_Open Space Developed_0'],
    'Pine Seed Orchards': ['CONUS_Pine seed orchards_0'],
    'Right of Way': ['CONUS_Right of Way_0', 'AK_Right of Way_0', 'CNMI_Right of Way_0',
                     'GU_Right of Way_0',
                     'HI_Right of Way_0', 'PR_Right of Way_0', 'VI_Right of Way_0','AS_Right of Way_0'],
    'Christmas Trees': ['CONUS_Christmas Trees_0'],
    'Golfcourses': ['CONUS_Golfcourses_0', 'AK_Golf Courses_0', 'GU_Golf Courses_0',
                    'HI_Golf Courses_0',
                    'PR_Golf Courses_0'],
    'Rangeland': ['CONUS_Cattle Eartag_0', 'AK_Cattle Eartag_0', 'CNMI_Cattle Eartag_0',
                  'GU_Cattle Eartag_0',
                  'HI_Cattle Eartag_0', 'PR_Cattle Eartag_0', 'VI_Cattle Eartag_0','AS_Cattle Eartag_0'],
    'Cull Piles': ['CONUS_Cull Piles_0'],
    'Mosquito Control': [''],
    'Wide Area Use': [''],
    'Alaska': ['AK_Ag_0'],
    'Hawaii': ['HI_Ag_0'],
    'Lower 48': ['CONUS_Cultivated_0'],
    'Guam': ['GU_Ag_0'],
    'Puerto Rico': ['PR_Ag_0'],
    'Virgin Islands': ['VI_Ag_0'],
    'American Samoa':['AS_Ag_0'],
    'Commonwealth of the Northern Mariana Islands': ['CNMI_Ag_0'],
    'Nurseries_DiazBuffer': ['CONUS_Nurseries_305', 'AK_Nurseries_305', 'HI_Nurseries_305',
                             'PR_Nurseries_305', 'VI_Nurseries_305'],
    'VegetablesGroundFruit_DiazBuffer': ['CONUS_Vegetables and Ground Fruit_765', 'HI_Veg Ground Fruit_765',
                                         'PR_Veg Ground Fruit_765'],
    'OrchardsVineyards_DiazBuffer': ['CONUS_Orchards and Vineyards_305', 'HI_Orchards and vineyards_305',
                                     'PR_Orchards and vineyards_305'],
    'Diazinon_ActionArea': ['CONUS_Diazinon_AA_765', 'AK_Diazinon_AA_765', 'CNMI_Diazinon_AA_765',
                            'GU_Diazinon_AA_765', 'HI_Diazinon_AA_765', 'PR_Diazinon_AA_765', 'VI_Diazinon_AA_765','AS_Diazinon_AA_765'],

    'Carbaryl_ActionArea': ['CONUS_Carbaryl_AA_765', 'AK_Carbaryl_AA_765', 'CNMI_Carbaryl_AA_765',
                            'GU_Carbaryl_AA_765', 'HI_Carbaryl_AA_765', 'PR_Carbaryl_AA_765', 'VI_Carbaryl_AA_765','AS_Carbaryl_AA_765'],
    'Methomyl_ActionArea': ['CONUS_Methomyl_AA_765', 'AK_Methomyl_AA_765', 'CNMI_Methomyl_AA_765',
                            'GU_Methomyl_AA_765', 'HI_Methomyl_AA_765', 'PR_Methomyl_AA_765', 'VI_Methomyl_AA_765','AS_Methomyl_AA_765'],

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
#print sorted(collapsed_df.columns.values.tolist())
#print (collapsed_df.columns.values.tolist())
final_df.to_csv(out_csv)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
