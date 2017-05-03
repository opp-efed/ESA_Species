import pandas as pd
import os
import numpy as np
import math

sample = 6
def _std (df_2010,df_2011):
    counter = len(df_2010)-1
    count =0
    while count <=counter:
        array = [[]]
# df_2010 = pd.read_csv(path + os.sep+ 'NonAg_Transposed_AllHUC2_2010.csv')
# df_2011 = pd.read_csv(path + os.sep+ 'NonAg_Transposed_AllHUC2_2011.csv')
# df_2012 = pd.read_csv(path + os.sep+ 'NonAg_Transposed_AllHUC2_2012.csv')
# df_2013 = pd.read_csv(path + os.sep+ 'NonAg_Transposed_AllHUC2_2013.csv')
# df_2014 = pd.read_csv(path + os.sep+ 'NonAg_Transposed_AllHUC2_2014.csv')
# df_2015 = pd.read_csv(path + os.sep+ 'NonAg_Transposed_AllHUC2_2015.csv')
def assign_curoff (row, df):
    entid = row['EntityID']
    list= df.loc[df['EntityID'] == entid].values.tolist()[0]

    for v in list:
        try:
            if int(v) < 10:
                if row['Check'] =='False':
                        return 'False'
                else:
                        return 'True'

            else:
                    return 'False'
        except ValueError, TypeError:
            pass


path = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Tabulated_NewComps\L48\FinalTables_Range\Archived\MagTool_20170130\Collapsed_20170130'
df_2010 = pd.read_csv(path + os.sep + 'R_Final_NonAg_Merge_2010_GAP.csv')
df_2011 = pd.read_csv(path + os.sep + 'R_Final_NonAg_Merge_2011_GAP.csv')
df_2012 = pd.read_csv(path + os.sep + 'R_Final_NonAg_Merge_2012_GAP.csv')
df_2013 = pd.read_csv(path + os.sep + 'R_Final_NonAg_Merge_2013_GAP.csv')
df_2014 = pd.read_csv(path + os.sep + 'R_Final_NonAg_Merge_2014_GAP.csv')
df_2015 = pd.read_csv(path + os.sep + 'R_Final_NonAg_Merge_2015_GAP.csv')

# df_col =['HUC12', 'Acres']
df_col = ['EntityID', 'Group', 'comname', 'sciname', 'status_text', 'Des_CH', 'CH_GIS', 'Acres_CONUS']
df_2010.sort_values(['EntityID'])
df_2011.sort_values(['EntityID'])
df_2012.sort_values(['EntityID'])
df_2013.sort_values(['EntityID'])
df_2014.sort_values(['EntityID'])
df_2015.sort_values(['EntityID'])

col = df_2011.columns.values.tolist()
print col
array_2010 = df_2010.as_matrix(columns= ['Corn', 'Cotton', 'Rice', 'Soybeans', 'Wheat', 'Vegetables and Ground Fruit',
                                'Orchards and Vineyards', 'Other Grains', 'Other RowCrops', 'Other Crops', 'Pasture',
                                'Cattle Ear Tag', 'Developed', 'Managed Forests', 'Nurseries', 'Open Space Developed',
                                'Right of Way', 'CullPiles', 'Pine seed orchards',
                                'Christmas Trees', 'Golfcourses', 'Mosquito Control', 'Wide Area Use'])
array_2011 = df_2011.as_matrix(columns= ['Corn', 'Cotton', 'Rice', 'Soybeans', 'Wheat', 'Vegetables and Ground Fruit',
                                'Orchards and Vineyards', 'Other Grains', 'Other RowCrops', 'Other Crops', 'Pasture',
                                'Cattle Ear Tag', 'Developed', 'Managed Forests', 'Nurseries', 'Open Space Developed',
                                'Right of Way', 'CullPiles', 'Pine seed orchards',
                                'Christmas Trees', 'Golfcourses', 'Mosquito Control', 'Wide Area Use'])

array_2012 = df_2012.as_matrix(columns= ['Corn', 'Cotton', 'Rice', 'Soybeans', 'Wheat', 'Vegetables and Ground Fruit',
                                'Orchards and Vineyards', 'Other Grains', 'Other RowCrops', 'Other Crops', 'Pasture',
                                'Cattle Ear Tag', 'Developed', 'Managed Forests', 'Nurseries', 'Open Space Developed',
                                'Right of Way', 'CullPiles', 'Pine seed orchards',
                                'Christmas Trees', 'Golfcourses', 'Mosquito Control', 'Wide Area Use'])

array_2013 = df_2013.as_matrix(columns= ['Corn', 'Cotton', 'Rice', 'Soybeans', 'Wheat', 'Vegetables and Ground Fruit',
                                'Orchards and Vineyards', 'Other Grains', 'Other RowCrops', 'Other Crops', 'Pasture',
                                'Cattle Ear Tag', 'Developed', 'Managed Forests', 'Nurseries', 'Open Space Developed',
                                'Right of Way', 'CullPiles', 'Pine seed orchards',
                                'Christmas Trees', 'Golfcourses', 'Mosquito Control', 'Wide Area Use'])
array_2014 = df_2014.as_matrix(columns= ['Corn', 'Cotton', 'Rice', 'Soybeans', 'Wheat', 'Vegetables and Ground Fruit',
                                'Orchards and Vineyards', 'Other Grains', 'Other RowCrops', 'Other Crops', 'Pasture',
                                'Cattle Ear Tag', 'Developed', 'Managed Forests', 'Nurseries', 'Open Space Developed',
                                'Right of Way', 'CullPiles', 'Pine seed orchards',
                                'Christmas Trees', 'Golfcourses', 'Mosquito Control', 'Wide Area Use'])
array_2015 = df_2015.as_matrix(columns= ['Corn', 'Cotton', 'Rice', 'Soybeans', 'Wheat', 'Vegetables and Ground Fruit',
                                'Orchards and Vineyards', 'Other Grains', 'Other RowCrops', 'Other Crops', 'Pasture',
                                'Cattle Ear Tag', 'Developed', 'Managed Forests', 'Nurseries', 'Open Space Developed',
                                'Right of Way', 'CullPiles', 'Pine seed orchards',
                                'Christmas Trees', 'Golfcourses', 'Mosquito Control', 'Wide Area Use'])

array_std = np.std((array_2010,array_2011,array_2012,array_2013,array_2014,array_2015),axis=0, ddof=1)
array_mean = np.mean((array_2010,array_2011,array_2012,array_2013,array_2014,array_2015),axis=0)

lead_col = df_2010.reindex(columns=df_col)

std_df = pd.DataFrame(data= array_std, columns=['Corn', 'Cotton', 'Rice', 'Soybeans', 'Wheat', 'Vegetables and Ground Fruit',
                                'Orchards and Vineyards', 'Other Grains', 'Other RowCrops', 'Other Crops', 'Pasture',
                                'Cattle Ear Tag', 'Developed', 'Managed Forests', 'Nurseries', 'Open Space Developed',
                                'Right of Way', 'CullPiles', 'Pine seed orchards',
                                'Christmas Trees', 'Golfcourses', 'Mosquito Control', 'Wide Area Use'])
sem_df = std_df.divide(math.sqrt(sample))

mean_df = pd.DataFrame(data= array_mean, columns=['Corn', 'Cotton', 'Rice', 'Soybeans', 'Wheat', 'Vegetables and Ground Fruit',
                                'Orchards and Vineyards', 'Other Grains', 'Other RowCrops', 'Other Crops', 'Pasture',
                                'Cattle Ear Tag', 'Developed', 'Managed Forests', 'Nurseries', 'Open Space Developed',
                                'Right of Way', 'CullPiles', 'Pine seed orchards',
                                'Christmas Trees', 'Golfcourses', 'Mosquito Control', 'Wide Area Use'])
mean_df=pd.concat([lead_col, mean_df], axis=1)
mean_df.to_csv(path + os.sep + 'Mean_overlap_2010_2015.csv')

me_90=sem_df.multiply(1.645)
crops_2010 =df_2010.reindex(columns= ['Corn', 'Cotton', 'Rice', 'Soybeans', 'Wheat', 'Vegetables and Ground Fruit',
                                'Orchards and Vineyards', 'Other Grains', 'Other RowCrops', 'Other Crops', 'Pasture',
                                'Cattle Ear Tag', 'Developed', 'Managed Forests', 'Nurseries', 'Open Space Developed',
                                'Right of Way', 'CullPiles', 'Pine seed orchards',
                                'Christmas Trees', 'Golfcourses', 'Mosquito Control', 'Wide Area Use'])
error_2010 = crops_2010 + me_90
error_2010 =pd.concat([lead_col, error_2010 ], axis=1)
error_2010['Check'] = error_2010.apply(lambda row: assign_curoff(row, error_2010), axis=1)
me_90=pd.concat([lead_col, me_90], axis=1)

me_95=sem_df.multiply(1.96)
me_95=pd.concat([lead_col, me_95], axis=1)

me_99=sem_df.multiply(2.575)
me_99=pd.concat([lead_col, me_99], axis=1)


std_df =pd.concat([lead_col, std_df], axis=1)
std_df.to_csv(path + os.sep + 'STD_overlap_2010_2015.csv')

sem_df =pd.concat([lead_col, sem_df ], axis=1)

sem_df.to_csv(path + os.sep + 'SEM_overlap_2010_2015.csv')

me_90.to_csv(path + os.sep + 'ME_90_overlap_2010_2015.csv')
me_95.to_csv(path + os.sep + 'ME_95_overlap_2010_2015.csv')
me_99.to_csv(path + os.sep + 'ME_99_overlap_2010_2015.csv')

error_2010.to_csv (path + os.sep + 'Error_90_overlap_2010_2015.csv')

# final = pd.concat([lead_col, mean], axis=1)
# # final.to_csv(path + os.sep + 'Mean_overlap_2010_2015.csv')
