import pandas as pd
import os

# df_2010 = pd.read_csv(path + os.sep+ 'NonAg_Transposed_AllHUC2_2010.csv')
# df_2011 = pd.read_csv(path + os.sep+ 'NonAg_Transposed_AllHUC2_2011.csv')
# df_2012 = pd.read_csv(path + os.sep+ 'NonAg_Transposed_AllHUC2_2012.csv')
# df_2013 = pd.read_csv(path + os.sep+ 'NonAg_Transposed_AllHUC2_2013.csv')
# df_2014 = pd.read_csv(path + os.sep+ 'NonAg_Transposed_AllHUC2_2014.csv')
# df_2015 = pd.read_csv(path + os.sep+ 'NonAg_Transposed_AllHUC2_2015.csv')

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

sum = df_2010 + df_2011 + df_2012 + df_2013 + df_2014 + df_2015
sum.columns = col

use_sum = (sum.reindex(columns=['Corn', 'Cotton', 'Rice', 'Soybeans', 'Wheat', 'Vegetables and Ground Fruit',
                                'Orchards and Vineyards', 'Other Grains', 'Other RowCrops', 'Other Crops', 'Pasture',
                                'Cattle Ear Tag', 'Developed', 'Managed Forests', 'Nurseries', 'Open Space Developed',
                                'Right of Way', 'CullPiles', 'Cultivated', 'NonCultivated', 'Pine seed orchards',
                                'Christmas Trees', 'Golfcourses', 'Mosquito Control', 'Wide Area Use']))
mean = use_sum / 6
lead_col = df_2010.reindex(columns=df_col)

final = pd.concat([lead_col, mean], axis=1)
final.to_csv(path + os.sep + 'Mean_overlap_2010_2015.csv')
