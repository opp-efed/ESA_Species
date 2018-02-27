import pandas as pd
import numpy as np
import arcpy
import os

combo_path = r"P:\GIS_Data\SAM_2018\bin\Preprocessed\Combos\07_2010.npz"
catchment_file =r'P:\GIS_Data\SAM_2018\bin\Layers\NHDCatchments\region07'
outpath= r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\SAM\QA'

matrix = pd.DataFrame(**np.load(combo_path))
sum = matrix.groupby('comid',as_index=False).sum()
filter_sum = sum.ix[:,['comid','area']]
filter_sum.columns = ['comid','combos_area']

array = arcpy.da.TableToNumPyArray(catchment_file, ['VALUE', 'COUNT'],)
df = pd.DataFrame(data=array)
df['area']= df.ix[:,'COUNT']*900
df['comid'] = df['VALUE'].map(lambda x: x)
df = df.groupby('comid',as_index=False).sum()
filter_df = df.ix[:,['comid','area']]
filter_df.columns = ['comid','catchment_area']



merge = pd.merge(filter_sum,filter_df,how='outer',on='comid')
merge['difference'] = merge['combos_area']- merge['catchment_area']


merge.to_csv(outpath +os.sep+os.path.basename(catchment_file) +'.csv')

