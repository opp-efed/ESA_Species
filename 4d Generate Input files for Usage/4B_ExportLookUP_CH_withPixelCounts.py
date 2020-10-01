
import arcpy
import os
import pandas as pd

filetype = 'fc'
# arcpy.env.workspace = r'D:\Species\UnionFile_Spring2020\Range\R_Raster_Clipped_Union_CntyInter_HUC2ABInter_20200427.gdb'
# path =r'D:\Species\UnionFile_Spring2020\Range\LookUp_R_Clipped_Union_CntyInter_HUC2ABInter_20200427'
arcpy.env.workspace = r'D:\Species\UnionFile_Spring2020\Range\R_Clipped_Union_CntyInter_HUC2ABInter_20200427.gdb'
path =r'D:\Species\UnionFile_Spring2020\Range\LookUp_R_Clipped_Union_CntyInter_HUC2ABInter_20200427'

out_path = r'D:\Species\UnionFile_Spring2020\Range\LookUp_Grid_byProjections_20200427'
if not os.path.exists(out_path):
    os.mkdir(out_path)

if filetype =='raster':
    list_csv = os.listdir(path)
    list_raster = arcpy.ListRasters()
    for raster in list_raster:
        print raster
        array = arcpy.da.TableToNumPyArray(raster, ['VALUE','COUNT'])
        att_df = pd.DataFrame(data=array)
        att_df['VALUE'] = att_df['VALUE'].map(lambda x: x).astype(str)
        csv = [v for v in list_csv if v.startswith(raster)][0]
        df = pd.read_csv(path+ os.sep +csv)
        df['HUCID'] = df['HUCID'].map(lambda x: x).astype(str)
        final_df = pd.merge(att_df,df, how='left', left_on = 'VALUE', right_on ='HUCID')
        final_df.to_csv(out_path +os.sep+csv)
else:
    list_fc = arcpy.ListFeatureClasses()
    list_csv = os.listdir(path)
    for fc in list_fc:
        print fc
        array = arcpy.da.TableToNumPyArray(fc, ['GEOID','ZoneID','Acres'])
        att_df = pd.DataFrame(data=array)
        att_df['ZoneID'] = att_df['ZoneID'].map(lambda x: str(x).split(".")[0]).astype(str)
        csv = [v for v in list_csv if v.startswith(fc)][0]
        df = pd.read_csv(path+ os.sep +csv)
        df = df[['ZoneID', 'EntityID']].copy()
        df['ZoneID'] = df['ZoneID'].map(lambda x: str(x).split(".")[0]).astype(str)
        final_df = pd.merge(att_df,df, how='left', on = 'ZoneID')
        final_df.drop_duplicates(inplace=True)
        final_df.to_csv(out_path +os.sep+csv)
