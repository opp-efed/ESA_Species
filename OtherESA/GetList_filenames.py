import os
import arcpy
#in_folder= r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\StreamCrosswalks\Range\DD_Sp_MasterList_20161124\CSV'
#out_folder =r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\StreamCrosswalks\Range\DD_Sp_MasterList_20161124'
ingdb = 'L:\Workspace\UseSites\ByProject\CONUS_UseLayer.gdb'

#print os.listdir(in_folder)
arcpy.env.workspace = ingdb
raster_list =arcpy.ListRasters()
print raster_list

