import arcpy
import pandas as pd
import datetime
import os

infc = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb'
in_path_NHD = 'L:\NHDPlusV2\NHDPlusZZ\WBDSnapshot\WBD\WBD_Subwatershed.shp'
outlocation = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\AquModeling\Heat_map_specHUC12'

HUC_2 = ['01', '02', '03N', '03S', '03W', '04', '05', '06', '07', '08', '09', '10L', '10U', '11', '12', '13', '14',
         '15', '16', '17', '18']
arcpy.env.workspace = infc
fc_list = arcpy.ListFeatureClasses()
prev_huc = 'ZZ'

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
skip_list =['R_Mammals_Composite_MAG_20161102','R_Flowering_Plants_Composite_MAG_20161102',
            'R_Fishes_Composite_MAG_20161102','R_Reptiles_Composite_MAG_20161102','R_Mammals_Composite_MAG_20161102_DD',
            'R_Reptiles_Composite_MAG_20161102_DD','R_Fishes_Composite_MAG_20161102_DD']
out_df_final = pd.DataFrame(columns=['HUC_12', 'Count'])
final_long_df = pd.DataFrame(columns=['EntityID', 'HUC_12'])
for fc in fc_list:
    if str(fc) not in skip_list :
            sp_group = fc.split("_")[1]
            arcpy.Delete_management('fc_lyr')
            arcpy.MakeFeatureLayer_management(infc + os.sep + fc, 'fc_lyr')
            out_df = pd.DataFrame(
                columns=['Join_Count', 'TARGET_FID', 'JOIN_FID', 'FileName', 'EntityID', 'NAME', 'Name_sci', 'SPCode',
                         'VIPCode', 'Status', 'Pop_Abb', 'Shape_Length', 'Shape_Area', 'OBJECTID_1', 'HUC_8', 'HUC_10',
                         'HUC_12', 'ACRES', 'NCONTRB_A', 'HU_10_GNIS', 'HU_12_GNIS', 'HU_10_DS', 'HU_10_NAME', 'HU_10_MOD',
                         'HU_10_TYPE', 'HU_12_DS', 'HU_12_NAME', 'HU_12_MOD', 'HU_12_TYPE', 'META_ID', 'STATES', 'GlobalID',
                         'SHAPE_Leng', 'SHAPE_Area_1', 'GAZ_ID'])
            for huc_2 in HUC_2:
                print '{1}, {0}'.format(huc_2, fc)
                huc = in_path_NHD.replace(prev_huc, huc_2)
                out_huc_csv = outlocation + os.sep + sp_group + "_" + huc_2 + '.csv'
                if not os.path.exists(out_huc_csv):
                    arcpy.Delete_management('huc_lyr')
                    arcpy.MakeFeatureLayer_management(huc, 'huc_lyr')
                    arcpy.Delete_management('in_memory/outfc')

                    # arcpy.SpatialJoin_analysis('fc_lyr', 'huc_lyr', r"L:\Workspace\ESA_Species\Step3\ToolDevelopment\AquModeling\Heat_map_specHUC12\working.gdb"+ os.sep+sp_group+"_"+huc_2,"JOIN_ONE_TO_MANY")

                    arcpy.SpatialJoin_analysis('fc_lyr', 'huc_lyr', r"in_memory/outfc", "JOIN_ONE_TO_MANY", "KEEP_COMMON")
                    list_fields = [f.name for f in arcpy.ListFields(r"in_memory/outfc") if not f.required]
                    att_array = arcpy.da.TableToNumPyArray(r"in_memory/outfc", ['EntityID','HUC_12'])
                    att_df = pd.DataFrame(data=att_array)
                    att_df.to_csv(out_huc_csv, encoding='utf-8')

                    out_df = df_new = pd.concat([out_df, att_df])
                else:
                    in_csv = pd.read_csv(out_huc_csv)
                    out_df = df_new = pd.concat([out_df, in_csv])

            out_df.to_csv(outlocation + os.sep + 'Final_' + sp_group + '.csv', encoding='utf-8')
            out_df = out_df.reindex(columns=['EntityID', 'HUC_12'])
            final_long_df = pd.concat([final_long_df, out_df])
            out_df.drop_duplicates(inplace=True)
            count_huc_12 = out_df['HUC_12'].value_counts()
            count_huc_12.columns = ['HUC_12', 'Count']
            count_huc_12.to_csv(outlocation + os.sep + 'Counts_' + sp_group + '.csv', encoding='utf-8')
    else:
        pass
final_long_df.drop_duplicates(inplace=True)
count_huc_12_final = out_df['HUC_12'].value_counts()
final_long_df.to_csv(outlocation + os.sep + 'AllSpecies_HUC12.csv', encoding='utf-8')
count_huc_12_final.to_csv(outlocation + os.sep + 'AllSpecies_HUC12Counts.csv', encoding='utf-8')

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
