import arcpy
import pandas as pd
import os
import datetime

inraster = r'L:\Workspace\ESA_Species\Step3\Step3_Proposal\GAP\National\natgaplandcov_v2_2_1.img'
in_crops = r'L:\Workspace\UseSites\ByProject\CONUS_UseLayer.gdb\Albers_Conical_Equal_Area_CONUS_CDL_1015_40x2_euc'
symbologyLayer = "L:\Workspace\UseSites\ByProject\SymblogyLayers\Albers_Conical_Equal_Area_CONUS_CDL_1015_100x2_euc.lyr"
in_sum_file = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb\R_Flowering_Plants_Composite_MAG_20161102'
outpath_final = r'L:\Workspace\ESA_Species\GMOs\isoxaflutole\GAP_Plants'
out_layers = r'L:\Workspace\ESA_Species\GMOs\isoxaflutole\layers'
table_suffix = 'R_Flowering_Plants_Composite_MAG_20161102GAP_Soybean_CONUS_CDL_1015_40x2'
snap_raster = r"L:\Workspace\UseSites\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.img"
failed = []
entid_list = ['633', '713', '807', '843', '906', '1061', '1125', '627', '9965', '558', '656', '870', '1073', '1047',
              '554', '6617', '624', '1204', '1028', '816', '837', '1209', '1199', '1220', '1203', '995', '625', '818',
              '677', '1003', '1039', '1064', '998', '1017', '1036', '994', '1415', '7167', '991', '764', '742', '7264',
              '1228', '905', '977', '655', '1019', '734', '819', '823', '852', '946', '1189', '976', '835', '628',
              '875', '620', '960', '1080', '891', '935',
              ]
# r'C:\Users\JConno02\Documents\ArcGIS\Default.gdb\Counties_all_Project'

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Check out the ArcGIS Spatial Analyst extension license
arcpy.CheckOutExtension("Spatial")

arcpy.MakeFeatureLayer_management(in_sum_file, 'fc')
arcpy.MakeRasterLayer_management(in_crops, "crop")

# array = arcpy.da.TableToNumPyArray(in_sum_file, [u'GEOID', u'EXT_MIN_X', u'EXT_MIN_Y', u'EXT_MAX_X', u'EXT_MAX_Y'],
#                                    skip_nulls=True)
# df = pd.DataFrame(array)
# df['EXT_MIN_X'] = df['EXT_MIN_X'].astype(str)
# df['EXT_MIN_Y'] = df['EXT_MIN_Y'].astype(str)
# df['EXT_MAX_X'] = df['EXT_MAX_X'].astype(str)
# df['EXT_MAX_Y'] = df['EXT_MAX_Y'].astype(str)
# df['Extent'] = df[['EXT_MIN_X', 'EXT_MIN_Y', 'EXT_MAX_X', 'EXT_MAX_Y']].apply(lambda x: ' '.join(x), axis=1)

array = arcpy.da.TableToNumPyArray(in_sum_file, [u'EntityID', ], skip_nulls=True)
df = pd.DataFrame(array)
row_count = len(df)
counter = 0
arcpy.MakeRasterLayer_management(in_crops, "crop_lyr")
while counter < row_count:
    geoid = str(df.iloc[counter, 0])
    # extent= str(df.iloc[counter, 5])
    if geoid not in entid_list:
        counter += 1
    else:
        out_csv = outpath_final + os.sep + geoid + "_" + table_suffix + '.csv'
        if not os.path.exists(out_csv):
            start_loop = datetime.datetime.now()
            print '\nWorking on county {0}; {1} of {2}'.format(geoid, counter, row_count)
            try:
                arcpy.env.snapRaster = snap_raster
                # whereclause = "GEOID = '%s'" % geoid
                whereclause = "EntityID = '%s'" % geoid
                arcpy.Delete_management("lyr")
                arcpy.Delete_management("crops")
                arcpy.Delete_management("test")
                arcpy.MakeFeatureLayer_management("fc", "lyr", whereclause)

                arcpy.Clip_management(inraster, "#", r"in_memory/raster_" + str(geoid), "lyr", "NoData",
                                      "ClippingGeometry",
                                      "MAINTAIN_EXTENT")
                # if we want to save the clipped inraster file uncomment this  line
                # arcpy.CopyRaster_management(r"in_memory/raster_" + str(geoid),out_layers +os.sep+ "raster_"+str(geoid))
                arcpy.BuildRasterAttributeTable_management(r"in_memory\\raster_" + str(geoid), "Overwrite")
                arcpy.MakeRasterLayer_management(in_crops, "crops")
                arcpy.ApplySymbologyFromLayer_management("crops", symbologyLayer)
                arcpy.gp.ZonalHistogram_sa("in_memory\\raster_" + str(geoid), "VALUE", "crops",
                                           r"in_memory/outtable_" + str(geoid))

                list_fields = [f.name for f in arcpy.ListFields(r"in_memory/outtable_" + str(geoid)) if not f.required]
                att_array = arcpy.da.TableToNumPyArray(r"in_memory/outtable_" + str(geoid), list_fields)
                att_df = pd.DataFrame(data=att_array)
                att_df['LABEL'] = att_df['LABEL'].map(lambda x: x).astype(str)
                att_df.to_csv(out_csv)
                print 'Completed in {0} '.format(datetime.datetime.now() - start_loop)
            except:
                print 'Failed to execute for {0}'.format(geoid)
                failed.append(geoid)
            counter += 1
        else:
            counter += 1

print 'Counties that failed {0}'.format(failed)
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
