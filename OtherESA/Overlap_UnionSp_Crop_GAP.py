import arcpy
import pandas as pd
import os
import datetime

inraster = r'L:\Workspace\ESA_Species\Step3\Step3_Proposal\GAP\National\natgaplandcov_v2_2_1.img'
in_crops = r'L:\Workspace\UseSites\ByProject\CONUS_UseLayer.gdb\Albers_Conical_Equal_Area_CONUS_CDL_1015_40x2_euc'
symbologyLayer = "L:\Workspace\UseSites\ByProject\SymblogyLayers\Albers_Conical_Equal_Area_CONUS_CDL_1015_100x2_euc.lyr"
in_sum_file = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\SpCompRaster_byProjection\Grids_byProjection\Albers_Conical_Equal_Area\r_flower'
outpath_final = r'L:\Workspace\ESA_Species\GMOs\isoxaflutole\GAP_Plants'
out_layers = r'L:\Workspace\ESA_Species\GMOs\isoxaflutole\layers'
table_suffix = 'R_Flowering_Plants_Composite_MAG_20161102GAP_Soybean_CONUS_CDL_1015_40x2'
snap_raster = r"L:\Workspace\UseSites\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.img"
failed = []

# r'C:\Users\JConno02\Documents\ArcGIS\Default.gdb\Counties_all_Project'

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Check out the ArcGIS Spatial Analyst extension license
arcpy.CheckOutExtension("Spatial")

arcpy.MakeRasterLayer_management(in_sum_file, 'fc')
arcpy.MakeRasterLayer_management(in_crops, "crop")

array = arcpy.da.TableToNumPyArray(in_sum_file, [u'VALUE', ], skip_nulls=True)
df = pd.DataFrame(array)
row_count = len(df)
counter = 0

while counter < row_count:
    geoid = df.iloc[counter, 0]
    out_csv = outpath_final + os.sep + str(geoid) + "_" + table_suffix + '.csv'
    if not os.path.exists(out_csv):
        start_loop = datetime.datetime.now()
        print '\nWorking on county {0}; {1} of {2}'.format(geoid, counter, row_count)

        arcpy.env.snapRaster = snap_raster
        # whereclause = "GEOID = '%s'" % geoid
        whereclause = "VALUE= '%s'" % geoid
        arcpy.Delete_management("lyr")
        arcpy.Delete_management("crops")
        arcpy.Delete_management("test")
        arcpy.MakeRasterLayer_management("fc", "lyr", whereclause)
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

    counter += 1
else:
    counter += 1

print 'Counties that failed {0}'.format(failed)
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
