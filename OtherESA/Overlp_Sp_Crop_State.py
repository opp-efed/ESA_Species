import arcpy
import pandas as pd
import os
import datetime

inraster = r'F:\Union Composites_ESA\Spring 2018\Range\SpCompRaster_byProjection\Grids_byProjection\CONUS_Albers_Conical_Equal_Area\r_snails'
in_crops = r'F:\UseSite_ESA\May 2018\ByProjection\CONUS_UseLayers.gdb\Albers_Conical_Equal_Area_CDL_1016_20x2_euc'
symbologyLayer = "F:\UseSite_ESA\May 2018\ByProjection\Symbol_Layers\Albers_Conical_Equal_Area_CDL_1016_110x2_euc.lyr"
in_sum_file = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive' \
              r'\Projects\ESA\_ExternalDrive\_CurrentSpeciesSpatialFiles\Boundaries.gdb\Counties_all_overlap'

outpath_final = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\Refinements\Sp_Use_State'
out_layers = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\Refinements\Sp_Use_State\Layers'
table_suffix = 'R_Birds_Composite_MAG_20161102_Soybean_CONUS_CDL_1015_40x2'
snap_raster = r"L:\Workspace\UseSites\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.img"
failed = []

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Check out the ArcGIS Spatial Analyst extension license
arcpy.CheckOutExtension("Spatial")

arcpy.MakeFeatureLayer_management(in_sum_file, 'fc')
arcpy.MakeRasterLayer_management(in_crops, "crop")
arcpy.MakeRasterLayer_management(inraster, "in_raster")

# array = arcpy.da.TableToNumPyArray(in_sum_file, [u'STUSPS', u'Region', ], skip_nulls=True) # for states
array = arcpy.da.TableToNumPyArray(in_sum_file, [u'GEOID', u'Region', ], skip_nulls=True)  # for counties

df = pd.DataFrame(array)
row_count = len(df)
counter = 0
arcpy.MakeRasterLayer_management(in_crops, "crop_lyr")
while counter < row_count:
    geoid = str(df.iloc[counter, 0])
    region = str(df.iloc[counter, 1])
    if region != 'L48':
        counter += 1
    else:
        out_csv = outpath_final + os.sep + geoid + "_" + table_suffix + '.csv'
        if not os.path.exists(out_csv):
            start_loop = datetime.datetime.now()
            print '\nWorking on county {0}; {1} of {2}'.format(geoid, counter, row_count)

            arcpy.env.snapRaster = snap_raster
            # whereclause = "STUSPS = '%s'" % geoid # for state
            whereclause = "GEOID = '%s'" % geoid # for county tuns
            arcpy.Delete_management("lyr")
            arcpy.Delete_management("crops")
            arcpy.Delete_management("test")
            arcpy.MakeFeatureLayer_management("fc", "lyr", whereclause)
            try:
                arcpy.Clip_management("in_raster", "#", r"in_memory/raster_" + str(geoid), "lyr", "NoData", "ClippingGeometry",
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
