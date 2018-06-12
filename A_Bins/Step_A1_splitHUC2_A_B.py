import os
import datetime
import pandas as pd

import arcpy

outlocation = 'F:\AquaticTables_ESA\Spring 2018\Update_AB_splits\SpatialJoins'
precip_points = r'F:\AquaticTables_ESA\Spring 2018\XYprcp_ann_in_project_20180611.shp'
split_list = ['10', '11', '12', '15', '16', '17', '18', '20']  # HUC2 with an a/b split and HUC12 (not HUC12 in AK)

# Be sure the HUC12 file include all area within US jurisdiction; island in HI, AS, CNMI and AK are not in seamless
# data set and manually added for ESA; data was also loaded into the F:\AquaticTables_ESA\Spring 2018 folder
HUC12_path = r'D:\ESA\NHDPlusNationalData\InfoAddedForESA\FilesAppended_ESA.gdb\HUC12_Merge'

outtables = outlocation + os.sep + 'tables_HUC2'


def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# location of out files
outgdb = outlocation + os.sep + 'FirstLoop.gdb'  # HUC12 with one or more points directly inside
no_precip_point = outlocation + os.sep + 'no_precip.gdb'  # HUC12 without any points
closest_point = outlocation + os.sep + 'closest.gdb'  # results of the one to many join to closest point; no pnt within

# Create out locations
if not os.path.exists(outgdb):
    create_gdb(outlocation, 'FirstLoop.gdb', outgdb)
if not os.path.exists(no_precip_point):
    create_gdb(outlocation, 'no_precip.gdb', no_precip_point)
if not os.path.exists(closest_point):
    create_gdb(outlocation, 'closest.gdb', closest_point)

arcpy.MakeFeatureLayer_management(precip_points, "pnt_lyr")

# out files, csv name with all values by HUC12, outfc from Spatial Join, out fc of HUC12 w/o point
outcsv = outtables + os.sep + "HUC2.csv"
outfc = outgdb + os.sep + "HUC2_SJ"
no_pnt = no_precip_point + os.sep + "HUC2_noPoint"

# FILTER SEAMLESS DATASET TO JUST HUC2s with a/b
arcpy.Delete_management("HUC_lyr")
arcpy.MakeFeatureLayer_management(HUC12_path, "HUC_lyr")
for v in split_list:
    print v
    if split_list.index(v) == 0:
        where_clause = '"HUC_2" = ' + "'%s'" % v
        arcpy.SelectLayerByAttribute_management("HUC_lyr", "NEW_SELECTION", where_clause)
        print('{} has {} records'.format("HUC_lyr", arcpy.GetCount_management("HUC_lyr")[0]))
    else:
        where_clause = '"HUC_2" = ' + "'%s'" % v
        arcpy.SelectLayerByAttribute_management("HUC_lyr", "ADD_TO_SELECTION", where_clause)
        print('{} has {} records'.format("HUC_lyr", arcpy.GetCount_management("HUC_lyr")[0]))

# RUN SPATIAL JOIN FOR POINT THAT INTERSECT DIRECTLY WITH HUC12S
# EXPORT LIST OF HUC 12 TO DATAFRAME FROM NHD ATT TABLE
array = arcpy.da.TableToNumPyArray("HUC_lyr", ['HUC_12'])
df = pd.DataFrame(array)
print df
if not arcpy.Exists(outfc):
    arcpy.SpatialJoin_analysis("HUC_lyr", "pnt_lyr", outfc, "JOIN_ONE_TO_MANY", "KEEP_ALL", "", "INTERSECT", "", "")
    print '\nCompleted first spatial join'

# CHECK FOR HUC12 WITHOUT POINTS WITHIN THE HUC12; SAVE AS SEPARATE FC THEN REMOVES FROM SJ (INTERSECTS) FILE
if not arcpy.Exists(no_pnt):
    arcpy.Delete_management("lyr")
    arcpy.MakeFeatureLayer_management(outfc, "lyr")
    qry = '"Annual_Prc" IS NULL'
    # " [Annual_Prc] = 'NULL' "
    arcpy.SelectLayerByAttribute_management("lyr", "NEW_SELECTION", qry)
    arcpy.CopyFeatures_management("lyr", no_pnt)
    fclist_field = [f.name for f in arcpy.ListFields(no_pnt) if not f.required]
    fclist_field.remove("HUC_12")
    arcpy.DeleteField_management(no_pnt, fclist_field)
    print 'Copied no point file'
    arcpy.DeleteRows_management("lyr")
    print 'Delete Nulls'

# RUN SPATIAL JOIN(CLOSEST_GEODESIC) FOR HUC12 W/O POINT DIRECTLY  INSIDE; CLOSEST POINT (ONE TO MANY)
arcpy.Delete_management("no_pnt_lyr")
arcpy.MakeFeatureLayer_management(no_pnt, "no_pnt_lyr")
outfc_2 = closest_point + os.sep + "HUC2_closestPnt"


if not arcpy.Exists(outfc_2):
    arcpy.SpatialJoin_analysis("no_pnt_lyr", "pnt_lyr", outfc_2, "JOIN_ONE_TO_MANY", "KEEP_ALL", "#",
                               "CLOSEST_GEODESIC")
    print 'Completed second spatial join'

# EXPORT RESULTS OF SPATIAL JOIN (INTERSECTS) TO DATAFRAME Selection on non-nulls - nulls are captured in the no_pnt
# layer and should be deleted from the layer with the spatial join
arcpy.MakeFeatureLayer_management(outfc, "lyr")
qry = '"Annual_Prc" IS NOT NULL'
arcpy.SelectLayerByAttribute_management("lyr", "NEW_SELECTION", qry)
array_2 = arcpy.da.TableToNumPyArray("lyr", [u'HUC_12', u'Annual_Prc', u'WeatherID'])
df_2 = pd.DataFrame(array_2)
print df_2

# EXPORT RESULTS OF SPATIAL JOIN (CLOSEST_GEODESIC) TO DATAFRAME
array_3 = arcpy.da.TableToNumPyArray(outfc_2, ['HUC_12', 'Annual_Prc','WeatherID'])
df_3 = pd.DataFrame(array_3)
print df_3

# MERGE RESULTS INTO ONE TABLE AND EXPORT; START WITH ORIGINAL NHD ATT TABLE SO THAT ALL HUC12 ARE ACCOUNTED FOR;
final_df = pd.merge(df, df_2, on='HUC_12', how='outer')
out_df = pd.merge(final_df, df_3, on='HUC_12', how='outer')
# Merges results from the two spatial joins together; only one set of columns is completed x or y so summing then
# gives the results of all HUCs in a single field
out_df ['Annual_Prc'] = out_df [['Annual_Prc_x', 'Annual_Prc_y']].sum(axis=1)
out_df ['WeatherID'] = out_df [['WeatherID_x', 'WeatherID_y']].sum(axis=1)

out_df.drop_duplicates(inplace=True)  # Remove duplicates from merges
out_df.to_csv(outcsv)
print 'Export csv {0}'.format(outcsv)
