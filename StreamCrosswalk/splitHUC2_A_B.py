import os
import datetime
import pandas as pd

import arcpy

outlocation = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\AquModeling\SpatialJoins'
precip_points = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\AquModeling\XYprcp_ann_in_project.shp'
outtables = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\AquModeling\SpatialJoins\tables_HUC2'
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

list_HUC2 = ['10L', '10U', '11', '12', '15', '16', '17', '18']


def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


outgdb = outlocation + os.sep + 'FirstLoop.gdb'
no_precip_point = outlocation + os.sep + 'no_precip.gdb'
closest_point = outlocation + os.sep + 'closest.gdb'
if not os.path.exists(outgdb):
    create_gdb(outlocation, 'FirstLoop.gdb', outgdb)
if not os.path.exists(no_precip_point):
    create_gdb(outlocation, 'no_precip.gdb', no_precip_point)
if not os.path.exists(closest_point):
    create_gdb(outlocation, 'closest.gdb', closest_point)

arcpy.MakeFeatureLayer_management(precip_points, "pnt_lyr")

for value in list_HUC2:
    print value
    outcsv = outtables + os.sep + "HUC2_" + value + '.csv'

    HUC12_path = r'L:\NHDPlusV2\NHDPlus{0}\WBDSnapshot\WBD\WBD_Subwatershed.shp'.format(value)

    array = arcpy.da.TableToNumPyArray(HUC12_path, ['HUC_12'])
    df = pd.DataFrame(array)

    outfc = outgdb + os.sep + "HUC2_" + value + '_SJ'
    no_pnt = no_precip_point + os.sep + "HUC2_" + value + '_noPoint'
    if not arcpy.Exists(outfc):
        arcpy.Delete_management("HUC_lyr")
        arcpy.MakeFeatureLayer_management(HUC12_path, "HUC_lyr")
        # arcpy.SpatialJoin_analysis ("JOIN_ONE_TO_MANY ","KEEP_ALL","#", , "#","#")
        arcpy.SpatialJoin_analysis("HUC_lyr", "pnt_lyr", outfc, "JOIN_ONE_TO_MANY", "KEEP_ALL", "", "INTERSECT", "", "")
        print '\nCompleted first spatial join for {0}'.format(value)

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

    array_2 = arcpy.da.TableToNumPyArray(outfc, ['HUC_12', 'Annual_Prc', 'WeatherID'])
    print array_2
    df_2 = pd.DataFrame(array_2)

    arcpy.Delete_management("no_pnt_lyr")
    arcpy.MakeFeatureLayer_management(no_pnt, "no_pnt_lyr")
    outfc_2 = closest_point + os.sep + "HUC2_" + value + "_closestPnt"

    if not arcpy.Exists(outfc_2):
        # arcpy.SpatialJoin_analysis ("no_pnt_lyr", "pnt_lyr", outfc_2,"JOIN_ONE_TO_MANY ","KEEP_ALL","#",  "CLOSEST_GEODESIC ", "#","#")
        arcpy.SpatialJoin_analysis("no_pnt_lyr", "pnt_lyr", outfc_2, "JOIN_ONE_TO_MANY", "KEEP_ALL", "#",
                                   "CLOSEST_GEODESIC")
        print 'Completed second spatial join for {0}'.format(value)

    array_3 = arcpy.da.TableToNumPyArray(outfc_2, ['HUC_12', 'Annual_Prc', 'WeatherID'])
    df_3 = pd.DataFrame(array_3)

    # Update how dfs are merge; this way a number of duplicate rows are be generated when there HUCs are same; HUC17
    # output has 44,915,255
    final_df = pd.merge(df, df_2, on='HUC_12', how='outer')
    final_df = pd.merge(final_df, df_3, on='HUC_12', how='outer')

    final_df.to_csv(outcsv)
    print 'Export csv {0}'.format(outcsv)
