import pandas as pd
import datetime
import arcpy
import os

in_HUC_base = 'L:\NHDPlusV2'
prj = 'L:\Workspace\projections\Albers_Conical_Equal_Area.prj'

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

dsc= arcpy.Describe(prj)
coord_sys = dsc.spatialReference

list_dir = os.listdir(in_HUC_base)
list_HUC2 = [huc2 for huc2 in list_dir if huc2.startswith('NHDPlus')]

acres_df = pd.DataFrame(columns=['HUC_12', 'ACRES'])
acres_df_false = pd.DataFrame(columns=['HUC_12', 'ACRES'])

for value in list_HUC2:
    print value
    HUC12_path = in_HUC_base + os.sep + value + os.sep + 'WBDSnapshot\WBD\WBD_Subwatershed.shp'
    out_fc = "L:\\Workspace\\ESA_Species\Step3\\ToolDevelopment\\acres_sum\\outfc\\{0}".format(value+"_WBD.shp")
    print out_fc

    arcpy.Project_management (HUC12_path, out_fc, coord_sys)
    arcpy.AddField_management(out_fc, 'Acres_prj', "DOUBLE", "", 0)
    exp = "!SHAPE.AREA@Acres!"
    print "Calc acres for  " + value
    arcpy.CalculateField_management(out_fc, 'Acres_prj', exp, "PYTHON_9.3")

    array = arcpy.da.TableToNumPyArray(out_fc, ['HUC_12', 'Acres_prj'])
    df = pd.DataFrame(array)
    dups = df.set_index('HUC_12').index.get_duplicates()

    att_sum = df.groupby(by=['HUC_12'])['Acres_prj'].sum()
    out_csv = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\acres_sum\{0}'.format(value + '.csv')
    att_sum.to_csv(out_csv)
    del out_fc,
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
