import arcpy
import os
import datetime

# Author J.Connolly
# Internal deliberative, do not cite or distribute

# Title- clip fc found in in_location by the clip_fc

# ingdb from Step 2
inlocation = r'L:\Workspace\StreamLine\Demo\Answer Key\UnionFiles_2019\CriticalHabitat\CH_SpGroup_Union_Final_20192018.gdb'
# file to be use as clip
clip_fc = r'C:\Users\JConno02\Environmental Protection Agency (EPA)\Endangered Species Pilot Assessments - PythonScript_SpatialTools\InputTables\Boundaries.gdb\Regions_dissolve'

# outlocation and suffix to be added to fc filename
outlocation = 'L:\Workspace\StreamLine\Demo\Answer Key\UnionFiles_2019\CriticalHabitat\CH_Clipped_Union_20191028.gdb'
Clipped_suffix = "_ClippedRegions_20191028"


# ###Functions
# Create a new GDB
def create_gdb(out_folder, out_name, out_path):
    if not arcpy.Exists(out_path):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")
        print 'Created GDB {0}'.format(out_name)


# runs clip on files in in location
def clip_feature(in_location, clip_fc_in, out_location, clipped_suffix):
    clip_time = datetime.datetime.now()
    arcpy.Delete_management("ClipFeatures")
    arcpy.MakeFeatureLayer_management(clip_fc_in, "ClipFeatures")
    xy_tolerance = ""
    # Set workspace location
    arcpy.env.workspace = in_location
    fclist = arcpy.ListFeatureClasses()
    # loop through all files and run intersect
    for fc in fclist:
        out_feature = out_location + os.sep + fc + clipped_suffix
        in_features = in_location + os.sep + fc
        try:
            if not arcpy.Exists(out_feature):
                print "\nAdding ZoneID for {0}".format(fc)
                arcpy.AddField_management(fc, "ZoneID", "DOUBLE")
                with arcpy.da.UpdateCursor(fc, ['OBJECTID','ZoneID']) as cursor:
                    for row in cursor:
                        row[1] = row[0]
                        cursor.updateRow(row)
                print "Clipping {0}".format(fc)
                arcpy.Delete_management("inFeatures")
                arcpy.MakeFeatureLayer_management(in_features, "inFeatures")
                arcpy.Clip_analysis("inFeatures", "ClipFeatures", out_feature, xy_tolerance)
                print 'Clipped CompFile {0} took {1}'.format(fc, datetime.datetime.now() - clip_time)
            else:
                print ('Already clipped {0}'.format(fc))
                arcpy.AddField_management(fc, "ZoneID", "DOUBLE")

        except Exception as error:
            print(error.args[0])
            arcpy.Delete_management(out_feature)



start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

if not arcpy.Exists(outlocation):
    path, gdb_file = os.path.split(outlocation)
    create_gdb(path, gdb_file, outlocation)
clip_feature(inlocation, clip_fc, outlocation, Clipped_suffix)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
