import arcpy
import os
import datetime

# Title- clip fc found in in_location by the clip_fc

# ingdb and file to be use as clip
inlocation = 'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\Range\R_SpGroup_Union_final_20160811.gdb'
clip_fc = r'J:\Workspace\ESA_Species\ForCoOccur\Composites\GDB\ArchivedComposites\20160503\CompositesForClip' \
          r'\Boundaries.gdb\Regions_dissolve'

# outlocation and suffix to be added to fc filename
outlocation = 'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\Range\R_Clipped_UnionRange_20160816.gdb'
Clipped_suffix = "_ClippedRegions_20160816"


# ###Functions
# Create a new GDB
def create_gdb(out_folder, out_name, out_path):
    if not arcpy.Exists(out_path):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")
        print 'Created GDB {0}'.format(out_name)


# runs clip on files in in location
def clip_feature(in_location, clip_fc_in, out_location, clipped_suffix):
    arcpy.Delete_management("ClipFeatures")
    arcpy.MakeFeatureLayer_management(clip_fc_in, "ClipFeatures")
    xy_tolerance = ""
    # Set workspace location
    arcpy.env.workspace = in_location
    fclist = arcpy.ListFeatureClasses()
    # loop through all files and run intersect
    for fc in fclist:
        print fc
        out_feature = out_location + os.sep + fc + clipped_suffix
        in_features = in_location + os.sep + fc
        arcpy.Delete_management("inFeatures")
        arcpy.MakeFeatureLayer_management(in_features, "inFeatures")
        try:
            if not arcpy.Exists(out_feature):
                arcpy.Clip_analysis("inFeatures", "ClipFeatures", out_feature, xy_tolerance)
                print 'Clipped CompFile {0}'.format(fc)
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
