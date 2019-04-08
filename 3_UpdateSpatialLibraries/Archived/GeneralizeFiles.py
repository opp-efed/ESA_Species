import arcpy
import os

in_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\SpatialLibrary\Range'
out_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\SpatialLibrary\Generalized_files\Range'

list_gdb = os.listdir(in_location)

for v in list_gdb:
    if len(v.split('.')) == 2:
        pass
    else:
        list_gdb.remove(v)


for i in list_gdb:
    print i
    in_gdb = in_location + os.sep + i
    out_gdb = out_location + os.sep + i
    if not os.path.exists(out_gdb):
        arcpy.CreateFileGDB_management(out_location, i, "CURRENT")
    arcpy.env.workspace = in_gdb
    list_fc = arcpy.ListFeatureClasses()
    for fc in list_fc:

        print ' Working on {0} of {1}...'.format((list_fc.index(fc))+ 1, len(list_fc))
        inFeatures = in_gdb + os.sep + fc
        copFeatures = out_gdb + os.sep + fc
        if not arcpy.Exists(copFeatures):
            # Since Generalize permanently updates the input, first make a copy of the original FC
            arcpy.CopyFeatures_management(inFeatures, copFeatures)
            arcpy.Generalize_edit(copFeatures)
