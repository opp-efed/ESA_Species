import arcpy
import os
in_location =r'L:\Projected.gdb'
out_location =r'L:\L48_intersect.gdb'


arcpy.env.workspace = in_location
list_species_vector = arcpy.ListFeatureClasses()


for species_comp in list_species_vector:
    invector = in_location + os.sep + species_comp
    outvector = out_location + os.sep + species_comp
    if not arcpy.Exists(outvector):
        arcpy.Delete_management("lyr")
        whereclause = "Region = '%s'" % 'L48'
        arcpy.MakeFeatureLayer_management(invector ,"lyr",whereclause)
        arcpy.CopyFeatures_management("lyr", outvector)
        print outvector