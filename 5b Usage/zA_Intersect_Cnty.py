import arcpy
import os
import datetime



invector_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\_MovedOneDrive\Union\Range' \
                    r'\R_Clipped_Union_MAG_20161102.gdb'
out_vector_projected = r'L:\Projected.gdb'
in_sum_file = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              r'\_ExternalDrive\_CurrentSpeciesSpatialFiles\Boundaries.gdb\Counties_all_overlap'
coordFile = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
            r'\_ExternalDrive\projections\Albers_Conical_Equal_Area.prj'


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

arcpy.env.workspace = invector_location
list_species_vector = arcpy.ListFeatureClasses()

arcpy.MakeFeatureLayer_management(in_sum_file, "fc")

dsc = arcpy.Describe(coordFile)
coord_sys = dsc.spatialReference

for species_comp in list_species_vector:

    arcpy.Delete_management("in_spe_vector")
    spe_vector = [fc for fc in list_species_vector if
                  fc.startswith(species_comp.split("_")[0].title() + "_" + species_comp.split("_")[1].title())]
    arcpy.Delete_management(r"in_memory\\vector_" + str(species_comp))
    invector = invector_location + os.sep + spe_vector[0]
    out_vector = out_vector_projected + os.sep + spe_vector[0]
    if not arcpy.Exists(out_vector):
        print species_comp
        arcpy.MakeFeatureLayer_management(invector, "in_spe_vector")
        arcpy.Intersect_analysis(["fc", "in_spe_vector"], r"in_memory\\vector_" + str(species_comp))
        arcpy.Dissolve_management(r"in_memory\\vector_" + str(species_comp),
                                  r"in_memory\\vector_" + str(species_comp) + 'd', ['GEOID', 'ZoneID', 'STUSPS', 'Region'])

        if not arcpy.Exists(out_vector):
            arcpy.Project_management(r"in_memory\\vector_" + str(species_comp) + "d", out_vector, coord_sys)
        print out_vector


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
