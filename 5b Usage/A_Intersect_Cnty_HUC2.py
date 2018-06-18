import arcpy
import os
import datetime

# Author: J. Connolly
# Description: Intersects the vector unioned species composite files to other vector information that is used to
# summarize data when applying usage. This is currently the political boundaries from the census (counties) and the huc2
# from the NHDPlus supplemented with the a/b splits for the aquatic modeling.
#
# After completed this step Step 4, 5 and 6 from 4b_GenerateSpatialFiles_forOverlap_Species can be implements to convert
# these files to raster, project them by region and export to GRIDs used for the combine (D_Combine)

# Note this is hard code to intersect two vector files to the spcies files
# #TODO make this a dynamaic process so the number of vector files can be dynamic

# Input files to be intersect with species must cover the extent of the regions, IE include AS, GU etc
in_huc_vector = r'D:\ESA\NHDPlusNationalData\InfoAddedForESA\FilesAppended_ESA.gdb\HUC12_Merge'
in_sum_file = r'D:\One_drive_old_computer_20180214\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive' \
              r'\Projects\ESA\_ExternalDrive\_CurrentSpeciesSpatialFiles\Boundaries.gdb\Counties_all_overlap'

# # Union species files in vector format
# invector_location = r'D:\ESA\UnionFiles_Winter2018\Range' \
#                     r'\R_Clipped_Union_20180110.gdb'
# # Output location for union species files intersected with counties and HUC12 (both vector) used for usage
# # adjustments
# out_vector_projected = r'D:\ESA\UnionFiles_Winter2018\Range' \
#                        r'\R_Clipped_Union_CntyInter_HUC2ABInter_20180612.gdb'

# Union ch files in vector format
invector_location = r'D:\ESA\UnionFiles_Winter2018\CriticalHabitat' \
                    r'\CH_Clipped_Union_20180110.gdb'
# Output location for union ch files intersected with counties and HUC12 (both vector) used for usage adjustments
out_vector_projected = r'D:\ESA\UnionFiles_Winter2018\CriticalHabitat' \
                       r'\CH_lipped_Union_CntyInter_HUC2ABInter_20180612.gdb'


# FUNCTIONS

def create_gdb(out_folder, out_name, outpath):
    if not arcpy.Exists(outpath):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
today = datetime.datetime.today()
date = today.strftime('%Y%m%d')

# Creates output gdb if it does not exists
create_gdb(os.path.dirname(out_vector_projected), os.path.basename(out_vector_projected), out_vector_projected)

# Creates feature layers of the county (sum) and huc vector files to be used in all of the intersections
arcpy.MakeFeatureLayer_management(in_sum_file, "fc")
arcpy.MakeFeatureLayer_management(in_huc_vector, "huc_fc")

# set workspace in the environmental setting in to get a list is of species files to run the intersect on
arcpy.env.workspace = invector_location
list_species_vector = arcpy.ListFeatureClasses()
print (list_species_vector)  # Print a list of files that will intersected

# Loop on each species composite file, performs the intersections in memory, add the ID field for each intersection so
# data can be extracted for the different data sources
for species_comp in list_species_vector:
    state_sp = datetime.datetime.now()

    # Deleted the previous species layer stored in memory
    arcpy.Delete_management("in_spe_vector")
    arcpy.Delete_management(r"in_memory\\vector_" + str(species_comp))

    # identifies the species files to be used and generates the full path to the input file and the corresponding output
    # path; outfile is printed
    spe_vector = [fc for fc in list_species_vector if
                  fc.startswith(species_comp.split("_")[0] + "_" + species_comp.split("_")[1].title())]
    invector = invector_location + os.sep + spe_vector[0]
    out_vector = out_vector_projected + os.sep + spe_vector[0] + "_" + date
    print (out_vector)
    # Checks if output already exists and if not runs the intersection to the county boundary file,dissolves the
    # intersected file to just the field needed for analysis and added the ID field 'InterID'; if out_vector was already
    # created it will skip to the next one.  NOTE - if script is interrupted an incomplete file may exists in the output
    # gdb that needs to be deleted.  This can be identified by the file having zero rows
    if not arcpy.Exists(out_vector):
        print '\n Starting {0}'.format(species_comp)
        arcpy.MakeFeatureLayer_management(invector, "in_spe_vector")
        arcpy.Intersect_analysis(["fc", "in_spe_vector"], r"in_memory\\vector_" + str(species_comp))
        cnty_inter_completed = datetime.datetime.now()
        print 'Dissolving by political boundary for {0}...Intersect took {1}'.format(species_comp,
                                                                                     (cnty_inter_completed - state_sp))
        arcpy.Dissolve_management(r"in_memory\\vector_" + str(species_comp),
                                  r"in_memory\\vector_" + str(species_comp) + 'd',
                                  ['GEOID', 'ZoneID', 'STUSPS', 'Region'])
        cnty_dissolve_completed = datetime.datetime.now()  # time tracker for intersection
        print ' Adding InterID...cnty dissolve took {1}'.format(cnty_dissolve_completed - cnty_inter_completed)
        arcpy.AddField_management(r"in_memory\\vector_" + str(species_comp) + 'd', "InterID", "DOUBLE")
        with arcpy.da.UpdateCursor(r"in_memory\\vector_" + str(species_comp) + 'd', ['OBJECTID', 'InterID']) as cursor:
            for row in cursor:
                row[1] = row[0]
                cursor.updateRow(row)

        #  runs the intersection to the huc boundary file file,dissolves the  intersected file to just the field needed
        # for analysis including the ones from the previous intersection and added the ID field 'HUCID'
        arcpy.Intersect_analysis([r"in_memory\\vector_" + str(species_comp) + 'd', "huc_fc"], r"in_memory\\vector_huc_"
                                 + str(species_comp))
        huc_inter_completed = datetime.datetime.now()
        print '   Dissolving by HUC2 boundary for {0}...Intersect took {1}'.format(species_comp, (
                huc_inter_completed - cnty_dissolve_completed))
        arcpy.Dissolve_management(r"in_memory\\vector_huc_" + str(species_comp),
                                  out_vector, ['GEOID', 'ZoneID', 'STUSPS', 'Region', 'InterID', 'HUC2_AB'])
        huc2_dissolve_completed = datetime.datetime.now()
        arcpy.AddField_management(out_vector, "HUCID", "DOUBLE")
        print '   Adding HUCID...huc2 dissolve took {0}'.format(huc2_dissolve_completed - huc_inter_completed)
        with arcpy.da.UpdateCursor(out_vector, ['OBJECTID', 'HUCID']) as cursor:
            for row in cursor:
                row[1] = row[0]
                cursor.updateRow(row)
        # Deletes all species files stored in memory
        arcpy.Delete_management(r"in_memory\\vector_" + str(species_comp))
        arcpy.Delete_management(r"in_memory\\vector_" + str(species_comp) + 'd')
        arcpy.Delete_management(r"in_memory\\vector_huc_" + str(species_comp))

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
