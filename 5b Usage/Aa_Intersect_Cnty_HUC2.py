import arcpy
import os
import datetime

invector_location = r'C:\Users\Admin\Documents\Jen\Workspace\StreamLine\ESA\UnionFiles_Winter2018\Range' \
                    r'\R_Clipped_Union_20180110.gdb'

out_vector_projected = r'C:\Users\Admin\Documents\Jen\Workspace\StreamLine\ESA\UnionFiles_Winter2018\Range' \
                       r'\R_Raster_Clipped_Union_CntyInter_HUC2Inter_20180523.gdb'

# invector_location = r'C:\Users\Admin\Documents\Jen\Workspace\StreamLine\ESA\UnionFiles_Winter2018\CriticalHabitat' \
#                     r'\CH_Clipped_Union_20180110.gdb'
#
# out_vector_projected = r'C:\Users\Admin\Documents\Jen\Workspace\StreamLine\ESA\UnionFiles_Winter2018\CriticalHabitat' \
#                        r'\CH_Raster_Clipped_Union_CntyInter_HUC2Inter_20180523.gdb'


in_huc_vector = r'C:\Users\Admin\Documents\Jen\Workspace\NHDPlus\NHDPlusNationalData\NHDPlusV21_National_Seamless.gdb' \
                r'\WBDSnapshot\HUC12'
in_sum_file = r'C:\Users\Admin\Documents\Jen\Workspace\StreamLine\Boundaries.gdb\Counties_all_overlap'


date ='_20180523'

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

arcpy.env.workspace = invector_location
list_species_vector = arcpy.ListFeatureClasses()

arcpy.MakeFeatureLayer_management(in_sum_file, "fc")
arcpy.MakeFeatureLayer_management(in_huc_vector , "huc_fc")
print list_species_vector
for species_comp in list_species_vector:
    state_sp = datetime.datetime.now()
    arcpy.Delete_management("in_spe_vector")

    spe_vector = [fc for fc in list_species_vector if
                  fc.startswith(species_comp.split("_")[0]+ "_" + species_comp.split("_")[1].title())]
    arcpy.Delete_management(r"in_memory\\vector_" + str(species_comp))
    invector = invector_location + os.sep + spe_vector[0]
    out_vector = out_vector_projected + os.sep + spe_vector[0] + date
    print out_vector
    if not arcpy.Exists(out_vector):
        print '\n Starting {0}'.format(species_comp)
        arcpy.MakeFeatureLayer_management(invector, "in_spe_vector")
        arcpy.Intersect_analysis(["fc", "in_spe_vector"], r"in_memory\\vector_" + str(species_comp))
        cnty_inter_completed = datetime.datetime.now()
        print 'Dissolving by political boundary for {0}...Intersect took {0}'.format(species_comp, (cnty_inter_completed  - state_sp))
        arcpy.Dissolve_management(r"in_memory\\vector_" + str(species_comp),
                                  r"in_memory\\vector_" + str(species_comp) + 'd', ['GEOID', 'ZoneID', 'STUSPS', 'Region'])
        cnty_dissolve_completed = datetime.datetime.now()
        print ' Adding InterID...cnty dissolve took {0}'.format(cnty_dissolve_completed-cnty_inter_completed)
        arcpy.AddField_management(r"in_memory\\vector_" + str(species_comp) + 'd', "InterID", "DOUBLE")
        with arcpy.da.UpdateCursor(r"in_memory\\vector_" + str(species_comp) + 'd', ['OBJECTID', 'InterID']) as cursor:
            for row in cursor:
                row[1] = row[0]
                cursor.updateRow(row)

        arcpy.Intersect_analysis([r"in_memory\\vector_" + str(species_comp) + 'd', "huc_fc"], r"in_memory\\vector_huc_" + str(species_comp))
        huc_inter_completed = datetime.datetime.now()
        print '   Dissolving by HUC2 boundary for {0}...Intersect took {0}'.format(species_comp, (
            huc_inter_completed- cnty_dissolve_completed))
        arcpy.Dissolve_management(r"in_memory\\vector_huc_" + str(species_comp),
                                  out_vector, ['GEOID', 'ZoneID', 'STUSPS', 'Region','InterID','HUC_2'])
        huc2_dissolve_completed = datetime.datetime.now()

        arcpy.AddField_management(out_vector, "HUCID", "DOUBLE")

        print '   Adding HUCID...huc2 dissolve took {0}'.format(huc2_dissolve_completed-huc_inter_completed)
        with arcpy.da.UpdateCursor(out_vector, ['OBJECTID', 'HUCID']) as cursor:
            for row in cursor:
                row[1] = row[0]
                cursor.updateRow(row)
        arcpy.Delete_management(r"in_memory\\vector_" + str(species_comp))
        arcpy.Delete_management(r"in_memory\\vector_" + str(species_comp) + 'd')
        arcpy.Delete_management(r"in_memory\\vector_huc_" + str(species_comp))

        # if not arcpy.Exists(out_vector):
        #     arcpy.Project_management( r"in_memory\\vector_" + str(species_comp) + 'huc_d', out_vector, coord_sys)
        # print out_vector


end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
