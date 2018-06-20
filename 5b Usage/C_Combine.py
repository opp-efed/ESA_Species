import arcpy
from arcpy.sa import *
import datetime
import os
import pandas as pd

today = datetime.datetime.today()
date = today.strftime('%Y%m%d')
start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()
arcpy.CheckOutExtension("Spatial")

in_directory_species_grids = r'L:\ESA\UnionFiles_Winter2018\Range\SpComp_UsageHUCAB_byProjection_2\Grids_byProjection'
raster_layer_libraries = r'L:\Workspace\UseSites\ByProjection'
out_directory = os.path.dirname(in_directory_species_grids) + os.sep + 'Grid_byProjections_Combined'

# 'AS','GU','CONUS','HI','PR','VI','AK'
skip_region = ['AK', 'CNMI','GU','HI','PR','VI']
# 'fishes','fishe','flower', 'flowe','conife','crust','crusta','snails''ferns','insect','insec',

#
skip_species = ['r','ch','mammal','mamma',  'birds','amphi','amphib','clams','mammal','mamma','reptil','repti','birds','conife','crust','crusta','snails''ferns','insect','insec','r_ferns']

snap_dict = {'CONUS': r'L:\\Workspace\UseSites\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_cultmask_2016',
             'HI': r'L:\\Workspace\UseSites\ByProjection\SnapRasters.gdb\NAD_1983_UTM_Zone_4N_HI_Ag',
             'AK': r'L:\\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_Albers_AK_Ag',
             'AS': r'L:\\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_2S_AS_Ag',
             'CNMI': r'L:\\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_CNMI_Ag',
             'GU': r'L:\\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_55N_GU_Ag_30',
             'PR': r'L:\\Workspace\UseSites\ByProjection\SnapRasters.gdb\Albers_Conical_Equal_Area_PR_Ag',
             'VI': r'L:\\Workspace\UseSites\ByProjection\SnapRasters.gdb\WGS_1984_UTM_Zone_20N_VI_Ag_30'}

# create out directory if it does not exist
if not os.path.exists(out_directory):
    os.mkdir(out_directory)

list_dir = os.listdir(in_directory_species_grids)

for folder in list_dir:
    region = folder.split('_')[0]
    print(region)
    if region in skip_region:
        continue
    else:
        # Create out folder if it doesn't exist
        out_folder = out_directory + os.sep + folder
        if not os.path.exists(out_folder):
            os.mkdir(out_folder)
        print('Generating lists of rasters to combine')
        # set snap raster and processing extent (equal to snap raster)
        snap_raster = Raster(snap_dict [region])
        arcpy.Delete_management("snap")
        arcpy.MakeRasterLayer_management(snap_dict[region], "snap")
        arcpy.env.snapRaster = "snap"

        # Set the processing extent to be equal to the use layer; only species within the extent will be
        # included in the output species file
        myExtent = snap_raster.extent
        arcpy.env.extent = myExtent


        # get list of the habitat and elevation files for region
        raster_list_combine = []
        elevation_habitat = raster_layer_libraries + os.sep + region + '_' + 'HabitatElevation.gdb'
        arcpy.env.workspace = elevation_habitat
        raster_list_elev_habitat = arcpy.ListRasters()
        raster_list_combine = [elevation_habitat+ os.sep + v for v in raster_list_elev_habitat]


        # get list of on/off field layers for region
        # on_off_field = raster_layer_libraries + os.sep + region + '_' + 'OnOffField.gdb'
        # arcpy.env.workspace = on_off_field
        # raster_list_on_off = arcpy.ListRasters()
        # # add path to the raster name for combine because the workspace is changing
        # # append the habitat and elevation raster to to lst of raster to be combine with the path to the gdb
        # # because the workspace is changing
        # for v in raster_list_on_off :
        #     raster_list_combine.append(on_off_field + os.sep + v)

        # build att table and builds for all rasters in combine
        for r in raster_list_combine:
            arcpy.BuildRasterAttributeTable_management(r)
            arcpy.BuildPyramids_management(r)
        # get list of species raster for region
        arcpy.env.workspace = in_directory_species_grids + os.sep + folder
        sp_list = arcpy.ListRasters()
        # combine each species file with the habitat and elevation rasters
        for spe_raster in sp_list:
            if spe_raster.split("_")[1] in skip_species:
                continue
            else:
                start_raster = datetime.datetime.now()
                if not arcpy.Exists(out_folder + os.sep + spe_raster):
                    in_spe = in_directory_species_grids + os.sep + folder + os.sep + spe_raster

                    # inset the path to the species file to list of raster to combine in index position 1
                    raster_list_combine.insert(0, in_spe)

                    # Set Snap Raster environment
                    arcpy.Delete_management("snap")
                    arcpy.MakeRasterLayer_management(snap_dict[region], "snap")
                    arcpy.env.snapRaster = "snap"
                    # run combine : includes species raster , on/off field, habitat and elevation
                    print 'Start Combine'
                    print raster_list_combine
                    try:
                        outCombine = Combine(raster_list_combine)
                        outCombine.save(out_folder + os.sep + spe_raster[:8])

                        print 'Saved {0}'.format(out_folder + os.sep + spe_raster[:8])
                        arcpy.BuildRasterAttributeTable_management(out_folder + os.sep + spe_raster[:8])

                        print 'Build pyramids'
                        arcpy.BuildPyramids_management(out_folder + os.sep + spe_raster[:8])

                        # add meaningful col headers for raster as they all start with the projection
                        field = [f.name for f in arcpy.ListFields(out_folder + os.sep + spe_raster[:8])]

                        for f in [u'Rowid', u'VALUE', u'COUNT']:  # remove count columns
                            field.remove(f)
                        current_header = []
                        desired_header = []
                        path_to_raster = []
                        if len(field) == len(raster_list_combine):
                            out_df = pd.DataFrame(index=(list(range(0, 10))))  # empty df to store look values w/ 10 rows
                            for raster in raster_list_combine:
                                current_header.append(field[raster_list_combine.index(raster)])
                                path_to_raster.append(raster)

                                base_file = os.path.basename(raster)
                                bool_pass = False
                                col_header = region

                                for v in base_file.split("_"):
                                    if v != region and v != 'ch' and v != 'R':
                                        pass
                                    else:
                                        bool_pass = True
                                    if bool_pass:
                                        if v == region:
                                            pass
                                        else:
                                            col_header = col_header + "_" + v
                                col_header = col_header.replace(region + '_', '')
                                if col_header.startswith('CH_') or col_header.startswith('R_') or col_header == region:
                                    col_header = os.path.basename(in_spe)
                                desired_header.append(col_header)

                            # all columns need to 10 rows - makes additional rows with value none
                            merge_list_c = current_header + ([None] * (10 - len(current_header)))
                            merge_list_d = desired_header + ([None] * (10 - len(desired_header)))
                            merge_list_p = path_to_raster + ([None] * (10 - len(path_to_raster)))

                            out_df['Default output header'] = pd.Series(merge_list_c).values
                            out_df['Desired output header'] = pd.Series(merge_list_d).values
                            out_df['Path to original raster'] = pd.Series(merge_list_p).values
                            out_df.to_csv(out_folder + os.sep + spe_raster [:8]+ '_lookup_rasters.csv')

                            # when exporting to array cols after the rowid, count and value are 0. Export using the table
                            # converstion then will join table back to the '_lookup_rasters.csv' exported above to get
                            # meaningful columns
                            arcpy.TableToTable_conversion(out_folder + os.sep + spe_raster[:8], out_folder, spe_raster[:8] +'_att.csv')




                        # removes current spe file to loop to next one; other rasters stay the same
                        raster_list_combine.remove(in_spe)
                        print 'Completed {0} in {1}'.format(out_folder + os.sep + spe_raster[:8], datetime.datetime.now()
                                                            - start_raster)
                    except Exception as error:
                        raster_list_combine.remove(in_spe)
                        print('Error was {0} elaspe time was {1}'.format(error.args[0], datetime.datetime.now() - start_raster))
                        if arcpy.Exists(out_folder + os.sep + spe_raster[:8]):
                            arcpy.Delete_management(out_folder + os.sep + spe_raster[:8])

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)

# TODO try to append cols not that the build raster attribute table is set to overwrite see tool help
# Update cursor for headers that would read in table with values in the combine columns as zero, using both update
# cursor and pandas. export a text file that can be use as a look up for the raster name instead due to time constraints

# Note Note: if using update cursor in att table col header is limited to 16 characters

#                            if len(col_header) >= 16:
#                              col_header = col_header[:16]

#             fields = [field[raster_list_combine.index(raster)], col_header]
#             arcpy.AddField_management(out_folder + os.sep + spe_raster, col_header, "DOUBLE")
#             exp = "'!"+str(field[raster_list_combine.index(raster)]) +"!'"
#             print exp
#             # arcpy.CalculateField_management(out_folder + os.sep + spe_raster, col_header, exp)
#             print ('Added column {0}'.format(col_header))
#             print field[raster_list_combine.index(raster)], col_header
#             with arcpy.da.UpdateCursor(out_folder + os.sep + spe_raster, fields) as cursor:
#                 for row in cursor:
#                     print fields[0], fields[1]
#                     val = row[0]
#                     print val
#                     row[1] =val
#                     row[0]= val
#                     cursor.updateRow(row)
