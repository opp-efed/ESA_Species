import arcpy
import pandas as pd
import os
import datetime

# SPEED THIS UP BY MAKE A ZONE BY CNTY LOOK UP TO SKIL COUNTIES WITH NO SPECIES
inraster_location = r'C:\Users\JConno02\One_Drive_fail\Documents_C_drive\Projects\ESA\_ExternalDrive' \
                    r'\_CurrentSpeciesSpatialFiles\Union\Range\SpCompRaster_byProjection\Grids_byProjection' \
                    r'\Albers_Conical_Equal_Area'

invector_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\_MovedOneDrive\Union\Range\R_Clipped_Union_MAG_20161102.gdb'
out_vector_projected = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
                       r'\_ExternalDrive\_CurrentResults\Results_usage_range_clipped\Layers\Projected.gdb'
sp_group_cnty = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
                r'\_ExternalDrive\_CurrentSupportingTables\ESA_SpeciesGroup_all_counties.csv'

in_sum_file = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
              r'\_ExternalDrive\_CurrentSpeciesSpatialFiles\Boundaries.gdb\Counties_all_overlap'
out_layers = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
             r'\_ExternalDrive\_CurrentResults\Results_usage\Layers'

snap_raster = r"L:\Workspace\UseSites\Cultivated_Layer\2015_Cultivated_Layer\2015_Cultivated_Layer.img"

coordFile = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
            r'\_ExternalDrive\projections\Albers_Conical_Equal_Area.prj'
# in_crops_location =  r"L:\Workspace\UseSites\CDL_Reclass\161031\CDL_Reclass_1015_161031.gdb"
in_crops_location = r'L:\Workspace\_MovedOneDrive\UseSites\ByProject\Diaz_Moasics\CONUS_UseLayer.gdb'

symbologyList = [
    r"L:\Workspace\_MovedOneDrive\UseSites\ByProject\Diaz_Moasics\Sym_Layers\Albers_Conical_Equal_Area_CONUS_CDL_1015_60x2_euc.lyr",
    r"L:\Workspace\_MovedOneDrive\UseSites\ByProject\Diaz_Moasics\Sym_Layers\Albers_Conical_Equal_Area_CONUS_CDL_2010_rec.lyr"]

outroot = r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\ESA' \
          r'\_ExternalDrive\_CurrentResults\Results_usage_range_clipped\L48'

find_file_type = inraster_location.split(os.sep)
if 'Range' in find_file_type:
    Range = True
    out_results = outroot + os.sep + 'Range'

else:
    Range = False
    out_results = outroot + os.sep + 'CriticalHabitat'

failed = []


def create_directory(dbf_dir):
    if not os.path.exists(dbf_dir):
        os.mkdir(dbf_dir)


start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

create_directory(out_results)
arcpy.env.workspace = inraster_location
list_species_comps = arcpy.ListRasters()

dsc = arcpy.Describe(coordFile)
coord_sys = dsc.spatialReference

arcpy.env.workspace = invector_location
list_species_vector = arcpy.ListFeatureClasses()

arcpy.env.workspace = in_crops_location
list_use_layers = arcpy.ListRasters()


list_use_layers = [u'Albers_Conical_Equal_Area_CONUS_Diazinon_euc']

spe_group_df = pd.read_csv(sp_group_cnty)

spe_group_df['GEOID'] = spe_group_df['GEOID'].map(lambda x: str(x)).astype(str)
spe_group_df['GEOID'] = spe_group_df['GEOID'].map(lambda x: '0' + x if len(x) == 4 else x).astype(str)
group_df = spe_group_df['Group']
unq_groups = group_df.drop_duplicates()

# Check out the ArcGIS Spatial Analyst extension license
arcpy.CheckOutExtension("Spatial")
arcpy.MakeFeatureLayer_management(in_sum_file, "fc")

for use_layer in list_use_layers:
    use_nm = use_layer.replace('Albers_Conical_Equal_Area_', '')
    symbology_flag = use_nm.split("_")[(len(use_nm.split("_")) - 1)]
    if symbology_flag == 'euc':
        outpath = out_results + os.sep + 'Agg_Layers'
        create_directory(outpath)
        symbologyLayer = symbologyList[0]
    else:
        outpath = out_results + os.sep + 'Indiv_Year_raw'
        create_directory(outpath)
        symbologyLayer = symbologyList[1]
    if not os.path.exists(outpath + os.sep + use_nm):
        create_directory(outpath + os.sep + use_nm)

    arcpy.Delete_management("crops")
    in_crops = in_crops_location + os.sep + use_layer
    arcpy.MakeRasterLayer_management(in_crops, "crops")
    arcpy.ApplySymbologyFromLayer_management("crops", symbologyLayer)

    for species_comp in list_species_comps:

        c_group = species_comp.split("_")[1]

        arcpy.env.workspace = inraster_location
        list_species_comps = arcpy.ListRasters()

        group = [v for v in unq_groups if v.startswith(c_group.title())]
        sp_cnties_filter = spe_group_df.loc[spe_group_df['Group'].isin(group)]
        sp_cnties = sp_cnties_filter['GEOID'].values.tolist()

        group = group[0]

        arcpy.Delete_management("in_spe_vector")
        spe_vector = [fc for fc in list_species_vector if
                      fc.startswith(species_comp.split("_")[0].title() + "_" + species_comp.split("_")[1].title())]
        arcpy.Delete_management(r"in_memory\\vector_" + str(species_comp))
        invector = invector_location + os.sep + spe_vector[0]
        print invector

        # Addresses edge effect by clipping counties by species ranges first due to project causing a 99999 error,
        # running  clip on unprojected files, then projetions
        arcpy.MakeFeatureLayer_management(invector, "in_spe_vector")
        arcpy.Intersect_analysis(["fc", "in_spe_vector"], r"in_memory\\vector_" + str(species_comp))
        arcpy.Dissolve_management(r"in_memory\\vector_" + str(species_comp),
                                  r"in_memory\\vector_" + str(species_comp) + 'd',
                                  ['GEOID', 'ZoneID', 'STUSPS', 'Region'])
        out_vector = out_vector_projected + os.sep + spe_vector[0]
        if not arcpy.Exists(out_vector):
            arcpy.Project_management(r"in_memory\\vector_" + str(species_comp) + "d", out_vector, coord_sys)

        array = arcpy.da.TableToNumPyArray(out_vector, [u'STUSPS', u'Region', u'GEOID', u'ZoneID'], skip_nulls=True)

        df = pd.DataFrame(array)
        df ['ZoneID'] = df ['ZoneID'].map(lambda x: str(x).split(".")[0]).astype(str)
        row_count = len(df)
        list_geoid = list(set(df['GEOID'].values.tolist()))

        inraster = inraster_location + os.sep + species_comp
        arcpy.MakeRasterLayer_management(inraster, "in_raster")

        for cnty in list_geoid:
            if not cnty.startswith('06'):
                print '\nWorking on county {0}; {1} of {2} for {3}'.format(cnty, list_geoid.index(cnty), len(list_geoid),
                                                                           species_comp)
                cnty_df = df.loc[df['GEOID']==cnty]

                row_count = len(cnty_df )
                counter =0
                while counter < row_count:

                    geoid = str(cnty_df  .iloc[counter, 2])
                    region = str(cnty_df  .iloc[counter, 1])
                    st_abb = str(cnty_df  .iloc[counter, 0])
                    zone = str(cnty_df  .iloc[counter, 3])

                    outpath_final = outpath + os.sep + use_nm + os.sep + st_abb + os.sep + geoid
                    if not os.path.exists(outpath + os.sep + use_nm + os.sep + st_abb):
                            create_directory(outpath + os.sep + use_nm + os.sep + st_abb)

                    if not os.path.exists(outpath_final):
                            create_directory(outpath_final)

                    out_csv = outpath_final + os.sep + species_comp + "_" + use_nm + '.csv'

                    if region != 'L48':
                        counter += 1
                    elif geoid not in sp_cnties:
                        counter += 1
                    else:
                        if not os.path.exists(out_csv):
                            start_loop = datetime.datetime.now()

                            arcpy.env.snapRaster = snap_raster
                            whereclause = "GEOID = '%s'" % geoid
                            whereclause2 = "ZoneID = {0} ".format(int(zone))


                            arcpy.Delete_management("lyr")
                            arcpy.Delete_management("lyr_geo")
                            arcpy.MakeFeatureLayer_management(out_vector,"lyr_geo",
                                                              whereclause2)

                            arcpy.MakeFeatureLayer_management( "lyr_geo","lyr",
                                                              whereclause)

                            try:
                                arcpy.Delete_management(r"in_memory\\raster_" + str(counter) + "_" + str(geoid))
                                arcpy.Delete_management(r"in_memory/outtable_" + str(counter) + "_" + str(geoid))
                                arcpy.Clip_management("in_raster", "#", r"in_memory\\raster_" + str(counter) + "_" +
                                                      str(geoid), "lyr","NoData", "NONE", "MAINTAIN_EXTENT")
                                print 'test'

                                # if we want to save the clipped inraster file uncomment this  line
                                # arcpy.CopyRaster_management(r"in_memory/raster_" + str(geoid),out_layers+os.sep+
                                #                             "raster_"+str(geoid))

                                # TODO COUNT ROWS IN FILE rather than try/except- FAIL WHEN CLIP IS EMPTY
                                try:
                                    arcpy.gp.ZonalHistogram_sa(r"in_memory/raster_" + str(counter) + "_" + str(geoid),
                                                               "VALUE", "crops", r"in_memory/outtable_" + str(counter) +
                                                               "_" + str(geoid))

                                    list_fields = [f.name for f in arcpy.ListFields(r"in_memory/outtable_" + str(counter) +
                                                                                    "_" + str(geoid)) if not f.required]
                                    att_array = arcpy.da.TableToNumPyArray(r"in_memory/outtable_" + str(counter) + "_" +
                                                                           str(geoid), list_fields)
                                    att_df = pd.DataFrame(data=att_array)
                                    att_df['LABEL'] = att_df['LABEL'].map(lambda x: x).astype(str)
                                    if counter == 0:
                                        working_df = att_df
                                    else:
                                        working_df = pd.merge(working_df, att_df, on='LABEL', how='outer')

                                    print 'Completed in {0} '.format(datetime.datetime.now() - start_loop)

                                    arcpy.Delete_management(r"in_memory\\raster_" + str(counter) + "_" + str(geoid))
                                    arcpy.Delete_management(r"in_memory/outtable_" + str(counter) + "_" + str(geoid))
                                    del att_df,list_fields,
                                    counter += 1
                                except:
                                    print 'Failed to execute for {0}'.format((str(geoid) + "_" + species_comp))
                                    failed.append((str(geoid) + "_" + species_comp))
                                    counter += 1
                                    arcpy.Delete_management(r"in_memory\\raster_" + str(counter) + "_" + str(geoid))
                                    arcpy.Delete_management(r"in_memory/outtable_" + str(counter) + "_" + str(geoid))
                            except:  # Catches zones that doesn't have a FIPS
                                # print 'Failed to execute for {0}'.format((str(geoid) + "_" + species_comp))
                                failed.append((str(geoid) + "_" + species_comp))
                                counter += 1
                                arcpy.Delete_management(r"in_memory\\raster_" + str(counter) + "_" + str(geoid))
                                arcpy.Delete_management(r"in_memory/outtable_" + str(counter) + "_" +  str(geoid))

                        else:
                            counter += 1
                if not os.path.exists(out_csv):
                    working_df.to_csv(out_csv)



print 'Counties that failed {0}'.format(failed)
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
