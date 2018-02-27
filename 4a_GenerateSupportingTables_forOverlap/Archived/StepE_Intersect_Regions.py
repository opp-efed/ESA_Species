import arcpy
import os
import datetime

# Title - Generates regional composites in correct projection to calc area of the species file found in each region

# master list and location by index number base zero of species information
masterlist = 'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\CSVs\MasterListESA_June2016_20170117.csv'
# col index values of species info cols in master list

ColIndexDict = dict(comname=4, sciname=5, spcode=14, vipcode=15, entid=0, group=7, PopName=9,status =6)

# Index order of how the information is loaded into the species dict this is alpha order based on col name in fc

final_fieldsindex = dict(NAME=0, Name_sci=4, SPCode=5, VIPCode=7, EntityID=1, PopName=3,Status=6)
#This is from the update all ATT scripte should this be the dict being used?
#final_fieldsindex = dict(NAME=1, Name_sci=4, SPCode=5, VIPCode=7, EntityID=2, PopName=0,Status=6)#group is in pos 3 and not being added

# input values and workspaces for range files
Range = True
if Range:
    # in folder with species group composite with all species unprojections and location of region feature class
    infc_gdb = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_SpGroupComposite.gdb'
    intersect_fc = r'C:\WorkSpace\FinalBE_EucDis_CoOccur\Boundaries.gdb\Regions_dissolve'

    # out locations of the species group composites intersected with region
    out_explode_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\Regional' \
                           r'\R_SpGroupComposite_ExplodeComp_20161102.gdb'
    out_explode_location_final = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\Regional' \
                                 r'\R_SpGroupComposite_ExplodeCompFinal_20161102.gdb'
    out_clip_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\Regional\R_SpGroupComposite_clipComp_20161102.gdb'
    out_intersect_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\Regional' \
                             r'\R_SpGroupComposite_IntersectRegion_20161102.gdb'
    out_spatial_join_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\Regional' \
                                r'\R_SpGroupComposite_spatialjoin_Region_20161102.gdb'
    out_Dissolve_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\Regional' \
                            r'\R_SpGroupComposite_DissolveRegion_20161102.gdb'
    out_Dissolve_location_final = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\Regional' \
                                  r'\R_SpGroupComposite_DissolveRegion_Final_20161102.gdb'
    DissolveFields = ['FileName', 'EntityID', 'NAME', 'Name_sci', 'SPCode', 'VIPCode', 'Status', 'Region']

    temp_regional = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\Regional' \
                    r'\R_SpGroupComposite_Regional_STDprj_20161102.gdb'
    # Projection information for the different regional projections
    temp_prj = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\Regional' \
               r'\R_SpGroupComposite_InterProjection_20161102.gdb'
    final_prj_comp = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\Regional' \
                     r'\R_SpGroupComposite_ProjectedtRegion_20161102.gdb'
# input values and workspaces for critical habitat
else:
    # in folder with species group composite with all species unprojections and location of region feature class
    infc_gdb = 'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_SpGroupComposite.gdb'
    intersect_fc = r'L:\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Boundaries.gdb\Regions_dissolve'

    # out locations of the species group composites intersected with region
    out_explode_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\Regional' \
                           r'\CH_SpGroupComposite_ExplodeComp_20161102.gdb'
    out_explode_location_final = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\Regional' \
                                 r'\CH_SpGroupComposite_ExplodeCompFinal_20161102.gdb'
    out_clip_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\Regional\CH_SpGroupComposite_clipComp_20161102.gdb'
    out_intersect_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\Regional' \
                             r'\CH_SpGroupComposite_IntersectRegion_20161102.gdb'
    out_spatial_join_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\Regional' \
                                r'\CH_SpGroupComposite_spatialjoin_Region_20161102.gdb'
    out_Dissolve_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\Regional' \
                            r'\CH_SpGroupComposite_DissolveRegion_20161102.gdb'
    out_Dissolve_location_final = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\Regional' \
                                  r'\CH_SpGroupComposite_DissolveRegion_Final_20161102.gdb'
    DissolveFields = ['FileName', 'EntityID', 'NAME', 'Name_sci', 'SPCode', 'VIPCode', 'Status', 'Region']

    temp_regional = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\Regional' \
                    r'\CH_SpGroupComposite_Regional_STDprj_20161102.gdb'
    # Projection information for the different regional projections
    temp_prj = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\Regional' \
               r'\CH_SpGroupComposite_InterProjection_20161102.gdb'
    final_prj_comp = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\Regional' \
                     r'\CH_SpGroupComposite_ProjectedtRegion_20161102.gdb'
# file suffix for each step
explode_suffix = '_ExplodeRegion_20161102'
spatial_join_suffix = '_SpatialJoinRegion_20161102'
dissolve_suffix = '_DissolveRegion_20161102'
clipped_suffix = '_ClippedLand_Region_20161102'
intersect_suffix = '_IntersectRegion_20161102'
# projections for different regions
prjFolder = "C:\Workspace\projections"
RegionalProjection_Dict = {'CONUS': 'Albers_Conical_Equal_Area.prj',
                           'HI': 'NAD_1983_Albers.prj',
                           'AK': 'WGS_1984_Albers.prj',
                           'AS': 'WGS 1984 UTM Zone  2S.prj',
                           'CNMI': 'WGS 1984 UTM Zone 55N.prj',
                           'GU': 'NAD_1983_Albers.prj',
                           'PR': 'NAD_1983_Albers.prj',
                           'VI': 'NAD_1983_Albers.prj',
                           'Howland_Baker_Jarvis': 'NAD_1983_Albers.prj',
                           'Johnston': 'NAD_1983_Albers.prj',
                           'Laysan': 'NAD_1983_Albers.prj',
                           'Mona': 'NAD_1983_Albers.prj',
                           'Necker': 'NAD_1983_Albers.prj',
                           'Nihoa': 'NAD_1983_Albers.prj',
                           'NorthwesternHI': 'NAD_1983_Albers.prj',
                           'Palmyra_Kingman': 'NAD_1983_Albers.prj',
                           'Wake': 'NAD_1983_Albers.prj'
                           }

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()


# Create a new GDB
def create_gdb(out_folder, out_name, out_path):
    if not arcpy.Exists(out_path):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")
        print 'Created GDB {0}'.format(out_name)


# TODO update from dict to pandas array
# loads species information into dictionary
def load_species_info_from_master(col_index_dict, master_list):
    species_info_dict = {}
    list_keys = col_index_dict.keys()
    list_keys.sort()

    list_keys = col_index_dict.keys()
    list_keys.sort()
    with open(master_list, 'rU') as inputFile:
        header = next(inputFile)
        for line in inputFile:
            speciesinfo = []
            line = line.split(',')
            entid = line[int(col_index_dict['entid'])]

            for v in list_keys:
                vars()[v] = line[int(col_index_dict[v])]
                speciesinfo.append(vars()[v])
            species_info_dict[entid] = speciesinfo
    inputFile.close()

    # print alpha_group
    del header
    return list_keys, species_info_dict


# update species info
def updateFilesloop(infc, user_dissolve_field, spe_info_dict, final_fields_index):
    fc_list_field = [f.name for f in arcpy.ListFields(infc) if not f.required]
    update_field = [word for word in fc_list_field if str(word) in user_dissolve_field]

    with arcpy.da.UpdateCursor(infc, update_field) as lookup:
        for row in lookup:
            entid = row[1]
            listspecies = spe_info_dict[entid]

            index = 2  # HARD CODE to start at field index 2; index 0 - filename,1- EnitityID neither of which are being
            # Updated here
            for field in update_field:
                if field == 'Region' or field == 'EntityID' or field == 'FileName':
                    pass
                else:
                    if row[index] is None:
                        indexfield = final_fields_index[field]
                        value = listspecies[indexfield]
                        row[index] = value
                        lookup.updateRow(row)
                        index += 1
                        print "     Updated {0} for files {1}".format(field, entid)
                    else:
                        index += 1
                        continue


# explodes sp comps into singlepart polygons to extract the area found within each reagion
def explode_polygons(in_location, out_location, previous_suffix, suffix):
    # Check if out_location was already created
    if not arcpy.Exists(out_location):
        path, gdb_file = os.path.split(out_location)
        create_gdb(path, gdb_file, out_location)
    arcpy.env.workspace = in_location
    fc_list = arcpy.ListFeatureClasses()
    for fc in fc_list:
        print fc
        in_fc = in_location + os.sep + fc
        out_name = fc + suffix
        out_name = out_name.replace(previous_suffix, "")
        out_feature = out_location + os.sep + out_name
        try:
            if not arcpy.Exists(out_feature):
                arcpy.Delete_management("inFeatures")
                arcpy.MakeFeatureLayer_management(in_fc, "inFeatures")
                arcpy.MultipartToSinglepart_management("inFeatures", out_feature)
            else:
                continue

        except Exception as error:
            print(error.args[0])
            arcpy.Delete_management(out_feature)


# intersects sp comps into single part polygons with regional fc
def intersect_analysis(intersect_fc_master, in_location, out_location, previous_suffix, suffix):
    if not arcpy.Exists(out_location):
        path, gdb_file = os.path.split(out_location)
        create_gdb(path, gdb_file, out_location)
    # Set workspace location
    arcpy.env.workspace = in_location
    fc_list = arcpy.ListFeatureClasses()
    # loop through all files and run intersect
    for fc in fc_list:
        print fc
        out_name = fc.replace(previous_suffix, suffix)
        out_feature = out_location + os.sep + out_name
        in_features = in_location + os.sep + fc

        arcpy.Delete_management('infc')
        arcpy.Delete_management('intersect')
        arcpy.MakeFeatureLayer_management(in_features, "infc")
        arcpy.MakeFeatureLayer_management(intersect_fc_master, "intersect")
        try:
            if not arcpy.Exists(out_feature):
                arcpy.Intersect_analysis(["infc", "intersect"], out_feature, "ALL", "", "")
                print 'Intersect CompFile {0}'.format(fc)
            else:
                continue

        except Exception as error:
            print(error.args[0])


# spatial join single part sp comp with regional fc
def spatial_join(intersect_fc_master, in_location, out_location, spe_info_dict, final_fields_index, previous_suffix,
                 suffix):
    # Check if out location was already created
    if not arcpy.Exists(out_location):
        path, gdb_file = os.path.split(out_location)
        create_gdb(path, gdb_file, out_location)
    # Set workspace location
    arcpy.env.workspace = in_location
    fc_list = arcpy.ListFeatureClasses()
    # loop through all files and run intersect
    for fc in fc_list:
        print fc
        out_name = fc.replace(previous_suffix, suffix)
        out_feature = out_location + os.sep + out_name
        in_features = in_location + os.sep + fc

        arcpy.Delete_management('infc')
        arcpy.Delete_management('intersect')
        arcpy.MakeFeatureLayer_management(in_features, "infc")
        arcpy.MakeFeatureLayer_management(intersect_fc_master, "intersect")
        try:
            if not arcpy.Exists(out_feature):
                arcpy.SpatialJoin_analysis("infc", "intersect", out_feature, 'JOIN_ONE_TO_MANY')
                print 'Spatial Join CompFile {0}'.format(fc)
                updateFilesloop(out_feature, DissolveFields, spe_info_dict, final_fields_index)
            else:
                continue

        except Exception as error:
            print(error.args[0])
            arcpy.Delete_management(out_feature)


# clips sp composites to regional file so only area on land include
def clip_feature(in_location, clip_fc, out_location, previous_suffix, suffix):
    # Check if out location was already created
    if not arcpy.Exists(out_location):
        path, gdb_file = os.path.split(out_location)
        create_gdb(path, gdb_file, out_location)

    arcpy.Delete_management("ClipFeatures")
    arcpy.MakeFeatureLayer_management(clip_fc, "ClipFeatures")
    xy_tolerance = ""
    # Set workspace location
    arcpy.env.workspace = in_location
    fclist = arcpy.ListFeatureClasses()
    # loop through all files and run intersect
    for fc in fclist:
        print fc
        outname = fc.replace(previous_suffix, suffix)
        out_feature = out_location + os.sep + outname
        in_features = in_location + os.sep + fc
        arcpy.Delete_management("inFeatures")
        arcpy.MakeFeatureLayer_management(in_features, "inFeatures")
        try:
            if not arcpy.Exists(out_feature):
                arcpy.Clip_analysis("inFeatures", "ClipFeatures", out_feature, xy_tolerance)
                print 'Clipped CompFile {0}'.format(fc)
            else:
                continue
        except Exception as error:
            print(error.args[0])
            arcpy.Delete_management(out_feature)


# Generates regional comps in std projection
def projected_comps(in_lyr, fc, prj_final, region_name, temp_gdb, final_gdb, prj_location):
    # Check if out location was already created
    if not arcpy.Exists(temp_gdb):
        path, gdb_file = os.path.split(temp_gdb)
        create_gdb(path, gdb_file, temp_gdb)
    if not arcpy.Exists(final_gdb):
        path, gdb_file = os.path.split(final_gdb)
        create_gdb(path, gdb_file, final_gdb)

    prj_name = prj_final.replace('.prj', '')
    prj_name = prj_name.replace(' ', '_')

    in_fc = in_lyr

    # Locations of WGS and desired projections
    wgs_coord_file = prj_location + os.sep + 'WGS 1984.prj'
    prj_file = prj_location + os.sep + prj_final
    # Extraction spatial info from these prj file
    dsc_wgs = arcpy.Describe(wgs_coord_file)
    wgs_coord_sys = dsc_wgs.spatialReference
    dsc_prj = arcpy.Describe(prj_file)
    prj_sr = dsc_prj.spatialReference
    prj_datum = prj_sr.GCS.datumName

    if not arcpy.Exists(temp_gdb):
        path, file_temp = os.path.split(temp_gdb)
        create_gdb(path, file_temp, temp_gdb)
    if not arcpy.Exists(final_gdb):
        path, file_final = os.path.split(final_gdb)
        create_gdb(path, file_final, final_gdb)

    if prj_datum == "D_WGS_1984":

        fc_other_geo = region_name + "_" + str(fc) + "_WGS84"
        out_other_fc = temp_gdb + os.sep + fc_other_geo

        prj_fc_name = fc_other_geo + "_" + prj_name
        prj_fc = final_gdb + os.sep + prj_fc_name

        if not arcpy.Exists(out_other_fc):
            arcpy.Project_management(in_fc, out_other_fc, wgs_coord_sys)
            print(arcpy.GetMessages(0))

        if not arcpy.Exists(prj_fc):
            arcpy.Project_management(out_other_fc, prj_fc, prj_sr)
            print(arcpy.GetMessages(0))
        else:
            print str(prj_fc) + " already exists"
    else:

        prj_fc_name = region_name + "_" + str(fc) + "_" + prj_name
        prj_fc = final_gdb + os.sep + prj_fc_name

        if not arcpy.Exists(prj_fc):
            arcpy.Project_management(in_fc, prj_fc, prj_sr)
            # print(arcpy.GetMessages(0))
            print 'Completed Projecitons  for {0}'.format(prj_fc)
        else:
            pass
    return prj_fc


# projects regional sp comp into project found in projection dict for the region
def project_regional_comps(in_location, temp_non_project_region, temp_gdb, final_gdb, prj_location):
    if not arcpy.Exists(temp_non_project_region):
        path, gdb_file = os.path.split(temp_non_project_region)
        create_gdb(path, gdb_file, temp_non_project_region)

        # Set workspace location
    arcpy.env.workspace = in_location
    fclist = arcpy.ListFeatureClasses()
    for fc in fclist:
        with arcpy.da.SearchCursor(fc, ['Region']) as cursor:
            region_list = sorted({row[0] for row in cursor})
        current_regions = RegionalProjection_Dict.keys()
        for region in region_list:
            if region not in current_regions:
                continue
            prj_for_fc = RegionalProjection_Dict[region]
            whereclause = '"Region" =' + "'%s'" % (str(region))
            arcpy.Delete_management('lyr')
            arcpy.MakeFeatureLayer_management(fc, 'lyr', whereclause)
            temp_fc = temp_non_project_region + os.sep + region + "_" + fc + "_STDprj"
            print temp_fc
            if not arcpy.Exists(temp_fc):
                arcpy.CopyFeatures_management('lyr', temp_fc)
            arcpy.Delete_management('lyr')
            final_prj_final = projected_comps(temp_fc, fc, prj_for_fc, region, temp_gdb, final_gdb, prj_location)
            arcpy.Delete_management("inFeatures")
            print final_prj_final
            arcpy.MakeFeatureLayer_management(final_prj_final, "inFeatures")
            add_acres("inFeatures", region, final_prj_final)


# dissolves single part polygons back to multipart polygons
def dissolve_files(in_location, dissolve_fields, out_location, previous_suffix, suffix):
    # Check if out location was already created
    if not arcpy.Exists(out_location):
        path, gdb_file = os.path.split(out_location)
        create_gdb(path, gdb_file, out_location)
    # Set workspace location
    arcpy.env.workspace = in_location
    fc_list = arcpy.ListFeatureClasses()
    # loop through all files and run dissolve
    for fc in fc_list:
        print fc
        field_list = [f.name for f in arcpy.ListFields(fc) if not f.required]
        dissolve_list = [word for word in field_list if str(word) in dissolve_fields]

        in_fc = in_location + os.sep + fc
        arcpy.Delete_management("inFeatures")
        arcpy.MakeFeatureLayer_management(in_fc, "inFeatures")

        out_name = fc.replace(previous_suffix, suffix)
        out_feature = out_location + os.sep + out_name
        try:
            if not arcpy.Exists(out_feature):
                arcpy.Dissolve_management("inFeatures", out_feature, dissolve_list)
                print 'Dissolved  {0}'.format(out_name)
            else:
                continue
        except Exception as error:
            print(error.args[0])
            arcpy.Delete_management(out_feature)


# delete field
def delete_field(in_location, delete_list):
    arcpy.env.workspace = in_location
    fc_list = arcpy.ListFeatureClasses()
    for fc in fc_list:
        arcpy.Delete_management("fc_lyr")
        arcpy.MakeFeatureLayer_management(fc, "fc_lyr")
        arcpy.DeleteField_management("fc_lyr", delete_list)


def add_acres(in_lyr, region, fc):
    current_region = 'Acres_' + str(region)
    arcpy.AddField_management(in_lyr, current_region, "DOUBLE", "", 0)
    with arcpy.da.SearchCursor(in_lyr, [current_region]) as cursor:
        for row in cursor:
            if row[0] is None:
                exp = "!SHAPE.AREA@Acres!"
                print "Calc acres for  " + fc
                arcpy.CalculateField_management(in_lyr, current_region, exp, "PYTHON_9.3")
            else:
                continue


listKeys, speciesinfo_dict = load_species_info_from_master(ColIndexDict, masterlist)
try:
    explode_polygons(infc_gdb, out_explode_location, " ", explode_suffix)
    clip_feature(out_explode_location, intersect_fc, out_clip_location, explode_suffix, clipped_suffix)
    spatial_join(intersect_fc, out_clip_location, out_spatial_join_location, speciesinfo_dict, final_fieldsindex,
                 clipped_suffix, spatial_join_suffix)
    dissolve_files(out_spatial_join_location, DissolveFields, out_Dissolve_location, spatial_join_suffix,
                   dissolve_suffix)
    # some of the large ranging species will have one polygon that found in multiple regions so the insterect is done
    # after dissolving the first spatial join because the ocean has been clipped out, Region col is delete then added
    # again for in intersection before generating the regional comps
    delete_field_list = ['Region']
    delete_field(out_Dissolve_location, delete_field_list)
    explode_polygons(out_Dissolve_location, out_explode_location_final, dissolve_suffix, explode_suffix)
    intersect_analysis(intersect_fc, out_Dissolve_location, out_intersect_location, explode_suffix, intersect_suffix)
    dissolve_files(out_intersect_location, DissolveFields, out_Dissolve_location_final, intersect_suffix,
                   dissolve_suffix)
    project_regional_comps(out_Dissolve_location_final, temp_regional, temp_prj, final_prj_comp, prjFolder)

except Exception as err:
    print(err.args[0])

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
