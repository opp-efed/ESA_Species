import arcpy
import os
import datetime


# Author J.Connolly
# Internal deliberative, do not cite or distribute

# Title - Generates regional composites in correct projection to calc area of the species file found in each region

# master list and location by index number base zero of species information
masterlist = r"E:\No_Call_Species\NoCall_MasterListESA_Dec2018_20190130.csv"

date = "_2020427"  # in yyyymmdd

# col index values of species info cols in master list - CONFIRM BEFORE RUNNING
ColIndexDict = dict(comname=6, sciname=7, spcode=12, vipcode=13, entid=1, group=15, popabb=9, status=8)

# Index order of how the information is loaded into the species dict see load_species_info_from_master functions;
# group is in pos 2 and is not included in dictionary because species group is in the composite name not the att table
# confirm this is in the correct order using the print out right after the start timer
final_fieldsindex = dict(NAME=2, Name_sci=3, SPCode=4, VIPCode=5, EntityID=1, PopName=7, Status=6)

# input values and workspaces for range files
Range = True # True the Range files are run, False the CH files are run
intersect_fc = r'E:\Workspace\StreamLine\ESA\Boundaries.gdb\Regions_dissolve'  # regional polygons
# Confirm names and dictionary below and the name for  GSC WGS 1984 line 296
prjFolder = "E:\Workspace\StreamLine\projections\FinalBE"  #prjfiles
base_outpath = r'E:\No_Call_Species\Composites_NoCall'

if Range:  # output file structure set using parameters above
    # in folder with species group composite with all species un-projected and location of region feature class
    infc_gdb = base_outpath + os.sep +'R_SpGroupComposite.gdb'

    root_outpath = base_outpath + os.sep + 'RegionalFiles\Range'
    # out locations of the species group composites intersected with region
    out_explode_location = root_outpath + os.sep + 'R_SpGroupComposite_ExplodeComp' + date + '.gdb'
    out_explode_location_final = root_outpath + os.sep + 'R_SpGroupComposite_ExplodeCompFinal' + date + '.gdb'
    out_clip_location = root_outpath + os.sep + 'R_SpGroupComposite_clipComp' + date + '.gdb'
    out_intersect_location = root_outpath + os.sep + 'R_SpGroupComposite_IntersectRegion' + date + '.gdb'
    out_spatial_join_location = root_outpath + os.sep + 'R_SpGroupComposite_spatialjoin_Region' + date + '.gdb'
    out_Dissolve_location = root_outpath + os.sep + 'R_SpGroupComposite_DissolveRegion' + date + '.gdb'
    out_Dissolve_location_final = root_outpath + os.sep + 'R_SpGroupComposite_DissolveRegion_Final' + date + '.gdb'
    DissolveFields = ['FileName', 'EntityID', 'NAME', 'Name_sci', 'SPCode', 'VIPCode', 'Status', 'Region']

    temp_regional = root_outpath + os.sep + 'R_SpGroupComposite_Regional_STDprj' + date + '.gdb'
    # Projection information for the different regional projections
    temp_prj = root_outpath + os.sep + 'R_SpGroupComposite_InterProjection' + date + '.gdb'
    final_prj_comp = root_outpath + os.sep + 'R_SpGroupComposite_ProjectedtRegion' + date + '.gdb'
# input values and workspaces for critical habitat
else:
    # in folder with species group composite with all species unprojections and location of region feature class
    infc_gdb = base_outpath + os.sep + 'CH_SpGroupComposite.gdb'
    root_outpath = base_outpath + os.sep +'RegionalFiles\CriticalHabitat'
    # out locations of the species group composites intersected with region
    out_explode_location = root_outpath + os.sep + 'CH_SpGroupComposite_ExplodeComp' + date + '.gdb'
    out_explode_location_final = root_outpath + os.sep + 'CH_SpGroupComposite_ExplodeCompFinal' + date + '.gdb'
    out_clip_location = root_outpath + os.sep + 'CH_SpGroupComposite_clipComp' + date + '.gdb'
    out_intersect_location = root_outpath + os.sep + 'CH_SpGroupComposite_IntersectRegion' + date + '.gdb'
    out_spatial_join_location = root_outpath + os.sep + 'CH_SpGroupComposite_spatialjoin_Region' + date + '.gdb'
    out_Dissolve_location = root_outpath + os.sep + r'CH_SpGroupComposite_DissolveRegion' + date + '.gdb'
    out_Dissolve_location_final = root_outpath + os.sep + r'CH_SpGroupComposite_DissolveRegion_Final' + date + '.gdb'
    DissolveFields = ['FileName', 'EntityID', 'NAME', 'Name_sci', 'SPCode', 'VIPCode', 'Status', 'Region']

    temp_regional = root_outpath + os.sep + r'CH_SpGroupComposite_Regional_STDprj' + date + '.gdb'
    # Projection information for the different regional projections
    temp_prj = root_outpath + os.sep + r'CH_SpGroupComposite_InterProjection' + date + '.gdb'
    final_prj_comp = root_outpath + os.sep + r'CH_SpGroupComposite_ProjectedtRegion' + date + '.gdb'
# file suffix for each step
explode_suffix = '_ExplodeRegion' + date
spatial_join_suffix = '_SpatialJoinRegion' + date
dissolve_suffix = '_DissolveRegion' + date
clipped_suffix = '_ClippedLand_Region' + date
intersect_suffix = '_IntersectRegion' + date
# projections for different regions

#TODO UPDATE REGION ID IN REGION_DISSOLVE FROM L48 to CONUS SO EVERYTHINGS IS CONUS
#UNTIL ABOVE TODO IS COMPLETE REGION NEED TO L48 and not CONUS'ex
RegionalProjection_Dict = {'L48': 'Albers_Conical_Equal_Area.prj',
                           'HI': 'NAD_1983_UTM_Zone_4N.prj',
                           'AK': 'WGS_1984_Albers.prj',
                           'AS': 'WGS_1984_UTM_Zone_2S.prj',
                           'CNMI': 'WGS_1984_UTM_Zone_55N.prj',
                           'GU': 'WGS_1984_UTM_Zone_55N.prj',
                           'PR': 'Albers_Conical_Equal_Area.prj',
                           'VI': 'WGS_1984_UTM_Zone_20N.prj',
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
            try:
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
            except KeyError:
                print ('Species {0} in composite {1} not in masterlist'.format(entid, infc))


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

        if not arcpy.Exists(out_feature):
                arcpy.SpatialJoin_analysis("infc", "intersect", out_feature, 'JOIN_ONE_TO_MANY')
                print fc
                print 'Spatial Join CompFile {0}'.format(fc)
                updateFilesloop(out_feature, DissolveFields, spe_info_dict, final_fields_index)
        else:
                continue



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
    wgs_coord_file = prj_location + os.sep + 'WGS_1984.prj'
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


# Creates the output locations based on the info in root_outpath and base_outpath
if not os.path.exists(root_outpath):
    path, folder = os.path.split(root_outpath)
    if not os.path.exists(path):
        os.mkdir(path)
    os.mkdir(root_outpath)

listKeys, speciesinfo_dict = load_species_info_from_master(ColIndexDict, masterlist)
print "Data loaded for species {0} is {1}".format(speciesinfo_dict.keys()[0],speciesinfo_dict[speciesinfo_dict.keys()[0]])
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
