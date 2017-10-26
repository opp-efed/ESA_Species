import os
import datetime
import numpy as np
import pandas as pd
import arcpy

# Title - Generate master union file by sp groups using the gdb in the spatial library

# NOTE NOTE if process interrupted by user incomplete file will be generated; try except loop deletes incomplete if
# script fails due to error
#
# Note: Union of Fish CH stuggles probably because of the buffer. Ran simplify but need a permanent solution- aqu
# decision with services may answer this; Flowering plants has over 1000000 zone takes awhile to run

# Runs union on complete spatial library by species group  use identify species list for use in co-occur


# TODO look for a way to that make deleting field easier, can field mapping be used to copy only - check fieldinfo visiable url below
# http://pro.arcgis.com/en/pro-app/arcpy/classes/fieldinfo.htm
# TODO the columns of interest to a new file?
# This can be done in arcmap by turning off fields then exporting, need to see if it can be done in a script

# TODO SET UP TO ADD ZONEID SO OBJECTID IS NOT USED- to prevent  objectID from change when clipped
Range = True
out_df = pd.DataFrame(index=(list(range(0, 1000))))
outcsv =r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\CriticalHabitat\Species_included_inUnion_2016020217.csv'
if Range:
    # Spatial library being used for union IE CritHab or Range; will loop by species group, or use entid fpr uniqu list
    inlocation = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\SpatialLibrary\Range'
    filetype = 'R_'

    # location for intermediate (raw) union file and the final cleaned union file with std att table
    out_inter_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\inter.gdb'
    finalfc = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\R_SpGroup_Union_final_20161102.gdb'
    # NOTE NOTE if process interrupted incomplete file will be generated

    start_union = True  # True runs full union and clean up of union, false runs just the clean up of att table

    # Species to include or exclude depending on if use is running all sp group or a subset of a group
    # species group to skip because there are no GIS files, ie there is no crithab for any lichens
    skipgroup = ['Amphibians','Arachnids' 'Birds', 'Conifers','Corals','Ferns','Flowering_Plants']

    # if True will only union entid listed in ent list if false will union all entid in gdb
    subset_group = False
    # filename sub set comp
    enlistfc_name = ''
    # list of entid to be include subset comp
    entlist = [
    ]

else:
    inlocation = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\SpatialLibrary\CriticalHabitat'
    filetype = 'CH_'

    # location for intermediate (raw) union file and the final cleaned union file with std att table
    out_inter_location = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\CriticalHabitat\CH_spGroup_Union_inter_20161102.gdb'
    finalfc = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\CriticalHabitat\CH_SpGroup_Union_final_20161102.gdb'
    # NOTE NOTE if process interupted incomplete file will be generated

    start_union = False  # True runs full union and clean up of union, false runs just the clean up of att table

    # Species to include or exclude depending on if use is running all sp group or a subset of a group
    # species group to skip because there are no GIS files, ie there is no crithab for any lichens
    skipgroup = ['Amphibians','Arachnids', 'Birds','Corals','Crustaceans','Flowering','Lichens','Insects','Mammals','Reptiles','Snails','Conifers','Corals','Ferns', 'Clams']

    # if True will only union entid listed in ent list if false will union all entid in gdb
    subset_group = False
    # filename sub set comp
    enlistfc_name = 'SubInsects_'
    # list of entid to be include subset comp
    entlist = []

# Static variable
file_suffix = '_Union_MAG_inter'
file_suffix_clean = '_Union_MAG_Final_201601102'


# Create a new GDB
def create_gdb(out_folder, out_name, out_path):
    if not arcpy.Exists(out_path):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")
        print 'Created GDB {0}'.format(out_name)


# runs union on spatial library indicate by user or subset of species to generate union file by sp group
def union_sp_files(in_ws, out_inter, subset_group_bool, ent_list):
    unionlist = []
    if subset_group:
        out_inter = out_inter_location + os.sep + enlistfc_name + '_inter'
        print out_inter
    if not arcpy.Exists(out_inter):
        start_union_time = datetime.datetime.now()
        print "\nStarting {0} at {1}".format(out_inter, start_union_time)
        arcpy.env.workspace = in_ws
        fc_list = arcpy.ListFeatureClasses()
        if subset_group_bool:
            for fcs in fc_list:
                entid = fcs.split('_')
                entid = str(entid[1])

                if entid in ent_list:
                    unionlist.append(str(in_ws + os.sep + str(fcs)))

        else:
            unionlist = fc_list
        try:
            arcpy.Union_analysis(unionlist, out_inter, "ALL")
        except Exception as error:
            print(error.args[0])
            arcpy.Delete_management(out_inter)
        print "\nCreated output {0} in {1}".format(out_inter, (datetime.datetime.now() - start_union_time))
    else:
        print '\nAlready union {0}'.format(out_inter)


# remove all the extra cols generated by union; NOTE if files has hundreds of file can turn off all fields expect the
# zone species and zoneid and export in arcmap; this is much faster look into coding this
def clean_unionfiles(outfc, final, group, df):
    listfields = [f.name for f in arcpy.ListFields(outfc)]

    ent_fields = []
    group_spe = []

    arcpy.Delete_management("out")
    arcpy.MakeFeatureLayer_management(outfc, "out")

    for field in listfields:
        if field.startswith('EntityID'):
            ent_fields.append(field)
    ent_fields.append('OBJECTID')
    # print ent_fields

    arcpy.AddField_management("out", 'ZoneSpecies', "TEXT", "", "", "1000", "", "NULLABLE", "NON_REQUIRED", "")

    zonesp = {}
    with arcpy.da.SearchCursor(outfc, ent_fields) as cursor:
        for row in cursor:
            listsp = []
            for field in ent_fields:
                index_f = ent_fields.index(field)
                if field == 'OBJECTID':
                    zoneid = row[index_f]
                else:
                    ent = row[index_f]
                    if str(ent) == '':
                        continue
                    else:
                        listsp.append(ent)
                        if str(ent) not in group_spe:
                            group_spe.append(ent)
            # print listsp
            zonesp[zoneid] = listsp
        del cursor, listsp

    with arcpy.da.UpdateCursor("out", ['OBJECTID', 'ZoneSpecies']) as cursor:
        for row in cursor:
            listsp = zonesp[row[0]]
            row[1] = str(listsp)
            cursor.updateRow(row)
        del cursor

    delfields = [f.name for f in arcpy.ListFields(outfc) if not f.required]
    delfields.remove('ZoneSpecies')
    layer = "temp_lyr"
    arcpy.MakeFeatureLayer_management(outfc, layer)

    desc = arcpy.Describe(layer)
    field_info = desc.fieldInfo
    # place holder for field mapping str- this is much faster than deleting field
    field_info_str = ''
    try:
        for index in range(0,field_info.count):
            field_nm = ("{0}".format(field_info.getFieldName(index)))
            if field_nm in delfields:
                # add column to field mapping str for layer creation; structure [field name] [field name] [HIDDEN]; next
                field_info_str += field_nm + ' ' + field_nm + ' HIDDEN;'
                #print("\tVisible:    {0}".format(field_info.getVisible(index)))
    except:
        pass
    # print field_info_str
    field_info_str.rstrip(';')
    arcpy.MakeFeatureLayer_management(outfc, "lyr", field_info=field_info_str)
    arcpy.CopyFeatures_management("lyr", final)
    arcpy.Delete_management("temp_lyr")
    arcpy.Delete_management("lyr")


    group_spe = list(set(group_spe))
    count = len(group_spe)
    remaining = [None]* (1000-count)
    mergelist = group_spe + remaining
    series_sp = pd.Series(mergelist)
    out_df[group] = series_sp.values


# TODO update to loops to functions

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

# Make sure out location have been created
if not arcpy.Exists(out_inter_location):
    path, gdb_file = os.path.split(out_inter_location)
    create_gdb(path, gdb_file, out_inter_location)

if not arcpy.Exists(finalfc):
    path, gdb_file = os.path.split(finalfc)
    create_gdb(path, gdb_file, finalfc)

# loop through gdb from in location get a list of all species of if subset_group is true the species id in entlist then
# will generate union by species group or the list the user inputs

if start_union:
    # checks to see if in location is single gdb or a folder with multiple gdb
    if inlocation[-3:] == 'gdb':
        sp_group = inlocation[:-4]
        sp_group = sp_group.replace(" ", "_")
        ingdb = inlocation
        outfc_inter = out_inter_location + os.sep + filetype + sp_group + file_suffix
        if not arcpy.Exists(outfc_inter):
            union_sp_files(ingdb, outfc_inter, subset_group, entlist)
    else:
        list_ws = os.listdir(inlocation)
        print list_ws
        for v in list_ws:
            if v[-3:] == 'gdb':
                sp_group = v[:-4]
                sp_group = sp_group.replace(" ", "_")
                if sp_group not in skipgroup:
                    ingdb = inlocation + os.sep + v
                    outfc_inter = out_inter_location + os.sep + filetype + sp_group + file_suffix
                    # print outfc_inter
                    if not arcpy.Exists(outfc_inter):
                        union_sp_files(ingdb, outfc_inter, subset_group, entlist)
                else:
                    continue

    arcpy.env.workspace = out_inter_location
    fclist = arcpy.ListFeatureClasses()
    for fc in fclist:
        sp_group = fc.split('_')
        sp_group = sp_group[1]
        print sp_group
        if sp_group in skipgroup:
            continue
        inter_fc = out_inter_location + os.sep + fc
        out_fc = fc.replace(file_suffix, file_suffix_clean)
        out_fc_final = finalfc + os.sep + out_fc
        if not arcpy.Exists(out_fc_final):
            clean_unionfiles(inter_fc, out_fc_final, sp_group, out_df)
        else:
            print '\nAlready cleaned {0}\n'.format(out_fc_final)
            continue
else:
    arcpy.env.workspace = out_inter_location
    fclist = arcpy.ListFeatureClasses()
    for fc in fclist:
        sp_group = fc.split('_')
        sp_group = sp_group[1]
        print sp_group
        if sp_group in skipgroup:
            continue
        inter_fc = out_inter_location + os.sep + fc
        print inter_fc
        out_fc = fc.replace(file_suffix, file_suffix_clean)
        out_fc_final = finalfc + os.sep + out_fc
        if not arcpy.Exists(out_fc_final):
            clean_unionfiles(inter_fc, out_fc_final, sp_group, out_df)
        else:
            print '\nAlready cleaned {0}\n'.format(out_fc_final)
            continue

print out_df
out_df.to_csv(outcsv)
end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
