import os
import datetime

import arcpy

# Title - Generate master union file by sp groups using the gdb in the spatial library

# NOTE NOTE if process interrupted by user incomplete file will be generated; try except loop deletes incomplete if
# script fails due to error
#
# Note: Union of Fish CH stuggles probably because of the buffer. Ran simplify but need a permanent solution- aqu
# decision with services may answer this; Flowering plants has over 1000000 zone takes awhile to run

# Runs union on complete spatial library by species group  use identify species list for use in co-occur


# TODO look for a way to that make deleting field easier, can field mapping be used to copy only
# TODO the columns of interest to a new file?
# This can be done in arcmap by turning off fields then exporting, need to see if it can be done in a script

# TODO SET UP TO ADD ZONEID SO OBJECTID IS NOT USED- to prevent  objectID from change when clipped
Range = True
if Range:
    # Spatial library being used for union IE CritHab or Range; will loop by species group, or use entid fpr uniqu list
    inlocation = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\Range\NAD83\Flowering Plants.gdb'
    filetype = 'R_'

    # location for intermediate (raw) union file and the final cleaned union file with std att table
    out_inter_location = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat' \
                         r'\inter.gdb'
    finalfc = r'C:\Users\Admin\Documents\Jen\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range' \
              r'\R_SpGroup_Union_final_20160907.gdb'
    # NOTE NOTE if process interrupted incomplete file will be generated

    start_union = True  # True runs full union and clean up of union, false runs just the clean up of att table

    # Species to include or exclude depending on if use is running all sp group or a subset of a group
    # species group to skip because there are no GIS files, ie there is no crithab for any lichens
    skipgroup = []

    # if True will only union entid listed in ent list if false will union all entid in gdb
    subset_group = True
    # filename sub set comp
    enlistfc_name = 'R_Flowering_Plants_Missing_20161004'
    # list of entid to be include subset comp
    entlist = ['513',
               '558',
               '569',
               '615',
               '624',
               '643',
               '705',
               '763',
               '798',
               '807',
               '817',
               '831',
               '836',
               '837',
               '852',
               '872',
               '960',
               '996',
               '1029',
               '1038',
               '1045',
               '1077',
               '1189',
               '1264',
               '1265',
               '1266',
               '1400',
               '3999',
               '6617',
               '6782',
               '7264',
               '7840',
               '9951',
               '9956',
               '9957',
               '10719',
               '10720',
               '10721',
               '10722',
               '10723',
               '10724',
               '10725',
               '10726',
               '10727',
               '10728',
               '11340'
               ]

else:
    inlocation = r'J:\Workspace\ESA_Species\CriticalHabitat\NAD_Final'
    filetype = 'CH_'

    # location for intermediate (raw) union file and the final cleaned union file with std att table
    out_inter_location = r'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\Critical Habitat' \
                         r'\CH_spGroup_Union_inter_201600907.gdb'
    finalfc = r'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\Critical Habitat\CH_SpGroup_Union_final_20160907.gdb'
    # NOTE NOTE if process interupted incomplete file will be generated

    start_union = True  # True runs full union and clean up of union, false runs just the clean up of att table

    # Species to include or exclude depending on if use is running all sp group or a subset of a group
    # species group to skip because there are no GIS files, ie there is no crithab for any lichens
    skipgroup = ['Conifers_and_Cycads', 'Lichens']

    # if True will only union entid listed in ent list if false will union all entid in gdb
    subset_group = False
    # filename sub set comp
    enlistfc_name = 'SubInsects_'
    # list of entid to be include subset comp
    entlist = []

# Static variable
file_suffix = '_Union_inter'
file_suffix_clean = '_Union_Final_20160907'


# Create a new GDB
def create_gdb(out_folder, out_name, out_path):
    if not arcpy.Exists(out_path):
        arcpy.CreateFileGDB_management(out_folder, out_name, "CURRENT")
        print 'Created GDB {0}'.format(out_name)


# runs union on spatial library indicate by user or subset of species to generate union file by sp group
def union_sp_files(in_ws, out_inter, subset_group_bool, ent_list):
    unionlist = []
    if subset_group:
        out_inter = out_inter_location+os.sep+enlistfc_name + '_inter'
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
def clean_unionfiles(outfc, final):
    listfields = [f.name for f in arcpy.ListFields(outfc)]

    ent_fields = []

    arcpy.Delete_management("out")
    arcpy.MakeFeatureLayer_management(outfc, "out")

    for field in listfields:
        if field.startswith('EntityID'):
            ent_fields.append(field)
    ent_fields.append('OBJECTID')
    print ent_fields

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
            print listsp
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

    arcpy.CopyFeatures_management(outfc, final)
    arcpy.Delete_management("final")
    arcpy.MakeFeatureLayer_management(final, 'final')

    ##ToDo figut out an easier way to delet all the fields
    for v in delfields:
        arcpy.DeleteField_management(final, v)
        print 'Deleting {0}'.format(v)
    arcpy.Delete_management("out")
    arcpy.Delete_management("final")
    print 'cleaned {0}\n'.format(final)


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
            clean_unionfiles(inter_fc, out_fc_final)
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
            clean_unionfiles(inter_fc, out_fc_final)
        else:
            print '\nAlready cleaned {0}\n'.format(out_fc_final)
            continue

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
