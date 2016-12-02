import os
import datetime

import arcpy

__author__ = 'JConno02'

inlocation = r'J:\Workspace\ESA_Species\Range\NAD83'
out_inter_loction = r'J:\Workspace\ESA_Species\FinalBE_ForCoOccur\Union_Range_inter.gdb'
finalfc = r'J:\Workspace\ESA_Species\FinalBE_ForCoOccur\Union_Range.gdb'

subset_group = False
start_union = False
entlist = ['5067', '438', '444', '446', '430', '462', '3412', '419', '450', '8083', '1984', '420', '433', '451', '421',
           '422', '4308', '5168', '4508', '423', '424', '9001', '425', '431', '432', '434', '10147', '10007', '426',
           '3670', '455', '427', '429', '428', '7495', '437']

unionlist = []


def union_sp_files(in_ws, out_inter, subset_group, unionlist):
    if not arcpy.Exists(out_inter):
        start_union = datetime.datetime.now()
        arcpy.env.workspace = in_ws
        fclist = arcpy.ListFeatureClasses()
        if subset_group:
            for fc in fclist:
                entid = fc.split('_')
                entid = str(entid[2])
                if entid in entlist:
                    unionlist.append(str(in_ws + os.sep + str(fc)))
        else:
            unionlist = fclist
        # print(unionlist)

        arcpy.Union_analysis(fclist, out_inter, "ALL")
        print "\nCreated output {0} in {1}".format(out_inter, (datetime.datetime.now() - start_union))
    else:
        print '\nAlready union {0}'.format(out_inter)


def clean_unionfiles(outfc, final):
    listfields = [f.name for f in arcpy.ListFields(outfc)]

    entFields = []

    arcpy.Delete_management("out")
    arcpy.MakeFeatureLayer_management(outfc, "out")

    for field in listfields:
        if field.startswith('EntityID'):
            entFields.append(field)
    entFields.append('OBJECTID')
    print entFields

    indexfield = len(entFields)
    arcpy.AddField_management("out", 'ZoneSpecies', "Text")

    zonesp = {}
    with arcpy.da.SearchCursor(outfc, entFields) as cursor:
        for row in cursor:
            count = 0
            listsp = []
            for field in entFields:
                index_f = entFields.index(field)
                if field == 'OBJECTID':
                    zoneid = row[index_f]
                else:
                    ent = row[index_f]
                    if str(ent) == '':
                        continue
                    else:
                        listsp.append(ent)
            #print listsp
            zonesp[zoneid] = listsp
        del cursor, listsp

    with arcpy.da.UpdateCursor("out", ['OBJECTID', 'ZoneSpecies']) as cursor:
        for row in cursor:
            listsp = zonesp[row[0]]
            #print listsp
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
        #print 'Deleting {0}'.format(v)
    arcpy.Delete_management("out")
    arcpy.Delete_management("final")
    print 'cleaned {0}'.format(final)


start_script = datetime.datetime.now()
print "Started at {0}".format(start_script)

if start_union:
    if inlocation[-3:] == 'gdb':
        sp_group = inlocation[:-4]
        sp_group = sp_group.replace(" ", "_")
        ingdb = inlocation
        outfc_inter = out_inter_loction + os.sep + sp_group + "_Union_inter"
        union_sp_files(ingdb, outfc_inter, subset_group, unionlist)
    else:
        list_ws = os.listdir(inlocation)
        print list_ws
        for v in list_ws:
            if v[-3:] == 'gdb':
                sp_group = v[:-4]
                sp_group = sp_group.replace(" ", "_")
                ingdb = inlocation + os.sep + v
                outfc_inter = out_inter_loction + os.sep + sp_group + "_Union_inter"
                # print outfc_inter
                union_sp_files(ingdb, outfc_inter, subset_group, unionlist)
            else:
                continue

    arcpy.env.workspace = out_inter_loction
    fclist = arcpy.ListFeatureClasses()
    for fc in fclist:
        fc = fc.replace(" ", "_")
        inter_fc = out_inter_loction + os.sep + fc
        outname = fc.replace("_Union_inter", "Union_Final")
        outfc_final = finalfc + os.sep + outname
        if not arcpy.Exists(outfc_final):
            clean_unionfiles(inter_fc, outfc_final)
        else:
            print '\nAlready cleaned {0}'.format(outfc_final)
            continue
else:
    arcpy.env.workspace = out_inter_loction
    fclist = arcpy.ListFeatureClasses()
    for fc in fclist:
        inter_fc = out_inter_loction + os.sep + fc
        outname = fc.replace("_Union_inter", "_Union_Final_20160705")
        outfc_final = finalfc + os.sep + outname
        if not arcpy.Exists(outfc_final):
            clean_unionfiles(inter_fc, outfc_final)
        else:
            print '\nAlready cleaned {0}'.format(outfc_final)
            continue

print "Completed in {0}".format(datetime.datetime.now() - start_script)
