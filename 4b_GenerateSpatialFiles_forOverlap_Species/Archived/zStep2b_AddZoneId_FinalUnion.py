import arcpy

# ##TODO Add in check for missing species in Un
infolder = r'C:\Users\JConno02\Documents\Projects\ESA\UnionFiles_Winter2018\Range\R_SpGroup_Union_final_20180110.gdb'

arcpy.env.workspace = infolder
fclist = arcpy.ListFeatureClasses()

for fc in fclist:
    print fc
    arcpy.AddField_management(fc, "ZoneID", "DOUBLE")
    with arcpy.da.UpdateCursor(fc, ['OBJECTID','ZoneID']) as cursor:
        for row in cursor:
            row[1] = row[0]
            cursor.updateRow(row)