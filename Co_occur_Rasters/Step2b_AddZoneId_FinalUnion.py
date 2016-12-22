import arcpy


infolder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Union\Range\R_SpGroup_Union_final_20161102.gdb'

arcpy.env.workspace = infolder
fclist = arcpy.ListFeatureClasses()

for fc in fclist:
    print fc
    arcpy.AddField_management(fc, "ZoneID", "DOUBLE")
    with arcpy.da.UpdateCursor(fc, ['OBJECTID','ZoneID']) as cursor:
        for row in cursor:
            row[1] = row[0]
            cursor.updateRow(row)