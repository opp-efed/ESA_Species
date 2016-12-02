import arcpy


infolder = r'L:\Workspace\ESA_Species\FinalBE_EucDis_CoOccur\Range\R_Clipped_UnionRange_20160907.gdb'

arcpy.env.workspace = infolder
fclist = arcpy.ListFeatureClasses()

for fc in fclist:
    print fc
    arcpy.AddField_management(fc, "ZoneID", "DOUBLE")
    with arcpy.da.UpdateCursor(fc, ['OBJECTID','ZoneID']) as cursor:
        for row in cursor:
            row[1] = row[0]
            cursor.updateRow(row)