import arcpy


arcpy.env.workspace = 'C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\Range\R_SpGroupComposite_ProjectedtRegion_20160909.gdb'

fclist= arcpy.ListFeatureClasses()

for fc in fclist:
    fclist_field = [f.name for f in arcpy.ListFields(fc) if not f.required]
    deletelist = [i for i in fclist_field if i.startswith('Acres')]
    for field in deletelist :
        arcpy.DeleteField_management(fc, field)
        print 'Del field {0}'.format(field)