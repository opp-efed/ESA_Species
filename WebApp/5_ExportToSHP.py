import arcpy, os, datetime


inCompGDB = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_WebApp_Composite.gdb'
outfolder = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\ShapeWebApp_Range'

#L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\CH_WebApp_Composite.gdb
#L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\ShapeWebApp_CH

#L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\R_WebApp_Composite.gdb
#L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\Range\ShapeWebApp_Range
arcpy.env.workspace = inCompGDB

fclist = arcpy.ListFeatureClasses()

start = datetime.datetime.now()
print "Started script at {0}".format(start)
for fc in fclist:
    fc_sp = fc.split('_')
    list_len = len(fc_sp)
    counter = 1
    outname = fc_sp[0]
    while counter < (list_len -1):
        outname = outname + "_" + str(fc_sp[counter])

        counter += 1

    outfc= outfolder + os.sep + str(outname)

    if arcpy.Exists(outfc + '.shp'):
        print 'Already exported: {0}'.format(outfc)

    else:
        print outname
        arcpy.CopyFeatures_management(fc, outfc)
        print "Exported {0}".format(outfc)

print "Elapse time {0}".format((datetime.datetime.now())- start)