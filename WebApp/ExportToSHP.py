import arcpy, os, datetime


inCompGDB = r'J:\Workspace\ESA_Species\ForCoOccur\Composites\CurrentComps\WebApp\R_WebApp_Composite.gdb'
outfolder = r'J:\Workspace\ESA_Species\ForCoOccur\Composites\CurrentComps\WebApp\ShapesforUpload\Range'

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
    print outname
    outfc= outfolder + os.sep + str(outname)
    arcpy.CopyFeatures_management(fc, outfc)
    print "Exported {0}".format(outfc)

print "Elapse tie {0}".format((datetime.datetime.now())- start)