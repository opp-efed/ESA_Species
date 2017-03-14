import arcpy
import os
import datetime

inlocation = r'L:\Workspace\ESA_Species\Step3\ToolDevelopment\TerrestrialGIS\CriticalHabitat\ShapeWebApp_CH\WebMercator'

# Set the workspace
arcpy.env.workspace = inlocation
fc_list = arcpy.ListFeatureClasses()

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

for fc in fc_list:
    try:
        start_loop = datetime.datetime.now()
        # Since Generalize permanently updates the input, first make a copy of the original FC
        # arcpy.CopyFeatures_management (inFeatures, copFeatures)

        # Use the Generalize tool to simplify the Buffer input to shorten Buffer processing time
        arcpy.Generalize_edit(inlocation + os.sep + fc)
        print 'Completed Generalization for {0} in {1}'.format(fc, (datetime.datetime.now() - start_loop))

    except Exception as err:
        print '\nFailed on file: {0}'.format(fc)
        print(err)

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
