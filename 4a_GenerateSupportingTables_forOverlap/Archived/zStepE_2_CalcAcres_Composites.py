import arcpy
import datetime

# Title - add acres to compfile in new field

ingdb = r'C:\Users\JConno02\Documents\Projects\ESA\CompositeFiles_Winter2018\RegionalFiles\Range\R_SpGroupComposite_WebMercator.gdb'

# field name to load acres value
field = "TotalAcres"


# recursively checks workspaces found within the inFileLocation and makes list of all feature class
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc

start_time = datetime.datetime.now()
print "Start Time: " + start_time.ctime()

for fc in fcs_in_workspace(ingdb):
    arcpy.AddField_management(fc, field, "DOUBLE", "", 0)
    exp = "!SHAPE.AREA@Acres!"
    print "Calc acres for  " + fc
    arcpy.CalculateField_management(fc, field, exp, "PYTHON_9.3")

end = datetime.datetime.now()
print "End Time: " + end.ctime()
elapsed = end - start_time
print "Elapsed  Time: " + str(elapsed)
