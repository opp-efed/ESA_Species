import arcpy

#E:\ESA_Species\ForCoOccur\Composites\GDB\L48_SpGroupComposites.gdb
#
#
#
#

ingdb= 'C:\Users\Admin\Documents\Jen\SpeceisToRun\MissingSpeceis_20151217.gdb'
#ingdb ='J:\Workspace\ESA_Species\CriticalHabitat\WGS_Albers\Snails.gdb'
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc

for fc in fcs_in_workspace(ingdb):

    arcpy.AddField_management(fc,"Acres", "DOUBLE","",0)
    with arcpy.da.SearchCursor(fc, ["Acres"]) as cursor:
        for row in cursor:
            if row[0] is None:
                exp = "!SHAPE.AREA@Acres!"
                print "Calc acres for  " +fc
                arcpy.CalculateField_management(fc,"Acres", exp, "PYTHON_9.3")
            else:
                continue