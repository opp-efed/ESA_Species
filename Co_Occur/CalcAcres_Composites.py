import arcpy
ingdb ='C:\WorkSpace\ESA_Species\FinalBE_EucDis_CoOccur\CriticalHabitat\CH_SpGroupComposite_WebMercator.gdb'
def fcs_in_workspace(workspace):
    arcpy.env.workspace = workspace
    for fc in arcpy.ListFeatureClasses():
        yield (fc)
    for ws in arcpy.ListWorkspaces():
        for fc in fcs_in_workspace(ws):
            yield fc

for fc in fcs_in_workspace(ingdb):

    arcpy.AddField_management(fc,"TotalAcres", "DOUBLE","",0)
    exp = "!SHAPE.AREA@Acres!"
    print "Calc acres for  " +fc
    arcpy.CalculateField_management(fc,"Acres", exp, "PYTHON_9.3")